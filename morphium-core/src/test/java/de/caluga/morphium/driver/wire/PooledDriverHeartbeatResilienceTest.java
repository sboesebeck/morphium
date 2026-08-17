package de.caluga.morphium.driver.wire;

import de.caluga.morphium.driver.MorphiumDriver.DriverStatsKey;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A driver must reconverge on a live replica set from every internal state - a fully unreachable
 * RS (rolling restarts, elections without quorum) is a normal condition, not a terminal one.
 * #304: after such a sequence clients stayed stuck in "No primary node found - not connected
 * yet?" until the application was restarted, because the heartbeat had stopped probing - either
 * because its scheduled task died on an exception, or because a host's bookkeeping entry was
 * left behind and made every later cycle skip that host.
 *
 * <p>Lives in the driver's own package to reach {@code hostThreads} and {@code normalizeHostKey}.
 */
@Tag("driver")
public class PooledDriverHeartbeatResilienceTest {

    private String deadHost() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return "localhost:" + s.getLocalPort();
        }
    }

    private void awaitOrFail(java.util.function.BooleanSupplier condition, long timeoutMs, String what)
            throws Exception {
        long until = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < until) {
            if (condition.getAsBoolean()) {
                return;
            }

            Thread.sleep(25);
        }

        throw new AssertionError(what);
    }

    @Test
    @Timeout(60)
    public void heartbeatSurvivesAnExceptionInOneOfItsCycles() throws Exception {
        AtomicInteger cycles = new AtomicInteger();
        PooledDriver drv = new PooledDriver() {
            @Override
            void reseedIfAllHostsEvicted() {
                // Blows up once, in the middle of a heartbeat cycle - the way an unexpected
                // runtime failure would during a cluster-wide outage.
                if (cycles.incrementAndGet() == 1) {
                    throw new RuntimeException("simulated failure inside a heartbeat cycle");
                }

                super.reseedIfAllHostsEvicted();
            }
        };
        drv.setHostSeed(List.of(deadHost()));
        drv.setHeartbeatFrequency(50);
        drv.setServerSelectionTimeout(200);

        try {
            try {
                drv.connect();
            } catch (Exception expected) {
                // no primary reachable - that is the situation under test
            }

            awaitOrFail(() -> cycles.get() >= 4, 10_000,
                    "the heartbeat stopped running after a single failing cycle - scheduled tasks are "
                            + "cancelled for good when they throw, so discovery could never resume");
        } finally {
            drv.close();
        }
    }

    @Test
    @Timeout(60)
    public void aLeftoverHostEntryDoesNotDisableDiscoveryForThatHost() throws Exception {
        String host = deadHost();
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed(List.of(host));
        // Slow heartbeat on purpose: the leftover entry has to be planted while no host check is
        // in flight, otherwise a running check would clear it on its way out and hide the defect.
        drv.setHeartbeatFrequency(1000);
        drv.setServerSelectionTimeout(200);

        try {
            try {
                drv.connect();
            } catch (Exception expected) {
                // no primary reachable - that is the situation under test
            }

            // Wait out the cycle that connect() kicked off, so nothing is in flight anymore.
            awaitOrFail(() -> drv.hostThreads.isEmpty(), 10_000, "host check did not finish");

            // Simulate the bookkeeping entry of a host check that finished before it was
            // registered: the thread is long dead, but the entry claims a check is in flight.
            Thread finished = new Thread(() -> { });
            finished.start();
            finished.join();
            drv.hostThreads.put(drv.normalizeHostKey(host), finished);

            // Several heartbeat periods have to pass before the verdict: the entry must be gone.
            Thread.sleep(3000);
            awaitOrFail(() -> drv.hostThreads.get(drv.normalizeHostKey(host)) != finished, 10_000,
                    "the leftover entry of an already finished check was never cleared - every later "
                            + "heartbeat skips that host, so discovery can never resume for it");
        } finally {
            drv.close();
        }
    }

    @Test
    @Timeout(60)
    public void closeClearsHostBookkeeping() throws Exception {
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed(List.of(deadHost()));
        drv.setHeartbeatFrequency(50);
        drv.setServerSelectionTimeout(200);

        try {
            drv.connect();
        } catch (Exception expected) {
            // no primary reachable
        }

        Thread finished = new Thread(() -> { });
        finished.start();
        finished.join();
        drv.hostThreads.put("leftover:27017", finished);

        drv.close();

        assertThat(drv.hostThreads)
                .as("a closed driver must not keep per-host check bookkeeping around")
                .isEmpty();
    }

    @Test
    @Timeout(60)
    public void askingForThePrimaryRevivesADeadHeartbeat() throws Exception {
        PooledDriver drv = new PooledDriver();
        // Two seeds: only a replica-set driver looks for a primary at all - which is the
        // situation #304 reported, a client waiting for a primary that discovery never finds.
        drv.setHostSeed(List.of(deadHost(), deadHost()));
        drv.setHeartbeatFrequency(100);
        drv.setServerSelectionTimeout(200);

        try {
            try {
                drv.connect();
            } catch (Exception expected) {
                // no primary reachable - that is the situation under test
            }

            // However the heartbeat died, the driver must not stay blind to its topology.
            drv.heartbeat.cancel(false);
            assertThat(drv.heartbeat.isDone()).isTrue();

            try {
                drv.getPrimaryConnection(null);
            } catch (Exception expected) {
                // still no primary - but asking must have restarted discovery
            }

            assertThat(drv.heartbeat != null)
                    .as("a driver whose heartbeat has died must restart it when asked for the primary - "
                            + "otherwise it can never re-discover a replica set that comes back")
                    .isTrue();
            assertThat(drv.heartbeat.isDone())
                    .as("the revived heartbeat must actually be scheduled again")
                    .isFalse();
        } finally {
            drv.close();
        }
    }
}
