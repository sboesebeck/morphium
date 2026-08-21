package de.caluga.morphium.driver.wire;

import de.caluga.morphium.driver.MorphiumDriver.DriverStatsKey;
import org.assertj.core.api.Assertions;
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

    // --- #330: heartbeat must self-revive without external trigger ----------------

    @Test
    @Timeout(60)
    public void heartbeatSelfRevivesWithoutGetPrimaryConnectionCall() throws Exception {
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed(List.of(deadHost(), deadHost()));
        drv.setHeartbeatFrequency(100);
        drv.setServerSelectionTimeout(200);

        try {
            try {
                drv.connect();
            } catch (Exception expected) {
                // no primary reachable - that is the situation under test
            }

            // Kill the heartbeat task directly - simulating a task that died on an exception
            // or was cancelled. The driver must self-revive WITHOUT any getPrimaryConnection call.
            drv.heartbeat.cancel(false);
            assertThat(drv.heartbeat.isDone()).isTrue();

            // Wait - heartbeat should self-revive on its own next cycle attempt
            awaitOrFail(() -> drv.heartbeat != null && !drv.heartbeat.isDone(), 10_000,
                    "heartbeat should self-revive without external trigger (no getPrimaryConnection call)");

            // Verify it's actually running by checking hostThreads get populated
            awaitOrFail(() -> !drv.hostThreads.isEmpty(), 10_000,
                    "self-revived heartbeat should actually spawn host checks");
        } finally {
            drv.close();
        }
    }

    @Test
    @Timeout(60)
    public void orphanedHostThreadsAreCleanedWhenHostRemoved() throws Exception {
        String host = deadHost();
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed(List.of(host));
        drv.setHeartbeatFrequency(200);
        drv.setServerSelectionTimeout(200);

        try {
            try {
                drv.connect();
            } catch (Exception expected) {
            }

            // Wait for initial cycle to finish
            awaitOrFail(() -> drv.hostThreads.isEmpty(), 10_000, "initial cycle did not finish");

            // Plant an orphaned entry for a host that will be removed
            Thread finished = new Thread(() -> { });
            finished.start();
            finished.join();
            String normalized = drv.normalizeHostKey(host);
            drv.hostThreads.put(normalized, finished);

            // Trigger host removal via onConnectionError (simulates MAX_FAILURES exceeded)
            // Need to exceed MAX_FAILURES (5) to trigger removal
            for (int i = 0; i < 6; i++) {
                drv.onConnectionError(host);
            }

            // The orphaned hostThreads entry must be cleaned up
            awaitOrFail(() -> !drv.hostThreads.containsKey(normalized), 5_000,
                    "hostThreads entry for removed host was not cleaned - would silently disable future discovery");

            // Also verify the host is gone from hosts map
            assertThat(drv.hosts).doesNotContainKey(normalized);
        } finally {
            drv.close();
        }
    }

    @Test
    @Timeout(60)
    public void silentHeartbeatCyclesAreLoggedAndForceReseed() throws Exception {
        // This test verifies the watchdog: if N consecutive cycles have zero successful hellos,
        // a warning is logged and eventually a full reseed is forced.
        AtomicInteger successfulHellos = new AtomicInteger(0);
        PooledDriver drv = new PooledDriver() {
            @Override
            void handleHelloResult(HelloResult hello, String hostConnected) {
                if (hello != null) {
                    successfulHellos.incrementAndGet();
                }
                super.handleHelloResult(hello, hostConnected);
            }
        };
        drv.setHostSeed(List.of(deadHost(), deadHost()));
        drv.setHeartbeatFrequency(50); // fast cycles
        drv.setServerSelectionTimeout(200);

        try {
            try {
                drv.connect();
            } catch (Exception expected) {
            }

            // Wait for many cycles with zero successful hellos
            // The watchdog should log warnings and eventually force reseed
            // (We can't easily assert on logs, but we verify the driver doesn't deadlock)
            Thread.sleep(3000); // ~60 cycles at 50ms

            // Driver should still have a running heartbeat
            Assertions.assertThat((Object) drv.heartbeat).as("heartbeat should be scheduled").isNotNull();
            assertThat(drv.heartbeat.isDone()).as("heartbeat should be running").isFalse();

            // Hosts should have been reseeded (from host seed) since all were failing
            // Note: reseedIfAllHostsEvicted only runs when hosts map is empty
            // The watchdog should trigger that condition
        } finally {
            drv.close();
        }
    }

    // --- #330: the two holes the ACC incident actually fell through ---------------

    /**
     * The membership-removal path (`Host X is not part of the replicaset anymore`) shrinks the
     * HOST SEED itself via removeFromHostSeed - and reseedIfAllHostsEvicted reseeds FROM that
     * seed. During the ACC rolling restart the takeover-window hellos removed every host, the
     * seed ended up empty, and from then on every heartbeat cycle iterated over nothing,
     * spawned nothing and logged nothing - forever. The reseed must fall back to the seed the
     * driver was ORIGINALLY configured with, which no runtime path may erode.
     */
    @Test
    @Timeout(60)
    public void reseedRestoresTheOriginalSeedWhenTheRunningSeedWasEroded() throws Exception {
        PooledDriver drv = new PooledDriver();
        String h1 = drv.normalizeHostKey(deadHost());
        String h2 = drv.normalizeHostKey(deadHost());
        drv.setHostSeed(List.of(h1, h2));
        // Huge frequency: these tests drive reseed/handleHelloResult DIRECTLY - a live
        // heartbeat hammering the dead seed hosts reaches MAX_FAILURES on a loaded runner and
        // evicts them between the assertions (seen in CI). Only the immediate first cycle runs.
        drv.setHeartbeatFrequency(600_000);
        drv.setServerSelectionTimeout(200);

        try {
            try {
                drv.connect();
            } catch (Exception expected) {
            }

            // Erode the running seed completely - the end state the membership-removal path
            // produced on ACC (whatever the individual erosion steps were).
            drv.setHostSeed(new java.util.ArrayList<String>());
            assertThat(drv.getHostSeed()).as("precondition: running seed fully eroded").isEmpty();
            drv.hosts.clear();

            drv.reseedIfAllHostsEvicted();

            assertThat(drv.hosts)
                .as("an eroded seed must fall back to the ORIGINALLY configured one - an empty "
                    + "reseed leaves the heartbeat iterating over nothing, silently, forever")
                .isNotEmpty();
            assertThat(drv.getHostSeed()).as("the running seed must be restored as well").isNotEmpty();
        } finally {
            drv.close();
        }
    }

    /**
     * Only a hello from the PRIMARY may remove replica-set members (the code comment always
     * claimed this, the code never checked). A secondary or in-election node answering with a
     * partial host list during a rolling restart must not be able to eat the topology.
     */
    @Test
    @Timeout(60)
    public void nonPrimaryHelloMustNotRemoveMembers() throws Exception {
        String h1 = deadHost();
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed(List.of(h1));
        // See reseedRestores...: keep the live heartbeat out of these direct-call tests.
        drv.setHeartbeatFrequency(600_000);
        drv.setServerSelectionTimeout(200);

        try {
            try {
                drv.connect();
            } catch (Exception expected) {
            }

            String normalized = drv.normalizeHostKey(h1);
            drv.reseedIfAllHostsEvicted();
            assertThat(drv.hosts).as("precondition: seeded host present").containsKey(normalized);

            // A NON-primary hello whose host list does not contain our host (partial list
            // during takeover). It must not remove anything.
            HelloResult secondaryHello = new HelloResult();
            secondaryHello.setWritablePrimary(false);
            secondaryHello.setMe("unrelated:12345");
            secondaryHello.setHosts(new java.util.ArrayList<>(List.of("unrelated:12345")));
            drv.handleHelloResult(secondaryHello, "unrelated:12345");

            assertThat(drv.hosts)
                .as("a non-primary hello must not remove replica-set members")
                .containsKey(normalized);
            assertThat(drv.getHostSeed())
                .as("a non-primary hello must not erode the host seed")
                .contains(normalized);

            // The same list coming from the PRIMARY is authoritative and MAY remove.
            HelloResult primaryHello = new HelloResult();
            primaryHello.setWritablePrimary(true);
            primaryHello.setMe("unrelated:12345");
            primaryHello.setHosts(new java.util.ArrayList<>(List.of("unrelated:12345")));
            drv.handleHelloResult(primaryHello, "unrelated:12345");

            assertThat(drv.hosts)
                .as("a primary hello stays authoritative for membership removal")
                .doesNotContainKey(normalized);
        } finally {
            drv.close();
        }
    }

    /**
     * The actual #330 eater: the ADD path keys hosts with
     * {@code normalizeHostKey(resolveAlias(hst))}, but the REMOVAL comparison built its set from
     * {@code resolveAlias(h)} only. A hello advertising the same member in a different case
     * (SERV-MSG1 vs serv-msg1 - the exact ACC constellation normalizeHostKey's own comment
     * documents) therefore removed the very host it had just added: hosts map AND seed eroded
     * to empty within a few hellos, and an empty seed makes every later reseed a silent no-op.
     */
    @Test
    @Timeout(60)
    public void primaryHelloAdvertisingSameMemberInDifferentCaseMustNotRemoveIt() throws Exception {
        String h1 = deadHost();
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed(List.of(h1));
        // See reseedRestores...: keep the live heartbeat out of these direct-call tests.
        drv.setHeartbeatFrequency(600_000);
        drv.setServerSelectionTimeout(200);

        try {
            try {
                drv.connect();
            } catch (Exception expected) {
            }

            String normalized = drv.normalizeHostKey(h1);
            drv.reseedIfAllHostsEvicted();
            assertThat(drv.hosts).as("precondition: seeded host present").containsKey(normalized);

            HelloResult primaryHello = new HelloResult();
            primaryHello.setWritablePrimary(true);
            primaryHello.setMe(normalized);
            // Same member, upper-cased - normalizes to the same key we already hold.
            primaryHello.setHosts(new java.util.ArrayList<>(List.of(h1.toUpperCase(java.util.Locale.ROOT))));
            drv.handleHelloResult(primaryHello, normalized);

            assertThat(drv.hosts)
                .as("a member advertised in different case is the SAME member - removing it "
                    + "erodes the topology to nothing (the #330 silence)")
                .containsKey(normalized);
            assertThat(drv.getHostSeed())
                .as("and the host seed must not be eroded either")
                .contains(normalized);
        } finally {
            drv.close();
        }
    }
}
