package de.caluga.morphium.driver.wire;

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
        AtomicInteger cycles = new AtomicInteger();
        PooledDriver drv = new PooledDriver() {
            @Override
            void reseedIfAllHostsEvicted() {
                // Only ever called from inside a heartbeat cycle (cycle start, plus the watchdog
                // branch at the cycle's end - itself unreachable without a cycle), so every
                // increment means a real cycle ran. A cycle that also hits the watchdog reseed
                // counts twice, which only makes the assertion below looser, never falsely true.
                // Same deterministic idiom as heartbeatSurvivesAnExceptionInOneOfItsCycles.
                cycles.incrementAndGet();
                super.reseedIfAllHostsEvicted();
            }
        };
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

            // A revived-but-idle heartbeat is not enough: cycles must KEEP running afterwards.
            // hostThreads was the previous proxy here and is a bad observable - its entries are
            // transient claims (put right before the check thread starts, removed again when the
            // refused connect fails microseconds later), so the map is empty for most of every
            // cycle. The fixed awaitOrFail poll of 25ms against the 100ms heartbeat period then
            // phase-locks: once the sampler's phases (0/25/50/75 mod 100) all miss the ~1ms
            // populated window, it misses it EVERY time. That is what makes the old assertion
            // fail systematically rather than flake occasionally - with independent sampling at
            // the same hit rate, missing all ~400 polls of a 10s wait would be vanishingly
            // unlikely, so only a phase-locked sampler explains failing run after run the way CI
            // did (the same failure on develop and master, not just on this branch).
            int cyclesAtBaseline = cycles.get();
            awaitOrFail(() -> cycles.get() > cyclesAtBaseline, 10_000,
                    "self-revived heartbeat should keep running cycles (frozen at " + cyclesAtBaseline + ")");
        } finally {
            drv.close();
        }
    }

    @Test
    @Timeout(60)
    public void orphanedHostThreadsAreCleanedWhenHostRemoved() throws Exception {
        String host = deadHost();
        AtomicInteger failedChecks = new AtomicInteger();
        PooledDriver drv = new PooledDriver() {
            @Override
            void onConnectionError(String h) {
                String key = normalizeHostKey(h);
                // Count only callbacks from the heartbeat's OWN check thread: it is recognizable
                // as the thread registered in hostThreads for that host (the claim is written
                // before start()). Reaching >=1 therefore PROVES a check ran - i.e. its put()
                // happened - which is what makes the hostThreads.isEmpty() half of the quiesce
                // gate below non-vacuous. Waiter/creator threads also report connection errors,
                // but never hold the claim.
                if (Thread.currentThread() == hostThreads.get(key)) {
                    failedChecks.incrementAndGet();
                }
                super.onConnectionError(h);
            }
        };
        drv.setHostSeed(List.of(host));
        // Huge frequency: this test drives onConnectionError DIRECTLY and asserts the eviction
        // outcome - a live heartbeat would race it twice. (1) The failed hellos of every cycle
        // increment the SAME failure counter, so the background can evict and reseed while the
        // synthetic errors are still being counted, splitting them across two Host objects -
        // neither of which then exceeds MAX_FAILURES. (2) reseedIfAllHostsEvicted runs at the
        // start of EVERY cycle (#233 self-healing), so an evicted seed host is legitimately
        // resurrected within one heartbeat period - asserting its absence from the hosts map
        // against a live 200ms heartbeat is a phase race, not a behavior check (flaked exactly
        // that way under parallel CI load). Same idiom as the other direct-call tests below.
        drv.setHeartbeatFrequency(600_000);
        drv.setServerSelectionTimeout(200);

        try {
            try {
                drv.connect();
            } catch (Exception expected) {
            }

            // Deliberately NOT cancelling the heartbeat here: the connection waiter's watchdog
            // (every 5s) treats a DONE heartbeat future as dead and revives discovery via
            // startHeartbeat() - which schedules with delay=0 and IGNORES the configured 600s
            // frequency. cancel() would therefore ARM a fresh cycle ~5s later, whose
            // reseedIfAllHostsEvicted resurrects the evicted host right under the hosts-map
            // assertion below. The PENDING future instead keeps isDone()==false, the watchdog
            // silent, and no second cycle can start within this test's lifetime.
            //
            // What still needs quiescing is the FIRST cycle (delay=0) finishing late: its check
            // uses the same key we plant below, and its own put/remove would silently clean the
            // orphan - the assertion would green WITHOUT onConnectionError's cleanup ever
            // running. The gate closes that hole deterministically: failedChecks>=1 proves a
            // check thread got as far as its failure callback (its put long since happened),
            // and hostThreads.isEmpty() then observes the POST-put empty, i.e. the check is gone.
            awaitOrFail(() -> failedChecks.get() >= 1 && drv.hostThreads.isEmpty(), 10_000,
                    "initial check did not complete");

            // Plant an orphaned entry for a host that will be removed
            Thread finished = new Thread(() -> { });
            finished.start();
            finished.join();
            String normalized = drv.normalizeHostKey(host);
            drv.hostThreads.put(normalized, finished);

            // Precondition made explicit: everything below assumes the seeded host is still in
            // the topology - if it were already evicted (e.g. by an unexpected background
            // path), the 6 synthetic errors would silently hit nothing.
            assertThat(drv.hosts).as("precondition: seeded host present").containsKey(normalized);

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
     * The silent-cycle watchdog must not fire while hellos ARE succeeding. Its counter used to
     * be reset at the START of each heartbeat cycle and read at its END - but the cycle itself
     * takes milliseconds while the hellos run asynchronously on the HeartbeatCheck threads and
     * land BETWEEN cycles, so the reset wiped them before they were ever read. The counter was
     * therefore ~always zero, and every 30 cycles the watchdog force-reseeded a perfectly
     * healthy topology - tearing down all pools, repeatedly, which broke failover recovery on
     * both RS backends (writesRecoverAfterFreeze, qualification run #2).
     */
    @Test
    @Timeout(60)
    public void watchdogStaysQuietWhileHellosSucceed() throws Exception {
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(PooledDriver.class);
        ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender =
                new ch.qos.logback.core.read.ListAppender<>();
        // The driver's heartbeat threads keep logging into this appender while the assertion
        // below reads it, and ListAppender.list is a plain ArrayList - streaming it while an
        // append lands throws ConcurrentModificationException. Copy-on-write makes the read
        // see a stable snapshot instead of failing the test for a reason it does not test.
        appender.list = new java.util.concurrent.CopyOnWriteArrayList<>();
        appender.start();
        logger.addAppender(appender);

        PooledDriver drv = new PooledDriver();
        String h1 = deadHost();
        drv.setHostSeed(List.of(h1));
        drv.setHeartbeatFrequency(50); // fast cycles: 30 silent ones take 1.5s
        drv.setServerSelectionTimeout(200);

        try {
            try {
                drv.connect();
            } catch (Exception expected) {
            }

            // Simulate what the async HeartbeatCheck threads do on a healthy cluster: hellos
            // keep arriving BETWEEN heartbeat cycles.
            long until = System.currentTimeMillis() + 3_000;

            while (System.currentTimeMillis() < until) {
                HelloResult hello = new HelloResult();
                hello.setWritablePrimary(true);
                hello.setMe(drv.normalizeHostKey(h1));
                drv.handleHelloResult(hello, drv.normalizeHostKey(h1));
                Thread.sleep(25);
            }

            boolean watchdogFired = appender.list.stream().anyMatch(e ->
                    e.getFormattedMessage().contains("zero successful hellos"));
            assertThat(watchdogFired)
                    .as("hellos arrived continuously - the silent-cycle watchdog must not warn, "
                            + "let alone force-reseed a healthy topology (that teardown of every "
                            + "pool is what broke failover recovery)")
                    .isFalse();
        } finally {
            logger.detachAppender(appender);
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
