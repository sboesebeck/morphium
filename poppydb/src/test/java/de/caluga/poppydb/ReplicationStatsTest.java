package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;
import de.caluga.test.mongo.suite.data.UncachedObject;

/**
 * Replication &amp; resync observability (Phase C hardening, final-review minor 5, ledger T8/T9).
 *
 * <p>Placed alongside {@link ReplicationResumeTest} / {@link InitialSyncTest} /
 * {@link ReplicationOrderingTest} in package {@code de.caluga.poppydb} (not
 * {@code de.caluga.test.poppydb} as the task brief's stale path suggested) so it can reach the
 * same package-private test seams those siblings already use ({@code getReplicationManagerForTest()},
 * {@code pauseReplicationForTest()} / {@code resumeReplicationForTest()}). A
 * {@code de.caluga.test.poppydb} test would have no way to force a resync at all.
 *
 * <ul>
 *   <li>{@link #getStatsExposesReplicationMetricsAfterMiniRun()} - after a small 2-node replica
 *       set converges, {@link ReplicationManager#getStats()} (and the nested map under
 *       {@link PoppyDB#getStats()}'s {@code "replication"} key) must carry the new observability
 *       keys with plausible values.</li>
 *   <li>{@link #repeatedResyncWithinWindowLogsWarn()} - forcing two buffer-miss resyncs back to
 *       back (well inside the 10-minute window) must log the "replication cannot keep up" WARN;
 *       forcing only the first must NOT.</li>
 * </ul>
 */
@Tag("server")
@Disabled("Disabled by default - starts real PoppyDB server(s) and is flaky under parallel test runs. Run manually with -Djunit.jupiter.conditions.deactivate=org.junit.*DisabledCondition (see ci/CLAUDE-testvm.md).")
public class ReplicationStatsTest {

    private static final Logger log = LoggerFactory.getLogger(ReplicationStatsTest.class);

    private static final String DB = "replicationstatstest";
    private static final String COLL = "objs";

    private int nextPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void startServer(PoppyDB srv, int port) throws Exception {
        srv.start();
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("localhost", port), 250);
                return;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(50);
            }
        }
    }

    private long count(PoppyDB node) throws Exception {
        return node.getDriver().count(DB, COLL, Doc.of(), null, null);
    }

    private void writeDocs(Morphium writer, int count, String prefix) {
        List<UncachedObject> batch = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            batch.add(new UncachedObject(prefix + "-" + i, i));
        }
        writer.storeList(batch, COLL);
    }

    private ReplicationManager waitForSecondaryReady(PoppyDB secondary) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            ReplicationManager rm = secondary.getReplicationManagerForTest();
            if (rm != null && rm.isInitialSyncComplete()) {
                return rm;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("secondary did not complete initial sync within 60s");
    }

    private boolean waitForCount(PoppyDB node, long expected, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (count(node) == expected) {
                return true;
            }
            Thread.sleep(100);
        }
        return count(node) == expected;
    }

    private boolean waitForResyncCount(ReplicationManager rm, long expected, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (rm.getResyncCount() >= expected) {
                return true;
            }
            Thread.sleep(100);
        }
        return rm.getResyncCount() >= expected;
    }

    /**
     * Stats-keys test: after a small replica set converges with a handful of live-replicated
     * writes, {@code getStats()} must carry all six observability keys with plausible values:
     * {@code resyncCount}, {@code lastAppliedSequence} (advanced past 0 - proves it tracks real
     * applied events, not just the idle-window seed from Task 2b), {@code eventQueueSize},
     * {@code eventQueueCapacity} (the configured 100_000 bound - see
     * {@code ReplicationManager.EVENT_QUEUE_CAPACITY}), {@code replicationLagEvents} (non-negative -
     * see {@link ReplicationManager#getLastKnownPrimarySequence()}), and {@code watchGeneration}
     * (at least 1 - bumped at every successful watch registration).
     */
    @Test
    public void getStatsExposesReplicationMetricsAfterMiniRun() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        PoppyDB primary = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB secondary = new PoppyDB(port2, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2);
        var prio = Map.of("localhost:" + port1, 300, "localhost:" + port2, 100);
        primary.configureReplicaSet("rsStats", hosts, prio);
        secondary.configureReplicaSet("rsStats", hosts, prio);

        Morphium writer = null;
        try {
            startServer(primary, port1);
            long primaryDeadline = System.currentTimeMillis() + 15_000;
            while (!primary.isPrimary() && System.currentTimeMillis() < primaryDeadline) {
                Thread.sleep(50);
            }
            assertTrue(primary.isPrimary(), "node1 must become primary");

            MorphiumConfig cfg = new MorphiumConfig();
            cfg.clusterSettings().setHostSeed("localhost:" + port1);
            cfg.connectionSettings().setDatabase(DB);
            cfg.connectionSettings().setMaxConnections(10);
            cfg.cacheSettings().setBufferedWritesEnabled(false);
            writer = new Morphium(cfg);

            startServer(secondary, port2);
            ReplicationManager rm = waitForSecondaryReady(secondary);

            // Live-replicated writes so lastAppliedSequence advances via real applied events
            // (not just the Task 2b idle-window seed at registration).
            writeDocs(writer, 25, "live");
            assertTrue(waitForCount(secondary, 25, 30_000),
                "secondary must live-replicate the batch (got " + count(secondary) + ")");

            Map<String, Object> stats = rm.getStats();
            log.info("Replication stats after mini run: {}", stats);

            assertEquals(0L, stats.get("resyncCount"), "no resync should have happened in a clean run");
            assertTrue((Long) stats.get("lastAppliedSequence") > 0,
                "lastAppliedSequence must have advanced past 0 (got " + stats.get("lastAppliedSequence") + ")");
            assertTrue((Integer) stats.get("eventQueueSize") >= 0, "eventQueueSize must be present and non-negative");
            assertEquals(100_000, stats.get("eventQueueCapacity"),
                "eventQueueCapacity must reflect the configured bound");
            assertTrue((Long) stats.get("replicationLagEvents") >= 0,
                "replicationLagEvents must never be negative (got " + stats.get("replicationLagEvents") + ")");
            assertTrue((Long) stats.get("watchGeneration") >= 1,
                "watchGeneration must be at least 1 after a successful watch registration");

            // Also verify the wiring through PoppyDB.getStats() (the public surface consumers use).
            @SuppressWarnings("unchecked")
            Map<String, Object> nested = (Map<String, Object>) secondary.getStats().get("replication");
            assertTrue(nested.containsKey("resyncCount"));
            assertTrue(nested.containsKey("lastAppliedSequence"));
            assertTrue(nested.containsKey("eventQueueSize"));
            assertTrue(nested.containsKey("eventQueueCapacity"));
            assertTrue(nested.containsKey("replicationLagEvents"));
            assertTrue(nested.containsKey("watchGeneration"));
        } finally {
            if (writer != null) {
                writer.close();
            }
            secondary.shutdown();
            primary.shutdown();
        }
    }

    /**
     * Repeated-resync WARN: shrinking the primary's replay buffer and forcing two buffer-miss
     * resyncs back to back (well inside the 10-minute window) must log the
     * "replication cannot keep up" WARN exactly once (on the second resync - the first has no
     * predecessor to compare against, so it must NOT warn). Captured via a
     * {@link ListAppender} attached directly to {@link ReplicationManager}'s logger, following the
     * pattern in {@code AggregatorFieldNameTranslationTest.runAndCaptureWarns}.
     */
    @Test
    public void repeatedResyncWithinWindowLogsWarn() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        PoppyDB primary = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB secondary = new PoppyDB(port2, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2);
        var prio = Map.of("localhost:" + port1, 300, "localhost:" + port2, 100);
        primary.configureReplicaSet("rsStatsWarn", hosts, prio);
        secondary.configureReplicaSet("rsStatsWarn", hosts, prio);

        ch.qos.logback.classic.Logger rmLogger =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ReplicationManager.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        rmLogger.addAppender(appender);

        Morphium writer = null;
        try {
            startServer(primary, port1);
            long primaryDeadline = System.currentTimeMillis() + 15_000;
            while (!primary.isPrimary() && System.currentTimeMillis() < primaryDeadline) {
                Thread.sleep(50);
            }
            assertTrue(primary.isPrimary(), "node1 must become primary");

            // Shrink the primary's replay buffer so a 100-doc gap cannot be replayed - forces a
            // buffer-miss resync instead of a clean resume (same trick as
            // ReplicationResumeTest.bufferMissTriggersResync).
            primary.getDriver().setChangeStreamHistoryLimit(10);

            MorphiumConfig cfg = new MorphiumConfig();
            cfg.clusterSettings().setHostSeed("localhost:" + port1);
            cfg.connectionSettings().setDatabase(DB);
            cfg.connectionSettings().setMaxConnections(10);
            cfg.cacheSettings().setBufferedWritesEnabled(false);
            writer = new Morphium(cfg);

            startServer(secondary, port2);
            ReplicationManager rm = waitForSecondaryReady(secondary);
            assertEquals(0, rm.getResyncCount(), "no resync should have happened yet");

            // Phase 1: a live-replicated batch first, mirroring
            // ReplicationResumeTest.runScenario() - establishes the watch as genuinely live before
            // it gets severed, which the buffer-miss trigger depends on.
            writeDocs(writer, 100, "pre");
            assertTrue(waitForCount(secondary, 100, 30_000),
                "secondary must live-replicate the first batch (got " + count(secondary) + ")");
            assertEquals(0, rm.getResyncCount(), "no resync should have happened yet");

            // First buffer-miss resync: sever, write a gap bigger than the (shrunk) buffer, heal.
            rm.pauseReplicationForTest();
            Thread.sleep(1000);
            writeDocs(writer, 100, "gap1");
            Thread.sleep(500);
            rm.resumeReplicationForTest();
            assertTrue(waitForResyncCount(rm, 1, 45_000),
                "first buffer-miss must trigger a resync (resyncCount=" + rm.getResyncCount() + ")");
            assertTrue(waitForCount(secondary, 200, 45_000),
                "secondary must converge after the first resync (got " + count(secondary) + ")");

            List<ILoggingEvent> warnsAfterFirst = warnsSoFar(appender);
            assertFalse(hasCannotKeepUpWarn(warnsAfterFirst),
                "the FIRST resync has no predecessor to compare against and must NOT warn");

            // Second buffer-miss resync, immediately after the first - well within the 10-minute
            // window.
            rm.pauseReplicationForTest();
            Thread.sleep(1000);
            writeDocs(writer, 100, "gap2");
            Thread.sleep(500);
            rm.resumeReplicationForTest();
            assertTrue(waitForResyncCount(rm, 2, 45_000),
                "second buffer-miss must trigger another resync (resyncCount=" + rm.getResyncCount() + ")");
            assertTrue(waitForCount(secondary, 300, 45_000),
                "secondary must converge after the second resync (got " + count(secondary) + ")");

            List<ILoggingEvent> warnsAfterSecond = warnsSoFar(appender);
            assertTrue(hasCannotKeepUpWarn(warnsAfterSecond),
                "a second resync within the 10-minute window must log the "
                    + "'replication cannot keep up' WARN");
        } finally {
            rmLogger.detachAppender(appender);
            if (writer != null) {
                writer.close();
            }
            secondary.shutdown();
            primary.shutdown();
        }
    }

    private List<ILoggingEvent> warnsSoFar(ListAppender<ILoggingEvent> appender) {
        return appender.list.stream()
            .filter(ev -> ev.getLevel() == Level.WARN)
            .collect(Collectors.toList());
    }

    private boolean hasCannotKeepUpWarn(List<ILoggingEvent> warns) {
        return warns.stream().anyMatch(ev -> ev.getFormattedMessage().contains("replication cannot keep up"));
    }
}
