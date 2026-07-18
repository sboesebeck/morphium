package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;
import de.caluga.test.mongo.suite.data.UncachedObject;

/**
 * Resume-after-disconnect (task 9).
 *
 * A 2-node PoppyDB replica set. The secondary applies an initial batch, then its replication
 * connection is severed (test hook) while the primary keeps writing. When the connection heals,
 * the secondary must catch up on every write that happened during the outage:
 *
 * <ul>
 *   <li>{@link #resumesFromBufferWithoutResync()} — the primary's replay buffer still covers the
 *       gap, so the secondary resumes from its last-applied sequence and converges WITHOUT a full
 *       re-sync ({@code resyncCount == 0}). Before the fix the reconnecting watch started "now",
 *       silently dropping the gap writes, so the secondary stayed stuck at the pre-outage count.</li>
 *   <li>{@link #bufferMissTriggersResync()} — the replay buffer is shrunk so the gap no longer fits.
 *       The primary must answer with an explicit "resume window lost" signal and the secondary must
 *       fall back to a full re-initial-sync ({@code resyncCount >= 1}) and still converge.</li>
 * </ul>
 */
@Tag("server")
public class ReplicationResumeTest {

    private static final Logger log = LoggerFactory.getLogger(ReplicationResumeTest.class);

    private static final String DB = "resumetest";
    private static final String COLL = "objs";
    private static final int BATCH = 100;

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

    /** Happy path: buffer covers the gap → clean resume, no re-sync. */
    @Test
    public void resumesFromBufferWithoutResync() throws Exception {
        runScenario(false);
    }

    /** Miss path: gap larger than the (shrunk) buffer → explicit signal → re-sync fallback. */
    @Test
    public void bufferMissTriggersResync() throws Exception {
        runScenario(true);
    }

    private void runScenario(boolean shrinkBuffer) throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        PoppyDB primary = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB secondary = new PoppyDB(port2, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2);
        var prio = Map.of("localhost:" + port1, 300, "localhost:" + port2, 100);
        primary.configureReplicaSet("rsResume", hosts, prio);
        secondary.configureReplicaSet("rsResume", hosts, prio);

        Morphium writer = null;
        try {
            startServer(primary, port1);
            long primaryDeadline = System.currentTimeMillis() + 15_000;
            while (!primary.isPrimary() && System.currentTimeMillis() < primaryDeadline) {
                Thread.sleep(50);
            }
            assertTrue(primary.isPrimary(), "node1 must become primary");

            if (shrinkBuffer) {
                // Shrink the primary's replay buffer so a 100-doc gap cannot be replayed.
                primary.getDriver().setChangeStreamHistoryLimit(10);
            }

            MorphiumConfig cfg = new MorphiumConfig();
            cfg.clusterSettings().setHostSeed("localhost:" + port1);
            cfg.connectionSettings().setDatabase(DB);
            cfg.connectionSettings().setMaxConnections(10);
            cfg.cacheSettings().setBufferedWritesEnabled(false);
            writer = new Morphium(cfg);

            startServer(secondary, port2);
            ReplicationManager rm = waitForSecondaryReady(secondary);

            // Phase 1: write BATCH docs, live-replicated to the secondary.
            writeDocs(writer, BATCH, "pre");
            assertEquals(BATCH, count(primary), "primary must hold the first batch");
            assertTrue(waitForCount(secondary, BATCH, 30_000),
                "secondary must live-replicate the first batch (got " + count(secondary) + ")");
            assertEquals(0, rm.getResyncCount(), "no re-sync should have happened yet");

            // Sever replication and let it settle.
            rm.pauseReplicationForTest();
            Thread.sleep(1000);

            // Phase 2: write BATCH more while the secondary is partitioned off.
            writeDocs(writer, BATCH, "gap");
            assertEquals(2 * BATCH, count(primary), "primary must hold both batches");
            // Prove the gap: the partitioned secondary has NOT seen the second batch.
            Thread.sleep(500);
            assertEquals(BATCH, count(secondary),
                "partitioned secondary must still be stuck at the first batch");

            // Heal the partition.
            rm.resumeReplicationForTest();

            // The secondary must converge to both batches.
            assertTrue(waitForCount(secondary, 2 * BATCH, 45_000),
                "secondary must converge to both batches after reconnect (got " + count(secondary) + ")");

            if (shrinkBuffer) {
                assertTrue(rm.getResyncCount() >= 1,
                    "buffer miss must trigger a re-sync fallback (resyncCount=" + rm.getResyncCount() + ")");
            } else {
                assertEquals(0, rm.getResyncCount(),
                    "clean resume from the buffer must NOT trigger a re-sync");
            }
            log.info("Scenario shrinkBuffer={} converged; resyncCount={}", shrinkBuffer, rm.getResyncCount());
        } finally {
            if (writer != null) {
                writer.close();
            }
            secondary.shutdown();
            primary.shutdown();
        }
    }

    /**
     * Idle-window resume hole (final-review follow-up 2). Initial sync completes with ZERO
     * applied change-stream events - the secondary's only data comes from the snapshot copy, never
     * from {@code applyChangeEvent} - so before the fix {@code lastAppliedSequence} stayed at its
     * initial 0 the whole time. A subsequent reconnect's {@code resumeSeq > 0} check then failed,
     * so the new watch sent no {@code resumeAfter} and silently started "from now", losing every
     * write that happened during the gap.
     *
     * <p>The fix has the primary piggyback its current change-stream sequence on the watch
     * registration reply ("poppyPrimarySequence", mirroring how "poppyResumeSequence" already rides
     * the resumeAfter token in the other direction) and has the secondary seed
     * {@code lastAppliedSequence} from it at registration time - so even a secondary that never
     * applies a single event still has a correct, non-zero resume point on its next reconnect.
     *
     * <p>The primary is warmed up with 10 docs BEFORE the secondary ever connects, so the primary's
     * sequence counter is already non-zero at registration time: this makes the seeded value
     * distinguishable from the pre-fix "never touched" 0 rather than accidentally matching it.
     */
    @Test
    public void resumesAfterIdleInitialSyncWithoutResync() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        PoppyDB primary = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB secondary = new PoppyDB(port2, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2);
        var prio = Map.of("localhost:" + port1, 300, "localhost:" + port2, 100);
        primary.configureReplicaSet("rsIdleResume", hosts, prio);
        secondary.configureReplicaSet("rsIdleResume", hosts, prio);

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

            // Warm up the primary's sequence counter BEFORE the secondary ever connects.
            writeDocs(writer, 10, "warmup");
            assertEquals(10, count(primary), "primary must hold the warmup batch");

            startServer(secondary, port2);
            ReplicationManager rm = waitForSecondaryReady(secondary);
            assertTrue(waitForCount(secondary, 10, 30_000),
                "secondary must copy the warmup batch via initial sync (got " + count(secondary) + ")");
            assertEquals(0, rm.getEventsApplied(),
                "the warmup batch must have been copied by the snapshot, not by change-stream events "
                    + "- this is the ZERO-applied-events precondition for the idle-window bug");
            assertEquals(0, rm.getResyncCount(), "no re-sync should have happened yet");

            // Sever replication IMMEDIATELY after initial sync - zero change-stream events are
            // ever applied on this secondary before the gap.
            rm.pauseReplicationForTest();
            Thread.sleep(1000);

            // Gap: the primary writes 50 more docs while the secondary is partitioned off.
            writeDocs(writer, 50, "gap");
            assertEquals(60, count(primary), "primary must hold warmup + gap batches");
            Thread.sleep(500);
            assertEquals(10, count(secondary),
                "partitioned secondary must still be stuck at the warmup batch");

            // Heal the partition.
            rm.resumeReplicationForTest();

            assertTrue(waitForCount(secondary, 60, 45_000),
                "secondary must converge to warmup + gap batches after reconnect (got " + count(secondary) + ")");
            assertEquals(0, rm.getResyncCount(),
                "an idle secondary (zero applied events) must still resume from its seeded sequence, "
                    + "not fall back to a full re-sync");
            log.info("Idle initial-sync resume converged; resyncCount={}", rm.getResyncCount());
        } finally {
            if (writer != null) {
                writer.close();
            }
            secondary.shutdown();
            primary.shutdown();
        }
    }
}
