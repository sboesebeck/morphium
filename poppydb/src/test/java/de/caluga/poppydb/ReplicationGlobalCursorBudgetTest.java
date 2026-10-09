package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

/**
 * The global-cursor byte budget (`global-cursor-budget`, #412/#321-addendum) evicts the cursor
 * that holds the most buffered bytes - and the replication watch, which receives every event, is
 * the most likely holder. Its kill is documented as the benign case: the secondary resumes from
 * its last-applied sequence, the replay-backlog gate answers 286, and the full re-sync is a
 * seconds-long snapshot. This test is the missing half of that claim - it proves the RECOVERY,
 * not just the kill.
 *
 * <p>Runs a REAL PoppyDB primary on a port (the kill lives in the primary's server-side
 * {@link de.caluga.poppydb.netty.WatchCursorManager}) with a tiny GLOBAL budget and a well-behaved
 * secondary, and asserts that replication converges anyway: no livelock, no data loss. The
 * existing {@code WatchCursorByteBudgetTest} pins the manager-level eviction policy; this one
 * pins that a replication client actually survives it end to end.
 */
@Tag("server")
public class ReplicationGlobalCursorBudgetTest {

    private static final String DB = "globalbudgettest";
    private static final String COLL = "objs";

    private final List<PoppyDB> nodes = new ArrayList<>();
    private ReplicationManager rm;
    private InMemoryDriver local;

    @AfterEach
    public void tearDown() {
        if (rm != null) {
            try {
                rm.stop();
            } catch (Exception ignored) {
            }
        }

        if (local != null) {
            try {
                local.close();
            } catch (Exception ignored) {
            }
        }

        for (int i = nodes.size() - 1; i >= 0; i--) {
            try {
                nodes.get(i).shutdown();
            } catch (Exception ignored) {
            }
        }

        nodes.clear();
    }

    private int nextPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private PoppyDB startStandalonePrimary(int port) throws Exception {
        PoppyDB srv = new PoppyDB(port, "localhost", 20, 5);
        nodes.add(srv);
        srv.start();
        long deadline = System.currentTimeMillis() + 10_000;

        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("localhost", port), 250);
                return srv;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }

                Thread.sleep(50);
            }
        }
    }

    private boolean poll(long timeoutMs, Callable<Boolean> condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) {
                return true;
            }

            Thread.sleep(100);
        }

        return Boolean.TRUE.equals(condition.call());
    }

    private Morphium writerFor(int port, String db) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.clusterSettings().setHostSeed("localhost:" + port);
        cfg.connectionSettings().setDatabase(db);
        cfg.connectionSettings().setMaxConnections(10);
        cfg.cacheSettings().setBufferedWritesEnabled(false);
        return new Morphium(cfg);
    }

    /**
     * A burst of writes larger than the global fleet budget forces the primary to evict a cursor.
     * The secondary must still end up with every document: either it drains fast enough (no kill),
     * or the kill happens, the session is retired, a fresh watch registers and the snapshot covers
     * the gap. Both outcomes are a pass; a livelock or a lost document is not.
     */
    @Test
    public void replicationConvergesDespiteAGlobalCursorBudgetEviction() throws Exception {
        int port = nextPort();
        PoppyDB primary = startStandalonePrimary(port);
        // A global fleet cap deliberately far below the burst that follows: whichever cursor
        // holds the most buffered bytes (the replication watch receives them all) gets evicted.
        primary.setGlobalCursorByteBudget(4096);

        Morphium writer = writerFor(port, DB);

        try {
            // Pre-existing data so the secondary has a real snapshot to take.
            for (int i = 0; i < 20; i++) {
                primary.getDriver().store(DB, COLL, List.of(Doc.of("_id", "pre-" + i, "v", i)), null);
            }

            local = new InMemoryDriver();
            local.connect();
            rm = new ReplicationManager(local, "localhost", port);
            // A small (but not pathological) event queue on the secondary, so it is a normally
            // behaving follower - not a blocked reader. The point is the PRIMARY-side global cap.
            rm.setEventQueueByteBudget(64 * 1024);
            rm.start();

            assertTrue(poll(20_000, () -> local.count(DB, COLL, Doc.of(), null, null) == 20),
                    "initial sync must bring over the pre-existing 20 documents");

            // Burst of ~1KB documents, far past the 4096-byte global fleet budget.
            for (int i = 0; i < 60; i++) {
                primary.getDriver().store(DB, COLL,
                        List.of(Doc.of("_id", "burst-" + i, "payload", "x".repeat(1024))), null);
            }

            // The replication must converge to all 80 documents despite the evictions: no
            // livelock (bounded by the poll), no data loss (exact count).
            assertTrue(poll(60_000, () -> local.count(DB, COLL, Doc.of(), null, null) == 80),
                    "replication must converge to all 80 documents despite the global-budget "
                            + "eviction (got " + local.count(DB, COLL, Doc.of(), null, null) + ")");
            assertEquals(80, local.count(DB, COLL, Doc.of(), null, null),
                    "no document may be lost across a global-budget cursor eviction");
        } finally {
            writer.close();
        }
    }
}