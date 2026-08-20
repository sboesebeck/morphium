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
 * #322: the initial sync could declare success over a DEAD watch.
 *
 * <p>During the snapshot the apply gate is closed, so replication events accumulate in the
 * secondary's event queue; a small byte budget blocks the watch reader (correct by design). The
 * primary however never blocks - it kills the cursor when the per-cursor buffer overflows. The
 * secondary cannot notice: the only thread that would flip {@code watchLive} is the blocked
 * reader itself, so {@code watchInvalidatedDuringSnapshot()} trusted a stale pair and opened the
 * gate over a dead watch with a real event gap.
 *
 * <p>The fix validates something the blocked reader cannot make stale: after the snapshot the
 * sync thread asks the PRIMARY whether the watch cursor still exists ({@code poppyCursorAlive}).
 * If not, the snapshot is discarded, the buffered events of the dead session are dropped (their
 * session epoch is retired, so events the freed reader still enqueues are discarded too instead
 * of being applied as stale upserts over the next snapshot), and the sync retries under a fresh
 * watch.
 *
 * <p>Wire-path-only by nature: the cursor-kill lives in the primary's server-side
 * {@code WatchCursorManager}, so this test runs a REAL PoppyDB primary on a port and wires a
 * {@link ReplicationManager} against it (pattern of {@link ReplicationFailClosedTest}) - an
 * in-process secondary would take the client-mode subscription path and never reproduce the bug.
 */
@Tag("server")
public class ReplicationDeadWatchGateTest {

    private static final String DB = "deadwatchtest";
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

    private int primaryWatchCursorCount(PoppyDB primary) {
        return (int) primary.getCursorManagerForTest().getStats().get("watchCursors");
    }

    @Test
    public void snapshotOverAKilledWatchIsDiscardedInsteadOfOpeningTheGate() throws Exception {
        int port = nextPort();
        PoppyDB primary = startStandalonePrimary(port);
        // Tiny per-cursor byte budget on the PRIMARY: a blocked reader's cursor dies after a
        // handful of ~1KB events instead of after 10,000 (the #321 injectability this test
        // was waiting for).
        primary.setCursorQueueByteBudget(4096);

        // Pre-existing data so the initial sync has something real to copy.
        Morphium writer = writerFor(port, DB);
        try {
            for (int i = 0; i < 10; i++) {
                primary.getDriver().store(DB, COLL, List.of(Doc.of("_id", "pre-" + i, "v", i)), null);
            }

            local = new InMemoryDriver();
            local.connect();
            rm = new ReplicationManager(local, "localhost", port);
            // Tiny byte budget on the SECONDARY: the watch reader blocks after ~2KB of buffered
            // events while the apply gate is still closed - the exact by-design backpressure
            // the bug needs.
            rm.setEventQueueByteBudget(2048);
            // Park the sync thread inside the snapshot phase so the gate stays closed while the
            // load lands.
            rm.armTestPauseInShortcutForTest();
            rm.start();

            assertTrue(poll(10_000, () -> rm.getConsistencyShortcutAttemptsForTest() >= 1),
                    "sync thread must be parked inside the snapshot phase");
            assertTrue(poll(10_000, () -> primaryWatchCursorCount(primary) >= 1),
                    "the replication watch cursor must be established on the primary");
            long generationOfDeadWatch = rm.watchGeneration.get();

            // Load while the gate is closed: the reader blocks on its 2KB budget, the primary's
            // 4KB cursor budget overflows, the primary kills the cursor.
            for (int i = 0; i < 30; i++) {
                primary.getDriver().store(DB, COLL,
                        List.of(Doc.of("_id", "load-" + i, "payload", "x".repeat(1024))), null);
            }

            assertTrue(poll(15_000, () -> primaryWatchCursorCount(primary) == 0),
                    "the primary must have killed the blocked reader's cursor");

            // Let the snapshot finish. The guard must now refuse to declare success under the
            // watch generation that is provably dead on the primary.
            rm.releaseTestPauseInShortcutForTest();

            assertTrue(poll(60_000, rm::isInitialSyncComplete),
                    "the sync must eventually complete - under a NEW watch");

            assertTrue(rm.getSnapshotsDiscardedDeadWatchForTest() >= 1,
                    "the guard must have discarded at least one snapshot because the primary "
                            + "reported the watch cursor dead - not opened the gate over it");
            assertTrue(rm.watchGeneration.get() > generationOfDeadWatch,
                    "success must have been declared under a NEW watch generation ("
                            + rm.watchGeneration.get() + "), never under the dead one ("
                            + generationOfDeadWatch + ")");

            // And the data must be complete despite the discarded first attempt.
            assertTrue(poll(15_000, () -> local.count(DB, COLL, Doc.of(), null, null) == 40),
                    "all 40 documents must have replicated (got "
                            + local.count(DB, COLL, Doc.of(), null, null) + ")");
            assertEquals(40, local.count(DB, COLL, Doc.of(), null, null));
        } finally {
            writer.close();
        }
    }
}
