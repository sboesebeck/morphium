package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;
import de.caluga.test.mongo.suite.data.UncachedObject;

/**
 * Race repro for lossless initial sync (task 8).
 *
 * A 2-node PoppyDB replica set: node1 (high priority) is primary, node2 (low priority)
 * joins later as a secondary and performs an initial sync. A background writer keeps
 * inserting documents on the primary the whole time the secondary is copying the (large)
 * snapshot. Once the secondary reports its initial sync complete, the writer stops and,
 * after the change stream has had a chance to drain, the secondary's per-collection
 * document counts must match the primary's exactly.
 *
 * Before the fix, the secondary only started its change-stream watch AFTER the snapshot
 * copy had finished (with no resume point), so every write that happened during the copy
 * -- including writes to collections that had already been scanned -- was silently lost.
 * The snapshot is intentionally large (50k docs across 5 collections) so the copy takes
 * long enough that the writer reliably lands writes inside the copy window, making the
 * loss deterministic.
 */
@Tag("server")
public class InitialSyncTest {

    private static final Logger log = LoggerFactory.getLogger(InitialSyncTest.class);

    private static final String DB = "initialsynctest";
    private static final int COLLECTIONS = 5;
    private static final int PRESEED_PER_COLLECTION = 10_000; // 50k total snapshot

    private int nextPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String coll(int i) {
        return "coll_" + i;
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

    private long totalCount(PoppyDB node) throws Exception {
        long total = 0;
        for (int i = 0; i < COLLECTIONS; i++) {
            total += node.getDriver().count(DB, coll(i), Doc.of(), null, null);
        }
        return total;
    }

    @Test
    public void writesDuringInitialSyncAreNotLost() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        PoppyDB primary = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB secondary = new PoppyDB(port2, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2);
        var prio = Map.of("localhost:" + port1, 300, "localhost:" + port2, 100);
        primary.configureReplicaSet("rsInitialSync", hosts, prio);
        secondary.configureReplicaSet("rsInitialSync", hosts, prio);

        Morphium writer = null;
        Thread writerThread = null;
        AtomicBoolean writing = new AtomicBoolean(true);
        AtomicInteger written = new AtomicInteger(0);

        try {
            // Bring up the primary and make sure it actually holds the primary role.
            startServer(primary, port1);
            long primaryDeadline = System.currentTimeMillis() + 15_000;
            while (!primary.isPrimary() && System.currentTimeMillis() < primaryDeadline) {
                Thread.sleep(50);
            }
            assertTrue(primary.isPrimary(), "node1 must become primary");

            // Client that writes only to the primary (over the wire, so change events fire).
            MorphiumConfig cfg = new MorphiumConfig();
            cfg.clusterSettings().setHostSeed("localhost:" + port1);
            cfg.connectionSettings().setDatabase(DB);
            cfg.connectionSettings().setMaxConnections(10);
            cfg.cacheSettings().setBufferedWritesEnabled(false);
            writer = new Morphium(cfg);

            // Pre-seed a large snapshot so the secondary's copy takes a while.
            for (int c = 0; c < COLLECTIONS; c++) {
                List<UncachedObject> batch = new ArrayList<>(PRESEED_PER_COLLECTION);
                for (int i = 0; i < PRESEED_PER_COLLECTION; i++) {
                    batch.add(new UncachedObject("preseed-" + c + "-" + i, i));
                }
                writer.storeList(batch, coll(c));
            }
            long preseedTotal = totalCount(primary);
            log.info("Pre-seeded {} documents on primary", preseedTotal);
            assertEquals((long) COLLECTIONS * PRESEED_PER_COLLECTION, preseedTotal,
                "pre-seed should have created the full snapshot on the primary");

            // Background writer: keep inserting into the already-existing collections for the
            // whole duration of the secondary's initial sync.
            final Morphium w = writer;
            writerThread = new Thread(() -> {
                int n = 0;
                while (writing.get()) {
                    try {
                        List<UncachedObject> batch = new ArrayList<>(50);
                        for (int i = 0; i < 50; i++) {
                            batch.add(new UncachedObject("live-" + n, n));
                            n++;
                        }
                        w.storeList(batch, coll(n % COLLECTIONS));
                        written.addAndGet(batch.size());
                    } catch (Exception e) {
                        // primary bounce etc. -- keep going
                    }
                }
            }, "test-live-writer");
            writerThread.setDaemon(true);
            writerThread.start();

            // Now bring up the secondary; it will elect node1 as primary and start initial sync
            // while the writer keeps inserting.
            startServer(secondary, port2);

            // Wait until the secondary reports its initial sync is complete.
            long syncDeadline = System.currentTimeMillis() + 60_000;
            boolean syncComplete = false;
            while (System.currentTimeMillis() < syncDeadline) {
                if (initialSyncComplete(secondary)) {
                    syncComplete = true;
                    break;
                }
                Thread.sleep(50);
            }
            assertTrue(syncComplete, "secondary initial sync must complete within 60s");
            log.info("Secondary reported initial sync complete; live writes so far: {}", written.get());

            // Let the writer run a touch longer so there's writing on both sides of the
            // sync-complete boundary, then stop it.
            Thread.sleep(500);
            writing.set(false);
            writerThread.join(5000);

            long primaryTotal = totalCount(primary);
            log.info("Writer stopped. Primary total: {} (live writes: {})", primaryTotal, written.get());

            // Wait for the change stream to drain so the secondary catches up to the primary.
            long catchupDeadline = System.currentTimeMillis() + 30_000;
            long secondaryTotal = 0;
            while (System.currentTimeMillis() < catchupDeadline) {
                secondaryTotal = totalCount(secondary);
                if (secondaryTotal == primaryTotal) {
                    break;
                }
                Thread.sleep(100);
            }

            log.info("Final counts -- primary: {}, secondary: {}", primaryTotal, secondaryTotal);
            for (int i = 0; i < COLLECTIONS; i++) {
                long p = primary.getDriver().count(DB, coll(i), Doc.of(), null, null);
                long s = secondary.getDriver().count(DB, coll(i), Doc.of(), null, null);
                assertEquals(p, s, "secondary must have every document the primary has in " + coll(i));
            }
            assertEquals(primaryTotal, secondaryTotal,
                "secondary must not lose any write that happened during initial sync");
        } finally {
            writing.set(false);
            if (writerThread != null) {
                writerThread.join(2000);
            }
            if (writer != null) {
                writer.close();
            }
            secondary.shutdown();
            primary.shutdown();
        }
    }

    @SuppressWarnings("unchecked")
    private boolean initialSyncComplete(PoppyDB node) {
        Object rep = node.getStats().get("replication");
        if (rep instanceof Map) {
            return Boolean.TRUE.equals(((Map<String, Object>) rep).get("initialSyncComplete"));
        }
        return false;
    }
}
