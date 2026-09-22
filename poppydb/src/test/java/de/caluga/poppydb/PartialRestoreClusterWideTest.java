package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

/**
 * #391: the #306 partial-restore guard is right for ONE broken node - it must not become
 * primary and push its incomplete state onto intact peers. But when the SAME dump file fails on
 * EVERY node (a dump-format bug like #390, a dump written by a newer version, a corrupted file
 * replicated to all nodes), all nodes hold back candidacy and the set has no primary until an
 * operator moves the file on each node and restarts. Seen on the test runner 2026-09-21: 25
 * minutes without a primary. Nobody holds a better copy in that situation, so there is nothing
 * the guard protects: the highest-priority node lifts its guard and the others sync from it.
 */
@Tag("server")
public class PartialRestoreClusterWideTest {

    private static final String RS = "rsPartialAll";
    private static final String BROKEN = "db_broken";
    private static final String BROKEN_FILE = BROKEN + ".morphium.gz";

    private final List<PoppyDB> nodes = new ArrayList<>();

    @AfterEach
    public void tearDown() {
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

    private void startServer(PoppyDB srv, int port) throws Exception {
        nodes.add(srv);
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

    private boolean poll(long timeoutMs, Callable<Boolean> condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) {
                return true;
            }
            Thread.sleep(150);
        }
        return Boolean.TRUE.equals(condition.call());
    }

    private PoppyDB newNode(int port, List<String> hosts, Map<String, Integer> prio, File dumpDir) {
        PoppyDB node = new PoppyDB(port, "localhost", 20, 5);
        node.setDumpDirectory(dumpDir);
        node.configureReplicaSet(RS, hosts, prio, true, null);
        return node;
    }

    private static void createDump(File dir, String dbName) throws Exception {
        dir.mkdirs();
        InMemoryDriver src = new InMemoryDriver();
        Map<String, List<Map<String, Object>>> db = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        docs.add(Doc.of("_id", dbName + "-doc1", "value", 42));
        db.put("test_coll", docs);
        src.setDatabase(dbName, db);
        src.dumpToFile(dbName, new File(dir, dbName + ".morphium.gz"));
    }

    /** The same unreadable file on every node - what a dump-format bug produces (#390). */
    private static void createGarbageDump(File dir, String name) throws Exception {
        dir.mkdirs();
        try (FileOutputStream fos = new FileOutputStream(new File(dir, name + ".morphium.gz"))) {
            fos.write("this is definitely not gzip".getBytes(StandardCharsets.UTF_8));
        }
    }

    private boolean isHealthy(PoppyDB node) {
        if (node.isPrimary()) {
            return true;
        }
        ReplicationManager rm = node.getReplicationManagerForTest();
        return rm != null && rm.initialSyncComplete.get();
    }

    private long healthyCount() {
        return nodes.stream().filter(this::isHealthy).count();
    }

    private long primaryCount() {
        return nodes.stream().filter(PoppyDB::isPrimary).count();
    }

    private PoppyDB anyPrimary() {
        return nodes.stream().filter(PoppyDB::isPrimary).findFirst().orElse(null);
    }

    @Test
    public void identicalPartialRestoreOnEveryNodeStillElectsTheHighestPriorityNode(@TempDir File base) throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        List<String> hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        Map<String, Integer> prio = Map.of(
                "localhost:" + port1, 100,
                "localhost:" + port2, 90,
                "localhost:" + port3, 80);
        File[] dirs = { new File(base, "n1"), new File(base, "n2"), new File(base, "n3") };
        for (File dir : dirs) {
            createDump(dir, "db_ok");
            createGarbageDump(dir, BROKEN);
        }

        PoppyDB node1 = newNode(port1, hosts, prio, dirs[0]);
        PoppyDB node2 = newNode(port2, hosts, prio, dirs[1]);
        PoppyDB node3 = newNode(port3, hosts, prio, dirs[2]);
        for (PoppyDB n : List.of(node1, node2, node3)) {
            // CLI startup order: restore synchronously BEFORE start() wires election/replication
            InMemoryDriver.DirectoryRestoreResult r = n.restoreFromDump();
            assertFalse(r.isComplete(), "the garbage file must fail the restore on every node");
            assertEquals(List.of(BROKEN_FILE), r.getFailedFiles());
            assertFalse(n.isLocalDataComplete(), "the #306 guard must be armed on every node");
        }
        startServer(node1, port1);
        startServer(node2, port2);
        startServer(node3, port3);

        // THE assertion: with every node missing the same file, nobody holds a better copy, so
        // the set must still elect a primary - the highest-priority node - instead of waiting
        // for an operator. Before the fix all three held back candidacy forever.
        boolean elected = poll(30_000, () -> primaryCount() == 1);
        assertTrue(elected,
                "every node failed the same dump file - nobody holds a better copy, so the set must "
                + "elect a primary within 30s instead of holding back candidacy on all nodes (#391); "
                + "got " + primaryCount() + " primaries");
        assertSame(node1, anyPrimary(), "the highest-priority node (100) must be the one that lifts its guard");
        assertTrue(node1.isLocalDataComplete(), "the elected node's guard must be lifted");

        // the others keep their guard and take the primary's state through the existing sync path
        assertTrue(poll(60_000, () -> healthyCount() == 3),
                "the remaining nodes must sync from the new primary and become healthy secondaries - got "
                + healthyCount() + "/3 healthy");
        assertEquals(1, primaryCount(), "still exactly one primary");
        for (PoppyDB n : nodes) {
            assertTrue(n.getDriver().listDatabases().contains("db_ok"),
                    "the intact database must be on every node");
        }
    }

    @Test
    public void aNodeWithACompleteCopyWinsAndNoGuardIsLifted(@TempDir File base) throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        List<String> hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        Map<String, Integer> prio = Map.of(
                "localhost:" + port1, 100,
                "localhost:" + port2, 90,
                "localhost:" + port3, 80);
        File dir1 = new File(base, "n1");
        File dir2 = new File(base, "n2");
        File dir3 = new File(base, "n3");
        // node1 and node2 fail the same file; node3 - lowest priority - holds a complete copy
        for (File dir : List.of(dir1, dir2, dir3)) {
            createDump(dir, "db_ok");
        }
        createGarbageDump(dir1, BROKEN);
        createGarbageDump(dir2, BROKEN);
        createDump(dir3, BROKEN);

        PoppyDB node1 = newNode(port1, hosts, prio, dir1);
        PoppyDB node2 = newNode(port2, hosts, prio, dir2);
        PoppyDB node3 = newNode(port3, hosts, prio, dir3);
        assertFalse(node1.restoreFromDump().isComplete());
        assertFalse(node2.restoreFromDump().isComplete());
        assertTrue(node3.restoreFromDump().isComplete(), "node3 must restore completely");
        startServer(node1, port1);
        startServer(node2, port2);
        startServer(node3, port3);

        // The existing behaviour must survive: a peer with a complete copy blocks the cluster-wide
        // acceptance, so the complete node wins - regardless of priority - and the broken nodes
        // sync from it. The first primary this set ever sees must be node3.
        AtomicReference<PoppyDB> firstPrimary = new AtomicReference<>();
        assertTrue(poll(30_000, () -> {
            PoppyDB p = anyPrimary();
            if (p != null) {
                firstPrimary.compareAndSet(null, p);
            }
            return p != null;
        }), "a node with a complete copy must win the election as before");
        assertSame(node3, firstPrimary.get(),
                "the complete node must be the first primary - a partially restored node must NOT lift "
                + "its guard while a peer holds a complete copy");

        // (no assertion on who leads AFTER the sync: once node1 is complete again the priority
        // takeover legitimately hands leadership to it)
        assertTrue(poll(60_000, () -> healthyCount() == 3),
                "the broken nodes must sync from the complete primary - got " + healthyCount() + "/3 healthy");
        for (PoppyDB n : nodes) {
            assertTrue(n.getDriver().listDatabases().contains(BROKEN),
                    "the database only node3 had must have reached every node via the sync");
        }
    }
}
