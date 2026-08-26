package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.test.mongo.suite.data.UncachedObject;

/**
 * E2E regression test for the ACC outage after the #340 rollout: a FULL cluster restart (all
 * nodes down at once, then all up) left two of three nodes stuck in recovery forever. Each
 * node restored its dump correctly - but the restore had registered every TTL index with
 * {@code expireAfterSeconds} as {@code Long}, and the first peer that listed those indexes
 * during its initial sync crashed in {@code IndexDescription.fromMap}
 * ({@code IllegalArgumentException}: Long into the Integer field), failing the sync in an
 * endless retry loop. The cluster ran on a single node until the indexes were recreated by
 * hand.
 *
 * <p>Why no test caught it: a ROLLING restart never triggers it (the restarted node gets its
 * indexes via initial sync from a healthy peer, correctly typed); restore and server restart
 * were each covered alone - the combination "restored from dump, then queried by a peer" was
 * not. This test pins the full production scenario: all nodes stop (each writing its final
 * dump), all start again (each restoring from its own dump, no healthy peer to sync from),
 * and the assertion is on CLUSTER HEALTH - every node must reach PRIMARY or a completed-sync
 * SECONDARY state again - not merely on the data being present.
 */
@Tag("server")
public class FullClusterRestartTest {

    private static final Logger log = LoggerFactory.getLogger(FullClusterRestartTest.class);

    private static final String DB = "fullrestart";
    private static final String COLL = "tasks";
    private static final int DOCS = 20;
    private static final String RS = "rsFullRestart";

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

    private boolean hasTtlIndex(PoppyDB node) {
        for (Map<String, Object> idx : node.getDriver().getIndexes(DB, COLL)) {
            Map<?, ?> opts = (Map<?, ?>) idx.get("$options");
            if (opts != null && "ended_on_ttl".equals(opts.get("name"))) {
                return true;
            }
        }
        return false;
    }

    /** A node counts as healthy when it is the primary, or a secondary whose initial sync completed. */
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

    @Test
    public void allNodesReturnToPrimaryOrSecondaryAfterFullClusterRestart(@TempDir File base) throws Exception {
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

        PoppyDB node1 = newNode(port1, hosts, prio, dir1);
        PoppyDB node2 = newNode(port2, hosts, prio, dir2);
        PoppyDB node3 = newNode(port3, hosts, prio, dir3);
        startServer(node1, port1);
        startServer(node2, port2);
        startServer(node3, port3);
        assertTrue(poll(30_000, () -> primaryCount() == 1), "cluster must elect a primary");
        PoppyDB primary = nodes.stream().filter(PoppyDB::isPrimary).findFirst().orElseThrow();
        int primaryPort = primary == node1 ? port1 : primary == node2 ? port2 : port3;

        // data + a TTL index, through the wire like any client
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.clusterSettings().setHostSeed("localhost:" + primaryPort);
        cfg.connectionSettings().setDatabase(DB);
        cfg.connectionSettings().setMaxConnections(10);
        try (Morphium m = new Morphium(cfg)) {
            List<UncachedObject> batch = new ArrayList<>();
            for (int i = 0; i < DOCS; i++) {
                batch.add(new UncachedObject("pre-" + i, i));
            }
            m.storeList(batch, COLL);

            var con = m.getDriver().getPrimaryConnection(null);
            CreateIndexesCommand cmd = new CreateIndexesCommand(con).setDb(DB).setColl(COLL)
                    .setIndexes(List.of(Doc.of("key", Doc.of("ended_on", 1),
                            "name", "ended_on_ttl", "expireAfterSeconds", 259200)));
            try {
                cmd.execute();
            } finally {
                cmd.releaseConnection();
            }
        }

        // data and index on ALL nodes before the outage (index replication runs with the
        // initial sync and the periodic 30s index diff, hence the generous poll)
        assertTrue(poll(60_000, () ->
                nodes.stream().allMatch(n -> n.getDriver().count(DB, COLL, Doc.of(), null, null) == DOCS)
                && nodes.stream().allMatch(this::hasTtlIndex)),
                "data and TTL index must be on every node before the full restart");

        log.info("Cluster converged - simulating the outage: ALL nodes down at once");
        // reverse order like a shutdown script; every node writes its final dump
        node3.shutdown();
        node2.shutdown();
        node1.shutdown();
        nodes.clear();

        log.info("Full restart: all nodes come back from their own dumps - no healthy peer exists");
        PoppyDB r1 = newNode(port1, hosts, prio, dir1);
        PoppyDB r2 = newNode(port2, hosts, prio, dir2);
        PoppyDB r3 = newNode(port3, hosts, prio, dir3);
        // CLI startup order: restore synchronously BEFORE start() wires election/replication
        assertTrue(r1.restoreFromDump().isComplete(), "node1 restore must be complete");
        assertTrue(r2.restoreFromDump().isComplete(), "node2 restore must be complete");
        assertTrue(r3.restoreFromDump().isComplete(), "node3 restore must be complete");
        startServer(r1, port1);
        startServer(r2, port2);
        startServer(r3, port3);

        // THE regression assertion: the whole cluster must become healthy again - one PRIMARY
        // and every other node a SECONDARY with a COMPLETED initial sync. Before the fix the
        // secondaries crashed in IndexDescription.fromMap during syncIndexesFrom (the restored
        // TTL index carried a Long expireAfterSeconds) and looped in recovery forever.
        boolean recovered = poll(90_000, () -> primaryCount() == 1 && healthyCount() == 3);
        assertTrue(recovered,
                "after a full cluster restart every node must return to PRIMARY or a synced SECONDARY - "
                + "got " + primaryCount() + " primaries and " + healthyCount() + "/3 healthy nodes "
                + "(a node stuck below is looping in initial-sync recovery; before the #340 follow-up fix "
                + "this was the Long-typed expireAfterSeconds from the dump crashing every peer's "
                + "ListIndexes/fromMap)");

        // and the TTL index actually survived, correctly typed, on every node
        for (PoppyDB n : nodes) {
            assertTrue(hasTtlIndex(n), "TTL index must exist on every node after the full restart");
            for (Map<String, Object> idx : n.getDriver().getIndexes(DB, COLL)) {
                Map<?, ?> opts = (Map<?, ?>) idx.get("$options");
                if (opts != null && "ended_on_ttl".equals(opts.get("name"))) {
                    Object expire = opts.get("expireAfterSeconds");
                    assertNotNull(expire);
                    assertInstanceOf(Integer.class, expire,
                            "expireAfterSeconds must be Int32-typed after restore, got " + expire.getClass());
                    assertEquals(259200, ((Number) expire).intValue());
                }
            }
            assertEquals(DOCS, n.getDriver().count(DB, COLL, Doc.of(), null, null),
                    "every node must hold the full data set again");
        }
    }
}
