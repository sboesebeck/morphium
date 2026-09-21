package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.poppydb.election.ElectionConfig;
import de.caluga.poppydb.election.ElectionManager;
import de.caluga.test.mongo.suite.data.UncachedObject;

/**
 * E2E regression test for #386: an index created on the primary shortly before a leader change
 * was lost cluster-wide. Index replication was periodic only ({@code INDEX_SYNC_INTERVAL_MS},
 * 30s), so a follower promoted inside that window had never seen the index, and the remaining
 * followers - the old primary among them - dropped it while aligning to the new primary
 * ({@code applyIndexDiff}: "dropped stale index"). Nothing recreated it afterwards. Seen twice
 * on {@code FullClusterRestartTest} whenever the lower-priority node won the first election
 * and the priority takeover (also 30s) moved leadership at exactly the wrong moment.
 *
 * <p>The failing sequence, made deterministic: node1 wins the first election (strictly ordered
 * priorities, takeover off), a TTL index is created on it through the wire like any client, and
 * node1 steps down as soon as a marker document written AFTER the index has reached every
 * follower - i.e. well inside the periodic sync window, but with the change stream provably
 * consumed up to and past the index. Whoever wins the re-election must hold the index, and
 * every node must still hold it after a full periodic sync interval has passed with the new
 * primary in charge.
 *
 * <p>Before the fix the followers do not hold the index when the marker arrives - the stream
 * carried no index DDL - which is where this test goes red first.
 */
@Tag("server")
public class IndexSurvivesLeaderChangeTest {

    private static final Logger log = LoggerFactory.getLogger(IndexSurvivesLeaderChangeTest.class);

    private static final String DB = "idxleader";
    private static final String COLL = "tasks";
    private static final int DOCS = 20;
    private static final String INDEX = "ended_on_ttl";
    // One periodic index sync interval (ReplicationManager.INDEX_SYNC_INTERVAL_MS) plus slack.
    private static final long ONE_SYNC_INTERVAL_MS = 35_000;

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

    /** 30s: the initial election of three concurrently started nodes, with room for a retry. */
    private void waitForPrimary(PoppyDB node) throws Exception {
        assertTrue(poll(30_000, node::isPrimary), "node must become primary");
    }

    /** Waits until one of {@code candidates} is primary and returns it (see StepdownReplicationTest). */
    private PoppyDB waitForNewPrimary(PoppyDB demoted, PoppyDB... candidates) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            for (PoppyDB c : candidates) {
                if (c.isPrimary()) {
                    assertFalse(demoted.isPrimary(), "the demoted node must not be primary again");
                    return c;
                }
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no survivor became primary within 30s after the stepdown");
    }

    /**
     * Takeover off, so the leader sequence is deterministic: node1 (100) first, then whichever
     * survivor wins the re-election - which one never matters here, see StepdownReplicationTest
     * for why it is not reliably node2. One instance per node (configureReplicaSet stores the
     * node's own priority in it).
     */
    private static ElectionConfig noPriorityTakeover() {
        return new ElectionConfig().setPriorityTakeoverEnabled(false);
    }

    private boolean hasTtlIndex(PoppyDB node) {
        for (Map<String, Object> idx : node.getDriver().getIndexes(DB, COLL)) {
            Map<?, ?> opts = (Map<?, ?>) idx.get("$options");
            if (opts != null && INDEX.equals(opts.get("name"))) {
                return true;
            }
        }
        return false;
    }

    private long countOn(PoppyDB node) {
        return node.getDriver().count(DB, COLL, Doc.of(), null, null);
    }

    private String indexReport() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nodes.size(); i++) {
            PoppyDB n = nodes.get(i);
            sb.append("node").append(i + 1).append(n.isPrimary() ? "(PRIMARY)" : "")
              .append(": index=").append(hasTtlIndex(n)).append(" docs=").append(countOn(n)).append("; ");
        }
        return sb.toString();
    }

    @Test
    public void indexCreatedShortlyBeforeStepdownSurvivesOnEveryNode() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        List<String> hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        Map<String, Integer> prio = Map.of(
                "localhost:" + port1, 100,
                "localhost:" + port2, 50,
                "localhost:" + port3, 10);
        PoppyDB node1 = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB node2 = new PoppyDB(port2, "localhost", 20, 5);
        PoppyDB node3 = new PoppyDB(port3, "localhost", 20, 5);
        node1.configureReplicaSet("rsIdxLeader", hosts, prio, true, noPriorityTakeover());
        node2.configureReplicaSet("rsIdxLeader", hosts, prio, true, noPriorityTakeover());
        node3.configureReplicaSet("rsIdxLeader", hosts, prio, true, noPriorityTakeover());
        startServer(node1, port1);
        startServer(node2, port2);
        startServer(node3, port3);
        waitForPrimary(node1);

        // data, then the TTL index, then ONE marker document - all through the wire like any client
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.clusterSettings().setHostSeed("localhost:" + port1);
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
                            "name", INDEX, "expireAfterSeconds", 259200)));
            try {
                cmd.execute();
            } finally {
                cmd.releaseConnection();
            }

            m.store(new UncachedObject("marker", DOCS), COLL);
        }

        // The marker was written after the index. Once it is on every follower, the stream has
        // been applied in order past the createIndexes event - so the index must be there too,
        // long before any periodic sync (30s after the followers started replicating) could
        // have caught up.
        assertTrue(poll(30_000, () -> nodes.stream().allMatch(n -> countOn(n) == DOCS + 1)),
                "the marker document written after the index must replicate to every node: " + indexReport());
        assertTrue(poll(5_000, () -> nodes.stream().allMatch(this::hasTtlIndex)),
                "an index created on the primary must reach the followers through the change "
                + "stream, before any leader change (#386) - " + indexReport());

        // Leader change inside the periodic sync window.
        ElectionManager leaderEm = node1.getElectionManager();
        assertNotNull(leaderEm, "leader must have an ElectionManager in election mode");
        log.info("Index is on every node - stepping node1 down inside the index sync window");
        assertTrue(leaderEm.stepDown(60, 0, true), "stepdown should succeed");
        PoppyDB newPrimary = waitForNewPrimary(node1, node2, node3);
        log.info("New primary: {}", newPrimary == node2 ? "node2" : "node3");

        assertTrue(hasTtlIndex(newPrimary), "the new primary must hold the index: " + indexReport());
        // node1 (now a follower of the new primary) must keep it - the diff against the new
        // primary must not see it as stale.
        assertTrue(poll(ONE_SYNC_INTERVAL_MS, () -> nodes.stream().allMatch(this::hasTtlIndex)),
                "every node must hold the index after the leader change: " + indexReport());

        // And it must still be there once every follower has run at least one periodic index
        // diff against the NEW primary - that diff is what deleted it before the fix.
        Thread.sleep(ONE_SYNC_INTERVAL_MS);
        assertTrue(nodes.stream().allMatch(this::hasTtlIndex),
                "the index must survive a full periodic index sync interval under the new primary: " + indexReport());
        assertTrue(nodes.stream().allMatch(n -> countOn(n) == DOCS + 1),
                "every node must still hold the full data set: " + indexReport());
    }
}
