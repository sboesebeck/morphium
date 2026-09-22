package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import de.caluga.poppydb.election.ElectionConfig;

/**
 * #393 against a live PoppyDB replica set: a {@link PooledDriver} does a primary read, the
 * primary is stepped down with {@code replSetStepDown} over the wire, and the very next primary
 * read - sent while the driver still holds the stepped-down node as primary, its heartbeat has
 * not run yet - must come back from the new primary instead of surfacing the node's
 * not-primary answer (13435, or 13436 once the demoted node has started its re-sync).
 *
 * <p>RS bootstrap follows {@link StepdownReplicationTest}: three in-process nodes with strictly
 * ordered priorities, priority takeover off so the demoted node does not get the leadership
 * handed back mid-test. Which survivor wins the re-election is not the point and not asserted.
 */
@Tag("server")
public class PrimaryReadStepdownRetryTest {

    private static final AtomicInteger MSG_ID = new AtomicInteger(1);

    private final List<PoppyDB> nodes = new ArrayList<>();
    private PooledDriver driver;

    @AfterEach
    public void tearDown() {
        if (driver != null) {
            try {
                driver.close();
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

    private static ElectionConfig noPriorityTakeover() {
        return new ElectionConfig().setPriorityTakeoverEnabled(false);
    }

    /** 30s: the initial election of three concurrently started nodes, with room for a retry. */
    private void waitForPrimary(PoppyDB node) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!node.isPrimary() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(node.isPrimary(), "node must become primary");
    }

    /** Send one OP_MSG command over a raw socket to a node and return the reply's first document. */
    private Map<String, Object> command(int port, Map<String, Object> cmd) throws Exception {
        try (Socket sock = new Socket()) {
            sock.connect(new InetSocketAddress("localhost", port), 2000);
            sock.setSoTimeout(15_000);
            OpMsg msg = new OpMsg();
            msg.setMessageId(MSG_ID.incrementAndGet());
            msg.setFlags(0);
            msg.setFirstDoc(cmd);
            sock.getOutputStream().write(msg.bytes());
            sock.getOutputStream().flush();
            OpMsg reply = (OpMsg) WireProtocolMessage.parseFromStream(sock.getInputStream());
            return reply.getFirstDoc();
        }
    }

    private static double okOf(Map<String, Object> reply) {
        Object v = reply.get("ok");
        return v instanceof Number ? ((Number) v).doubleValue() : 0.0;
    }

    @Test
    @Timeout(120)
    public void primaryReadAfterStepdownLandsOnTheNewPrimary() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        PoppyDB node1 = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB node2 = new PoppyDB(port2, "localhost", 20, 5);
        PoppyDB node3 = new PoppyDB(port3, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        var prio = Map.of("localhost:" + port1, 100,
                          "localhost:" + port2, 50,
                          "localhost:" + port3, 10);
        node1.configureReplicaSet("rsReadRetry", hosts, prio, true, noPriorityTakeover());
        node2.configureReplicaSet("rsReadRetry", hosts, prio, true, noPriorityTakeover());
        node3.configureReplicaSet("rsReadRetry", hosts, prio, true, noPriorityTakeover());

        startServer(node1, port1);
        startServer(node2, port2);
        startServer(node3, port3);
        waitForPrimary(node1);

        driver = new PooledDriver();
        driver.setHostSeed(hosts);
        driver.setConnectionTimeout(2000);
        driver.setMaxWaitTime(5000);
        // the re-election takes an election timeout (2-4 s) plus a heartbeat to be seen; the
        // primary wait inside the retry must cover that
        driver.setServerSelectionTimeout(15_000);
        driver.setHeartbeatFrequency(1000);
        driver.setRetriesOnNetworkError(5);
        driver.setSleepBetweenErrorRetries(100);
        driver.connect();
        String node1Address = "localhost:" + port1;
        assertEquals(node1Address, driver.getPrimaryNode(), "the driver must have found node1 as primary");

        // a primary read before the stepdown: the baseline
        List<String> before = driver.listCollections("testdb", null);
        assertNotNull(before);

        Map<String, Object> stepdown = command(port1,
            Doc.of("replSetStepDown", 30, "force", true, "$db", "admin"));
        assertEquals(1.0, okOf(stepdown), "replSetStepDown on the primary must succeed: " + stepdown);
        assertFalse(node1.isPrimary(), "node1 must have stepped down");

        // the stepped-down node's own hello: not primary, and it must not name ITSELF as the
        // primary either - the MongoCommandHandler fallback did, after #392 had cleared the
        // ElectionManager's leader, and the driver's re-resolution then stayed on this node
        Map<String, Object> hello = command(port1, Doc.of("hello", 1, "$db", "admin"));
        assertEquals(Boolean.FALSE, hello.get("isWritablePrimary"), "stepped-down node must answer isWritablePrimary:false: " + hello);
        assertNotEquals(node1Address, hello.get("primary"),
            "a stepped-down node must not advertise itself as primary: " + hello);

        // the driver's heartbeat (1 s) has not run: it still holds node1 as primary, and node1
        // answers a primary read with not-primary. This read must nevertheless succeed.
        List<String> after = driver.listCollections("testdb", null);
        assertNotNull(after);

        String primaryAfter = driver.getPrimaryNode();
        assertNotNull(primaryAfter, "the driver must know the new primary after the read");
        assertNotEquals(node1Address, primaryAfter, "the read must have moved the driver off the stepped-down node");
        PoppyDB newPrimary = primaryAfter.endsWith(":" + port2) ? node2 : node3;
        assertTrue(newPrimary.isPrimary(), "the node the driver now calls primary must be the primary: " + primaryAfter);
    }
}
