package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import de.caluga.poppydb.election.ElectionConfig;

/**
 * #530 on a real three-node set: a primary held at its client connection limit must still be
 * reachable for its own set - a leader change away and back, and the secondaries' new replication
 * connections and progress reports to the returning primary.
 *
 * <p>All three nodes run on one host, so by address every connection would be a member
 * connection and the test would prove nothing. The member check is therefore injected: the idle
 * "client" sockets this test opens are registered by their local port and count as clients,
 * everything else - the peers' election, replication and driver traffic, and also this test's
 * own command connections - counts as member traffic.
 */
@Tag("server")
public class ConnectionLimitClusterTest {

    private static final String RS_NAME = "rsConnectionLimit";
    private static final int CLIENT_LIMIT = 6;
    private static final AtomicInteger MSG_ID = new AtomicInteger(1);

    private final List<PoppyDB> nodes = new ArrayList<>();
    private final List<Socket> clients = new ArrayList<>();
    private final Set<Integer> clientPorts = ConcurrentHashMap.newKeySet();
    private PoppyDB node1;
    private PoppyDB node2;
    private PoppyDB node3;
    private int port1;

    @AfterEach
    public void tearDown() {
        for (Socket s : clients) {
            try {
                s.close();
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

    private static int nextPort() throws Exception {
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

    private static ElectionConfig electionConfig() {
        return new ElectionConfig()
                .setElectionTimeoutMinMs(400)
                .setElectionTimeoutMaxMs(800)
                .setHeartbeatIntervalMs(150)
                .setLeaderLeaseTimeoutMs(4000)
                .setPriorityTakeoverEnabled(true)
                .setPriorityTakeoverCheckIntervalMs(300)
                .setPriorityTakeoverMinStabilityMs(1000)
                .setPriorityTakeoverStepDownSecs(1);
    }

    private void startThreeNodeSet() throws Exception {
        port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        node1 = new PoppyDB(port1, "localhost", CLIENT_LIMIT, 300);
        node2 = new PoppyDB(port2, "localhost", CLIENT_LIMIT, 300);
        node3 = new PoppyDB(port3, "localhost", CLIENT_LIMIT, 300);

        List<String> hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        Map<String, Integer> prio = Map.of("localhost:" + port1, 100,
                                           "localhost:" + port2, 50,
                                           "localhost:" + port3, 10);

        for (PoppyDB node : List.of(node1, node2, node3)) {
            node.configureReplicaSet(RS_NAME, hosts, prio, true, electionConfig());
            node.setMemberConnectionCheckForTest(remote -> !clientPorts.contains(remote.getPort()));
        }

        startServer(node1, port1);
        startServer(node2, port2);
        startServer(node3, port3);
    }

    private boolean poll(long timeoutMs, Callable<Boolean> condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) {
                return true;
            }
            Thread.sleep(50);
        }

        return Boolean.TRUE.equals(condition.call());
    }

    private PoppyDB currentPrimaryOtherThan(PoppyDB notThis) {
        for (PoppyDB node : List.of(node1, node2, node3)) {
            if (node != notThis && node.isPrimary()) {
                return node;
            }
        }
        return null;
    }

    /** An idle client connection, registered by its local port before it connects. */
    private Socket openClient(int port) throws Exception {
        Socket sock = new Socket();
        clients.add(sock);
        sock.bind(new InetSocketAddress(0));
        clientPorts.add(sock.getLocalPort());
        sock.connect(new InetSocketAddress("localhost", port), 2000);
        return sock;
    }

    private static boolean isRejected(Socket sock) throws Exception {
        sock.setSoTimeout(2000);

        try {
            return sock.getInputStream().read() == -1;
        } catch (SocketTimeoutException e) {
            return false;
        } catch (SocketException e) {
            return true;
        }
    }

    private Map<String, Object> command(int port, Map<String, Object> cmd) throws Exception {
        try (Socket sock = new Socket()) {
            sock.connect(new InetSocketAddress("localhost", port), 2000);
            sock.setSoTimeout(20_000);
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

    /** A w=3 write on node1 needs both secondaries to replicate from it and report back. */
    private Map<String, Object> awaitW3Write(String id, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        Map<String, Object> last = null;

        while (System.currentTimeMillis() < deadline) {
            try {
                last = command(port1, Doc.of(
                        "insert", "docs",
                        "documents", List.of(Doc.of("_id", id + "-" + System.nanoTime())),
                        "$db", "connlimit",
                        "writeConcern", Doc.of("w", 3, "wtimeout", 3_000)));

                if (Double.valueOf(1.0).equals(last.get("ok")) && last.get("writeConcernError") == null) {
                    return last;
                }
            } catch (Exception e) {
                // node in transition - retried
            }
            Thread.sleep(100);
        }

        throw new AssertionError("no acknowledged w=3 write on node1 within " + timeoutMs + " ms, last reply: " + last);
    }

    @Test
    @Timeout(240)
    public void aPrimaryAtItsClientLimitStillServesItsReplicaSet() throws Exception {
        startThreeNodeSet();
        assertTrue(poll(40_000, node1::isPrimary), "node1 must become primary");
        awaitW3Write("steady", 30_000);

        // Hold node1 at its client limit with idle client connections.
        for (int i = 0; i < CLIENT_LIMIT; i++) {
            openClient(port1);
        }
        assertTrue(poll(5_000, () -> node1.getClientConnectionCount() == CLIENT_LIMIT),
                "node1 must hold " + CLIENT_LIMIT + " client connections, has " + node1.getClientConnectionCount());
        assertTrue(isRejected(openClient(port1)), "a further client must be rejected");

        // Leader change away from node1 and back: the secondaries' replication managers are
        // replaced on every change, so the way back needs NEW member connections to node1.
        assertTrue(node1.getElectionManager().stepDown(1, 0, true), "node1 must step down");
        assertTrue(poll(40_000, () -> currentPrimaryOtherThan(node1) != null),
                "another node must take over after node1's step-down");
        PoppyDB otherPrimary = currentPrimaryOtherThan(node1);
        assertNotNull(otherPrimary);

        if (!poll(15_000, node1::isPrimary)) {
            otherPrimary.getElectionManager().stepDown(1, 0, true);
        }
        assertTrue(poll(60_000, node1::isPrimary), "node1 must take the lead back while at its client limit");

        // Both secondaries replicate from node1 again and report their progress to it.
        awaitW3Write("after-failback", 60_000);

        assertEquals(CLIENT_LIMIT, node1.getClientConnectionCount(), "the idle clients are still there");
        assertTrue(node1.getMemberConnectionCount() > 0, "the set's traffic rides on member connections");
        assertTrue(isRejected(openClient(port1)), "the client limit still holds");
        assertTrue(node1.getRejectedConnectionCount() >= 2, "both rejected clients are counted");
    }
}
