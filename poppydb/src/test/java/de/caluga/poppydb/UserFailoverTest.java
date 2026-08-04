package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnection;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

/**
 * End-to-end proof that user replication (user-replication tasks 1-4) survives an actual
 * failover, not just steady-state replication ({@link UserReplicationTest} covers that). A
 * 3-node replica set runs in election mode with distinct priorities so the initial primary is
 * deterministic; a user created there is confirmed loginable on both secondaries, the primary
 * is then killed (modeled on {@link de.caluga.test.poppydb.FailoverTests}'
 * {@code pooledDriverPrimaryChangeTest} shutdown pattern), and after the new election the
 * SAME user must still work against the new primary - and a user created on the new primary
 * must reach the one remaining secondary.
 *
 * Note: an ERROR log line "Duplicate _id! admin.root" can legitimately appear on a secondary
 * during the root-user-create-vs-initial-sync race (the primary's leadership hook creates root
 * concurrently with a secondary's initial sync snapshotting it) - it is handled by an
 * idempotent fallback in the create path and is expected noise here, not a failure signal.
 */
@Tag("server")
public class UserFailoverTest {

    private static final AtomicInteger MSG_ID = new AtomicInteger(1);

    /** Started nodes, shut down in reverse start order on teardown. */
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

    // ---- RS bootstrap helpers (pattern of UserReplicationTest / FailoverTests) ----

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

    private void waitForPrimary(PoppyDB node) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (!node.isPrimary() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(node.isPrimary(), "node must become primary");
    }

    /** Poll a condition with generous timeout - replication/election is asynchronous, never fixed-sleep. */
    private boolean poll(long timeoutMs, Callable<Boolean> condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    // ---- wire helpers --------------------------------------------------------------------

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

    private double okOf(Map<String, Object> reply) {
        Object v = reply.get("ok");
        return v instanceof Number ? ((Number) v).doubleValue() : 0.0;
    }

    /**
     * Real SCRAM client login against ONE specific node (modeled on AuthTlsWireE2ETest's
     * connect): SingleMongoConnection performs the SCRAM handshake during connect, and the
     * server verifies it against its own local admin.system.users. True iff the handshake
     * succeeds.
     */
    private boolean scramLoginWorks(int port, String user, String password) {
        PooledDriver carrier = new PooledDriver();
        carrier.setConnectionTimeout(3000);
        SingleMongoConnection con = new SingleMongoConnection();
        con.setCredentials("admin", user, password);
        try {
            con.connect(carrier, "localhost", port);
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            try {
                con.close();
            } catch (Exception ignored) {
            }
            try {
                carrier.close();
            } catch (Exception ignored) {
            }
        }
    }

    // ---- test ------------------------------------------------------------------------------

    @Test
    public void usersSurviveFailover() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        PoppyDB node1 = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB node2 = new PoppyDB(port2, "localhost", 20, 5);
        PoppyDB node3 = new PoppyDB(port3, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        // Distinct, strictly ordered priorities: node1 wins the initial election deterministically,
        // and once it is killed node2 - the higher-priority survivor - must win the re-election
        // over node3 just as deterministically. Election-mode priorities are clamped to 0-100
        // (see PoppyDB#configureReplicaSet - unlike static mode, which uses the raw values), so
        // stay within that range.
        var prio = Map.of("localhost:" + port1, 100,
                          "localhost:" + port2, 50,
                          "localhost:" + port3, 10);
        node1.configureReplicaSet("rsUserFailover", hosts, prio, true, null);
        node2.configureReplicaSet("rsUserFailover", hosts, prio, true, null);
        node3.configureReplicaSet("rsUserFailover", hosts, prio, true, null);

        // Election mode needs a majority of the 3-node cluster to vote, so all nodes must be up
        // before waiting for leadership to settle (same reasoning as
        // UserReplicationTest#rootUserIsCreatedByPrimaryAndReplicated).
        startServer(node1, port1);
        startServer(node2, port2);
        startServer(node3, port3);
        waitForPrimary(node1);

        Map<String, Object> createReply = command(port1, Doc.of(
                "createUser", "failover-user", "pwd", "failover-pw", "roles", List.of(), "$db", "admin"));
        assertEquals(1.0, okOf(createReply), "createUser on the initial primary must succeed: " + createReply);

        // Wait until the user has replicated to BOTH secondaries before pulling the rug out -
        // otherwise a failure after the kill would be ambiguous (was it never replicated, or did
        // the failover lose it?).
        assertTrue(poll(30_000, () -> scramLoginWorks(port2, "failover-user", "failover-pw")),
                "user created on the primary must replicate to node2 before the failover");
        assertTrue(poll(30_000, () -> scramLoginWorks(port3, "failover-user", "failover-pw")),
                "user created on the primary must replicate to node3 before the failover");

        node1.shutdown(); // forcing failover
        nodes.remove(node1);

        // node2 has the higher priority of the two survivors, so it must win the re-election.
        waitForPrimary(node2);

        // The user created before the failover must still be usable against the NEW primary -
        // this is the crux of the test: replicated state survives a leadership change.
        assertTrue(poll(15_000, () -> scramLoginWorks(port3, "failover-user", "failover-pw")),
                "the pre-failover user must remain loginable on the surviving secondary");
        assertTrue(scramLoginWorks(port2, "failover-user", "failover-pw"),
                "the pre-failover user must be loginable on the new primary itself");

        // A user created AFTER the failover must replicate through the new primary just like
        // before - proving the new leader's replication path, not just its pre-existing state.
        Map<String, Object> postFailoverReply = command(port2, Doc.of(
                "createUser", "post-failover-user", "pwd", "post-failover-pw", "roles", List.of(), "$db", "admin"));
        assertEquals(1.0, okOf(postFailoverReply),
                "createUser on the new primary must succeed: " + postFailoverReply);

        assertTrue(poll(30_000, () -> scramLoginWorks(port3, "post-failover-user", "post-failover-pw")),
                "a user created on the NEW primary must replicate to the remaining secondary");
    }
}
