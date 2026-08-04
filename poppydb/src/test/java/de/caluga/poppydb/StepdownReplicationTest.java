package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import de.caluga.poppydb.election.AppendEntriesRequest;
import de.caluga.poppydb.election.ElectionManager;

/**
 * <p><b>Coverage note (task-4 follow-up item B):</b> this test PINS the fixed behavior (it fails
 * loudly if a future change breaks the demoted node's ability to resume replication toward the
 * new primary), but it empirically CANNOT detect a revert of the {@code onLeadershipChange(false)}
 * hook that this fix touches. The bug depends on {@code onLeadershipChange(false)} and
 * {@code onLeaderDiscovered(newLeader)} racing on ElectionManager's thread pool in the adversarial
 * order, and that race is unforceable from the public API alone: both the natural-timing version
 * of this test (5 runs) and the forced-race version below, which injects a synthetic heartbeat
 * immediately after {@code stepDown()} (10 runs), came back GREEN pre-fix - 15/15 total, never RED
 * - because task A (`onLeadershipChange`) is always submitted to the executor measurably before
 * task B on this environment's timing, regardless of the underlying code being fixed or broken.
 * The fix's correctness guarantee therefore rests on the code-level trace in task-1-report.md (the
 * {@code !primary} guard combined with ElectionManager's "fires only on leader change" semantics),
 * not on this test having demonstrated the pre-fix failure. Treat a green run here as regression
 * coverage for the fixed code path, not as proof the hook still exists.
 *
 * <p>Regression test for the leadership-callback ordering gap (follow-up task 1 of the
 * user-replication series): on a LIVE stepdown - primary demoted but never killed -
 * ElectionManager dispatches {@code onLeadershipChange(false)} and
 * {@code onLeaderDiscovered(newLeader)} on its 3-thread pool in nondeterministic order. If
 * discovery lands first, it observes {@code PoppyDB#primary} still {@code true} and no-ops; since
 * ElectionManager only re-fires discovery on an actual leader CHANGE, it never fires again for
 * that leader, and the demoted node is left replicating nothing until some unrelated later
 * leader change happens to bail it out.
 *
 * Modeled on {@link UserFailoverTest} (RS bootstrap, wire helpers, SCRAM login helper) but
 * exercises a graceful stepdown (mirrors {@code MultiNodeElectionTest#testGracefulStepDown})
 * instead of a kill: the bug only manifests when the demoted node survives and must resume
 * replication toward the new primary on its own.
 *
 * <p><b>Why the test injects a synthetic heartbeat instead of just waiting for the real
 * election:</b> {@code onLeadershipChange(false)} is submitted to ElectionManager's pool the
 * instant {@code stepDown()} is called locally; a real election on the two survivors takes at
 * least one election timeout (seconds by default) before their winner's first heartbeat reaches
 * the demoted node and submits {@code onLeaderDiscovered}. That multi-second gap means an idle
 * 3-thread pool drains the first submission long before the second exists, so the two callbacks
 * essentially never race under real timing - confirmed empirically: 5/5 pre-fix runs of a
 * version of this test that just waited for the real election came back GREEN, not flaky-RED
 * (see task-1-report.md for the raw evidence). To exercise the actual race the bug report
 * describes, this test calls {@link ElectionManager#handleAppendEntries} directly - a PUBLIC
 * method, not a test-only hook added to production code - immediately after
 * {@link ElectionManager#stepDown}, from the same test thread. This submits the
 * onLeaderDiscovered task within microseconds of the onLeadershipChange(false) task instead of
 * seconds, reproducing the adversarial ordering the bug depends on. The real election among the
 * two survivors still runs concurrently and independently decides the real new primary; the
 * injected heartbeat only forces the demoted node's callback timing.
 *
 * Note: an ERROR/WARN "Duplicate _id" log line can legitimately appear during sync races (a
 * user create racing a secondary's own catch-up read) - expected noise here, not a failure
 * signal, per the other tests in this package.
 */
@Tag("server")
public class StepdownReplicationTest {

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

    // ---- RS bootstrap helpers (pattern of UserFailoverTest / UserReplicationTest) ----

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
    public void demotedLeaderResumesReplicationTowardNewPrimary() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        PoppyDB node1 = new PoppyDB(port1, "localhost", 20, 5);
        PoppyDB node2 = new PoppyDB(port2, "localhost", 20, 5);
        PoppyDB node3 = new PoppyDB(port3, "localhost", 20, 5);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        // Distinct, strictly ordered priorities: node1 wins the initial election deterministically,
        // and once it steps down, node2 - the higher-priority survivor - must win the re-election
        // over node3 just as deterministically (same reasoning as UserFailoverTest).
        var prio = Map.of("localhost:" + port1, 100,
                          "localhost:" + port2, 50,
                          "localhost:" + port3, 10);
        node1.configureReplicaSet("rsStepdown", hosts, prio, true, null);
        node2.configureReplicaSet("rsStepdown", hosts, prio, true, null);
        node3.configureReplicaSet("rsStepdown", hosts, prio, true, null);

        // Election mode needs a majority of the 3-node cluster to vote, so all nodes must be up
        // before waiting for leadership to settle.
        startServer(node1, port1);
        startServer(node2, port2);
        startServer(node3, port3);
        waitForPrimary(node1);

        // Sanity: a user created before the stepdown must reach node1's normal followers -
        // proves steady-state replication works, isolating the stepdown-specific bug below.
        Map<String, Object> preReply = command(port1, Doc.of(
                "createUser", "pre-stepdown-user", "pwd", "pre-stepdown-pw", "roles", List.of(), "$db", "admin"));
        assertEquals(1.0, okOf(preReply), "createUser on the initial primary must succeed: " + preReply);
        assertTrue(poll(30_000, () -> scramLoginWorks(port2, "pre-stepdown-user", "pre-stepdown-pw")),
                "user created before the stepdown must replicate to node2");
        assertTrue(poll(30_000, () -> scramLoginWorks(port3, "pre-stepdown-user", "pre-stepdown-pw")),
                "user created before the stepdown must replicate to node3");

        // Force a LIVE stepdown - node1 stays running, unlike UserFailoverTest's shutdown - and
        // is blocked from re-election for long enough that node2 (next highest priority) wins.
        ElectionManager leaderEm = node1.getElectionManager();
        assertNotNull(leaderEm, "leader must have an ElectionManager in election mode");
        long termAtStepdown = leaderEm.getCurrentTerm();
        String node2Address = "localhost:" + port2;
        assertTrue(leaderEm.stepDown(10, 0, true), "stepdown should succeed");

        // Force the exact ordering the bug depends on - see the class javadoc for why this is
        // necessary instead of just waiting for the real election's heartbeat to arrive. Same
        // term as the stepdown: not treated as a fresher term, just a plain "here's who the
        // leader is now" notification, exactly like a heartbeat that lost the race against
        // onLeadershipChange(false) would look.
        leaderEm.handleAppendEntries(
                AppendEntriesRequest.heartbeat(termAtStepdown, node2Address, 0, 0, 0));

        // node2 has the higher priority of the two non-demoted nodes, so it must win the real
        // re-election regardless of the synthetic injection above, which only ever affected
        // node1's own view of who the leader is.
        waitForPrimary(node2);

        // A user created on the NEW primary must reach every OTHER node - including node1, which
        // is still running, demoted to secondary, and must have resumed replication toward node2
        // on its own (this is the crux of the ordering-gap bug: node1 must NOT need a further
        // leader change to start replicating again).
        Map<String, Object> postStepdownReply = command(port2, Doc.of(
                "createUser", "post-stepdown-user", "pwd", "post-stepdown-pw", "roles", List.of(), "$db", "admin"));
        assertEquals(1.0, okOf(postStepdownReply),
                "createUser on the new primary must succeed: " + postStepdownReply);

        assertTrue(poll(30_000, () -> scramLoginWorks(port3, "post-stepdown-user", "post-stepdown-pw")),
                "a user created on the NEW primary must replicate to node3");
        assertTrue(poll(30_000, () -> scramLoginWorks(port1, "post-stepdown-user", "post-stepdown-pw")),
                "the DEMOTED node (node1, still running) must resume replication toward the new primary");
    }
}
