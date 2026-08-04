package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
 * Regression test for Finding B of the leadership-hardening task: a follower whose FIRST
 * {@code startReplicationToLeader()} attempt fails must not stay replication-less forever -
 * ElectionManager only re-fires {@code onLeaderDiscovered()} on an actual leader CHANGE, so
 * without an independent retry the follower would need a whole new leader change (a real
 * failover) to get another chance. See PoppyDB's {@code scheduleReplicationRetry}/
 * {@code retryReplicationStart} (1s-doubling backoff capped at 30s), wired from the
 * package-private {@code handleReplicationStartFailure} seam this test drives directly.
 *
 * <p><b>Why a seam, not a port-blocking RS test:</b> the task brief's preferred approach - start
 * a follower pointing at a leader whose port isn't listening yet, so the first attempt fails -
 * was tried first and is empirically NOT viable against the current Morphium client: for the
 * single-host, non-replica-set-configured target {@code ReplicationManager} always uses,
 * {@code PooledDriver.connect()} swallows an unreachable seed internally (deferring to its own
 * background heartbeat reconnection) instead of throwing synchronously. Proof this isn't just a
 * theoretical concern: a port-blocking version of this test was written and PASSED even with
 * {@link PoppyDB#handleReplicationStartFailure} 's retry-scheduling call temporarily disabled -
 * i.e. it passed for a reason having nothing to do with the fix under test, driven entirely by
 * the Morphium client's own reconnection. That result is recorded in task-1-report.md. This test
 * instead drives the exact failure-handling code directly, per the brief's sanctioned fallback.
 *
 * <p>The two tests below are deliberately independent: the first proves the retry-scheduling and
 * "no leader known yet" guard never crash and never fabricate progress when nothing is known
 * about a leader; the second proves that once a leader IS known and reachable, the scheduled
 * retry actually results in live replication. The second test's leader-knowledge is supplied via
 * {@link ElectionManager#handleAppendEntries} (the same public-API injection technique
 * {@link StepdownReplicationTest} established) - which, being the first-ever change of
 * currentLeader, also fires ElectionManager's own {@code onLeaderDiscovered} dispatch once. That
 * dispatch and this test's own scheduled retry both target the same (by-then reachable) leader,
 * so this second test alone cannot prove exclusivity the way the first test's null-leader case
 * does - it verifies the retry chain's wiring end-to-end (guards, re-reading the current leader,
 * eventually-live replication), which is what the brief asks for ("succeeds against a
 * now-reachable leader"), not that the retry was the ONLY possible path to success.
 */
@Tag("server")
public class ReplicationStartRetryTest {

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

    // ---- RS bootstrap helpers (pattern of UserFailoverTest / StepdownReplicationTest) ----

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

    /** Poll a condition with generous timeout - replication/retry is asynchronous, never fixed-sleep. */
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
     * Real SCRAM client login against ONE specific node (modeled on {@link UserFailoverTest}):
     * SingleMongoConnection performs the SCRAM handshake during connect, and the server verifies
     * it against its own local admin.system.users. True iff the handshake succeeds.
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

    // ---- tests -----------------------------------------------------------------------------

    /**
     * A scheduled retry that fires while no leader is known yet must safely no-op (never crash,
     * never fabricate a ReplicationManager) - {@code retryReplicationStart}'s
     * {@code electionManager.getCurrentLeader() == null} guard.
     */
    @Test
    public void scheduledRetryIsSafeNoOpWhenNoLeaderIsKnown() throws Exception {
        int followerPort = nextPort();
        int fakeLeaderPort = nextPort(); // never bound - address is never dialed, guard bails first
        String fakeLeaderAddress = "localhost:" + fakeLeaderPort;
        String followerAddress = "localhost:" + followerPort;
        List<String> hosts = List.of(fakeLeaderAddress, followerAddress);
        Map<String, Integer> prio = Map.of(fakeLeaderAddress, 100, followerAddress, 50);

        PoppyDB follower = new PoppyDB(followerPort, "localhost", 20, 5);
        follower.configureReplicaSet("rsRetryNoLeader", hosts, prio, true, null);
        startServer(follower, followerPort);

        assertNull(follower.getElectionManager().getCurrentLeader(), "sanity: no leader known yet");
        assertNull(follower.getReplicationManagerForTest(), "sanity: nothing replicating yet");

        ReplicationManager dummy = new ReplicationManager(follower.getDriver(), "localhost", fakeLeaderPort);
        follower.handleReplicationStartFailure(fakeLeaderAddress, dummy,
                new Exception("simulated Finding B failure"), PoppyDB.REPLICATION_RETRY_INITIAL_DELAY_MS);

        // Wait past the scheduled retry (fires at +1s) - it must find no leader known and no-op,
        // not crash and not have connected to anything.
        Thread.sleep(PoppyDB.REPLICATION_RETRY_INITIAL_DELAY_MS + 500);
        assertNull(follower.getReplicationManagerForTest(),
                "retry must stay a no-op while no leader is known - nothing to connect to");
    }

    /**
     * The Finding B scenario itself: a simulated failed first attempt schedules a retry: once a
     * leader is known and reachable, the retry chain results in live replication without this
     * test ever calling {@code startReplicationToLeader} a second time itself - only PoppyDB's
     * own retry does that.
     */
    @Test
    public void followerRetriesFailedReplicationStartAndEventuallyReplicates() throws Exception {
        int leaderPort = nextPort();
        int followerPort = nextPort();
        String leaderAddress = "localhost:" + leaderPort;
        String followerAddress = "localhost:" + followerPort;
        List<String> hosts = List.of(leaderAddress, followerAddress);
        Map<String, Integer> prio = Map.of(leaderAddress, 100, followerAddress, 50);

        // Leader up first and reachable throughout - static (non-election) mode, so it never
        // runs its own ElectionManager and can't independently send follower a heartbeat outside
        // the one injected below.
        PoppyDB leader = new PoppyDB(leaderPort, "localhost", 20, 5);
        leader.configureReplicaSet("rsRetrySucceeds", hosts, prio);
        startServer(leader, leaderPort);
        assertTrue(leader.isPrimary(), "leader must be primary (higher static-mode priority)");

        PoppyDB follower = new PoppyDB(followerPort, "localhost", 20, 5);
        follower.configureReplicaSet("rsRetrySucceeds", hosts, prio, true, null);
        startServer(follower, followerPort);

        ElectionManager followerEm = follower.getElectionManager();
        assertNotNull(followerEm, "follower must have an ElectionManager in election mode");
        assertNull(follower.getReplicationManagerForTest(), "no replication attempted yet");

        // Simulate that a first attempt against leaderAddress already failed (Finding B's catch
        // block, driven directly) - schedules a retry for +1s.
        ReplicationManager dummy = new ReplicationManager(follower.getDriver(), "localhost", leaderPort);
        follower.handleReplicationStartFailure(leaderAddress, dummy,
                new Exception("simulated Finding B failure"), PoppyDB.REPLICATION_RETRY_INITIAL_DELAY_MS);
        assertNull(follower.getReplicationManagerForTest(),
                "the simulated failure itself must not have started anything");

        // Make the leader resolvable via ElectionManager so the retry's re-read of
        // getCurrentLeader() has something to find - see the class javadoc for the caveat this
        // also fires ElectionManager's own onLeaderDiscovered dispatch once.
        long term = followerEm.getCurrentTerm();
        followerEm.handleAppendEntries(AppendEntriesRequest.heartbeat(term, leaderAddress, 0, 0, 0));

        Map<String, Object> createReply = command(leaderPort, Doc.of(
                "createUser", "retry-user", "pwd", "retry-pw", "roles", List.of(), "$db", "admin"));
        assertEquals(1.0, okOf(createReply), "createUser on the leader must succeed: " + createReply);

        assertTrue(poll(30_000, () -> follower.getReplicationManagerForTest() != null),
                "the follower must end up replicating from the leader");
        assertTrue(poll(30_000, () -> scramLoginWorks(followerPort, "retry-user", "retry-pw")),
                "a user created on the leader must replicate to the follower");
    }
}
