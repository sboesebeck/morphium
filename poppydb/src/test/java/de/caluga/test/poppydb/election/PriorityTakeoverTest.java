package de.caluga.test.poppydb.election;

import de.caluga.poppydb.PoppyDB;
import de.caluga.poppydb.election.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for priority takeover (#177): a leader voluntarily steps down once a node with
 * higher priority is online and caught up, so a failover to a lower-priority node does
 * not become permanent.
 */
public class PriorityTakeoverTest {

    private static final Logger log = LoggerFactory.getLogger(PriorityTakeoverTest.class);

    private static final String ME = "localhost:27017";
    private static final String PEER = "localhost:27018";

    private final List<ElectionManager> managers = new ArrayList<>();
    private final List<PoppyDB> servers = new ArrayList<>();

    @AfterEach
    void cleanup() {
        for (ElectionManager manager : managers) {
            try {
                manager.stop();
            } catch (Exception e) {
                // ignore
            }
        }
        managers.clear();

        for (PoppyDB server : servers) {
            try {
                server.shutdown();
            } catch (Exception e) {
                log.debug("Error shutting down server: {}", e.getMessage());
            }
        }
        servers.clear();
    }

    /**
     * Two-node manager that wins its election because we answer its vote request, and whose
     * heartbeats are answered by a peer of the given priority. Replication progress is faked
     * via the sequence suppliers: both start at 0 and only advance once we are leader, which
     * is what the takeover check measures (progress made during our own term).
     */
    private ElectionManager leaderWithPeer(ElectionConfig config, int peerPriority,
                                           long localSequence, long peerSequence) throws Exception {
        return leaderWithPeer(config, peerPriority, localSequence, peerSequence, null);
    }

    /**
     * {@code peerElectable} is what the stubbed peer reports in its heartbeat responses: true or
     * false as a live value (a supplier, so a test can flip it), null to omit the field the way a
     * node from before #385 does.
     */
    private ElectionManager leaderWithPeer(ElectionConfig config, int peerPriority,
                                           long localSequence, long peerSequence,
                                           java.util.function.Supplier<Boolean> peerElectable) throws Exception {
        return leaderWithPeer(config, peerPriority, localSequence, peerSequence, peerElectable, null);
    }

    /**
     * {@code peerAppliedSequence} is the replication position the stubbed peer reports in its
     * heartbeat responses (#388) - null to omit the field the way a node from before #388 does.
     * {@code peerSequence} stays what the leader's own ack bookkeeping (replSetProgress) knows.
     */
    private ElectionManager leaderWithPeer(ElectionConfig config, int peerPriority,
                                           long localSequence, long peerSequence,
                                           java.util.function.Supplier<Boolean> peerElectable,
                                           java.util.function.LongSupplier peerAppliedSequence) throws Exception {
        ElectionManager manager = new ElectionManager(ME, List.of(ME, PEER), config);
        managers.add(manager);

        AtomicLong local = new AtomicLong(0);
        AtomicLong acked = new AtomicLong(0);
        manager.setLocalSequenceSupplier(local::get);
        manager.setPeerSequenceSupplier(peer -> acked.get());

        manager.setSendVoteRequest((peer, request) ->
                manager.handleVoteResponse(peer, request, new VoteResponse(request.getTerm(), true, peer)));
        manager.setSendAppendEntries((peer, request) ->
                manager.handleAppendEntriesResponse(peer, new AppendEntriesResponse(request.getTerm(), true)
                        .setFollowerId(peer).setPriority(peerPriority)
                        .setElectable(peerElectable == null ? null : peerElectable.get())
                        .setAppliedSequence(peerAppliedSequence == null ? null : peerAppliedSequence.getAsLong())));

        CountDownLatch leaderLatch = new CountDownLatch(1);
        manager.setOnLeadershipChange(isLeader -> {
            if (isLeader) leaderLatch.countDown();
        });

        manager.start();
        assertTrue(leaderLatch.await(2, TimeUnit.SECONDS), "should become leader");

        // writes happen after we took over, so they count against the peer's replication progress
        acked.set(peerSequence);
        local.set(localSequence);
        return manager;
    }

    private ElectionConfig takeoverConfig() {
        return new ElectionConfig()
                .setElectionTimeoutMinMs(100)
                .setElectionTimeoutMaxMs(200)
                .setHeartbeatIntervalMs(50)
                .setElectionPriority(50)
                .setPriorityTakeoverEnabled(true)
                .setPriorityTakeoverCheckIntervalMs(100)
                .setPriorityTakeoverMinStabilityMs(200)
                .setPriorityTakeoverStepDownSecs(5);
    }

    private void assertStepsDown(ElectionManager manager) throws Exception {
        for (int i = 0; i < 50 && manager.getState() == ElectionState.LEADER; i++) {
            Thread.sleep(100);
        }
        assertEquals(ElectionState.FOLLOWER, manager.getState(), "leader should have yielded to higher priority peer");
    }

    private void assertStaysLeader(ElectionManager manager) throws Exception {
        Thread.sleep(1000);
        assertEquals(ElectionState.LEADER, manager.getState());
    }

    @Test
    void testYieldsToHigherPriorityPeer() throws Exception {
        ElectionManager manager = leaderWithPeer(takeoverConfig(), 100, 0, 0);
        assertStepsDown(manager);

        // and it must not immediately re-elect itself, or the successor never gets a chance
        assertTrue(manager.isElectionBlocked(), "should refuse re-election for the step-down period");
    }

    @Test
    void testKeepsLeadershipWhenPeerHasLowerPriority() throws Exception {
        ElectionManager manager = leaderWithPeer(takeoverConfig(), 10, 0, 0);
        assertStaysLeader(manager);
    }

    @Test
    void testKeepsLeadershipWhenPeerHasEqualPriority() throws Exception {
        ElectionManager manager = leaderWithPeer(takeoverConfig(), 50, 0, 0);
        assertStaysLeader(manager);
    }

    @Test
    void testKeepsLeadershipWhileHigherPriorityPeerIsLagging() throws Exception {
        // we replicated up to sequence 100, the peer only acknowledged 40
        ElectionManager manager = leaderWithPeer(takeoverConfig(), 100, 100, 40);
        assertStaysLeader(manager);
    }

    @Test
    void testKeepsLeadershipWhenPeerNeverReportedProgress() throws Exception {
        // -1 = no progress report; we must not hand over blindly
        ElectionManager manager = leaderWithPeer(takeoverConfig(), 100, 100, -1);
        assertStaysLeader(manager);
    }

    @Test
    void testLaggingPeerWithinConfiguredMaxLagStillTakesOver() throws Exception {
        ElectionManager manager = leaderWithPeer(takeoverConfig().setPriorityTakeoverMaxLag(10), 100, 100, 95);
        assertStepsDown(manager);
    }

    /**
     * A node that must never lead (arbiter) advertises no priority, whatever its configured value:
     * handing leadership to it would leave the cluster without a leader until the step-down expires.
     */
    @Test
    void testArbiterNeverTriggersTakeover() throws Exception {
        ElectionConfig arbiterConfig = new ElectionConfig().setElectionPriority(100).setCanBecomeLeader(false);
        ElectionManager arbiter = new ElectionManager(PEER, List.of(ME, PEER), arbiterConfig);
        managers.add(arbiter);

        AppendEntriesResponse response = arbiter.handleAppendEntries(
                AppendEntriesRequest.heartbeat(1, ME, 0, 0, 0));
        assertEquals(-1, response.getPriority(), "arbiter must not advertise itself as successor");

        ElectionManager manager = leaderWithPeer(takeoverConfig(), response.getPriority(), 0, 0);
        assertStaysLeader(manager);
    }

    @Test
    void testDisabledTakeoverKeepsLeadership() throws Exception {
        ElectionManager manager = leaderWithPeer(takeoverConfig().setPriorityTakeoverEnabled(false), 100, 0, 0);
        assertStaysLeader(manager);
    }

    @Test
    void testStabilityWindowDelaysTakeover() throws Exception {
        ElectionConfig config = takeoverConfig().setPriorityTakeoverMinStabilityMs(2000);
        ElectionManager manager = leaderWithPeer(config, 100, 0, 0);

        Thread.sleep(500);
        assertEquals(ElectionState.LEADER, manager.getState(), "must not yield inside the stability window");

        assertStepsDown(manager);
    }

    /**
     * #385: a higher-priority peer that reports itself as not electable - it is inside its own
     * stepdown block - must not be handed leadership. Handing over anyway leaves the cluster
     * without a leader until the block expires; seen as a cascade over three nodes where terms 2,
     * 3 and 4 each lasted a second (StepdownReplicationTest with the takeover at test speed).
     */
    @Test
    void testDoesNotYieldToPeerInsideItsStepdownBlock() throws Exception {
        ElectionManager manager = leaderWithPeer(takeoverConfig(), 100, 0, 0, () -> false);
        assertStaysLeader(manager);
    }

    /**
     * The flip side: the moment the blocked peer reports itself electable again, the takeover
     * proceeds - the fix must delay the handover, not prevent it.
     */
    @Test
    void testYieldsOnceBlockedPeerIsElectableAgain() throws Exception {
        java.util.concurrent.atomic.AtomicBoolean electable = new java.util.concurrent.atomic.AtomicBoolean(false);
        ElectionManager manager = leaderWithPeer(takeoverConfig(), 100, 0, 0, electable::get);
        assertStaysLeader(manager);
        electable.set(true);
        assertStepsDown(manager);
    }

    /**
     * Rolling upgrade: a peer from before #385 sends no electable field. It is treated as
     * electable - today's behaviour, pinned so the new check cannot silently disable takeovers
     * against older nodes. Green before and after the fix by design.
     */
    @Test
    void testPeerWithoutElectableFieldIsTreatedAsElectable() throws Exception {
        ElectionManager manager = leaderWithPeer(takeoverConfig(), 100, 0, 0, null);
        assertStepsDown(manager);
    }

    /**
     * The follower side of #385: while inside its stepdown block a node must say so in its
     * heartbeat responses, and stop saying so once the block is over.
     */
    @Test
    void testFollowerReportsItsStepdownBlockInHeartbeatResponses() throws Exception {
        ElectionManager manager = leaderWithPeer(takeoverConfig().setPriorityTakeoverEnabled(false), 10, 0, 0);
        assertTrue(manager.stepDown(1, 0, true), "stepdown should succeed");
        AppendEntriesResponse blocked = manager.handleAppendEntries(
                AppendEntriesRequest.heartbeat(manager.getCurrentTerm() + 1, PEER, 0, 0, 0));
        assertEquals(Boolean.FALSE, blocked.getElectable(), "inside the stepdown block the follower must report electable=false");
        Thread.sleep(1200);
        AppendEntriesResponse free = manager.handleAppendEntries(
                AppendEntriesRequest.heartbeat(manager.getCurrentTerm() + 1, PEER, 0, 0, 0));
        assertEquals(Boolean.TRUE, free.getElectable(), "after the block the follower must report electable=true");
    }

    /** The field must survive the wire: present when set, absent when unknown. */
    @Test
    void testElectableSurvivesToMapFromMap() {
        assertEquals(Boolean.FALSE, AppendEntriesResponse.fromMap(new AppendEntriesResponse(1, true).setElectable(false).toMap()).getElectable());
        assertEquals(Boolean.TRUE, AppendEntriesResponse.fromMap(new AppendEntriesResponse(1, true).setElectable(true).toMap()).getElectable());
        assertNull(AppendEntriesResponse.fromMap(new AppendEntriesResponse(1, true).toMap()).getElectable());
        assertFalse(new AppendEntriesResponse(1, true).toMap().containsKey("electable"), "unknown must not be sent as a value");
    }

    // ==================== #388: takeover does not fire ====================

    /**
     * #388: the leader's ack bookkeeping (replSetProgress into a ReplicationCoordinator that is
     * recreated on every leadership change) may never hear from a peer that is in fact fully
     * caught up - the follower only reports when its position moved past what it last reported,
     * to whichever primary that was. Since #385 the heartbeat response is the channel a follower
     * tells the leader about itself; its replication position takes the same way. A peer that
     * says "I am at 100" in its heartbeats while the ack bookkeeping knows nothing (-1) must be
     * handed leadership.
     */
    @Test
    void testYieldsToPeerThatReportsItsPositionInHeartbeats() throws Exception {
        ElectionManager manager = leaderWithPeer(takeoverConfig(), 100, 100, -1, null, () -> 100);
        assertStepsDown(manager);
    }

    /** The heartbeat-reported position is subject to the same lag rule as the ack. */
    @Test
    void testKeepsLeadershipWhenHeartbeatReportsLag() throws Exception {
        ElectionManager manager = leaderWithPeer(takeoverConfig(), 100, 100, -1, null, () -> 40);
        assertStaysLeader(manager);
    }

    /**
     * #388, the busy set: "lag == 0 against the leader's sequence of this very instant" is a
     * condition whose outcome depends on where the sample falls relative to the last write and
     * the follower's ack, not on whether the follower keeps pace. The rule is a freshness
     * window, as mongod's priority takeover uses one: a peer that has acknowledged everything
     * the leader had at the PREVIOUS takeover check - one check interval ago - has caught up.
     * Here the peer is always exactly one event behind the instant the check samples, i.e. it
     * has applied everything that existed a moment ago, and must be handed leadership.
     */
    @Test
    void testYieldsToPeerThatKeepsPaceWithAContinuousWriteLoad() throws Exception {
        ElectionConfig config = takeoverConfig();
        ElectionManager manager = new ElectionManager(ME, List.of(ME, PEER), config);
        managers.add(manager);

        // every read of the local sequence is a new write; the peer has always acked the
        // previous one
        AtomicLong local = new AtomicLong(0);
        manager.setLocalSequenceSupplier(local::incrementAndGet);
        manager.setPeerSequenceSupplier(peer -> local.get() - 1);

        manager.setSendVoteRequest((peer, request) ->
                manager.handleVoteResponse(peer, request, new VoteResponse(request.getTerm(), true, peer)));
        manager.setSendAppendEntries((peer, request) ->
                manager.handleAppendEntriesResponse(peer, new AppendEntriesResponse(request.getTerm(), true)
                        .setFollowerId(peer).setPriority(100)));

        CountDownLatch leaderLatch = new CountDownLatch(1);
        manager.setOnLeadershipChange(isLeader -> {
            if (isLeader) leaderLatch.countDown();
        });
        manager.start();
        assertTrue(leaderLatch.await(2, TimeUnit.SECONDS), "should become leader");

        assertStepsDown(manager);
    }

    /** A peer that stopped acknowledging is still not caught up, whatever the write load does. */
    @Test
    void testKeepsLeadershipWhenPeerFallsBehindUnderWriteLoad() throws Exception {
        ElectionConfig config = takeoverConfig();
        ElectionManager manager = new ElectionManager(ME, List.of(ME, PEER), config);
        managers.add(manager);

        AtomicLong local = new AtomicLong(0);
        manager.setLocalSequenceSupplier(local::incrementAndGet);
        manager.setPeerSequenceSupplier(peer -> 1L);  // acked the first write, nothing since

        manager.setSendVoteRequest((peer, request) ->
                manager.handleVoteResponse(peer, request, new VoteResponse(request.getTerm(), true, peer)));
        manager.setSendAppendEntries((peer, request) ->
                manager.handleAppendEntriesResponse(peer, new AppendEntriesResponse(request.getTerm(), true)
                        .setFollowerId(peer).setPriority(100)));

        CountDownLatch leaderLatch = new CountDownLatch(1);
        manager.setOnLeadershipChange(isLeader -> {
            if (isLeader) leaderLatch.countDown();
        });
        manager.start();
        assertTrue(leaderLatch.await(2, TimeUnit.SECONDS), "should become leader");

        assertStaysLeader(manager);
    }

    /**
     * The follower side of #388: a node reports its replication position (in the leader's
     * sequence space) in every heartbeat response - and omits the field when it has none, so
     * a leader never mistakes "unknown" for "at 0".
     */
    @Test
    void testFollowerReportsItsAppliedSequenceInHeartbeatResponses() throws Exception {
        ElectionManager follower = new ElectionManager(PEER, List.of(ME, PEER), takeoverConfig());
        managers.add(follower);

        assertNull(follower.handleAppendEntries(AppendEntriesRequest.heartbeat(1, ME, 0, 0, 0)).getAppliedSequence(),
                "without a position supplier the field must be omitted");

        follower.setAppliedSequenceSupplier(() -> 42L);
        assertEquals(42L, follower.handleAppendEntries(AppendEntriesRequest.heartbeat(1, ME, 0, 0, 0)).getAppliedSequence());

        follower.setAppliedSequenceSupplier(() -> -1L);
        assertNull(follower.handleAppendEntries(AppendEntriesRequest.heartbeat(1, ME, 0, 0, 0)).getAppliedSequence(),
                "a negative position means unknown and must be omitted");
    }

    /** The field must survive the wire: present when set, absent when unknown. */
    @Test
    void testAppliedSequenceSurvivesToMapFromMap() {
        assertEquals(17L, AppendEntriesResponse.fromMap(new AppendEntriesResponse(1, true).setAppliedSequence(17L).toMap()).getAppliedSequence());
        assertNull(AppendEntriesResponse.fromMap(new AppendEntriesResponse(1, true).toMap()).getAppliedSequence());
        assertFalse(new AppendEntriesResponse(1, true).toMap().containsKey("appliedSequence"), "unknown must not be sent as a value");
    }

    /**
     * #388, ask 3: the reason a takeover is withheld must be visible - per peer, in the stats
     * (and at INFO in the log, throttled), not only at DEBUG.
     */
    @Test
    void testWithheldTakeoverReasonIsExposedInStats() throws Exception {
        ElectionManager manager = leaderWithPeer(takeoverConfig(), 100, 100, 40);

        long until = System.currentTimeMillis() + 5_000;
        Map<?, ?> reasons = null;
        while (System.currentTimeMillis() < until) {
            Object r = manager.getStats().get("takeoverWithheld");
            if (r instanceof Map<?, ?> m && m.containsKey(PEER)) {
                reasons = m;
                break;
            }
            Thread.sleep(25);
        }
        assertNotNull(reasons, "the leader must expose why it does not yield to " + PEER);
        assertTrue(String.valueOf(reasons.get(PEER)).contains("lagging"), "reason: " + reasons.get(PEER));
        assertEquals(ElectionState.LEADER, manager.getState());
    }

    /**
     * #388, side note: an AppendEntries that arrives while the manager is shutting down ran
     * becomeFollower() into a shut-down scheduler - RejectedExecutionException, logged at ERROR
     * with no functional effect. The callback dispatch must be guarded.
     */
    @Test
    void testHeartbeatAfterStopDoesNotThrow() throws Exception {
        ElectionManager manager = leaderWithPeer(takeoverConfig().setPriorityTakeoverEnabled(false), 10, 0, 0);
        long term = manager.getCurrentTerm();
        manager.stop();
        assertDoesNotThrow(() -> manager.handleAppendEntries(AppendEntriesRequest.heartbeat(term + 1, PEER, 0, 0, 0)));
    }

    @Test
    void testPeerPrioritiesAreExposedInStats() throws Exception {
        ElectionManager manager = leaderWithPeer(takeoverConfig().setPriorityTakeoverEnabled(false), 100, 0, 0);

        // A peer's priority only becomes known once its first heartbeat response has been
        // handled, and that runs on the election scheduler - independently of the leadership
        // callback leaderWithPeer() waits for. Asserting straight away is a race that only
        // shows up on a loaded machine, so wait for the response to land.
        long until = System.currentTimeMillis() + 10_000;

        while (System.currentTimeMillis() < until
                && !Map.of(PEER, 100).equals(manager.getStats().get("peerPriorities"))) {
            Thread.sleep(25);
        }

        Map<String, Object> stats = manager.getStats();
        assertEquals(false, stats.get("priorityTakeoverEnabled"));
        assertEquals(Map.of(PEER, 100), stats.get("peerPriorities"));
    }

    /**
     * The scenario from #177: after a failover, restarting the original high-priority node
     * must bring leadership back to it, like MongoDB's priority takeover.
     */
    @Test
    void testOriginalPrimaryReclaimsLeadershipAfterRestart() throws Exception {
        List<String> hosts = List.of("localhost:27110", "localhost:27111", "localhost:27112");
        Map<String, Integer> priorities = Map.of(
                "localhost:27110", 100,
                "localhost:27111", 50,
                "localhost:27112", 50);

        for (String host : hosts) {
            servers.add(startNode(host, hosts, priorities));
        }

        PoppyDB preferred = servers.get(0);
        assertTrue(awaitLeader(preferred, 5000), "highest-priority node should win the initial election");

        log.info("Stopping preferred primary to force a failover");
        preferred.shutdown();
        servers.remove(preferred);

        PoppyDB temporaryLeader = awaitAnyLeader(5000);
        assertNotNull(temporaryLeader, "a lower-priority node should take over");
        log.info("Temporary leader: {}:{}", temporaryLeader.getHost(), temporaryLeader.getPort());

        log.info("Restarting the preferred primary");
        PoppyDB restarted = startNode(hosts.get(0), hosts, priorities);
        servers.add(restarted);

        assertTrue(awaitLeader(restarted, 15000), "restarted high-priority node should reclaim leadership");
        assertFalse(temporaryLeader.isPrimary(), "temporary leader should have stepped down");
    }

    private PoppyDB startNode(String address, List<String> hosts, Map<String, Integer> priorities) throws Exception {
        int port = Integer.parseInt(address.split(":")[1]);

        // one config instance per node: configureReplicaSet() writes this node's priority into it
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(150)
                .setElectionTimeoutMaxMs(300)
                .setHeartbeatIntervalMs(50)
                .setPriorityTakeoverCheckIntervalMs(200)
                .setPriorityTakeoverMinStabilityMs(500)
                .setPriorityTakeoverStepDownSecs(3);

        PoppyDB server = new PoppyDB(port, "localhost", 100, 60);
        server.configureReplicaSet("rs0", hosts, priorities, true, config);
        server.start();
        return server;
    }

    private boolean awaitLeader(PoppyDB server, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            ElectionManager em = server.getElectionManager();
            if (em != null && em.getState() == ElectionState.LEADER) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    private PoppyDB awaitAnyLeader(long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (PoppyDB server : servers) {
                ElectionManager em = server.getElectionManager();
                if (em != null && em.getState() == ElectionState.LEADER) {
                    return server;
                }
            }
            Thread.sleep(100);
        }
        return null;
    }
}
