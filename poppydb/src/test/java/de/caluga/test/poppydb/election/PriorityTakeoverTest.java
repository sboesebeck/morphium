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
        ElectionManager manager = new ElectionManager(ME, List.of(ME, PEER), config);
        managers.add(manager);

        AtomicLong local = new AtomicLong(0);
        AtomicLong acked = new AtomicLong(0);
        manager.setLocalSequenceSupplier(local::get);
        manager.setPeerSequenceSupplier(peer -> acked.get());

        manager.setSendVoteRequest((peer, request) ->
                manager.handleVoteResponse(peer, new VoteResponse(request.getTerm(), true, peer)));
        manager.setSendAppendEntries((peer, request) ->
                manager.handleAppendEntriesResponse(peer, new AppendEntriesResponse(request.getTerm(), true)
                        .setFollowerId(peer).setPriority(peerPriority)));

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

    @Test
    void testPeerPrioritiesAreExposedInStats() throws Exception {
        ElectionManager manager = leaderWithPeer(takeoverConfig().setPriorityTakeoverEnabled(false), 100, 0, 0);

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
