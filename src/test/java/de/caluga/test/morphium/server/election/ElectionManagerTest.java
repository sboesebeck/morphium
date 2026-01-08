package de.caluga.test.morphium.server.election;

import de.caluga.morphium.server.election.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ElectionManager.
 */
public class ElectionManagerTest {

    private static final Logger log = LoggerFactory.getLogger(ElectionManagerTest.class);

    private List<ElectionManager> managers = new ArrayList<>();

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
    }

    @Test
    void testSingleNodeAutoElect() throws Exception {
        log.info("Testing single node auto-election");

        // Single node cluster should auto-elect as leader
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(50)
                .setElectionTimeoutMaxMs(100);

        List<String> hosts = List.of("localhost:27017");
        ElectionManager manager = new ElectionManager("localhost:27017", hosts, config);
        managers.add(manager);

        CountDownLatch leaderLatch = new CountDownLatch(1);
        manager.setOnLeadershipChange(isLeader -> {
            if (isLeader) {
                log.info("Node became leader");
                leaderLatch.countDown();
            }
        });

        manager.start();

        // Should become leader quickly (single node)
        assertTrue(leaderLatch.await(500, TimeUnit.MILLISECONDS), "Should become leader within 500ms");
        assertEquals(ElectionState.LEADER, manager.getState());
        assertTrue(manager.isLeader());
        assertEquals("localhost:27017", manager.getCurrentLeader());
    }

    @Test
    void testVoteRequestGranted() throws Exception {
        log.info("Testing vote request - vote granted");

        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(1000)  // Long timeout to prevent auto-election
                .setElectionTimeoutMaxMs(2000);

        List<String> hosts = List.of("localhost:27017", "localhost:27018", "localhost:27019");
        ElectionManager manager = new ElectionManager("localhost:27017", hosts, config);
        managers.add(manager);

        manager.start();

        // Wait a bit for initialization
        Thread.sleep(50);

        // Manager should be follower (hasn't timed out yet)
        assertEquals(ElectionState.FOLLOWER, manager.getState());

        // Send vote request from another node with higher term
        VoteRequest request = new VoteRequest(1, "localhost:27018", 0, 0);
        VoteResponse response = manager.handleVoteRequest(request);

        assertTrue(response.isVoteGranted(), "Vote should be granted");
        assertEquals(1, response.getTerm());
    }

    @Test
    void testVoteRequestDeniedLowerTerm() throws Exception {
        log.info("Testing vote request - denied due to lower term");

        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(1000)
                .setElectionTimeoutMaxMs(2000);

        List<String> hosts = List.of("localhost:27017", "localhost:27018");
        ElectionManager manager = new ElectionManager("localhost:27017", hosts, config);
        managers.add(manager);

        manager.start();
        Thread.sleep(50);

        // First, update manager's term by handling a higher term vote request
        VoteRequest request1 = new VoteRequest(5, "localhost:27018", 0, 0);
        manager.handleVoteRequest(request1);
        assertEquals(5, manager.getCurrentTerm());

        // Now try with lower term - should be denied
        VoteRequest request2 = new VoteRequest(3, "localhost:27019", 0, 0);
        VoteResponse response = manager.handleVoteRequest(request2);

        assertFalse(response.isVoteGranted(), "Vote should be denied for lower term");
        assertEquals(5, response.getTerm());
    }

    @Test
    void testVoteRequestDeniedAlreadyVoted() throws Exception {
        log.info("Testing vote request - denied because already voted");

        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(1000)
                .setElectionTimeoutMaxMs(2000);

        List<String> hosts = List.of("localhost:27017", "localhost:27018", "localhost:27019");
        ElectionManager manager = new ElectionManager("localhost:27017", hosts, config);
        managers.add(manager);

        manager.start();
        Thread.sleep(50);

        // Vote for first candidate
        VoteRequest request1 = new VoteRequest(1, "localhost:27018", 0, 0);
        VoteResponse response1 = manager.handleVoteRequest(request1);
        assertTrue(response1.isVoteGranted(), "First vote should be granted");

        // Second candidate in same term should be denied
        VoteRequest request2 = new VoteRequest(1, "localhost:27019", 0, 0);
        VoteResponse response2 = manager.handleVoteRequest(request2);
        assertFalse(response2.isVoteGranted(), "Second vote in same term should be denied");
    }

    @Test
    void testHeartbeatResetsElectionTimer() throws Exception {
        log.info("Testing heartbeat resets election timer");

        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(100)
                .setElectionTimeoutMaxMs(150);

        List<String> hosts = List.of("localhost:27017", "localhost:27018");
        ElectionManager manager = new ElectionManager("localhost:27017", hosts, config);
        managers.add(manager);

        manager.start();
        Thread.sleep(30);

        assertEquals(ElectionState.FOLLOWER, manager.getState());

        // Keep sending heartbeats to prevent election
        for (int i = 0; i < 10; i++) {
            AppendEntriesRequest heartbeat = AppendEntriesRequest.heartbeat(1, "localhost:27018", 0, 0, 0);
            manager.handleAppendEntries(heartbeat);
            Thread.sleep(50);
        }

        // Should still be follower after 500ms because heartbeats reset timer
        assertEquals(ElectionState.FOLLOWER, manager.getState());
        assertEquals("localhost:27018", manager.getCurrentLeader());
    }

    @Test
    void testStepDownOnHigherTerm() throws Exception {
        log.info("Testing step down on higher term heartbeat");

        // Use longer election timeout to prevent re-election during test
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(50)
                .setElectionTimeoutMaxMs(100);

        // Single node - will become leader
        List<String> hosts = List.of("localhost:27017");
        ElectionManager manager = new ElectionManager("localhost:27017", hosts, config);
        managers.add(manager);

        CountDownLatch leaderLatch = new CountDownLatch(1);
        CountDownLatch stepDownLatch = new CountDownLatch(1);

        manager.setOnLeadershipChange(isLeader -> {
            if (isLeader) {
                leaderLatch.countDown();
            } else {
                stepDownLatch.countDown();
            }
        });

        manager.start();
        assertTrue(leaderLatch.await(500, TimeUnit.MILLISECONDS));
        assertEquals(ElectionState.LEADER, manager.getState());

        long originalTerm = manager.getCurrentTerm();

        // Receive heartbeat from node claiming to be leader with higher term
        // This simulates a scenario where another leader exists with higher term
        AppendEntriesRequest higherTermHeartbeat = AppendEntriesRequest.heartbeat(
                originalTerm + 10,  // Much higher term
                "localhost:27018",
                0, 0, 0
        );

        manager.handleAppendEntries(higherTermHeartbeat);

        // Wait for step down callback to confirm state transition
        assertTrue(stepDownLatch.await(100, TimeUnit.MILLISECONDS), "Should step down from leader");

        // Verify state was updated (check immediately after callback - state may change again due to re-election)
        assertEquals(originalTerm + 10, manager.getCurrentTerm(), "Term should be updated to higher term");
        assertEquals("localhost:27018", manager.getCurrentLeader(), "Leader should be updated");
        // Note: State may become LEADER again in single-node mode after re-election,
        // but the stepDownLatch confirms the step-down did occur
    }

    @Test
    void testVoteRequestLogComparison() throws Exception {
        log.info("Testing vote request log comparison");

        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(1000)
                .setElectionTimeoutMaxMs(2000);

        List<String> hosts = List.of("localhost:27017", "localhost:27018");
        ElectionManager manager = new ElectionManager("localhost:27017", hosts, config);
        managers.add(manager);

        // Set our log state to be ahead
        manager.updateLogIndex(10, 2);

        manager.start();
        Thread.sleep(50);

        // Request from candidate with older log (lower term)
        VoteRequest oldLogRequest = new VoteRequest(3, "localhost:27018", 5, 1);
        VoteResponse response1 = manager.handleVoteRequest(oldLogRequest);
        assertFalse(response1.isVoteGranted(), "Should deny vote to candidate with older log (lower term)");

        // Request from candidate with up-to-date log
        VoteRequest upToDateRequest = new VoteRequest(4, "localhost:27019", 10, 2);
        VoteResponse response2 = manager.handleVoteRequest(upToDateRequest);
        assertTrue(response2.isVoteGranted(), "Should grant vote to candidate with up-to-date log");
    }

    @Test
    void testGetStats() throws Exception {
        log.info("Testing getStats");

        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(50)
                .setElectionTimeoutMaxMs(100);

        List<String> hosts = List.of("localhost:27017");
        ElectionManager manager = new ElectionManager("localhost:27017", hosts, config);
        managers.add(manager);

        manager.start();
        Thread.sleep(200);  // Allow time to become leader

        var stats = manager.getStats();

        assertEquals("localhost:27017", stats.get("myAddress"));
        assertEquals("LEADER", stats.get("state"));
        assertNotNull(stats.get("term"));
        assertEquals("localhost:27017", stats.get("leader"));
        assertEquals(0, stats.get("peerCount"));
        assertTrue((Boolean) stats.get("running"));
    }

    @Test
    void testForceStepDown() throws Exception {
        log.info("Testing forced step down");

        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(50)
                .setElectionTimeoutMaxMs(100);

        List<String> hosts = List.of("localhost:27017");
        ElectionManager manager = new ElectionManager("localhost:27017", hosts, config);
        managers.add(manager);

        CountDownLatch leaderLatch = new CountDownLatch(1);
        manager.setOnLeadershipChange(isLeader -> {
            if (isLeader) leaderLatch.countDown();
        });

        manager.start();
        assertTrue(leaderLatch.await(500, TimeUnit.MILLISECONDS));
        assertEquals(ElectionState.LEADER, manager.getState());

        // Force step down
        manager.stepDown();

        assertEquals(ElectionState.FOLLOWER, manager.getState());
    }
}
