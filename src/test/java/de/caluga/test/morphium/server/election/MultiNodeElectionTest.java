package de.caluga.test.morphium.server.election;

import de.caluga.morphium.server.MorphiumServer;
import de.caluga.morphium.server.election.ElectionConfig;
import de.caluga.morphium.server.election.ElectionManager;
import de.caluga.morphium.server.election.ElectionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for multi-node election.
 * Starts multiple MorphiumServer instances and tests leader election.
 */
public class MultiNodeElectionTest {

    private static final Logger log = LoggerFactory.getLogger(MultiNodeElectionTest.class);

    private List<MorphiumServer> servers = new ArrayList<>();

    @AfterEach
    void cleanup() {
        log.info("Cleaning up {} servers", servers.size());
        for (MorphiumServer server : servers) {
            try {
                server.shutdown();
            } catch (Exception e) {
                log.debug("Error shutting down server: {}", e.getMessage());
            }
        }
        servers.clear();
    }

    @Test
    void testSingleNodeElection() throws Exception {
        log.info("Testing single node election");

        // Single node should become leader automatically
        List<String> hosts = List.of("localhost:27100");

        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(100)
                .setElectionTimeoutMaxMs(200);

        MorphiumServer server = new MorphiumServer(27100, "localhost", 100, 60);
        server.configureReplicaSet("rs0", hosts, null, true, config);
        servers.add(server);

        server.start();

        // Wait for election
        Thread.sleep(500);

        assertTrue(server.isPrimary(), "Single node should be primary");
        assertEquals("localhost:27100", server.getPrimaryHost());

        ElectionManager em = server.getElectionManager();
        assertNotNull(em);
        assertEquals(ElectionState.LEADER, em.getState());
    }

    @Test
    void testThreeNodeElection() throws Exception {
        log.info("Testing three node election");

        List<String> hosts = List.of("localhost:27100", "localhost:27101", "localhost:27102");

        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(150)
                .setElectionTimeoutMaxMs(300)
                .setHeartbeatIntervalMs(50);

        // Create and start servers
        for (int i = 0; i < 3; i++) {
            int port = 27100 + i;
            MorphiumServer server = new MorphiumServer(port, "localhost", 100, 60);
            server.configureReplicaSet("rs0", hosts, null, true, config);
            servers.add(server);
        }

        // Start all servers
        for (MorphiumServer server : servers) {
            server.start();
        }

        // Wait for election to complete
        Thread.sleep(2000);

        // Count leaders and followers
        int leaderCount = 0;
        int followerCount = 0;
        String leaderAddress = null;

        for (MorphiumServer server : servers) {
            ElectionManager em = server.getElectionManager();
            if (em != null) {
                if (em.getState() == ElectionState.LEADER) {
                    leaderCount++;
                    leaderAddress = server.getHost() + ":" + server.getPort();
                    log.info("Leader: {}:{}", server.getHost(), server.getPort());
                } else if (em.getState() == ElectionState.FOLLOWER) {
                    followerCount++;
                    log.info("Follower: {}:{} (leader: {})", server.getHost(), server.getPort(), em.getCurrentLeader());
                }
            }
        }

        assertEquals(1, leaderCount, "Should have exactly one leader");
        assertEquals(2, followerCount, "Should have two followers");
        assertNotNull(leaderAddress);

        // Verify all servers agree on who the leader is
        for (MorphiumServer server : servers) {
            ElectionManager em = server.getElectionManager();
            if (em != null && em.getState() == ElectionState.FOLLOWER) {
                assertEquals(leaderAddress, em.getCurrentLeader(),
                        "All followers should agree on leader");
            }
        }

        // Verify primary flags are consistent with election state
        for (MorphiumServer server : servers) {
            ElectionManager em = server.getElectionManager();
            if (em != null) {
                boolean isLeader = em.getState() == ElectionState.LEADER;
                assertEquals(isLeader, server.isPrimary(),
                        "Primary flag should match election state");
            }
        }
    }

    @Test
    void testLeaderFailover() throws Exception {
        log.info("Testing leader failover");

        List<String> hosts = List.of("localhost:27100", "localhost:27101", "localhost:27102");

        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(150)
                .setElectionTimeoutMaxMs(300)
                .setHeartbeatIntervalMs(50);

        // Create and start servers
        for (int i = 0; i < 3; i++) {
            int port = 27100 + i;
            MorphiumServer server = new MorphiumServer(port, "localhost", 100, 60);
            server.configureReplicaSet("rs0", hosts, null, true, config);
            servers.add(server);
        }

        for (MorphiumServer server : servers) {
            server.start();
        }

        // Wait for initial election
        Thread.sleep(2000);

        // Find the current leader
        MorphiumServer leader = null;
        for (MorphiumServer server : servers) {
            ElectionManager em = server.getElectionManager();
            if (em != null && em.getState() == ElectionState.LEADER) {
                leader = server;
                break;
            }
        }
        assertNotNull(leader, "Should have a leader");
        String originalLeaderAddress = leader.getHost() + ":" + leader.getPort();
        log.info("Original leader: {}", originalLeaderAddress);

        // Stop the leader
        log.info("Stopping leader...");
        leader.shutdown();
        servers.remove(leader);

        // Wait for new election
        Thread.sleep(2000);

        // Count leaders among remaining servers
        int newLeaderCount = 0;
        String newLeaderAddress = null;
        for (MorphiumServer server : servers) {
            ElectionManager em = server.getElectionManager();
            if (em != null && em.getState() == ElectionState.LEADER) {
                newLeaderCount++;
                newLeaderAddress = server.getHost() + ":" + server.getPort();
                log.info("New leader: {}", newLeaderAddress);
            }
        }

        assertEquals(1, newLeaderCount, "Should have exactly one new leader");
        assertNotEquals(originalLeaderAddress, newLeaderAddress, "New leader should be different");
    }

    @Test
    void testNoElectionWithoutQuorum() throws Exception {
        log.info("Testing no election without quorum");

        List<String> hosts = List.of("localhost:27100", "localhost:27101", "localhost:27102");

        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(100)
                .setElectionTimeoutMaxMs(200)
                .setHeartbeatIntervalMs(50);

        // Start only one server (can't get majority of 3)
        MorphiumServer server = new MorphiumServer(27100, "localhost", 100, 60);
        server.configureReplicaSet("rs0", hosts, null, true, config);
        servers.add(server);

        server.start();

        // Wait for several election attempts
        Thread.sleep(1500);

        // Should still be candidate or follower - can't become leader without majority
        ElectionManager em = server.getElectionManager();
        assertNotNull(em);

        // Without other nodes responding, it will keep trying to elect itself
        // but won't succeed as it can't get majority
        // It may be CANDIDATE (trying to elect) or FOLLOWER (after timeout)
        log.info("Single node in 3-node cluster state: {}", em.getState());

        // The node should not be primary since it can't win election
        // Note: It keeps trying elections but can't get majority votes
        assertFalse(server.isPrimary(), "Single node in 3-node cluster should not become primary");
    }
}
