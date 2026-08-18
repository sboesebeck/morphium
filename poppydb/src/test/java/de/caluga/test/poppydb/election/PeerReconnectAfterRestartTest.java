package de.caluga.test.poppydb.election;

import de.caluga.poppydb.PoppyDB;
import de.caluga.poppydb.election.ElectionConfig;
import de.caluga.poppydb.election.ElectionManager;
import de.caluga.poppydb.election.ElectionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A follower that restarts must be picked up by the running leader again.
 *
 * <p>End-to-end guard for the property, not for the bug: this test passes against the code
 * that produced the 2026-08-18 ACC outage as well. A clean {@code shutdown()} closes the
 * socket, so the leader-side connection fails in a way the driver's own heartbeat still
 * repairs. The incident state specifically required {@code SingleMongoConnectDriver.close()}
 * to have run - which cancels that heartbeat - and only then does the cached driver stay dead
 * forever. That state is covered by {@code ElectionNetworkClientReconnectTest}, which is the
 * actual regression test; this one guards the surrounding behaviour.
 */
public class PeerReconnectAfterRestartTest {

    private static final Logger log = LoggerFactory.getLogger(PeerReconnectAfterRestartTest.class);

    private final List<PoppyDB> servers = new ArrayList<>();

    @AfterEach
    void cleanup() {
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
     * Ports the OS just handed out - fixed ports collide when surefire runs forks in parallel.
     */
    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private ElectionConfig configFor(int priority) {
        return new ElectionConfig()
                .setElectionTimeoutMinMs(150)
                .setElectionTimeoutMaxMs(300)
                .setHeartbeatIntervalMs(50)
                .setPersistState(false)
                .setElectionPriority(priority);
    }

    private PoppyDB startNode(int port, List<String> hosts, int priority) throws Exception {
        PoppyDB server = new PoppyDB(port, "localhost", 100, 60);
        server.configureReplicaSet("rs0", hosts, null, true, configFor(priority));
        servers.add(server);
        server.start();
        return server;
    }

    /**
     * Waits for a condition instead of sleeping a fixed span: the point of the test is whether
     * the leader re-establishes contact AT ALL, so a generous deadline keeps it from turning
     * into a timing test.
     */
    private boolean waitFor(java.util.function.BooleanSupplier condition, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(50);
        }
        return condition.getAsBoolean();
    }

    private static PoppyDB leaderAmong(PoppyDB... nodes) {
        for (PoppyDB node : nodes) {
            if (node.getElectionManager() != null
                    && node.getElectionManager().getState() == ElectionState.LEADER) {
                return node;
            }
        }
        return null;
    }

    @Test
    void restartedFollowerIsHeartbeatedAgain() throws Exception {
        int firstPort = freePort();
        int middlePort = freePort();
        int restartPort = freePort();
        List<String> hosts = List.of("localhost:" + firstPort, "localhost:" + middlePort,
                "localhost:" + restartPort);

        // Descending priorities: the node we restart (lowest) stays a follower as long as the
        // leader keeps running, so rejoining is only possible through the leader's heartbeats -
        // exactly the situation on ACC.
        //
        // Which of the two higher-priority nodes wins the initial election is a race, not a
        // guarantee: the first node spends its blocking start() alone and leaderless, so its
        // priority hold window (#312) has long expired by the time the second node comes up,
        // and either of them may legitimately take term 1. The test only needs SOME stable
        // leader among the two - it must not assume it is the priority-100 node.
        PoppyDB first = startNode(firstPort, hosts, 100);
        PoppyDB middle = startNode(middlePort, hosts, 50);
        PoppyDB restarting = startNode(restartPort, hosts, 25);

        assertTrue(waitFor(() -> leaderAmong(first, middle) != null, 10000),
                "one of the two higher-priority nodes should have become leader");
        PoppyDB leader = leaderAmong(first, middle);
        assertNotNull(leader, "leader disappeared between the wait and the lookup");
        String leaderAddress = "localhost:" + (leader == first ? firstPort : middlePort);

        assertTrue(waitFor(() -> leaderAddress.equals(restarting.getElectionManager().getCurrentLeader()), 10000),
                "follower should know the leader before the restart");

        log.info("Restarting follower localhost:{} while the leader keeps running", restartPort);
        restarting.shutdown();
        servers.remove(restarting);

        PoppyDB restarted = startNode(restartPort, hosts, 25);

        assertTrue(waitFor(() -> leaderAddress.equals(restarted.getElectionManager().getCurrentLeader()), 15000),
                "restarted follower must be reached by the still-running leader again - "
                        + "the leader has to dial a peer that went away, not keep using its dead connection");

        ElectionManager em = restarted.getElectionManager();
        assertEquals(ElectionState.FOLLOWER, em.getState(), "restarted node should settle as follower");
        assertEquals(ElectionState.LEADER, leader.getElectionManager().getState(),
                "the restart must not have cost the leader its leadership");
    }
}
