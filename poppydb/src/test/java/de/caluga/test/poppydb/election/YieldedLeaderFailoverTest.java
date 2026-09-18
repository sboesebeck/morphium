package de.caluga.test.poppydb.election;

import de.caluga.poppydb.election.*;
import de.caluga.test.mongo.suite.base.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A leader that yielded to a higher-priority peer (priority takeover) refuses to seek election
 * for {@code priorityTakeoverStepDownSecs} - the window exists so the successor can win the
 * election the yield triggers. Once the successor HAS won, the window has done its job; if that
 * successor then dies inside the window, the yielded node is still the best candidate and must
 * not sit out the rest of it. Seen as FastResyncTest's 13-18s failovers (testrunner under load,
 * and ~1 in 5 runs locally): node2 (priority 50) won the parallel start, yielded to node1
 * (priority 100) 30s later, node1 was shut down by the test seconds after taking over - and
 * node2 answered every election timeout with "blocked from election (recent stepdown)" while
 * still denying node3 (priority 10) on priority, until the #312 hold expired 12s later.
 *
 * <p>Same in-memory transport harness as {@link PriorityVetoElectionDeadlockTest}.
 */
public class YieldedLeaderFailoverTest {

    private static final String N1 = "node1:17017";
    private static final String N2 = "node2:17017";
    private static final String N3 = "node3:17017";
    private static final List<String> HOSTS = List.of(N1, N2, N3);

    private final Map<String, ElectionManager> nodes = new ConcurrentHashMap<>();
    private final ExecutorService network = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "yielded-leader-test-net");
        t.setDaemon(true);
        return t;
    });

    @AfterEach
    void cleanup() {
        for (ElectionManager manager : nodes.values()) {
            try {
                manager.stop();
            } catch (Exception e) {
                // ignore
            }
        }
        nodes.clear();
        network.shutdownNow();
    }

    private void wire(ElectionManager source) {
        source.setSendVoteRequest((peer, request) -> network.submit(() -> {
            ElectionManager target = nodes.get(peer);
            if (target == null || !target.isRunning()) {
                return;
            }
            VoteResponse response = target.handleVoteRequest(request);
            source.handleVoteResponse(peer, request, response);
        }));
        source.setSendAppendEntries((peer, request) -> network.submit(() -> {
            ElectionManager target = nodes.get(peer);
            if (target == null || !target.isRunning()) {
                return;
            }
            AppendEntriesResponse response = target.handleAppendEntries(request);
            source.handleAppendEntriesResponse(peer, response);
        }));
    }

    private ElectionConfig cfg(int priority) {
        return new ElectionConfig()
                .setElectionTimeoutMinMs(300)
                .setElectionTimeoutMaxMs(600)
                .setHeartbeatIntervalMs(50)
                .setElectionPriority(priority)
                .setPriorityTakeoverEnabled(true)
                .setPriorityTakeoverCheckIntervalMs(100)
                .setPriorityTakeoverMinStabilityMs(200)
                // The production default: long against these timers, exactly like the 10s
                // default is long against FastResyncTest's 2-4s election timeouts.
                .setPriorityTakeoverStepDownSecs(10);
    }

    private String currentLeaderAddress() {
        for (ElectionManager manager : nodes.values()) {
            if (manager.getState() == ElectionState.LEADER) {
                return manager.getMyAddress();
            }
        }
        return null;
    }

    /**
     * The FastResyncTest shape: n2 wins first (n1 not up yet), yields to n1 once n1 joins, n1
     * dies right after taking over. n2 must win the failover promptly - not n3 after the
     * priority hold expires, and not n2 after the full stepdown window.
     */
    @Test
    void yieldedLeaderCampaignsAgainOnceItsSuccessorIsGone() throws Exception {
        ElectionManager n2 = new ElectionManager(N2, HOSTS, cfg(50));
        ElectionManager n3 = new ElectionManager(N3, HOSTS, cfg(10));
        nodes.put(N2, n2);
        nodes.put(N3, n3);
        wire(n2);
        wire(n3);
        n2.start();
        n3.start();
        TestUtils.waitForConditionToBecomeTrue(10000, "n2 must win the initial election while n1 is absent",
                () -> N2.equals(currentLeaderAddress()));

        ElectionManager n1 = new ElectionManager(N1, HOSTS, cfg(100));
        nodes.put(N1, n1);
        wire(n1);
        n1.start();
        TestUtils.waitForConditionToBecomeTrue(10000, "n2 must yield to the higher-priority n1",
                () -> N1.equals(currentLeaderAddress()) && n2.getState() == ElectionState.FOLLOWER);

        // n1 dies inside n2's stepdown window (10s, of which barely a second has passed).
        n1.stop();
        nodes.remove(N1);
        long failoverStart = System.currentTimeMillis();

        TestUtils.waitForConditionToBecomeTrue(4000,
                "the yielded node must take the failover back promptly - not sit out its stepdown window",
                () -> N2.equals(currentLeaderAddress()));
        long took = System.currentTimeMillis() - failoverStart;
        assertTrue(took < 4000, "failover took " + took + "ms");
        assertEquals(N2, currentLeaderAddress(), "the higher-priority survivor must lead, not n3");
    }

    /**
     * A voter that cannot campaign itself has no priority claim to defend: denying a lower-
     * priority candidate "to give ourselves a chance first" only delays the election when
     * there is no such chance (frozen, or inside a stepdown window). Unit-level, no timers.
     */
    @Test
    void voterThatCannotCampaignDoesNotDenyOnPriorityAlone() {
        ElectionManager n2 = new ElectionManager(N2, HOSTS, cfg(50));
        nodes.put(N2, n2);
        n2.freeze(60);
        assertTrue(n2.isFrozen(), "precondition: n2 cannot campaign");

        VoteRequest preVote = new VoteRequest(0, N3, 0, 0, 10).setPreVote(true).setRoundId(1);
        assertTrue(n2.handleVoteRequest(preVote).isVoteGranted(),
                "a frozen voter must pre-grant a lower-priority candidate - it cannot run itself");

        VoteRequest vote = new VoteRequest(1, N3, 0, 0, 10).setRoundId(2);
        assertTrue(n2.handleVoteRequest(vote).isVoteGranted(),
                "a frozen voter must grant the real vote too - consistent with its PreVote answer");
    }

    /**
     * The same rule for the candidacy restraints: a node whose initial sync did not complete
     * (the primary died mid-sync - the shape the loaded run showed) still votes but holds
     * back its own candidacy, so its priority must not count against the candidate either.
     */
    @Test
    void voterWithIncompleteDataDoesNotDenyOnPriorityAlone() {
        ElectionManager n2 = new ElectionManager(N2, HOSTS, cfg(50));
        nodes.put(N2, n2);
        n2.setDataComplete(false);

        VoteRequest preVote = new VoteRequest(0, N3, 0, 0, 10).setPreVote(true).setRoundId(1);
        assertTrue(n2.handleVoteRequest(preVote).isVoteGranted(),
                "a data-incomplete voter cannot campaign - it must not deny on priority");
    }

    /** Guard: an electable higher-priority voter keeps denying on priority (the #312 preference). */
    @Test
    void electableVoterStillDeniesOnPriority() {
        ElectionManager n2 = new ElectionManager(N2, HOSTS, cfg(50));
        nodes.put(N2, n2);
        VoteRequest preVote = new VoteRequest(0, N3, 0, 0, 10).setPreVote(true).setRoundId(1);
        assertFalse(n2.handleVoteRequest(preVote).isVoteGranted(),
                "an electable higher-priority voter denies on priority while the hold lasts");
    }
}
