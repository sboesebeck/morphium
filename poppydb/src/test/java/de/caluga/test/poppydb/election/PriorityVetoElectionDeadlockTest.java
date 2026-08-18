package de.caluga.test.poppydb.election;

import de.caluga.poppydb.election.*;
import de.caluga.test.mongo.suite.base.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for issue #312: the priority veto (a voter that could lead itself denies
 * every lower-priority candidate) combined with the log-recency veto can leave a replica set
 * with NO electable node at all - indefinitely, because both vetoes are stable properties,
 * not races.
 *
 * <p>The ACC incident shape (2026-08-18, priorities msg1=100, msg2=50, msg3=25): a restore
 * does not advance the change-stream sequence, so the two restored nodes came back with
 * lastLogIndex 0 and held back their own candidacy, while the one node that had kept running
 * stood at index 1174838 - and was denied by both restored nodes on priority alone. Every
 * candidate vetoed by someone, no leader for ~5 minutes, resolved only by restarting all
 * three nodes at once.
 *
 * <p>Fix under test: the priority preference is bounded in time - it may DELAY an election
 * (the higher-priority node still wins whenever it can win), but after the cluster has been
 * leaderless longer than the bound, voters stop denying on priority alone and the only
 * electable node gets through.
 *
 * <p>Same in-memory transport harness as {@link PreVoteLivelockTest} - node addresses are
 * plain labels, no sockets are bound, so there is nothing port-related to collide on.
 */
public class PriorityVetoElectionDeadlockTest {

    private static final Logger log = LoggerFactory.getLogger(PriorityVetoElectionDeadlockTest.class);

    private static final String N1 = "node1:17017";
    private static final String N2 = "node2:17017";
    private static final String N3 = "node3:17017";
    private static final List<String> HOSTS = List.of(N1, N2, N3);

    private final Map<String, ElectionManager> nodes = new ConcurrentHashMap<>();
    private final ExecutorService network = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "priority-deadlock-test-net");
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

    /**
     * Full-mesh in-memory wiring, asynchronous like a real network (synchronous cross-manager
     * calls could deadlock on the two state locks). Everything is always delivered - the #312
     * deadlock needs no partition, it is stable under perfect connectivity.
     */
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

    private ElectionConfig cfg(int minTimeout, int maxTimeout, int priority) {
        return new ElectionConfig()
                .setElectionTimeoutMinMs(minTimeout)
                .setElectionTimeoutMaxMs(maxTimeout)
                .setHeartbeatIntervalMs(50)
                .setElectionPriority(priority)
                .setPriorityTakeoverEnabled(false);
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
     * The #312 deadlock, constructed directly: n3 holds real data (the incident's index
     * 1174838) but has the LOWEST priority; n1 and n2 are empty after a restore (index 0,
     * dataComplete=false - a restore does not advance the change-stream sequence) with the
     * higher priorities. n1/n2 hold back their own candidacy (partial-restore guard) but still
     * vote; n3 is the only node that can pass everyone's log-recency check - and before the
     * fix, n1 and n2 both denied it on priority alone, forever. Every candidate vetoed by
     * someone, no leader, no timeout dissolves it.
     *
     * <p>The assertion is the whole point of variant A: a leader (necessarily n3) MUST be
     * elected within a bounded time.
     */
    @Test
    void priorityVetoMustNotDeadlockElectionForever() throws Exception {
        // Same timeout config on all nodes - the deadlock is not a timing artifact.
        ElectionManager n1 = new ElectionManager(N1, HOSTS, cfg(300, 600, 100));
        ElectionManager n2 = new ElectionManager(N2, HOSTS, cfg(300, 600, 50));
        ElectionManager n3 = new ElectionManager(N3, HOSTS, cfg(300, 600, 25));

        // n1/n2: restored - data present on disk but sequence reset to 0, marked incomplete
        // until an authoritative sync. They vote, but must not campaign (existing guard).
        n1.setDataComplete(false);
        n2.setDataComplete(false);

        // n3 kept running through the incident and holds the only real log position.
        n3.updateLogIndex(1174838, 1);

        nodes.put(N1, n1);
        nodes.put(N2, n2);
        nodes.put(N3, n3);
        wire(n1);
        wire(n2);
        wire(n3);

        n1.start();
        n2.start();
        n3.start();

        // Bounded time: the fix lifts the priority hold after a few election timeouts
        // (multiples of electionTimeoutMaxMs=600ms here), so 20s is generous - while the
        // unfixed code never elects anyone, no matter how long we wait.
        TestUtils.waitForConditionToBecomeTrue(20000,
                "deadlock not resolved: no leader elected although n3 is electable (#312)",
                () -> currentLeaderAddress() != null);

        String leader = currentLeaderAddress();
        assertEquals(N3, leader,
                "only the data-bearing node may win this election (n1/n2 are empty restores)");

        // And the whole set must converge on it.
        TestUtils.waitForConditionToBecomeTrue(10000,
                "followers did not converge on the elected leader",
                () -> N3.equals(n1.getCurrentLeader()) && N3.equals(n2.getCurrentLeader())
                        && n1.getState() == ElectionState.FOLLOWER
                        && n2.getState() == ElectionState.FOLLOWER
                        && n3.getState() == ElectionState.LEADER);

        log.info("Deadlock resolved: {} elected at term {}", leader, n3.getCurrentTerm());
    }

    /**
     * Guard for the property the priority veto exists for: bounding the veto must NOT make a
     * healthy cluster elect a low-priority node. Adversarial timing on purpose - the
     * low-priority node gets a much faster election timer than the preferred node, so it
     * campaigns first and repeatedly. All logs are equal, so only the priority preference
     * stands between it and the leadership. The bound must be wide enough that the
     * high-priority node's own (slower) timer still fires and wins first.
     */
    @Test
    void healthyClusterStillElectsHighestPriorityNode() throws Exception {
        // n3: lowest priority, aggressively fast timer (fires ~4-10x before n1's first timeout)
        ElectionManager n3 = new ElectionManager(N3, HOSTS, cfg(100, 200, 25));
        // n1: the preferred node, deliberately slow timer
        ElectionManager n1 = new ElectionManager(N1, HOSTS, cfg(600, 900, 100));
        // n2: mid priority, timer strictly SLOWER than n1's whole window. With the same 600-900
        // base as n1 the two effective windows overlap ([600,900] vs [750,1050] after the
        // priority delay) and n2 legitimately wins whenever its timer happens to fire first
        // (~12.5%, plus scheduler noise - seen on the loaded testrunner 2026-08-18): n1's veto
        // alone cannot stop n2, because n3 (lower priority than n2) must grant and n2+n3 is
        // already a majority. The priority preference between two ELECTABLE nodes is only
        // timer-based and therefore probabilistic; disjoint windows are what makes "n1 wins
        // first" a real guarantee (up to >1.8s of scheduler delay on n1's timer, instead of 0ms
        // of margin before).
        ElectionManager n2 = new ElectionManager(N2, HOSTS, cfg(3000, 3600, 50));

        // A healthy set: everyone holds the same data.
        n1.updateLogIndex(5000, 1);
        n2.updateLogIndex(5000, 1);
        n3.updateLogIndex(5000, 1);

        // Record which node wins the FIRST election - that is where the preference must show.
        AtomicReference<String> firstLeader = new AtomicReference<>();
        n1.setOnLeadershipChange(isLeader -> {
            if (isLeader) {
                firstLeader.compareAndSet(null, N1);
            }
        });
        n2.setOnLeadershipChange(isLeader -> {
            if (isLeader) {
                firstLeader.compareAndSet(null, N2);
            }
        });
        n3.setOnLeadershipChange(isLeader -> {
            if (isLeader) {
                firstLeader.compareAndSet(null, N3);
            }
        });

        nodes.put(N1, n1);
        nodes.put(N2, n2);
        nodes.put(N3, n3);
        wire(n1);
        wire(n2);
        wire(n3);

        n1.start();
        n2.start();
        n3.start();

        TestUtils.waitForConditionToBecomeTrue(10000,
                "no leader elected in a fully healthy cluster",
                () -> firstLeader.get() != null);

        assertEquals(N1, firstLeader.get(),
                "the highest-priority node must win a healthy cluster's election, even though the "
                        + "low-priority node campaigns first and more often");

        TestUtils.waitForConditionToBecomeTrue(10000,
                "cluster did not converge on the highest-priority leader",
                () -> n1.getState() == ElectionState.LEADER
                        && N1.equals(n2.getCurrentLeader())
                        && N1.equals(n3.getCurrentLeader()));
    }
}
