package de.caluga.test.poppydb.election;

import de.caluga.poppydb.election.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reproduces the #306 "disruptive server" livelock with an in-memory transport (no ports):
 * an empty, freshly (re)started node next to a healthy, data-bearing leader can never WIN an
 * election - the log-recency veto in {@link ElectionManager#handleVoteRequest} works - but
 * before PreVote it kept CAMPAIGNING every election timeout, each campaign bumping the term,
 * and every RequestVote carrying a higher term forced the healthy primary to step down.
 * Production outcome (ACC, 2026-08-17): ~15 terms/min, primary flapping, eventually no
 * writable primary at all.
 *
 * <p>The decisive invariant tested here: across many election timeouts, the empty node must
 * neither inflate the cluster term nor dethrone the healthy leader - even during the startup
 * window in which it has not yet received a single heartbeat from the leader (in the incident
 * that window came from the leader's dead cached connection to the restarted node; here it is
 * simulated by gating heartbeat delivery). The pre-existing candidacy restraint (empty node +
 * known data-bearing peer) cannot cover that window, because the node only learns about
 * data-bearing peers FROM heartbeats - and once it turned CANDIDATE at a higher term it
 * rejected all heartbeats as stale, keeping itself in the campaign loop forever.
 */
public class PreVoteLivelockTest {

    private static final Logger log = LoggerFactory.getLogger(PreVoteLivelockTest.class);

    private static final String N1 = "node1:27017";
    private static final String N2 = "node2:27017";
    private static final String N3 = "node3:27017";
    private static final List<String> HOSTS = List.of(N1, N2, N3);

    private final Map<String, ElectionManager> nodes = new ConcurrentHashMap<>();
    private final ExecutorService network = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "prevote-test-net");
        t.setDaemon(true);
        return t;
    });

    /** Simulates the incident's reconnect window: while false, heartbeats to N3 are dropped. */
    private final AtomicBoolean heartbeatsToN3 = new AtomicBoolean(false);

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

    private ElectionConfig cfg(int minTimeout, int maxTimeout) {
        return new ElectionConfig()
                .setElectionTimeoutMinMs(minTimeout)
                .setElectionTimeoutMaxMs(maxTimeout)
                .setHeartbeatIntervalMs(50)
                .setPriorityTakeoverEnabled(false);
    }

    /**
     * Full-mesh in-memory wiring: vote and heartbeat traffic is delivered asynchronously (as on
     * a real network - synchronous cross-manager calls could deadlock on the two state locks).
     * Vote traffic is ALWAYS delivered; heartbeats to N3 are gated by {@link #heartbeatsToN3},
     * because the disruption travels via the empty node's outgoing RequestVotes while the
     * leader's heartbeats to it are still failing.
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
            if (peer.equals(N3) && !heartbeatsToN3.get()) {
                return;  // reconnect window: restarted node not reachable for heartbeats yet
            }
            ElectionManager target = nodes.get(peer);
            if (target == null || !target.isRunning()) {
                return;
            }
            AppendEntriesResponse response = target.handleAppendEntries(request);
            source.handleAppendEntriesResponse(peer, response);
        }));
    }

    @Test
    void emptyRestartedNodeMustNotDethroneHealthyLeaderNorInflateTerm() throws Exception {
        // n1: healthy leader with data (fast timeouts, wins the initial election deterministically)
        ElectionManager n1 = new ElectionManager(N1, HOSTS, cfg(150, 250));
        // n2: healthy data-bearing follower (slow timeouts so it never races n1)
        ElectionManager n2 = new ElectionManager(N2, HOSTS, cfg(700, 900));
        nodes.put(N1, n1);
        nodes.put(N2, n2);
        wire(n1);
        wire(n2);

        // Both healthy nodes hold real data - the incident's myIndex=422831
        n1.updateLogIndex(422831, 1);
        n2.updateLogIndex(422831, 1);

        CountDownLatch n1Leader = new CountDownLatch(1);
        n1.setOnLeadershipChange(isLeader -> {
            if (isLeader) {
                n1Leader.countDown();
            }
        });

        n1.start();
        n2.start();
        assertTrue(n1Leader.await(5, TimeUnit.SECONDS), "healthy data-bearing node should become leader");

        long termBefore = n1.getCurrentTerm();
        log.info("Healthy leader established: {} at term {}", N1, termBefore);

        // Watchdog samples the leader continuously from BEFORE the empty node starts, so a
        // transient dethrone during the reconnect window cannot escape the assertions below.
        AtomicBoolean watching = new AtomicBoolean(true);
        AtomicLong dethroneObservations = new AtomicLong(0);
        AtomicReference<String> firstDethrone = new AtomicReference<>();
        AtomicLong n3CandidateObservations = new AtomicLong(0);
        Thread watchdog = new Thread(() -> {
            while (watching.get()) {
                if (n1.getState() != ElectionState.LEADER) {
                    dethroneObservations.incrementAndGet();
                    firstDethrone.compareAndSet(null, "n1 state=" + n1.getState()
                            + " n1Term=" + n1.getCurrentTerm());
                }
                ElectionManager n3 = nodes.get(N3);
                if (n3 != null && n3.getState() == ElectionState.CANDIDATE) {
                    n3CandidateObservations.incrementAndGet();
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "leader-watchdog");
        watchdog.start();

        try {
            // n3 restarts EMPTY: log index 0, term 0 (no persisted state), and - crucially - it
            // does not receive a single heartbeat for a while (leader still redialing it).
            ElectionManager n3 = new ElectionManager(N3, HOSTS, cfg(150, 300));
            nodes.put(N3, n3);
            wire(n3);
            n3.start();

            // Reconnect window: several of n3's election timeouts pass without any heartbeat.
            // Before PreVote this is where n3 turned CANDIDATE, bumped the term and dethroned n1.
            Thread.sleep(1500);
            heartbeatsToN3.set(true);
            log.info("Heartbeat delivery to {} restored", N3);

            // Observation phase: many more election timeouts with full connectivity.
            Thread.sleep(3000);

            assertEquals(0, dethroneObservations.get(),
                    "healthy leader must never be dethroned by the empty node's campaigning"
                            + " (first observation: " + firstDethrone.get() + ")");
            assertEquals(termBefore, n1.getCurrentTerm(),
                    "cluster term must not be inflated by an un-electable empty candidate");
            assertTrue(n3.getCurrentTerm() <= termBefore,
                    "empty node must not race its term past the healthy leader's (n3 term="
                            + n3.getCurrentTerm() + ", leader term=" + termBefore + ")");
            assertEquals(0, n3CandidateObservations.get(),
                    "empty node must never reach CANDIDATE state while a data-bearing leader is healthy");
            assertEquals(ElectionState.LEADER, n1.getState(), "healthy leader must still lead at the end");
        } finally {
            watching.set(false);
            watchdog.join(2000);
        }
    }
}
