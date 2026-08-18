package de.caluga.test.poppydb.election;

import de.caluga.poppydb.election.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * D1: the election log-recency check ({@link ElectionManager#handleVoteRequest}'s
 * isLogAtLeastAsUpToDate comparison) must be fed by the real replication sequence instead of
 * staying vacuously 0/0 on every node - see the bug this closes: a freshly restarted, empty
 * node winning an election against nodes still holding data because {@code lastLogIndex} was
 * never updated by any production caller.
 *
 * <p>The deny-case tests deliberately set up the voter's data via the same production
 * mechanism (leader-side {@code localSequenceSupplier} synced while heartbeating) rather than
 * poking {@link ElectionManager#updateLogIndex} directly - that method already worked correctly
 * before this fix (see {@code ElectionManagerTest#testVoteRequestLogComparison}); the bug was
 * that nothing production ever called it.
 *
 * <p>{@link #deniesVoteFromEmptyCandidateWithHigherStaleTermThanVoter} covers a second-round
 * review finding: {@code isLogAtLeastAsUpToDate} must NOT fall back to comparing {@code
 * lastLogTerm} when indices differ, because that term is only a {@code currentTerm} stand-in
 * fed independently on each node - a once-elected, now-empty candidate can carry a higher stale
 * term than a data-holding voter, and a term-first comparison would wrongly grant it the vote.
 *
 * <p>{@link #updateLogIndexNeverLowersTheIndex} covers a third-round review finding:
 * {@link ElectionManager#updateLogIndex} must use max (monotonic) semantics. Without it, a
 * freshly-synced-then-silent node's seeded index (see {@code ReplicationManager}'s
 * initial-sync-completion call site, and the dedicated integration test {@code
 * InitialSyncElectionSeedTest}) would be silently regressed back to {@code 0} by the very next
 * leader-side heartbeat tick (which reads the LOCAL driver's change-stream sequence - unrelated
 * to, and possibly lower than, the synced position - see updateLogIndex's javadoc).
 */
public class ElectionLogRecencyTest {

    private static final Logger log = LoggerFactory.getLogger(ElectionLogRecencyTest.class);

    private final List<ElectionManager> managers = new ArrayList<>();

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

    /** Poll-based wait (no sleep+assert) matching the pattern used across the election suite. */
    private static void awaitCondition(String description, long timeoutMs, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean(), "Timed out waiting for: " + description);
    }

    /**
     * Single-node cluster that auto-elects itself leader, with its localSequenceSupplier wired
     * to a fixed "real replication sequence" - exactly the supplier PoppyDB wires to
     * {@code driver::getChangeStreamSequence} in production. Waits for that sequence to actually
     * show up in {@link ElectionManager#getLastLogIndex()} through whatever production feeds it.
     */
    private ElectionManager singleNodeLeaderWithSequence(String address, long sequence) throws InterruptedException {
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(50)
                .setElectionTimeoutMaxMs(100);
        ElectionManager manager = new ElectionManager(address, List.of(address), config);
        managers.add(manager);
        manager.setLocalSequenceSupplier(() -> sequence);

        CountDownLatch leaderLatch = new CountDownLatch(1);
        manager.setOnLeadershipChange(isLeader -> {
            if (isLeader) {
                leaderLatch.countDown();
            }
        });
        manager.start();
        assertTrue(leaderLatch.await(2, TimeUnit.SECONDS), address + " should have become leader (single node)");

        awaitCondition(address + " lastLogIndex synced to real sequence " + sequence, 2000,
                () -> manager.getLastLogIndex() == sequence);
        return manager;
    }

    @Test
    void deniesVoteFromEmptyCandidateWhenVoterHoldsData() throws Exception {
        ElectionManager voter = singleNodeLeaderWithSequence("voter-with-data:27017", 500);

        // An empty (freshly restarted) candidate: log index/term both 0, but a higher election
        // term than the voter - this is exactly how the real bug won: the empty node out-races
        // the data-holding node's term through repeated candidacy retries, forcing the voter to
        // adopt the higher term before the log check runs.
        VoteRequest emptyCandidateRequest = new VoteRequest(
                voter.getCurrentTerm() + 1, "empty-candidate:27017", 0, 0);
        VoteResponse response = voter.handleVoteRequest(emptyCandidateRequest);

        assertFalse(response.isVoteGranted(),
                "must deny vote to an empty candidate (log behind) when the voter holds real replicated data");
    }

    @Test
    void deniesVoteFromEmptyCandidateWithHigherStaleTermThanVoter() throws Exception {
        ElectionManager voter = singleNodeLeaderWithSequence("voter-with-data:27020", 500);

        // Adversarial case from review: an empty candidate (index 0) whose lastLogTerm happens
        // to be HIGHER than the voter's currentTerm - e.g. it was elected once before while
        // still empty, or simply raced its own currentTerm up through repeated candidacy
        // retries. A term-first Raft-style comparison would grant this vote (candidateLastTerm >
        // myLastTerm), reopening the exact empty-node-wipe bug. Must still be denied on index
        // alone.
        VoteRequest staleHighTermEmptyCandidateRequest = new VoteRequest(
                voter.getCurrentTerm() + 1, "empty-candidate-high-term:27020", 0, voter.getCurrentTerm() + 100);
        VoteResponse response = voter.handleVoteRequest(staleHighTermEmptyCandidateRequest);

        assertFalse(response.isVoteGranted(),
                "must deny an empty candidate even when its (stand-in) lastLogTerm is higher than the voter's");
    }

    @Test
    void grantsVoteFromCaughtUpCandidateEvenWhenVoterIsEmpty() throws Exception {
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(1000)
                .setElectionTimeoutMaxMs(2000);
        ElectionManager voter = new ElectionManager("empty-voter:27018", List.of("empty-voter:27018", "candidate:27018"), config);
        managers.add(voter);
        voter.start();

        VoteRequest caughtUpCandidateRequest = new VoteRequest(
                voter.getCurrentTerm() + 1, "candidate:27018", 500, 0);
        VoteResponse response = voter.handleVoteRequest(caughtUpCandidateRequest);

        assertTrue(response.isVoteGranted(),
                "must grant vote to a candidate that is at least as up to date, even from an empty voter");
    }

    @Test
    void voterSeededViaUpdateLogIndexDeniesEmptyCandidate() throws Exception {
        // Direct-call variant (as opposed to deniesVoteFromEmptyCandidateWhenVoterHoldsData's
        // production-wiring variant): confirms the seed-then-deny path also works when fed the
        // way ReplicationManager's initial-sync-completion call site feeds it - a single
        // updateLogIndex() call with no further live events.
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(60_000)
                .setElectionTimeoutMaxMs(60_000);
        ElectionManager voter = new ElectionManager("seeded-voter:27021", List.of("seeded-voter:27021", "candidate:27021"), config);
        managers.add(voter);
        voter.updateLogIndex(500, 0);
        voter.start();

        VoteRequest emptyCandidateRequest = new VoteRequest(
                voter.getCurrentTerm() + 1, "empty-candidate:27021", 0, 0);
        VoteResponse response = voter.handleVoteRequest(emptyCandidateRequest);

        assertFalse(response.isVoteGranted(),
                "a voter seeded via updateLogIndex (e.g. after an initial sync) must deny an empty candidate");
    }

    @Test
    void updateLogIndexNeverLowersTheIndex() throws Exception {
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(60_000)
                .setElectionTimeoutMaxMs(60_000);
        ElectionManager manager = new ElectionManager("monotonic:27022", List.of("monotonic:27022"), config);
        managers.add(manager);

        manager.updateLogIndex(500, 3);
        assertEquals(500, manager.getLastLogIndex(), "index must be set on the first call");

        // Simulates the leader-side heartbeat feed reading the local driver's change-stream
        // sequence (0, since initial sync runs under suppressChangeStreamEvents) right after the
        // seed above - must not regress the already-known, higher index.
        manager.updateLogIndex(0, 3);
        assertEquals(500, manager.getLastLogIndex(),
                "a lower index must never overwrite a higher one already recorded");

        // A genuinely higher index must still win.
        manager.updateLogIndex(600, 3);
        assertEquals(600, manager.getLastLogIndex(), "a higher index must still advance the value");
    }

    @Test
    void emptyNodeWithDataBearingPeerDelaysCandidacyUntilSyncedOrPeerNeverSeen() throws Exception {
        // D3: candidacy restraint. Short timeouts so several election-timeout cycles fit into
        // the sleep window below - if the guard did not hold, at least one of them would flip
        // this node to CANDIDATE.
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(50)
                .setElectionTimeoutMaxMs(100);
        ElectionManager restrained = new ElectionManager("restrained:27023",
                List.of("restrained:27023", "data-peer:27023"), config);
        managers.add(restrained);
        // Since PreVote (#306), candidacy additionally requires a pre-granted majority before
        // the CANDIDATE transition. This test is about the D3 candidacy-restraint guard (which
        // runs BEFORE any PreVote probe is sent), so wire an always-granting peer: the guard's
        // hold-back below is then provably the guard's doing, and once the guard lifts, the
        // PreVote round passes immediately and candidacy proceeds.
        restrained.setSendVoteRequest((peer, request) -> restrained.handleVoteResponse(peer, request,
                new VoteResponse(request.getTerm(), true, peer)));
        restrained.start();

        // Simulate this node observing AppendEntries traffic from a leader that reports a real,
        // non-zero log index - exactly what handleAppendEntries sees in production heartbeats.
        AppendEntriesRequest fromDataPeer = AppendEntriesRequest.heartbeat(
                restrained.getCurrentTerm(), "data-peer:27023", 500, 0, 500);
        restrained.handleAppendEntries(fromDataPeer);

        // Own index is still 0 (nothing applied/produced this process lifetime). Despite
        // repeated election timeouts, this node must never transition to CANDIDATE while a
        // data-bearing peer is known - it can only lose that election and would just inflate
        // the term, forcing the legitimate leader into a pointless step-down.
        Thread.sleep(600);
        assertEquals(ElectionState.FOLLOWER, restrained.getState(),
                "empty node must hold back candidacy while a data-bearing peer is known, not race to CANDIDATE");

        // Once its own index catches up (sync completed - Task 1's seed makes this prompt), the
        // guard must no longer apply and candidacy becomes eligible again on the very next
        // timeout. With the always-granting peer above, the node passes both the PreVote round
        // and the real election, so CANDIDATE is a transient state on the way to LEADER - poll
        // for "left FOLLOWER" instead of racing the CANDIDATE->LEADER transition.
        restrained.updateLogIndex(10, restrained.getCurrentTerm());
        awaitCondition("restrained starts an election (leaves FOLLOWER) once its own index is no longer 0", 1000,
                () -> restrained.getState() != ElectionState.FOLLOWER);
    }

    @Test
    void coldStartGrantsVoteWhenBothSidesAreEmpty() throws Exception {
        // Cold-start invariant: three freshly started nodes, all at log index 0, must still be
        // able to elect a leader - equal (0 == 0) indices must GRANT, not deadlock forever.
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(1000)
                .setElectionTimeoutMaxMs(2000);
        ElectionManager voter = new ElectionManager("cold-voter:27019", List.of("cold-voter:27019", "cold-candidate:27019"), config);
        managers.add(voter);
        voter.start();

        VoteRequest emptyCandidateRequest = new VoteRequest(
                voter.getCurrentTerm() + 1, "cold-candidate:27019", 0, 0);
        VoteResponse response = voter.handleVoteRequest(emptyCandidateRequest);

        assertTrue(response.isVoteGranted(),
                "three empty nodes at cold start must still be able to elect a leader");
    }
}
