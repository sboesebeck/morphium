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
 * <p>The deny-case test deliberately sets up the voter's data via the same production
 * mechanism (leader-side {@code localSequenceSupplier} synced while heartbeating) rather than
 * poking {@link ElectionManager#updateLogIndex} directly - that method already worked correctly
 * before this fix (see {@code ElectionManagerTest#testVoteRequestLogComparison}); the bug was
 * that nothing production ever called it.
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
