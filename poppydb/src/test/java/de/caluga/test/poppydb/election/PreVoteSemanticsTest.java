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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the PreVote (#306, Raft §4.2.3/§9.6) and leader-stickiness semantics in
 * {@link ElectionManager#handleVoteRequest}:
 *
 * <ul>
 *   <li>a PreVote request is answered strictly read-only - no term adoption, no votedFor, no
 *       election timer reset on the responder, whatever the answer;</li>
 *   <li>the PreVote answer applies the same checks a real vote would face (term recency,
 *       log recency, priority) plus leader stickiness;</li>
 *   <li>a REAL vote request carrying a higher term is ignored (denied WITHOUT adopting the
 *       term) while a healthy leader is in contact - the second half of the disruptive-server
 *       fix, protecting new nodes from old (pre-PreVote) campaigners during rolling
 *       upgrades.</li>
 * </ul>
 */
public class PreVoteSemanticsTest {

    private static final Logger log = LoggerFactory.getLogger(PreVoteSemanticsTest.class);

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

    /** Follower with long timeouts (never campaigns on its own during a test). */
    private ElectionManager follower(String address) {
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(60_000)
                .setElectionTimeoutMaxMs(60_000);
        ElectionManager manager = new ElectionManager(address,
                List.of(address, "peer-a:27017", "peer-b:27017"), config);
        managers.add(manager);
        manager.start();
        return manager;
    }

    @Test
    void preVoteIsAnsweredReadOnly() throws Exception {
        ElectionManager voter = follower("readonly-voter:27017");
        long termBefore = voter.getCurrentTerm();

        // A granted PreVote must not consume the voter's real vote or touch its term
        VoteResponse granted = voter.handleVoteRequest(
                new VoteRequest(termBefore, "peer-a:27017", 0, 0).setPreVote(true));
        assertTrue(granted.isVoteGranted(), "equal-term, equal-log PreVote should be pre-granted");
        assertEquals(termBefore, voter.getCurrentTerm(), "PreVote must never change the responder's term");

        // The real vote for a DIFFERENT candidate in the same term must still be grantable:
        // if the PreVote above had set votedFor, this would be denied.
        VoteResponse realVote = voter.handleVoteRequest(
                new VoteRequest(termBefore, "peer-b:27017", 0, 0));
        assertTrue(realVote.isVoteGranted(),
                "a pre-granted PreVote must not consume the voter's real vote (votedFor must stay null)");
    }

    @Test
    void preVoteFromEmptyCandidateIsDeniedByDataHolder() throws Exception {
        ElectionManager voter = follower("data-voter:27017");
        voter.updateLogIndex(422831, 1);

        VoteResponse response = voter.handleVoteRequest(
                new VoteRequest(voter.getCurrentTerm(), "empty-candidate:27017", 0, 0).setPreVote(true));

        assertFalse(response.isVoteGranted(),
                "an empty candidate must fail PreVote against a data-holding voter - that is the livelock fix");
        assertEquals(0, voter.getCurrentTerm(), "the denial must not have touched the voter's term");
    }

    @Test
    void preVoteWithStaleTermIsDeniedAndReportsCurrentTerm() throws Exception {
        ElectionManager voter = follower("term-voter:27017");
        // Raise the voter's term via a real (non-PreVote) request it denies on log grounds
        voter.updateLogIndex(100, 1);
        voter.handleVoteRequest(new VoteRequest(7, "empty:27017", 0, 0));
        assertEquals(7, voter.getCurrentTerm());

        VoteResponse response = voter.handleVoteRequest(
                new VoteRequest(3, "behind:27017", 100, 1).setPreVote(true));

        assertFalse(response.isVoteGranted(), "a candidate whose term is behind must not pass PreVote");
        assertEquals(7, response.getTerm(), "the response must report the voter's term so the candidate can catch up");
    }

    @Test
    void leaderDeniesPreVote() throws Exception {
        // Single-node manager that auto-elects itself leader
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(50)
                .setElectionTimeoutMaxMs(100);
        ElectionManager leader = new ElectionManager("leader:27017", List.of("leader:27017"), config);
        managers.add(leader);

        CountDownLatch leaderLatch = new CountDownLatch(1);
        leader.setOnLeadershipChange(isLeader -> {
            if (isLeader) {
                leaderLatch.countDown();
            }
        });
        leader.start();
        assertTrue(leaderLatch.await(2, TimeUnit.SECONDS));

        long termBefore = leader.getCurrentTerm();
        VoteResponse response = leader.handleVoteRequest(
                new VoteRequest(termBefore, "challenger:27017", 999, 1).setPreVote(true));

        assertFalse(response.isVoteGranted(), "a healthy leader must deny PreVotes - it IS the working leader");
        assertEquals(termBefore, leader.getCurrentTerm());
        assertEquals(ElectionState.LEADER, leader.getState(), "the PreVote must not have disturbed the leader");
    }

    @Test
    void stickinessDeniesPreVoteWhileLeaderIsInContact() throws Exception {
        ElectionManager voter = follower("sticky-voter:27017");

        // A heartbeat from the current leader has just arrived
        voter.handleAppendEntries(AppendEntriesRequest.heartbeat(1, "leader:27017", 0, 0, 0));

        VoteResponse response = voter.handleVoteRequest(
                new VoteRequest(voter.getCurrentTerm(), "challenger:27017", 0, 0).setPreVote(true));

        assertFalse(response.isVoteGranted(),
                "PreVote must be denied while a heartbeat was heard within the last election-timeout window");
    }

    @Test
    void stickinessIgnoresHigherTermRealVoteWithoutAdoptingIt() throws Exception {
        // Rolling-upgrade protection: an OLD (pre-PreVote) empty node campaigns for real with
        // an inflated term. A new-code follower in live contact with a healthy leader must
        // deny WITHOUT adopting the term - otherwise its next heartbeat response would carry
        // the inflated term and dethrone the leader (the exact #306 cascade).
        ElectionManager voter = follower("lease-voter:27017");
        voter.handleAppendEntries(AppendEntriesRequest.heartbeat(5, "leader:27017", 400, 1, 400));
        assertEquals(5, voter.getCurrentTerm());

        VoteResponse response = voter.handleVoteRequest(
                new VoteRequest(17, "old-disruptor:27017", 0, 0));

        assertFalse(response.isVoteGranted(), "the disruptive vote request must be denied");
        assertEquals(5, voter.getCurrentTerm(),
                "the inflated term must NOT be adopted while the leader is in live contact");
        assertEquals(5, response.getTerm());
    }

    @Test
    void higherTermRealVoteIsHonoredOnceLeaderContactExpired() throws Exception {
        // The stickiness window is the MINIMUM election timeout: once the leader has been
        // silent longer than that, normal Raft behavior (term adoption, vote by log/priority
        // rules) must resume, or a legitimate failover could never elect anyone.
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(100)   // short stickiness window for the test...
                .setElectionTimeoutMaxMs(60_000);  // ...but never campaigns itself
        ElectionManager voter = new ElectionManager("failover-voter:27017",
                List.of("failover-voter:27017", "leader:27017", "candidate:27017"), config);
        managers.add(voter);
        voter.start();

        voter.handleAppendEntries(AppendEntriesRequest.heartbeat(5, "leader:27017", 0, 0, 0));
        Thread.sleep(250);  // leader silent for > electionTimeoutMinMs

        VoteResponse response = voter.handleVoteRequest(
                new VoteRequest(6, "candidate:27017", 0, 0));

        assertTrue(response.isVoteGranted(),
                "after the stickiness window expired a legitimate higher-term candidate must get the vote");
        assertEquals(6, voter.getCurrentTerm());
    }

    @Test
    void preVoteRoundAcceptsPlainGrantsAsOldNodesSendThem() throws Exception {
        // Rolling-upgrade fallback, candidate side: an OLD node answering the PreVote probe
        // treats it as an ordinary same-term real vote request and replies with a plain
        // VoteResponse (no PreVote awareness). Those grants MUST count toward the PreVote
        // majority, or a new node in an old cluster could never campaign at all.
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(100)
                .setElectionTimeoutMaxMs(150)
                .setElectionPriority(100);
        ElectionManager candidate = new ElectionManager("new-node:27017",
                List.of("new-node:27017", "old-node-a:27017", "old-node-b:27017"), config);
        managers.add(candidate);

        CountDownLatch leaderLatch = new CountDownLatch(1);
        candidate.setOnLeadershipChange(isLeader -> {
            if (isLeader) {
                leaderLatch.countDown();
            }
        });
        // Old-node simulation: grant everything with a response that carries no PreVote
        // context - exactly what the old binary's handleVoteRequest returns.
        candidate.setSendVoteRequest((peer, request) -> candidate.handleVoteResponse(peer,
                new VoteResponse(request.getTerm(), true, peer)));
        candidate.start();

        assertTrue(leaderLatch.await(3, TimeUnit.SECONDS),
                "plain grants from PreVote-unaware peers must satisfy the PreVote round and lead to leadership");
        assertEquals(ElectionState.LEADER, candidate.getState());
    }
}
