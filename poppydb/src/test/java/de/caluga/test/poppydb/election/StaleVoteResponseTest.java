package de.caluga.test.poppydb.election;

import de.caluga.poppydb.election.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * #306 P1-3: {@link ElectionManager#handleVoteResponse} used to credit EVERY incoming grant to
 * the currently open PreVote round - there was no round correlation, no request-type check, and
 * lower response terms were not discarded. In a three-node set, the self-vote plus ONE grant
 * straggling in from an earlier round is already a "majority": a real election starts, the term
 * jumps, and under network latency the pre-#306 livelock can come back through this side door.
 *
 * <p>The fix stamps every outgoing (Pre)Vote request batch with a sender-local round id
 * ({@link VoteRequest#getRoundId()}, never serialized - the sending side holds the request and
 * hands it back with the response) and discards responses whose round is no longer the current
 * one.
 */
@Tag("poppydb")
public class StaleVoteResponseTest {

    private static final String ME = "correlating:27017";
    private static final String PEER_A = "peer-a:27017";
    private static final String PEER_B = "peer-b:27017";

    private ElectionManager manager;

    @AfterEach
    void cleanup() {
        if (manager != null) {
            try {
                manager.stop();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private ElectionManager node(int timeoutMs) {
        ElectionConfig config = new ElectionConfig()
                .setElectionTimeoutMinMs(timeoutMs)
                .setElectionTimeoutMaxMs(timeoutMs);
        manager = new ElectionManager(ME, List.of(ME, PEER_A, PEER_B), config);
        return manager;
    }

    @Test
    @Timeout(30)
    void lateGrantFromEarlierPreVoteRoundMustNotTriggerRealElection() throws Exception {
        ElectionManager node = node(150);
        List<VoteRequest> sent = new CopyOnWriteArrayList<>();
        // Peers never answer in time - every election timeout opens a fresh PreVote round.
        node.setSendVoteRequest((peer, request) -> sent.add(request));
        node.start();

        // Wait until at least three rounds of probes went out (2 peers per round), so the
        // first round is decidedly OVER when its answer finally "arrives".
        long until = System.currentTimeMillis() + 10_000;
        while (sent.size() < 6 && System.currentTimeMillis() < until) {
            Thread.sleep(20);
        }
        assertTrue(sent.size() >= 6, "test setup: expected at least 3 PreVote rounds of probes");
        assertTrue(sent.get(0).isPreVote(), "test setup: probes must be PreVote requests");

        long termBefore = node.getCurrentTerm();
        VoteRequest firstRoundProbe = sent.get(0);

        // The network finally delivers peer-a's grant - for the FIRST round, several rounds
        // ago. Together with the self-vote this is 2 of 3: a "majority", if the stale grant is
        // credited to the currently open round.
        node.handleVoteResponse(PEER_A, firstRoundProbe,
                new VoteResponse(firstRoundProbe.getTerm(), true, PEER_A));
        Thread.sleep(300);

        assertEquals(termBefore, node.getCurrentTerm(),
                "a grant from an earlier PreVote round must not be credited to the current round - "
                        + "self-vote plus one stale grant would fake a majority and start a real "
                        + "election with its term bump");
        assertTrue(sent.stream().allMatch(VoteRequest::isPreVote),
                "no REAL vote request may have been sent - the stale grant must not complete any round");
        assertNotEquals(ElectionState.CANDIDATE, node.getState(),
                "the node must not have turned CANDIDATE on the strength of a stale grant");
    }

    @Test
    @Timeout(30)
    void responseToARealVoteRequestMustNotCountTowardAPreVoteRound() throws Exception {
        // Type correlation: a straggling response to a REAL vote request (from an election
        // that has since been abandoned) arrives while a PreVote round is open. It must not be
        // credited as a pre-granted vote - the peer answered a different question.
        ElectionManager node = node(150);
        List<VoteRequest> sent = new CopyOnWriteArrayList<>();
        node.setSendVoteRequest((peer, request) -> sent.add(request));
        node.start();

        long until = System.currentTimeMillis() + 10_000;
        while (sent.size() < 2 && System.currentTimeMillis() < until) {
            Thread.sleep(20);
        }
        assertTrue(sent.size() >= 2, "test setup: expected an open PreVote round");

        long termBefore = node.getCurrentTerm();
        // What a dead real election's straggler looks like: a REAL (non-PreVote) request from
        // a round that is long over, answered with a grant at its own term.
        VoteRequest staleRealRequest = new VoteRequest(termBefore, ME, 0, 0)
                .setRoundId(Long.MIN_VALUE);
        node.handleVoteResponse(PEER_A, staleRealRequest,
                new VoteResponse(termBefore, true, PEER_A));
        Thread.sleep(300);

        assertEquals(termBefore, node.getCurrentTerm(),
                "a response to a real vote request must not be credited to a PreVote round");
        assertTrue(sent.stream().allMatch(VoteRequest::isPreVote),
                "the mistyped grant must not have completed the PreVote round (no real election started)");
    }

    @Test
    @Timeout(30)
    void grantsForTheCurrentRoundStillElect() throws Exception {
        // Positive control: correlation must only drop STALE responses - promptly answered
        // probes of the current round still lead to a real election and leadership.
        ElectionManager node = node(150);
        node.setSendVoteRequest((peer, request) -> node.handleVoteResponse(peer, request,
                new VoteResponse(request.getTerm(), true, peer)));
        node.start();

        long until = System.currentTimeMillis() + 5000;
        while (node.getState() != ElectionState.LEADER && System.currentTimeMillis() < until) {
            Thread.sleep(20);
        }
        assertEquals(ElectionState.LEADER, node.getState(),
                "current-round grants must still win elections - correlation must not block normal operation");
    }
}
