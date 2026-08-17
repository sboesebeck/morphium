package de.caluga.poppydb.election;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Request for vote from a candidate to other nodes.
 * Based on Raft RequestVote RPC.
 */
public class VoteRequest {

    /**
     * Candidate's term number.
     */
    private long term;

    /**
     * Candidate's address (host:port) requesting the vote.
     */
    private String candidateId;

    /**
     * Index of candidate's last log entry.
     * Used to determine if candidate's log is at least as up-to-date as voter's.
     */
    private long lastLogIndex;

    /**
     * Term of candidate's last log entry.
     * Used together with lastLogIndex to compare log freshness.
     */
    private long lastLogTerm;

    /**
     * Candidate's election priority (0-100).
     * Higher priority candidates are preferred when logs are equally up-to-date.
     * Similar to MongoDB's replica set member priority.
     */
    private int candidatePriority;

    /**
     * PreVote (Raft §4.2.3 / §9.6, #306): when true, this is NOT a real vote request but the
     * question "would you grant me a vote if I started an election?". The sender has NOT
     * incremented its term ({@code term} carries the sender's CURRENT term, not the term it
     * would campaign at), and the receiver must not change any state (no term adoption, no
     * votedFor, no election timer reset) when answering.
     *
     * <p>Wire/rolling-upgrade compatibility: this rides as an extra field on the existing
     * {@code requestVote} command rather than a new command, and it carries the sender's
     * CURRENT term precisely so that an OLD node which does not know the field treats it as an
     * ordinary same-term vote request - which is non-disruptive (no term bump on the old node)
     * and whose grant/deny answer approximates the PreVote answer (same log-recency, term and
     * priority checks). A grant from an old node therefore counts toward the PreVote majority,
     * so a new node in an old cluster is never blocked by peers that lack PreVote support.
     */
    private boolean preVote;

    /**
     * Sender-local correlation id (#306 P1-3): identifies the (Pre)Vote round this request was
     * sent for, so the sender can discard responses that answer an EARLIER round instead of
     * crediting them to the current one - in a three-node set, one late grant from an old
     * PreVote round plus the self-vote is already a "majority" and triggers a real election
     * with its term bump. Deliberately NOT serialized ({@link #toMap()}): the response travels
     * back on the same code path that sent the request (ElectionNetworkClient holds the
     * request object and hands it to {@code handleVoteResponse} together with the response),
     * so correlation never needs to cross the wire and the protocol stays untouched.
     */
    private long roundId;

    public VoteRequest() {
    }

    public VoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        this(term, candidateId, lastLogIndex, lastLogTerm, ElectionConfig.MAX_PRIORITY / 2);
    }

    public VoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm, int candidatePriority) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
        this.candidatePriority = candidatePriority;
    }

    public long getTerm() {
        return term;
    }

    public VoteRequest setTerm(long term) {
        this.term = term;
        return this;
    }

    public String getCandidateId() {
        return candidateId;
    }

    public VoteRequest setCandidateId(String candidateId) {
        this.candidateId = candidateId;
        return this;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public VoteRequest setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
        return this;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public VoteRequest setLastLogTerm(long lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
        return this;
    }

    public int getCandidatePriority() {
        return candidatePriority;
    }

    public VoteRequest setCandidatePriority(int candidatePriority) {
        this.candidatePriority = candidatePriority;
        return this;
    }

    public boolean isPreVote() {
        return preVote;
    }

    public VoteRequest setPreVote(boolean preVote) {
        this.preVote = preVote;
        return this;
    }

    /** Sender-local round correlation id - see the field's javadoc; never on the wire. */
    public long getRoundId() {
        return roundId;
    }

    public VoteRequest setRoundId(long roundId) {
        this.roundId = roundId;
        return this;
    }

    /**
     * Convert to Map for wire protocol transmission.
     * Uses LinkedHashMap to ensure command name is first key (MongoDB wire protocol requirement).
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("requestVote", 1);  // Must be first for MongoDB wire protocol
        map.put("term", term);
        map.put("candidateId", candidateId);
        map.put("lastLogIndex", lastLogIndex);
        map.put("lastLogTerm", lastLogTerm);
        map.put("candidatePriority", candidatePriority);
        if (preVote) {
            // Only serialized when set - keeps real vote requests byte-identical to what old
            // nodes send/expect; old receivers simply ignore the unknown key (see the field's
            // javadoc for why that fallback is safe).
            map.put("preVote", true);
        }
        return map;
    }

    /**
     * Parse from Map received over wire protocol.
     */
    public static VoteRequest fromMap(Map<String, Object> map) {
        VoteRequest req = new VoteRequest();
        req.setTerm(((Number) map.get("term")).longValue());
        req.setCandidateId((String) map.get("candidateId"));
        req.setLastLogIndex(((Number) map.get("lastLogIndex")).longValue());
        req.setLastLogTerm(((Number) map.get("lastLogTerm")).longValue());
        // Handle backwards compatibility - default to mid priority if not present
        if (map.containsKey("candidatePriority")) {
            req.setCandidatePriority(((Number) map.get("candidatePriority")).intValue());
        } else {
            req.setCandidatePriority(ElectionConfig.MAX_PRIORITY / 2);
        }
        req.setPreVote(Boolean.TRUE.equals(map.get("preVote")));
        return req;
    }

    @Override
    public String toString() {
        return "VoteRequest{" +
                "term=" + term +
                ", candidateId='" + candidateId + '\'' +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                ", candidatePriority=" + candidatePriority +
                ", preVote=" + preVote +
                '}';
    }
}
