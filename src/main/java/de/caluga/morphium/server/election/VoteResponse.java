package de.caluga.morphium.server.election;

import java.util.Map;
import java.util.HashMap;

/**
 * Response to a vote request.
 * Based on Raft RequestVote RPC response.
 */
public class VoteResponse {

    /**
     * Current term of the responding node.
     * Used by candidate to update its term if it's stale.
     */
    private long term;

    /**
     * True if the vote was granted to the candidate.
     */
    private boolean voteGranted;

    /**
     * Address of the node that responded (for debugging/logging).
     */
    private String voterId;

    public VoteResponse() {
    }

    public VoteResponse(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public VoteResponse(long term, boolean voteGranted, String voterId) {
        this.term = term;
        this.voteGranted = voteGranted;
        this.voterId = voterId;
    }

    public long getTerm() {
        return term;
    }

    public VoteResponse setTerm(long term) {
        this.term = term;
        return this;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public VoteResponse setVoteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
        return this;
    }

    public String getVoterId() {
        return voterId;
    }

    public VoteResponse setVoterId(String voterId) {
        this.voterId = voterId;
        return this;
    }

    /**
     * Convert to Map for wire protocol transmission.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("ok", 1);
        map.put("term", term);
        map.put("voteGranted", voteGranted);
        if (voterId != null) {
            map.put("voterId", voterId);
        }
        return map;
    }

    /**
     * Parse from Map received over wire protocol.
     */
    public static VoteResponse fromMap(Map<String, Object> map) {
        VoteResponse resp = new VoteResponse();
        resp.setTerm(((Number) map.get("term")).longValue());
        resp.setVoteGranted((Boolean) map.get("voteGranted"));
        if (map.containsKey("voterId")) {
            resp.setVoterId((String) map.get("voterId"));
        }
        return resp;
    }

    @Override
    public String toString() {
        return "VoteResponse{" +
                "term=" + term +
                ", voteGranted=" + voteGranted +
                ", voterId='" + voterId + '\'' +
                '}';
    }
}
