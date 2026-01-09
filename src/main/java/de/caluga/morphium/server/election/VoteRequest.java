package de.caluga.morphium.server.election;

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
                '}';
    }
}
