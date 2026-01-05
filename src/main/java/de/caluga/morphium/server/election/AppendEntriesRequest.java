package de.caluga.morphium.server.election;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Heartbeat and log replication message from leader to followers.
 * Based on Raft AppendEntries RPC.
 *
 * When entries is empty, this serves as a heartbeat.
 * When entries contains data, it's also replicating log entries.
 */
public class AppendEntriesRequest {

    /**
     * Leader's current term.
     */
    private long term;

    /**
     * Leader's address (host:port) so followers know who the leader is.
     */
    private String leaderId;

    /**
     * Index of log entry immediately preceding the new entries.
     * Used by followers to ensure log consistency.
     */
    private long prevLogIndex;

    /**
     * Term of the prevLogIndex entry.
     */
    private long prevLogTerm;

    /**
     * Log entries to replicate (empty for heartbeat).
     * Each entry is a Map containing the log data.
     */
    private List<Map<String, Object>> entries = new ArrayList<>();

    /**
     * Leader's commit index (highest log entry known to be committed).
     * Followers use this to advance their own commit index.
     */
    private long leaderCommit;

    public AppendEntriesRequest() {
    }

    /**
     * Create a heartbeat (no entries).
     */
    public static AppendEntriesRequest heartbeat(long term, String leaderId,
            long prevLogIndex, long prevLogTerm, long leaderCommit) {
        AppendEntriesRequest req = new AppendEntriesRequest();
        req.setTerm(term);
        req.setLeaderId(leaderId);
        req.setPrevLogIndex(prevLogIndex);
        req.setPrevLogTerm(prevLogTerm);
        req.setLeaderCommit(leaderCommit);
        return req;
    }

    public long getTerm() {
        return term;
    }

    public AppendEntriesRequest setTerm(long term) {
        this.term = term;
        return this;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public AppendEntriesRequest setLeaderId(String leaderId) {
        this.leaderId = leaderId;
        return this;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public AppendEntriesRequest setPrevLogIndex(long prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
        return this;
    }

    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    public AppendEntriesRequest setPrevLogTerm(long prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
        return this;
    }

    public List<Map<String, Object>> getEntries() {
        return entries;
    }

    public AppendEntriesRequest setEntries(List<Map<String, Object>> entries) {
        this.entries = entries != null ? entries : new ArrayList<>();
        return this;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }

    public AppendEntriesRequest setLeaderCommit(long leaderCommit) {
        this.leaderCommit = leaderCommit;
        return this;
    }

    /**
     * Check if this is a heartbeat (no log entries).
     */
    public boolean isHeartbeat() {
        return entries == null || entries.isEmpty();
    }

    /**
     * Convert to Map for wire protocol transmission.
     * Uses LinkedHashMap to ensure command name is first key (MongoDB wire protocol requirement).
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("appendEntries", 1);  // Must be first for MongoDB wire protocol
        map.put("term", term);
        map.put("leaderId", leaderId);
        map.put("prevLogIndex", prevLogIndex);
        map.put("prevLogTerm", prevLogTerm);
        map.put("entries", entries);
        map.put("leaderCommit", leaderCommit);
        return map;
    }

    /**
     * Parse from Map received over wire protocol.
     */
    @SuppressWarnings("unchecked")
    public static AppendEntriesRequest fromMap(Map<String, Object> map) {
        AppendEntriesRequest req = new AppendEntriesRequest();
        req.setTerm(((Number) map.get("term")).longValue());
        req.setLeaderId((String) map.get("leaderId"));
        req.setPrevLogIndex(((Number) map.get("prevLogIndex")).longValue());
        req.setPrevLogTerm(((Number) map.get("prevLogTerm")).longValue());
        req.setLeaderCommit(((Number) map.get("leaderCommit")).longValue());

        Object entriesObj = map.get("entries");
        if (entriesObj instanceof List) {
            req.setEntries((List<Map<String, Object>>) entriesObj);
        }

        return req;
    }

    @Override
    public String toString() {
        return "AppendEntriesRequest{" +
                "term=" + term +
                ", leaderId='" + leaderId + '\'' +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", entriesCount=" + (entries != null ? entries.size() : 0) +
                ", leaderCommit=" + leaderCommit +
                '}';
    }
}
