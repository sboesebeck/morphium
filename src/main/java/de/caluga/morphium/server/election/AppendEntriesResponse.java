package de.caluga.morphium.server.election;

import java.util.HashMap;
import java.util.Map;

/**
 * Response to an AppendEntries (heartbeat/replication) request.
 * Based on Raft AppendEntries RPC response.
 */
public class AppendEntriesResponse {

    /**
     * Current term of the responding node.
     * Used by leader to discover if its term is stale.
     */
    private long term;

    /**
     * True if the follower successfully processed the request.
     * False if term was stale or log consistency check failed.
     */
    private boolean success;

    /**
     * Follower's last log index after processing this request.
     * Used by leader to track replication progress.
     */
    private long matchIndex;

    /**
     * Address of the responding node (for debugging/logging).
     */
    private String followerId;

    public AppendEntriesResponse() {
    }

    public AppendEntriesResponse(long term, boolean success) {
        this.term = term;
        this.success = success;
    }

    public AppendEntriesResponse(long term, boolean success, long matchIndex) {
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
    }

    public long getTerm() {
        return term;
    }

    public AppendEntriesResponse setTerm(long term) {
        this.term = term;
        return this;
    }

    public boolean isSuccess() {
        return success;
    }

    public AppendEntriesResponse setSuccess(boolean success) {
        this.success = success;
        return this;
    }

    public long getMatchIndex() {
        return matchIndex;
    }

    public AppendEntriesResponse setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
        return this;
    }

    public String getFollowerId() {
        return followerId;
    }

    public AppendEntriesResponse setFollowerId(String followerId) {
        this.followerId = followerId;
        return this;
    }

    /**
     * Convert to Map for wire protocol transmission.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("ok", 1);
        map.put("term", term);
        map.put("success", success);
        map.put("matchIndex", matchIndex);
        if (followerId != null) {
            map.put("followerId", followerId);
        }
        return map;
    }

    /**
     * Parse from Map received over wire protocol.
     */
    public static AppendEntriesResponse fromMap(Map<String, Object> map) {
        AppendEntriesResponse resp = new AppendEntriesResponse();
        resp.setTerm(((Number) map.get("term")).longValue());
        resp.setSuccess((Boolean) map.get("success"));
        if (map.containsKey("matchIndex")) {
            resp.setMatchIndex(((Number) map.get("matchIndex")).longValue());
        }
        if (map.containsKey("followerId")) {
            resp.setFollowerId((String) map.get("followerId"));
        }
        return resp;
    }

    @Override
    public String toString() {
        return "AppendEntriesResponse{" +
                "term=" + term +
                ", success=" + success +
                ", matchIndex=" + matchIndex +
                ", followerId='" + followerId + '\'' +
                '}';
    }
}
