package de.caluga.poppydb.election;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A node's restore finding, as exchanged between the members of a leaderless set (#391): whether
 * its local data is complete, which dump files failed to restore if not, its election priority,
 * and whether it currently knows a working leader.
 *
 * <p>Carried on its own admin command ({@code poppyRestoreStatus}) rather than on the heartbeat
 * or the (Pre)Vote traffic. The heartbeat cannot carry it: only a leader sends AppendEntries, and
 * the finding is needed precisely while there is NO leader. The PreVote round could, but a node
 * that holds back candidacy for incomplete data sends no PreVote at all - that is an invariant
 * ({@code PartialRestoreElectionGuardTest}: "not even a PreVote probe"), and turning the
 * PreVote into a probe would have every held-back node "win" PreVote rounds it may not act on,
 * INFO-logging a started election every timeout. A dedicated read-only request keeps the
 * election protocol untouched, is trivially rolling-upgrade safe (an older peer answers with an
 * unknown-command error, which counts as "unknown" and blocks the automatic acceptance - fail
 * closed) and costs one round trip per election timeout on held-back nodes only.
 *
 * <p>Wire form of the request: {@code {poppyRestoreStatus: 1, senderId: <host:port>}}. Wire form
 * of the answer: {@code {ok: 1, nodeId, dataComplete, failedFiles: [...], priority, leaderKnown}}.
 */
public final class RestoreStatus {

    public static final String COMMAND = "poppyRestoreStatus";

    private final String nodeId;
    private final boolean dataComplete;
    private final List<String> failedFiles;
    private final int priority;
    private final boolean leaderKnown;

    public RestoreStatus(String nodeId, boolean dataComplete, List<String> failedFiles, int priority,
            boolean leaderKnown) {
        this.nodeId = nodeId;
        this.dataComplete = dataComplete;
        this.failedFiles = failedFiles == null ? List.of() : List.copyOf(failedFiles);
        this.priority = priority;
        this.leaderKnown = leaderKnown;
    }

    public String getNodeId() {
        return nodeId;
    }

    /** Whether the node considers its local data complete (the #306 guard is not armed). */
    public boolean isDataComplete() {
        return dataComplete;
    }

    /** File names of the dump files whose restore failed; empty when complete, or when the data is
     * incomplete for another reason (store emptied for a sync). */
    public List<String> getFailedFiles() {
        return failedFiles;
    }

    public int getPriority() {
        return priority;
    }

    /** Whether the node is a leader itself or heard one within the last election timeout. */
    public boolean isLeaderKnown() {
        return leaderKnown;
    }

    /** The probe a held-back node sends to a peer. */
    public static Map<String, Object> requestMap(String senderId) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(COMMAND, 1);  // command name first (wire protocol)
        map.put("senderId", senderId);
        return map;
    }

    /** The answer to a probe. */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("ok", 1.0);
        map.put("nodeId", nodeId);
        map.put("dataComplete", dataComplete);
        map.put("failedFiles", new ArrayList<>(failedFiles));
        map.put("priority", priority);
        map.put("leaderKnown", leaderKnown);
        return map;
    }

    /**
     * Parse a probe answer. Returns {@code null} for anything that is not one - an error answer
     * from a peer that does not know the command (older version), or a truncated document - so
     * the caller treats the peer as unknown.
     */
    @SuppressWarnings("unchecked")
    public static RestoreStatus fromMap(Map<String, Object> map) {
        if (map == null || !(map.get("dataComplete") instanceof Boolean complete)
                || !(map.get("priority") instanceof Number prio)) {
            return null;
        }
        List<String> files = new ArrayList<>();
        if (map.get("failedFiles") instanceof List<?> list) {
            for (Object o : list) {
                files.add(String.valueOf(o));
            }
        }
        return new RestoreStatus((String) map.get("nodeId"), complete, files, prio.intValue(),
                Boolean.TRUE.equals(map.get("leaderKnown")));
    }

    @Override
    public String toString() {
        return "RestoreStatus{nodeId='" + nodeId + "', dataComplete=" + dataComplete
                + ", failedFiles=" + failedFiles + ", priority=" + priority + ", leaderKnown=" + leaderKnown + '}';
    }
}
