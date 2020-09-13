package de.caluga.morphium.replicaset;

import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.Utils;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 10.08.12
 * Time: 15:05
 * <p/>
 * Replicaset Status
 */
@SuppressWarnings("UnusedDeclaration")
@Embedded(translateCamelCase = false)
public class ReplicaSetStatus {
    private String set;
    private int myState;
    private String syncSourceHost;
    private Date date;
    private int term;
    private int syncSourceId;
    private long heartbeatIntervalMillis;
    private int majorityVoteCount;
    private int writeMajorityCount;
    private int votingMembersCount;
    private int writableVotingMembersCount;
    private long lastStableRecoveryTimestamp;
    private List<ReplicaSetNode> members;
    private Map<String, Object> optimes;
    private Map<String, Object> electionCandidateMetrics;

    @Transient
    private ReplicaSetConf config;

    public String getSet() {
        return set;
    }

    public void setSet(String set) {
        this.set = set;
    }

    public int getMyState() {
        return myState;
    }

    public void setMyState(int myState) {
        this.myState = myState;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public List<ReplicaSetNode> getMembers() {
        return members;
    }

    public void setMembers(List<ReplicaSetNode> members) {
        this.members = members;
    }

    public ReplicaSetConf getConfig() {
        return config;
    }

    public void setConfig(ReplicaSetConf config) {
        this.config = config;

    }

    public String getSyncSourceHost() {
        return syncSourceHost;
    }

    public void setSyncSourceHost(String syncSourceHost) {
        this.syncSourceHost = syncSourceHost;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public int getSyncSourceId() {
        return syncSourceId;
    }

    public void setSyncSourceId(int syncSourceId) {
        this.syncSourceId = syncSourceId;
    }

    public long getHeartbeatIntervalMillis() {
        return heartbeatIntervalMillis;
    }

    public void setHeartbeatIntervalMillis(long heartbeatIntervalMillis) {
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
    }

    public int getMajorityVoteCount() {
        return majorityVoteCount;
    }

    public void setMajorityVoteCount(int majorityVoteCount) {
        this.majorityVoteCount = majorityVoteCount;
    }

    public int getWriteMajorityCount() {
        return writeMajorityCount;
    }

    public void setWriteMajorityCount(int writeMajorityCount) {
        this.writeMajorityCount = writeMajorityCount;
    }

    public int getVotingMembersCount() {
        return votingMembersCount;
    }

    public void setVotingMembersCount(int votingMembersCount) {
        this.votingMembersCount = votingMembersCount;
    }

    public int getWritableVotingMembersCount() {
        return writableVotingMembersCount;
    }

    public void setWritableVotingMembersCount(int writableVotingMembersCount) {
        this.writableVotingMembersCount = writableVotingMembersCount;
    }

    public long getLastStableRecoveryTimestamp() {
        return lastStableRecoveryTimestamp;
    }

    public void setLastStableRecoveryTimestamp(long lastStableRecoveryTimestamp) {
        this.lastStableRecoveryTimestamp = lastStableRecoveryTimestamp;
    }

    public Map<String, Object> getOptimes() {
        return optimes;
    }

    public void setOptimes(Map<String, Object> optimes) {
        this.optimes = optimes;
    }

    public Map<String, Object> getElectionCandidateMetrics() {
        return electionCandidateMetrics;
    }

    public void setElectionCandidateMetrics(Map<String, Object> electionCandidateMetrics) {
        this.electionCandidateMetrics = electionCandidateMetrics;
    }

    public int getActiveNodes() {
        if (members == null) {
            return 0;
        }
        int up = 0;
        for (ReplicaSetNode n : members) {
            if (n.getState() <= 2) {
                boolean ignore = false;
                for (ConfNode c : config.getMembers()) {
                    if (c.getId() == n.getId() && c.getHidden() != null && c.getHidden()) {
                        ignore = true;
                    }
                }
                if (!ignore) {
                    up++;
                }
            }
        }
        //        for (ConfNode c:config.getMembers()) {
        //            if (c.getHidden()!=null && c.getHidden()) {
        //                up--; //removing hidden nodes
        //            }
        //        }
        return up;
    }

    @Override
    public String toString() {
        return Utils.toJsonString(new ObjectMapperImpl().serialize(this));

    }
}
