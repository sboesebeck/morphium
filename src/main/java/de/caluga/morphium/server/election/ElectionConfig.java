package de.caluga.morphium.server.election;

/**
 * Configuration for the election protocol.
 * All timeouts are in milliseconds.
 */
public class ElectionConfig {

    /**
     * Minimum election timeout in milliseconds.
     * The actual timeout is randomized between min and max to prevent split votes.
     * Default: 150ms (Raft paper recommendation)
     */
    private int electionTimeoutMinMs = 150;

    /**
     * Maximum election timeout in milliseconds.
     * Default: 300ms (Raft paper recommendation)
     */
    private int electionTimeoutMaxMs = 300;

    /**
     * Interval at which the leader sends heartbeats to followers.
     * Should be significantly less than election timeout to prevent unnecessary elections.
     * Default: 50ms
     */
    private int heartbeatIntervalMs = 50;

    /**
     * Time without majority contact before leader steps down.
     * Prevents split-brain by ensuring leader maintains quorum.
     * Default: 10 seconds
     */
    private int leaderLeaseTimeoutMs = 10000;

    /**
     * Maximum time to wait for secondaries to catch up during stepdown.
     * Default: 30 seconds
     */
    private int stepdownCatchupTimeoutMs = 30000;

    /**
     * Priority for this node in leader election.
     * Higher priority nodes are more likely to become leader.
     * Used as tie-breaker when logs are equally up-to-date.
     * Default: 1
     */
    private int electionPriority = 1;

    /**
     * Whether this node can become leader.
     * Set to false for arbiter-like nodes that participate in voting but never lead.
     * Default: true
     */
    private boolean canBecomeLeader = true;

    /**
     * Timeout for vote requests in milliseconds.
     * If a node doesn't respond within this time, assume vote denied.
     * Default: 100ms
     */
    private int voteRequestTimeoutMs = 100;

    /**
     * Whether to persist election state (term, votedFor) to disk.
     * Required for correctness across restarts in production.
     * Default: false (for easier testing)
     */
    private boolean persistState = false;

    /**
     * Path to persist election state if persistState is true.
     */
    private String statePersistencePath = null;

    // Getters and setters

    public int getElectionTimeoutMinMs() {
        return electionTimeoutMinMs;
    }

    public ElectionConfig setElectionTimeoutMinMs(int electionTimeoutMinMs) {
        this.electionTimeoutMinMs = electionTimeoutMinMs;
        return this;
    }

    public int getElectionTimeoutMaxMs() {
        return electionTimeoutMaxMs;
    }

    public ElectionConfig setElectionTimeoutMaxMs(int electionTimeoutMaxMs) {
        this.electionTimeoutMaxMs = electionTimeoutMaxMs;
        return this;
    }

    public int getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public ElectionConfig setHeartbeatIntervalMs(int heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        return this;
    }

    public int getLeaderLeaseTimeoutMs() {
        return leaderLeaseTimeoutMs;
    }

    public ElectionConfig setLeaderLeaseTimeoutMs(int leaderLeaseTimeoutMs) {
        this.leaderLeaseTimeoutMs = leaderLeaseTimeoutMs;
        return this;
    }

    public int getStepdownCatchupTimeoutMs() {
        return stepdownCatchupTimeoutMs;
    }

    public ElectionConfig setStepdownCatchupTimeoutMs(int stepdownCatchupTimeoutMs) {
        this.stepdownCatchupTimeoutMs = stepdownCatchupTimeoutMs;
        return this;
    }

    public int getElectionPriority() {
        return electionPriority;
    }

    public ElectionConfig setElectionPriority(int electionPriority) {
        this.electionPriority = electionPriority;
        return this;
    }

    public boolean isCanBecomeLeader() {
        return canBecomeLeader;
    }

    public ElectionConfig setCanBecomeLeader(boolean canBecomeLeader) {
        this.canBecomeLeader = canBecomeLeader;
        return this;
    }

    public int getVoteRequestTimeoutMs() {
        return voteRequestTimeoutMs;
    }

    public ElectionConfig setVoteRequestTimeoutMs(int voteRequestTimeoutMs) {
        this.voteRequestTimeoutMs = voteRequestTimeoutMs;
        return this;
    }

    public boolean isPersistState() {
        return persistState;
    }

    public ElectionConfig setPersistState(boolean persistState) {
        this.persistState = persistState;
        return this;
    }

    public String getStatePersistencePath() {
        return statePersistencePath;
    }

    public ElectionConfig setStatePersistencePath(String statePersistencePath) {
        this.statePersistencePath = statePersistencePath;
        return this;
    }

    /**
     * Generate a random election timeout within the configured range.
     */
    public int randomElectionTimeout() {
        int range = electionTimeoutMaxMs - electionTimeoutMinMs;
        return electionTimeoutMinMs + (int) (Math.random() * range);
    }

    @Override
    public String toString() {
        return "ElectionConfig{" +
                "electionTimeoutMinMs=" + electionTimeoutMinMs +
                ", electionTimeoutMaxMs=" + electionTimeoutMaxMs +
                ", heartbeatIntervalMs=" + heartbeatIntervalMs +
                ", leaderLeaseTimeoutMs=" + leaderLeaseTimeoutMs +
                ", electionPriority=" + electionPriority +
                ", canBecomeLeader=" + canBecomeLeader +
                '}';
    }
}
