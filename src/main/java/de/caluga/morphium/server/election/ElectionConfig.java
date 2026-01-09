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
     * Priority for this node in leader election (0-100).
     * Higher priority nodes are more likely to become leader due to shorter election timeouts.
     * - Priority 0: Node can never become primary (like MongoDB arbiter)
     * - Priority 1-100: Higher values = shorter election timeout = more likely to win elections
     * Default: 50 (middle priority)
     *
     * Similar to MongoDB's replica set member priority.
     */
    private int electionPriority = 50;

    /**
     * Maximum priority value. Used to calculate relative election timeout delays.
     */
    public static final int MAX_PRIORITY = 100;

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
     * Generate a random election timeout within the configured range,
     * adjusted by this node's priority.
     *
     * Higher priority nodes get shorter timeouts, making them more likely
     * to start elections first and become leader.
     *
     * Formula: baseTimeout + priorityDelay
     * - baseTimeout: random value between min and max
     * - priorityDelay: additional delay for lower priority nodes
     *   - Priority 100 (max): no additional delay
     *   - Priority 50: adds ~50% of the timeout range as delay
     *   - Priority 1: adds ~99% of the timeout range as delay
     *   - Priority 0: should never call this (handled by canBecomeLeader)
     *
     * This ensures higher priority nodes have election timeouts that are
     * consistently shorter than lower priority nodes, similar to MongoDB.
     */
    public int randomElectionTimeout() {
        int range = electionTimeoutMaxMs - electionTimeoutMinMs;
        int baseTimeout = electionTimeoutMinMs + (int) (Math.random() * range);

        // Priority 0 nodes should not start elections (handled elsewhere),
        // but if they do, give them maximum delay
        if (electionPriority <= 0) {
            return baseTimeout + range * 2;  // Very long timeout
        }

        // Calculate priority-based delay:
        // Priority 100 -> delay factor 0 (no delay)
        // Priority 50 -> delay factor 0.5 (50% of range added)
        // Priority 1 -> delay factor 0.99 (99% of range added)
        double priorityFactor = 1.0 - ((double) electionPriority / MAX_PRIORITY);
        int priorityDelay = (int) (range * priorityFactor);

        return baseTimeout + priorityDelay;
    }

    /**
     * Check if this node can become leader based on priority.
     * Priority 0 nodes can never become leader (like MongoDB arbiters).
     */
    public boolean canBecomeLeaderByPriority() {
        return electionPriority > 0 && canBecomeLeader;
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
