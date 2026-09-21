package de.caluga.poppydb.election;

/**
 * Configuration for the election protocol.
 * All timeouts are in milliseconds.
 */
public class ElectionConfig {

    /**
     * Minimum election timeout in milliseconds.
     * The actual timeout is randomized between min and max to prevent split votes.
     * Default: 2000ms (more production-ready than Raft paper's 150ms)
     *
     * Note: The Raft paper recommends 150ms, but in practice this is too aggressive
     * for systems under high load. MongoDB uses 10 seconds by default.
     * Can be overridden via system property "morphiumserver.electionTimeoutMinMs".
     */
    private int electionTimeoutMinMs = Integer.getInteger("morphiumserver.electionTimeoutMinMs", 2000);

    /**
     * Maximum election timeout in milliseconds.
     * Default: 4000ms (more production-ready than Raft paper's 300ms)
     * Can be overridden via system property "morphiumserver.electionTimeoutMaxMs".
     */
    private int electionTimeoutMaxMs = Integer.getInteger("morphiumserver.electionTimeoutMaxMs", 4000);

    /**
     * Interval at which the leader sends heartbeats to followers.
     * Should be significantly less than election timeout to prevent unnecessary elections.
     * Default: 500ms (allows for GC pauses and high load situations)
     * Can be overridden via system property "morphiumserver.heartbeatIntervalMs".
     */
    private int heartbeatIntervalMs = Integer.getInteger("morphiumserver.heartbeatIntervalMs", 500);

    /**
     * Time without majority contact before leader steps down.
     * Prevents split-brain by ensuring leader maintains quorum.
     * Default: 10 seconds
     * Can be overridden via system property "morphiumserver.leaderLeaseTimeoutMs".
     */
    private int leaderLeaseTimeoutMs = Integer.getInteger("morphiumserver.leaderLeaseTimeoutMs", 10000);

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
     * Priority distance that guarantees an election ORDER (#387): two nodes whose priorities
     * differ by at least this many points never draw overlapping election timeouts - every
     * timeout of the higher-priority node is shorter than every timeout of the lower-priority
     * one. Nodes closer than a step (or equal) still randomize against each other.
     * Default: 10, so the usual 100/50/25 or 100/50/10 layouts are strictly ordered.
     * Can be overridden via system property "morphiumserver.electionPriorityStep".
     */
    private int electionPriorityStep = Integer.getInteger("morphiumserver.electionPriorityStep", 10);

    /**
     * Whether this node can become leader.
     * Set to false for arbiter-like nodes that participate in voting but never lead.
     * Default: true
     */
    private boolean canBecomeLeader = true;

    /**
     * Timeout for vote requests in milliseconds.
     * If a node doesn't respond within this time, assume vote denied.
     * Default: 1000ms (allows for network latency and load)
     * Can be overridden via system property "morphiumserver.voteRequestTimeoutMs".
     */
    private int voteRequestTimeoutMs = Integer.getInteger("morphiumserver.voteRequestTimeoutMs", 1000);

    /**
     * Whether a leader voluntarily steps down when a node with higher priority is
     * online and caught up ("priority takeover", like MongoDB).
     * Without this, a temporary failover to a lower-priority node stays permanent.
     * Default: true
     * Can be overridden via system property "morphiumserver.priorityTakeoverEnabled".
     */
    private boolean priorityTakeoverEnabled =
            Boolean.parseBoolean(System.getProperty("morphiumserver.priorityTakeoverEnabled", "true"));

    /**
     * Interval at which the leader looks for a higher-priority successor.
     * Default: 2000ms
     * Can be overridden via system property "morphiumserver.priorityTakeoverCheckIntervalMs".
     */
    private int priorityTakeoverCheckIntervalMs = Integer.getInteger("morphiumserver.priorityTakeoverCheckIntervalMs", 2000);

    /**
     * Minimum time a node must have been leader before it may yield to a higher-priority peer.
     * Prevents leadership flapping while a cluster is still settling.
     * Default: 30000ms (same order as MongoDB's catchUpTakeoverDelayMillis)
     * Can be overridden via system property "morphiumserver.priorityTakeoverMinStabilityMs".
     */
    private int priorityTakeoverMinStabilityMs = Integer.getInteger("morphiumserver.priorityTakeoverMinStabilityMs", 30000);

    /**
     * How far a peer may lag behind the leader (in replicated change stream events)
     * and still count as "caught up" for a priority takeover.
     * Default: 0 (must be fully caught up)
     */
    private int priorityTakeoverMaxLag = Integer.getInteger("morphiumserver.priorityTakeoverMaxLag", 0);

    /**
     * Seconds the yielding leader refuses to seek election again, giving the
     * higher-priority node time to win the election it triggers.
     * Default: 10
     */
    private int priorityTakeoverStepDownSecs = Integer.getInteger("morphiumserver.priorityTakeoverStepDownSecs", 10);

    /**
     * Whether to persist election state (currentTerm, votedFor) to disk, as Raft requires
     * (#306: a node restarting at term 0 contributed to the term churn during the ACC
     * incident).
     * Default: false for tests/embedded use, but automatically true when a persistence path is
     * configured via the "morphiumserver.electionStatePath" system property - so an operator
     * can enable it without any code wiring. Can also be forced on via
     * "morphiumserver.electionStatePersist" (which then still needs a path to take effect).
     */
    private boolean persistState = System.getProperty("morphiumserver.electionStatePath") != null
            || Boolean.getBoolean("morphiumserver.electionStatePersist");

    /**
     * Path of the FILE the election state is persisted to (a small properties file, written
     * atomically). Convention: place it next to the dump directory, e.g.
     * {@code <dumpDir>/election-state.properties}.
     * Default: the "morphiumserver.electionStatePath" system property, or null (no persistence).
     */
    private String statePersistencePath = System.getProperty("morphiumserver.electionStatePath");

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

    public int getElectionPriorityStep() {
        return electionPriorityStep;
    }

    public ElectionConfig setElectionPriorityStep(int electionPriorityStep) {
        if (electionPriorityStep < 1 || electionPriorityStep > MAX_PRIORITY) {
            throw new IllegalArgumentException("electionPriorityStep must be between 1 and " + MAX_PRIORITY + ", got " + electionPriorityStep);
        }
        this.electionPriorityStep = electionPriorityStep;
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

    public boolean isPriorityTakeoverEnabled() {
        return priorityTakeoverEnabled;
    }

    public ElectionConfig setPriorityTakeoverEnabled(boolean priorityTakeoverEnabled) {
        this.priorityTakeoverEnabled = priorityTakeoverEnabled;
        return this;
    }

    public int getPriorityTakeoverCheckIntervalMs() {
        return priorityTakeoverCheckIntervalMs;
    }

    public ElectionConfig setPriorityTakeoverCheckIntervalMs(int priorityTakeoverCheckIntervalMs) {
        this.priorityTakeoverCheckIntervalMs = priorityTakeoverCheckIntervalMs;
        return this;
    }

    public int getPriorityTakeoverMinStabilityMs() {
        return priorityTakeoverMinStabilityMs;
    }

    public ElectionConfig setPriorityTakeoverMinStabilityMs(int priorityTakeoverMinStabilityMs) {
        this.priorityTakeoverMinStabilityMs = priorityTakeoverMinStabilityMs;
        return this;
    }

    public int getPriorityTakeoverMaxLag() {
        return priorityTakeoverMaxLag;
    }

    public ElectionConfig setPriorityTakeoverMaxLag(int priorityTakeoverMaxLag) {
        this.priorityTakeoverMaxLag = priorityTakeoverMaxLag;
        return this;
    }

    public int getPriorityTakeoverStepDownSecs() {
        return priorityTakeoverStepDownSecs;
    }

    public ElectionConfig setPriorityTakeoverStepDownSecs(int priorityTakeoverStepDownSecs) {
        this.priorityTakeoverStepDownSecs = priorityTakeoverStepDownSecs;
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
     * Fraction of a priority slot that the random jitter may use. The rest of the slot is the
     * guaranteed gap between two adjacent priority steps - the room a higher-priority node has
     * to finish its PreVote and vote rounds before the next step's timer can fire at all.
     * With the defaults (2000..4000ms, step 10) a slot is 200ms: 100ms jitter, 100ms gap; on
     * the 5000..10000ms test runner 250ms/250ms.
     */
    static final double SLOT_JITTER_FRACTION = 0.5;

    /**
     * Generate a random election timeout within the configured range, ordered by this node's
     * priority (#387).
     *
     * <p>The range {@code [electionTimeoutMinMs, electionTimeoutMaxMs)} is divided into
     * {@code MAX_PRIORITY / electionPriorityStep} slots of equal width. A node's slot is
     * {@code (MAX_PRIORITY - priority) / step}: priority 100 owns the first slot at the
     * minimum, each step down moves one slot later. Within its slot a node randomizes over the
     * first {@link #SLOT_JITTER_FRACTION} of the width, so
     * <ul>
     *   <li>two nodes whose priorities are at least one step apart NEVER overlap - every
     *       timeout of the higher one is shorter than every timeout of the lower one, with the
     *       rest of the slot as guaranteed gap;</li>
     *   <li>nodes of equal priority (or closer than a step) still randomize against each
     *       other, which is what prevents split votes between them.</li>
     * </ul>
     *
     * <p>The previous formula, {@code random(min..max) + range * (1 - priority/100)}, gave
     * only a bias: its random span was as wide as the whole priority delay, so any two
     * priorities less than 100 apart drew overlapping windows (100 vs 90: 2.0-4.0s against
     * 2.2-4.2s) and the lower one campaigned first in a sizeable share of elections - one in
     * five for 50 vs 10, followed by a priority takeover 30s later and a second round of lost
     * resume tokens on every client.
     *
     * <p>Timeouts scale with the configured min/max, nothing is hard-coded; the highest
     * priority now times out at the minimum instead of anywhere in the range, and the lowest
     * stays below the maximum instead of a full range above it.
     */
    public int randomElectionTimeout() {
        int range = electionTimeoutMaxMs - electionTimeoutMinMs;

        // Priority 0 nodes should not start elections (handled elsewhere),
        // but if they do, give them maximum delay
        if (electionPriority <= 0) {
            return electionTimeoutMaxMs + range * 2;  // Very long timeout
        }

        int slots = MAX_PRIORITY / electionPriorityStep;
        int slotWidth = Math.max(1, range / slots);
        int slot = Math.min((MAX_PRIORITY - electionPriority) / electionPriorityStep, slots - 1);
        int jitterSpan = Math.max(1, (int) (slotWidth * SLOT_JITTER_FRACTION));
        int jitter = (int) (Math.random() * jitterSpan);

        return electionTimeoutMinMs + slot * slotWidth + jitter;
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
                ", electionPriorityStep=" + electionPriorityStep +
                ", canBecomeLeader=" + canBecomeLeader +
                ", priorityTakeoverEnabled=" + priorityTakeoverEnabled +
                '}';
    }
}
