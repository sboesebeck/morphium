package de.caluga.morphium.server.election;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Manages leader election for MorphiumServer replica set.
 * Implements a Raft-inspired election protocol.
 *
 * Thread-safety: All state modifications are synchronized via the stateLock.
 * Callbacks are invoked outside the lock to prevent deadlocks.
 */
public class ElectionManager {

    private static final Logger log = LoggerFactory.getLogger(ElectionManager.class);

    // Configuration
    private final ElectionConfig config;
    private final String myAddress;
    private final List<String> peerAddresses;

    // Election state (protected by stateLock)
    private final ReentrantLock stateLock = new ReentrantLock();
    private volatile ElectionState state = ElectionState.FOLLOWER;
    private final AtomicLong currentTerm = new AtomicLong(0);
    private volatile String votedFor = null;
    private volatile String currentLeader = null;

    // Log state (for election comparison)
    private final AtomicLong lastLogIndex = new AtomicLong(0);
    private final AtomicLong lastLogTerm = new AtomicLong(0);

    // Election bookkeeping
    private final Set<String> votesReceived = ConcurrentHashMap.newKeySet();
    private volatile long lastHeartbeatTime = 0;
    private volatile long leaseExpiryTime = 0;

    // Timers and executors
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> electionTimerTask;
    private ScheduledFuture<?> heartbeatTask;
    private ScheduledFuture<?> leaseCheckTask;

    // Callbacks for integration with MorphiumServer
    private Consumer<Boolean> onLeadershipChange;  // Called with true when becoming leader, false when stepping down
    private Consumer<String> onLeaderDiscovered;   // Called when a new leader is discovered
    private BiConsumer<String, VoteRequest> sendVoteRequest;  // Send vote request to peer
    private BiConsumer<String, AppendEntriesRequest> sendAppendEntries;  // Send heartbeat to peer

    // State for tracking pending vote requests
    private volatile boolean electionInProgress = false;

    // Frozen state - prevents this node from starting elections
    private volatile boolean frozen = false;
    private volatile long frozenUntil = 0;  // System.currentTimeMillis() when freeze expires

    // Step down state - prevents re-election after stepping down
    private volatile long noElectionUntil = 0;  // System.currentTimeMillis() when we can seek election again

    // Running state
    private volatile boolean running = false;

    public ElectionManager(String myAddress, List<String> allHosts, ElectionConfig config) {
        this.myAddress = myAddress;
        this.config = config != null ? config : new ElectionConfig();

        // Filter out self from peer list
        this.peerAddresses = new ArrayList<>();
        for (String host : allHosts) {
            if (!host.equals(myAddress)) {
                peerAddresses.add(host);
            }
        }

        log.info("ElectionManager created for {} with {} peers: {}", myAddress, peerAddresses.size(), peerAddresses);
    }

    /**
     * Start the election manager.
     * Begins as FOLLOWER and starts election timer.
     */
    public void start() {
        if (running) {
            log.warn("ElectionManager already running");
            return;
        }

        log.info("Starting ElectionManager for {}", myAddress);
        running = true;

        scheduler = Executors.newScheduledThreadPool(3, r -> {
            Thread t = new Thread(r, "ElectionManager-" + myAddress);
            t.setDaemon(true);
            return t;
        });

        // Start as follower
        becomeFollower(currentTerm.get(), null);

        // Start election timer
        resetElectionTimer();

        log.info("ElectionManager started for {} in state {}", myAddress, state);
    }

    /**
     * Stop the election manager.
     */
    public void stop() {
        if (!running) {
            return;
        }

        log.info("Stopping ElectionManager for {}", myAddress);
        running = false;

        if (electionTimerTask != null) {
            electionTimerTask.cancel(true);
        }
        if (heartbeatTask != null) {
            heartbeatTask.cancel(true);
        }
        if (leaseCheckTask != null) {
            leaseCheckTask.cancel(true);
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    // ==================== State Transitions ====================

    /**
     * Transition to FOLLOWER state.
     */
    private void becomeFollower(long term, String leaderId) {
        stateLock.lock();
        try {
            ElectionState previousState = state;
            boolean wasLeader = (previousState == ElectionState.LEADER);

            state = ElectionState.FOLLOWER;

            if (term > currentTerm.get()) {
                currentTerm.set(term);
                votedFor = null;  // Reset vote when term changes
            }

            if (leaderId != null) {
                currentLeader = leaderId;
            }

            // Cancel heartbeat sending (only leaders send heartbeats)
            if (heartbeatTask != null) {
                heartbeatTask.cancel(false);
                heartbeatTask = null;
            }

            // Cancel lease check (only leaders check lease)
            if (leaseCheckTask != null) {
                leaseCheckTask.cancel(false);
                leaseCheckTask = null;
            }

            electionInProgress = false;
            votesReceived.clear();

            log.info("{} became FOLLOWER at term {} (leader: {}, was: {})",
                    myAddress, currentTerm.get(), currentLeader, previousState);

            // Notify leadership change outside the lock
            if (wasLeader && onLeadershipChange != null) {
                scheduler.execute(() -> onLeadershipChange.accept(false));
            }

            // Restart election timer
            resetElectionTimer();

        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Transition to CANDIDATE state and start election.
     */
    private void becomeCandidate() {
        if (!config.isCanBecomeLeader()) {
            log.debug("{} cannot become leader (canBecomeLeader=false), staying follower", myAddress);
            resetElectionTimer();
            return;
        }

        // Check if frozen
        if (isFrozen()) {
            log.debug("{} is frozen, cannot start election", myAddress);
            resetElectionTimer();
            return;
        }

        // Check if blocked from recent stepdown
        if (isElectionBlocked()) {
            log.debug("{} is blocked from election (recent stepdown), waiting...", myAddress);
            resetElectionTimer();
            return;
        }

        stateLock.lock();
        try {
            // Increment term and vote for self
            long newTerm = currentTerm.incrementAndGet();
            state = ElectionState.CANDIDATE;
            votedFor = myAddress;
            currentLeader = null;
            electionInProgress = true;

            // Clear and add self vote
            votesReceived.clear();
            votesReceived.add(myAddress);

            log.info("{} became CANDIDATE at term {}, starting election", myAddress, newTerm);

            // Reset election timer with new random timeout
            resetElectionTimer();

            // Request votes from all peers
            requestVotes(newTerm);

        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Transition to LEADER state.
     */
    private void becomeLeader() {
        stateLock.lock();
        try {
            if (state != ElectionState.CANDIDATE) {
                log.warn("{} tried to become leader but not a candidate (state={})", myAddress, state);
                return;
            }

            state = ElectionState.LEADER;
            currentLeader = myAddress;
            electionInProgress = false;

            // Cancel election timer (leaders don't need it)
            if (electionTimerTask != null) {
                electionTimerTask.cancel(false);
                electionTimerTask = null;
            }

            // Update lease expiry
            leaseExpiryTime = System.currentTimeMillis() + config.getLeaderLeaseTimeoutMs();

            log.info("{} became LEADER at term {}", myAddress, currentTerm.get());

            // Start sending heartbeats
            startHeartbeat();

            // Start lease checking
            startLeaseCheck();

        } finally {
            stateLock.unlock();
        }

        // Notify leadership change outside the lock
        if (onLeadershipChange != null) {
            scheduler.execute(() -> onLeadershipChange.accept(true));
        }
    }

    // ==================== Election Timer ====================

    /**
     * Reset the election timer with a new random timeout.
     * Called when receiving valid heartbeat or starting as follower.
     */
    public void resetElectionTimer() {
        if (!running) {
            return;
        }

        stateLock.lock();
        try {
            // Cancel existing timer
            if (electionTimerTask != null) {
                electionTimerTask.cancel(false);
            }

            // Don't set timer for leader
            if (state == ElectionState.LEADER) {
                return;
            }

            // Schedule new timer with random timeout
            int timeout = config.randomElectionTimeout();
            electionTimerTask = scheduler.schedule(this::onElectionTimeout, timeout, TimeUnit.MILLISECONDS);

            log.trace("{} election timer reset to {}ms", myAddress, timeout);

        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Called when election timer expires without receiving heartbeat.
     */
    private void onElectionTimeout() {
        if (!running) {
            return;
        }

        stateLock.lock();
        try {
            if (state == ElectionState.LEADER) {
                // Leaders don't have election timeouts
                return;
            }

            log.info("{} election timeout expired (state={}, term={})", myAddress, state, currentTerm.get());

            // Start new election
            becomeCandidate();

        } finally {
            stateLock.unlock();
        }
    }

    // ==================== Vote Handling ====================

    /**
     * Request votes from all peers.
     */
    private void requestVotes(long term) {
        if (peerAddresses.isEmpty()) {
            // Single node cluster - automatically become leader
            log.info("{} is single node, becoming leader", myAddress);
            becomeLeader();
            return;
        }

        VoteRequest request = new VoteRequest(term, myAddress, lastLogIndex.get(), lastLogTerm.get());

        log.debug("{} requesting votes for term {} from {} peers", myAddress, term, peerAddresses.size());

        for (String peer : peerAddresses) {
            if (sendVoteRequest != null) {
                try {
                    sendVoteRequest.accept(peer, request);
                } catch (Exception e) {
                    log.warn("{} failed to send vote request to {}: {}", myAddress, peer, e.getMessage());
                }
            }
        }

        // Check if we already have majority (in case of single peer that's down)
        checkMajority();
    }

    /**
     * Handle incoming vote request from a candidate.
     *
     * @return VoteResponse to send back
     */
    public VoteResponse handleVoteRequest(VoteRequest request) {
        stateLock.lock();
        try {
            long requestTerm = request.getTerm();
            long myTerm = currentTerm.get();

            log.debug("{} received vote request from {} for term {} (my term={})",
                    myAddress, request.getCandidateId(), requestTerm, myTerm);

            // If request term is higher, update our term and become follower
            if (requestTerm > myTerm) {
                log.info("{} discovered higher term {} from {}, updating from {}",
                        myAddress, requestTerm, request.getCandidateId(), myTerm);
                becomeFollower(requestTerm, null);
                myTerm = currentTerm.get();
            }

            // Deny vote if request term is lower than ours
            if (requestTerm < myTerm) {
                log.debug("{} denying vote to {} (term {} < my term {})",
                        myAddress, request.getCandidateId(), requestTerm, myTerm);
                return new VoteResponse(myTerm, false, myAddress);
            }

            // Check if we can grant the vote
            boolean canVote = (votedFor == null || votedFor.equals(request.getCandidateId()));

            // Check if candidate's log is at least as up-to-date as ours
            boolean logOk = isLogAtLeastAsUpToDate(request.getLastLogTerm(), request.getLastLogIndex());

            if (canVote && logOk) {
                votedFor = request.getCandidateId();
                resetElectionTimer();  // Reset timer since we're participating in election

                log.info("{} granted vote to {} for term {}",
                        myAddress, request.getCandidateId(), requestTerm);
                return new VoteResponse(myTerm, true, myAddress);
            } else {
                log.debug("{} denied vote to {} (canVote={}, logOk={}, votedFor={})",
                        myAddress, request.getCandidateId(), canVote, logOk, votedFor);
                return new VoteResponse(myTerm, false, myAddress);
            }

        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Handle vote response from a peer.
     */
    public void handleVoteResponse(String peer, VoteResponse response) {
        stateLock.lock();
        try {
            // Ignore if not a candidate anymore
            if (state != ElectionState.CANDIDATE) {
                log.trace("{} ignoring vote response (not a candidate)", myAddress);
                return;
            }

            long responseTerm = response.getTerm();
            long myTerm = currentTerm.get();

            // If response has higher term, become follower
            if (responseTerm > myTerm) {
                log.info("{} discovered higher term {} from vote response, stepping down", myAddress, responseTerm);
                becomeFollower(responseTerm, null);
                return;
            }

            // Ignore votes from old terms
            if (responseTerm < myTerm) {
                log.trace("{} ignoring stale vote response (term {} < {})", myAddress, responseTerm, myTerm);
                return;
            }

            // Record vote
            if (response.isVoteGranted()) {
                votesReceived.add(peer);
                log.debug("{} received vote from {} (total votes: {})", myAddress, peer, votesReceived.size());

                checkMajority();
            } else {
                log.debug("{} vote denied by {}", myAddress, peer);
            }

        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Check if we have majority votes and should become leader.
     */
    private void checkMajority() {
        int totalNodes = peerAddresses.size() + 1;  // +1 for self
        int majority = (totalNodes / 2) + 1;
        int votes = votesReceived.size();

        log.debug("{} checking majority: {} votes, need {} (total {})", myAddress, votes, majority, totalNodes);

        if (votes >= majority && state == ElectionState.CANDIDATE) {
            log.info("{} won election with {}/{} votes", myAddress, votes, totalNodes);
            becomeLeader();
        }
    }

    /**
     * Check if candidate's log is at least as up-to-date as ours.
     * Per Raft: compare by (lastLogTerm, lastLogIndex) - term is more important.
     */
    private boolean isLogAtLeastAsUpToDate(long candidateLastTerm, long candidateLastIndex) {
        long myLastTerm = lastLogTerm.get();
        long myLastIndex = lastLogIndex.get();

        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIndex >= myLastIndex;
    }

    // ==================== Heartbeat Handling ====================

    /**
     * Start sending heartbeats to all followers.
     */
    private void startHeartbeat() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
        }

        heartbeatTask = scheduler.scheduleAtFixedRate(
                this::sendHeartbeats,
                0,
                config.getHeartbeatIntervalMs(),
                TimeUnit.MILLISECONDS
        );

        log.debug("{} started heartbeat with interval {}ms", myAddress, config.getHeartbeatIntervalMs());
    }

    /**
     * Send heartbeats to all followers.
     */
    private void sendHeartbeats() {
        if (!running || state != ElectionState.LEADER) {
            return;
        }

        AppendEntriesRequest heartbeat = AppendEntriesRequest.heartbeat(
                currentTerm.get(),
                myAddress,
                lastLogIndex.get(),
                lastLogTerm.get(),
                lastLogIndex.get()  // leaderCommit = our log index
        );

        log.trace("{} sending heartbeats to {} peers", myAddress, peerAddresses.size());

        for (String peer : peerAddresses) {
            if (sendAppendEntries != null) {
                try {
                    sendAppendEntries.accept(peer, heartbeat);
                } catch (Exception e) {
                    log.warn("{} failed to send heartbeat to {}: {}", myAddress, peer, e.getMessage());
                }
            }
        }
    }

    /**
     * Handle incoming AppendEntries (heartbeat) from leader.
     *
     * @return AppendEntriesResponse to send back
     */
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        stateLock.lock();
        try {
            long requestTerm = request.getTerm();
            long myTerm = currentTerm.get();

            log.trace("{} received appendEntries from {} (term={}, myTerm={})",
                    myAddress, request.getLeaderId(), requestTerm, myTerm);

            // If request term is higher, update our term and become follower
            if (requestTerm > myTerm) {
                log.info("{} discovered higher term {} from leader {}", myAddress, requestTerm, request.getLeaderId());
                becomeFollower(requestTerm, request.getLeaderId());
                myTerm = currentTerm.get();
            }

            // Reject if term is lower
            if (requestTerm < myTerm) {
                log.debug("{} rejecting appendEntries from {} (term {} < {})",
                        myAddress, request.getLeaderId(), requestTerm, myTerm);
                return new AppendEntriesResponse(myTerm, false, lastLogIndex.get()).setFollowerId(myAddress);
            }

            // Valid heartbeat from current leader
            lastHeartbeatTime = System.currentTimeMillis();
            currentLeader = request.getLeaderId();

            // If we were a candidate, step down
            if (state == ElectionState.CANDIDATE) {
                log.info("{} stepping down from candidate (received heartbeat from leader {})",
                        myAddress, request.getLeaderId());
                becomeFollower(requestTerm, request.getLeaderId());
            }

            // Reset election timer
            resetElectionTimer();

            // Notify of leader discovery
            if (onLeaderDiscovered != null) {
                String leader = request.getLeaderId();
                scheduler.execute(() -> onLeaderDiscovered.accept(leader));
            }

            // For now, just acknowledge (log replication will be added later)
            return new AppendEntriesResponse(myTerm, true, lastLogIndex.get()).setFollowerId(myAddress);

        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Handle response to our heartbeat.
     */
    public void handleAppendEntriesResponse(String peer, AppendEntriesResponse response) {
        stateLock.lock();
        try {
            if (state != ElectionState.LEADER) {
                return;
            }

            long responseTerm = response.getTerm();
            long myTerm = currentTerm.get();

            // If response has higher term, step down
            if (responseTerm > myTerm) {
                log.info("{} discovered higher term {} from {}, stepping down", myAddress, responseTerm, peer);
                becomeFollower(responseTerm, null);
                return;
            }

            if (response.isSuccess()) {
                // Extend our lease since we got a response
                leaseExpiryTime = System.currentTimeMillis() + config.getLeaderLeaseTimeoutMs();
                log.trace("{} received heartbeat ack from {}", myAddress, peer);
            }

        } finally {
            stateLock.unlock();
        }
    }

    // ==================== Leader Lease ====================

    /**
     * Start checking leader lease.
     */
    private void startLeaseCheck() {
        if (leaseCheckTask != null) {
            leaseCheckTask.cancel(false);
        }

        // Check lease at half the lease timeout interval
        long checkInterval = config.getLeaderLeaseTimeoutMs() / 2;
        leaseCheckTask = scheduler.scheduleAtFixedRate(
                this::checkLease,
                checkInterval,
                checkInterval,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Check if leader lease is still valid.
     * Leader steps down if it can't confirm majority contact.
     */
    private void checkLease() {
        if (!running || state != ElectionState.LEADER) {
            return;
        }

        long now = System.currentTimeMillis();
        if (now > leaseExpiryTime) {
            log.warn("{} leader lease expired, stepping down", myAddress);
            becomeFollower(currentTerm.get(), null);
        }
    }

    // ==================== Callbacks ====================

    public void setOnLeadershipChange(Consumer<Boolean> callback) {
        this.onLeadershipChange = callback;
    }

    public void setOnLeaderDiscovered(Consumer<String> callback) {
        this.onLeaderDiscovered = callback;
    }

    public void setSendVoteRequest(BiConsumer<String, VoteRequest> callback) {
        this.sendVoteRequest = callback;
    }

    public void setSendAppendEntries(BiConsumer<String, AppendEntriesRequest> callback) {
        this.sendAppendEntries = callback;
    }

    // ==================== State Accessors ====================

    public ElectionState getState() {
        return state;
    }

    public long getCurrentTerm() {
        return currentTerm.get();
    }

    public String getCurrentLeader() {
        return currentLeader;
    }

    public String getMyAddress() {
        return myAddress;
    }

    public boolean isLeader() {
        return state == ElectionState.LEADER;
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * Update log index/term (called after writes on leader).
     */
    public void updateLogIndex(long index, long term) {
        lastLogIndex.set(index);
        lastLogTerm.set(term);
    }

    /**
     * Get current log index.
     */
    public long getLastLogIndex() {
        return lastLogIndex.get();
    }

    /**
     * Get current log term.
     */
    public long getLastLogTerm() {
        return lastLogTerm.get();
    }

    /**
     * Get the number of peers.
     */
    public int getPeerCount() {
        return peerAddresses.size();
    }

    /**
     * Get peer addresses.
     */
    public List<String> getPeerAddresses() {
        return Collections.unmodifiableList(peerAddresses);
    }

    /**
     * Simple stepdown - immediately becomes follower.
     */
    public void stepDown() {
        stepDown(60, 0, true);
    }

    /**
     * Graceful stepdown with configurable parameters.
     *
     * @param stepDownSecs Seconds to refuse re-election after stepping down
     * @param catchUpSecs  Seconds to wait for secondaries to catch up (not yet implemented)
     * @param force        If true, step down even if no secondary is caught up
     * @return true if stepdown succeeded, false otherwise
     */
    public boolean stepDown(int stepDownSecs, int catchUpSecs, boolean force) {
        log.info("{} stepdown requested: stepDownSecs={}, catchUpSecs={}, force={}",
                myAddress, stepDownSecs, catchUpSecs, force);

        stateLock.lock();
        try {
            if (state != ElectionState.LEADER) {
                log.warn("{} cannot step down - not leader (state={})", myAddress, state);
                return false;
            }

            // TODO: In future, implement catch-up wait
            // For now, we proceed directly if force=true or skip the wait
            if (!force && catchUpSecs > 0) {
                log.info("{} would wait {} seconds for catch-up (not implemented yet)", myAddress, catchUpSecs);
                // In future: wait for followers to acknowledge current sequence
            }

            // Set the no-election period
            if (stepDownSecs > 0) {
                noElectionUntil = System.currentTimeMillis() + (stepDownSecs * 1000L);
                log.info("{} will not seek election until {} ms", myAddress, noElectionUntil);
            }

            // Become follower
            becomeFollower(currentTerm.get(), null);
            log.info("{} successfully stepped down from leader", myAddress);
            return true;

        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Freeze this node - prevent it from seeking election.
     * Used for maintenance operations.
     *
     * @param freezeSecs Seconds to remain frozen
     */
    public void freeze(int freezeSecs) {
        log.info("{} freezing for {} seconds", myAddress, freezeSecs);
        frozen = true;
        frozenUntil = System.currentTimeMillis() + (freezeSecs * 1000L);
    }

    /**
     * Unfreeze this node - allow it to seek election again.
     */
    public void unfreeze() {
        log.info("{} unfreezing", myAddress);
        frozen = false;
        frozenUntil = 0;
    }

    /**
     * Check if this node is currently frozen (cannot seek election).
     */
    public boolean isFrozen() {
        if (!frozen) {
            return false;
        }
        // Check if freeze has expired
        if (System.currentTimeMillis() > frozenUntil) {
            frozen = false;
            frozenUntil = 0;
            return false;
        }
        return true;
    }

    /**
     * Check if this node is currently blocked from seeking election (due to recent stepdown).
     */
    public boolean isElectionBlocked() {
        if (noElectionUntil == 0) {
            return false;
        }
        if (System.currentTimeMillis() > noElectionUntil) {
            noElectionUntil = 0;
            return false;
        }
        return true;
    }

    /**
     * Get statistics for monitoring.
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("myAddress", myAddress);
        stats.put("state", state.name());
        stats.put("term", currentTerm.get());
        stats.put("leader", currentLeader);
        stats.put("votedFor", votedFor);
        stats.put("lastLogIndex", lastLogIndex.get());
        stats.put("lastLogTerm", lastLogTerm.get());
        stats.put("peerCount", peerAddresses.size());
        stats.put("peers", peerAddresses);
        stats.put("running", running);
        if (state == ElectionState.LEADER) {
            stats.put("leaseExpiryMs", Math.max(0, leaseExpiryTime - System.currentTimeMillis()));
        }
        return stats;
    }
}
