package de.caluga.poppydb.election;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

/**
 * Manages leader election for PoppyDB replica set.
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
    private long electionTimerGeneration = 0; // guarded by stateLock

    // Log state (for election comparison)
    private final AtomicLong lastLogIndex = new AtomicLong(0);
    private final AtomicLong lastLogTerm = new AtomicLong(0);

    // Candidacy restraint (D3, empty-node-wipe fix): highest lastLogIndex this process has ever
    // observed reported by ANY peer via AppendEntries/heartbeat traffic - the leader's own index
    // (advertised as prevLogIndex while we are a follower) or a follower's matchIndex (while we
    // are the leader). Used solely to hold back becomeCandidate() while we are empty; see the
    // guard there and its javadoc for the full rationale.
    private final AtomicLong highestPeerLogIndexSeen = new AtomicLong(0);

    // Election bookkeeping
    private final Set<String> votesReceived = ConcurrentHashMap.newKeySet();
    private volatile long lastHeartbeatTime = 0;
    private volatile long leaseExpiryTime = 0;

    // PreVote bookkeeping (#306, Raft §4.2.3/§9.6): a real election - with its term increment -
    // is only started after a majority has confirmed in a PreVote round that it WOULD grant its
    // vote. The round runs entirely in FOLLOWER state and changes no persistent state on either
    // side, so an un-electable candidate (empty/log-behind) can retry forever without inflating
    // the term or dethroning a healthy leader. Guarded by stateLock.
    private final Set<String> preVotesReceived = ConcurrentHashMap.newKeySet();
    private volatile boolean preVoteInProgress = false;

    // Priority takeover bookkeeping (leader only): what we learned from heartbeat responses
    private final Map<String, Integer> peerPriorities = new ConcurrentHashMap<>();
    private final Map<String, Long> peerLastContact = new ConcurrentHashMap<>();
    private volatile long leaderSince = 0;
    private volatile long leaderStartSequence = 0;

    // Replication progress hooks, injected by PoppyDB. Defaults make every peer count as caught up.
    private volatile LongSupplier localSequenceSupplier = () -> 0L;
    private volatile ToLongFunction<String> peerSequenceSupplier = peer -> -1L;

    // Timers and executors
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> electionTimerTask;
    private ScheduledFuture<?> heartbeatTask;
    private ScheduledFuture<?> leaseCheckTask;
    private ScheduledFuture<?> priorityTakeoverTask;

    // Callbacks for integration with PoppyDB
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

        // Raft requires currentTerm and votedFor to survive restarts (#306: a node coming back
        // at term 0 contributed to the term churn). Loaded before the initial becomeFollower so
        // the very first vote/heartbeat exchange already runs on the restored term.
        loadPersistedState();

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
        if (priorityTakeoverTask != null) {
            priorityTakeoverTask.cancel(true);
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
        becomeFollower(term, leaderId, true);
    }

    /**
     * Transition to FOLLOWER state.
     *
     * @param resetTimer whether to restart the election timer as part of this transition.
     *     Must be {@code true} for every caller that represents actual contact with a current
     *     or future leader (a heartbeat, or granting a vote) — that contact is exactly what the
     *     timer exists to detect, so it's correct to defer our own candidacy further. Must be
     *     {@code false} for a bare term bump learned from a vote REQUEST we go on to deny (see
     *     {@link #handleVoteRequest}): otherwise a lower-priority node whose own timeout fires
     *     first can keep starting new terms every timeout interval, and each of those requests —
     *     though correctly denied by the priority check below — would still reset a higher-
     *     priority denier's timer via this method, indefinitely deferring the very candidacy the
     *     priority check exists to protect. Found via a real 42s election (vs. the ~8s typical
     *     for this cluster) on poppydb.fritz.box during the 6.3.0 pre-release full suite run:
     *     the lowest-priority node retried across 4 terms, each retry re-arming the
     *     second-priority node's timer moments before it would have fired on its own.
     */
    private void becomeFollower(long term, String leaderId, boolean resetTimer) {
        stateLock.lock();
        try {
            ElectionState previousState = state;
            boolean wasLeader = (previousState == ElectionState.LEADER);

            state = ElectionState.FOLLOWER;

            if (term > currentTerm.get()) {
                currentTerm.set(term);
                votedFor = null;  // Reset vote when term changes
                persistElectionState();
            }

            if (leaderId != null) {
                currentLeader = leaderId;
            }

            // Cancel heartbeat sending (only leaders send heartbeats)
            if (heartbeatTask != null) {
                heartbeatTask.cancel(true);
                heartbeatTask = null;
            }

            // Cancel lease check (only leaders check lease)
            if (leaseCheckTask != null) {
                leaseCheckTask.cancel(true);
                leaseCheckTask = null;
            }

            // Cancel priority takeover check (only leaders yield leadership).
            // cancel(false): the check itself calls stepDown(), so it must not interrupt its own thread.
            if (priorityTakeoverTask != null) {
                priorityTakeoverTask.cancel(false);
                priorityTakeoverTask = null;
            }
            leaderSince = 0;

            electionInProgress = false;
            votesReceived.clear();
            preVoteInProgress = false;
            preVotesReceived.clear();

            log.info("{} became FOLLOWER at term {} (leader: {}, was: {})",
                    myAddress, currentTerm.get(), currentLeader, previousState);

            // Notify leadership change outside the lock
            if (wasLeader && onLeadershipChange != null) {
                scheduler.execute(() -> onLeadershipChange.accept(false));
            }

            // Restart election timer — see the resetTimer javadoc above for why this is
            // conditional rather than unconditional.
            if (resetTimer) {
                resetElectionTimer();
            }

        } finally {
            stateLock.unlock();
        }
    }

    /**
     * All the reasons this node must NOT start (or pre-start) an election right now. Shared by
     * the PreVote entry point ({@link #startElectionRound()}) and {@link #becomeCandidate()}
     * (re-checked there because conditions can change between winning a PreVote round and
     * acting on it). Every deny path re-arms the election timer so the node re-evaluates on the
     * next timeout.
     */
    private boolean isEligibleForCandidacy() {
        if (!config.canBecomeLeaderByPriority()) {
            log.debug("{} cannot become leader (priority={}, canBecomeLeader={}), staying follower",
                    myAddress, config.getElectionPriority(), config.isCanBecomeLeader());
            resetElectionTimer();
            return false;
        }

        // Check if frozen
        if (isFrozen()) {
            log.debug("{} is frozen, cannot start election", myAddress);
            resetElectionTimer();
            return false;
        }

        // Check if blocked from recent stepdown
        if (isElectionBlocked()) {
            log.debug("{} is blocked from election (recent stepdown), waiting...", myAddress);
            resetElectionTimer();
            return false;
        }

        // Candidacy restraint (D3, empty-node-wipe fix; #306 point 3): we are empty (nothing
        // applied/produced this process lifetime) but have observed a peer that holds real
        // data. Starting an election now can only lose - handleVoteRequest's
        // isLogAtLeastAsUpToDate check denies us on every data-holding voter - while still
        // inflating the term and forcing the legitimate leader into a pointless step-down.
        // Hold back until either our own index catches up (sync completes - Task 1's seed
        // makes this prompt) or - cold start, no data-bearing peer ever observed - there is
        // nothing to defer to. PreVote covers the startup window this guard cannot: before the
        // first heartbeat arrives, highestPeerLogIndexSeen is still 0 here, but the PreVote
        // round then fails against the data-holding voters without any term damage.
        if (lastLogIndex.get() == 0 && highestPeerLogIndexSeen.get() > 0) {
            log.debug("{} holding back candidacy: empty (index=0) but a peer has reported index {} - waiting for sync",
                    myAddress, highestPeerLogIndexSeen.get());
            resetElectionTimer();
            return false;
        }

        return true;
    }

    /**
     * Election-timeout entry point (#306): instead of jumping straight to CANDIDATE (term
     * increment!), first run a PreVote round - ask every peer "would you grant me a vote?"
     * without touching any term. Only a majority of pre-granted votes leads to
     * {@link #becomeCandidate()} and thus a real election. An un-electable node (empty, log
     * behind, next to a healthy leader) fails the round every time and retries forever WITHOUT
     * inflating the term or dethroning the healthy primary - the "disruptive server" fix.
     *
     * <p>Single-node clusters skip PreVote (there is no peer whose state could be disrupted,
     * and no one to ask).
     */
    private void startElectionRound() {
        if (!isEligibleForCandidacy()) {
            return;
        }

        if (peerAddresses.isEmpty()) {
            // Single node cluster - no one to disrupt, elect directly
            becomeCandidate();
            return;
        }

        stateLock.lock();
        try {
            if (state == ElectionState.CANDIDATE) {
                // A real election we started (after a won PreVote round) timed out without a
                // majority - e.g. split vote or quorum loss. Fall back to FOLLOWER without
                // touching the term and re-qualify via PreVote like everyone else, instead of
                // the pre-#306 behavior of bumping the term again on every retry.
                becomeFollower(currentTerm.get(), null, false);
            }

            preVoteInProgress = true;
            preVotesReceived.clear();
            preVotesReceived.add(myAddress);  // we would obviously vote for ourselves

            long term = currentTerm.get();
            VoteRequest request = new VoteRequest(term, myAddress, lastLogIndex.get(), lastLogTerm.get(),
                    config.getElectionPriority()).setPreVote(true);

            log.debug("{} starting PreVote round at term {} ({} peers)", myAddress, term, peerAddresses.size());

            // Re-arm the timer: if the round doesn't reach a majority, the next timeout simply
            // starts a fresh one.
            resetElectionTimer();

            for (String peer : peerAddresses) {
                if (sendVoteRequest != null) {
                    try {
                        sendVoteRequest.accept(peer, request);
                    } catch (Exception e) {
                        log.warn("{} failed to send PreVote request to {}: {}", myAddress, peer, e.getMessage());
                    }
                }
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Transition to CANDIDATE state and start a real election (term increment). Only reached
     * after a won PreVote round - or directly for single-node clusters (see
     * {@link #startElectionRound()}).
     */
    private void becomeCandidate() {
        if (!isEligibleForCandidacy()) {
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
            preVoteInProgress = false;
            preVotesReceived.clear();

            // Clear and add self vote
            votesReceived.clear();
            votesReceived.add(myAddress);

            persistElectionState();

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
                electionTimerTask.cancel(true);
                electionTimerTask = null;
            }

            // Update lease expiry
            leaseExpiryTime = System.currentTimeMillis() + config.getLeaderLeaseTimeoutMs();
            leaderSince = System.currentTimeMillis();

            // Progress reports from previous terms mean nothing to us: peers acknowledge
            // sequences of the leader that produced them. Only what we replicate counts.
            leaderStartSequence = localSequenceSupplier.getAsLong();

            // Peer liveness must be re-established from our own heartbeats; priorities stay valid
            peerLastContact.clear();

            log.info("{} became LEADER at term {}", myAddress, currentTerm.get());

            // Start sending heartbeats
            startHeartbeat();

            // Start lease checking
            startLeaseCheck();

            // Watch for a higher-priority successor
            startPriorityTakeoverCheck();

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
            // Cancel existing timer — use cancel(true) to interrupt running callbacks
            if (electionTimerTask != null) {
                electionTimerTask.cancel(true);
            }

            // Don't set timer for leader
            if (state == ElectionState.LEADER) {
                return;
            }

            // Schedule new timer with random timeout.
            // Increment generation so stale callbacks (from cancelled-but-already-running timers) are ignored.
            long gen = ++electionTimerGeneration;
            int timeout = config.randomElectionTimeout();
            electionTimerTask = scheduler.schedule(() -> onElectionTimeout(gen), timeout, TimeUnit.MILLISECONDS);

            log.trace("{} election timer reset to {}ms (gen={})", myAddress, timeout, gen);

        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Called when election timer expires without receiving heartbeat.
     * The generation parameter prevents stale timer callbacks from triggering elections
     * after the timer was reset by a concurrent heartbeat.
     */
    private void onElectionTimeout(long generation) {
        if (!running) {
            return;
        }

        stateLock.lock();
        try {
            // Stale timer callback — a newer resetElectionTimer() already replaced us
            if (generation != electionTimerGeneration) {
                log.trace("{} ignoring stale election timeout (gen={}, current={})", myAddress, generation, electionTimerGeneration);
                return;
            }

            if (state == ElectionState.LEADER) {
                return;
            }

            log.info("{} election timeout expired (state={}, term={})", myAddress, state, currentTerm.get());

            // Start a new election round - PreVote first (#306), real election only after a
            // majority pre-granted.
            startElectionRound();

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

        VoteRequest request = new VoteRequest(term, myAddress, lastLogIndex.get(), lastLogTerm.get(),
                config.getElectionPriority());

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
     * Priority-aware voting logic (similar to MongoDB):
     * 1. Standard Raft checks: term comparison, log up-to-date check
     * 2. Priority consideration: prefer higher priority candidates when logs are equal
     * 3. If we're a higher priority node and can become leader, we may delay voting
     *    for lower priority candidates to give ourselves a chance to run
     *
     * @return VoteResponse to send back
     */
    public VoteResponse handleVoteRequest(VoteRequest request) {
        stateLock.lock();
        try {
            // PreVote requests are answered read-only - they must never mutate term, votedFor
            // or the election timer on this side (#306).
            if (request.isPreVote()) {
                return handlePreVoteRequest(request);
            }

            long requestTerm = request.getTerm();
            long myTerm = currentTerm.get();
            int candidatePriority = request.getCandidatePriority();
            int myPriority = config.getElectionPriority();

            log.debug("{} received vote request from {} for term {} (my term={}, candidate priority={}, my priority={})",
                    myAddress, request.getCandidateId(), requestTerm, myTerm, candidatePriority, myPriority);

            // Leader stickiness (#306 point 2, Raft §4.2.3's second half / mongod-style leader
            // lease): while we ARE the leader with a live lease, or we have heard a heartbeat
            // from a current leader within the last minimum election timeout, a RequestVote is
            // by definition coming from a disruptive server - the cluster has a working leader.
            // Deny it WITHOUT adopting its (possibly inflated) term: adopting the term is
            // exactly what let an un-electable candidate dethrone the healthy primary. A
            // legitimately dead leader stops heartbeating, the window expires on every voter
            // within one election timeout, and normal Raft behavior resumes below.
            if (requestTerm > myTerm
                    && ((state == ElectionState.LEADER && System.currentTimeMillis() < leaseExpiryTime)
                            || heardFromLeaderRecently())) {
                log.info("{} ignoring vote request from {} for term {} (healthy leader {} in contact - "
                        + "not adopting the term, denying the vote)",
                        myAddress, request.getCandidateId(), requestTerm,
                        state == ElectionState.LEADER ? myAddress : currentLeader);
                return new VoteResponse(myTerm, false, myAddress);
            }

            // If request term is higher, update our term and become follower. Don't reset our
            // own election timer here — this is only a vote REQUEST, not confirmed contact with
            // a leader, and we may go on to deny it below (priorityOk). The timer is reset
            // further down, but only on the branch where we actually grant the vote — see
            // becomeFollower's resetTimer javadoc for why this distinction matters.
            if (requestTerm > myTerm) {
                log.info("{} discovered higher term {} from {}, updating from {}",
                        myAddress, requestTerm, request.getCandidateId(), myTerm);
                becomeFollower(requestTerm, null, false);
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

            if (!logOk) {
                // Operator-visible at INFO: this is the exact line that must show up when a
                // freshly restarted (empty) node tries to win an election against a node that
                // still holds data - see the empty-node-wipe bug this check exists to prevent.
                log.info("{} denied vote to {} (candidate log behind: candidateIndex={} < myIndex={})",
                        myAddress, request.getCandidateId(), request.getLastLogIndex(), lastLogIndex.get());
            }

            // Priority-based voting decision:
            // If we're a higher priority node that can become leader and haven't voted yet,
            // we should not vote for a lower priority candidate (give ourselves a chance first)
            boolean priorityOk = true;
            if (canVote && logOk && votedFor == null) {
                // We haven't voted yet - consider priority
                if (config.canBecomeLeaderByPriority() && myPriority > candidatePriority) {
                    // We're higher priority - don't vote for lower priority candidate
                    // This gives us a chance to start our own election
                    log.debug("{} (priority {}) not voting for lower priority candidate {} (priority {})",
                            myAddress, myPriority, request.getCandidateId(), candidatePriority);
                    priorityOk = false;
                }
            }

            if (canVote && logOk && priorityOk) {
                votedFor = request.getCandidateId();
                persistElectionState();  // Raft: votedFor must be durable before the response leaves
                resetElectionTimer();  // Reset timer since we're participating in election

                log.info("{} granted vote to {} for term {} (candidate priority={})",
                        myAddress, request.getCandidateId(), requestTerm, candidatePriority);
                return new VoteResponse(myTerm, true, myAddress);
            } else {
                log.debug("{} denied vote to {} (canVote={}, logOk={}, priorityOk={}, votedFor={})",
                        myAddress, request.getCandidateId(), canVote, logOk, priorityOk, votedFor);
                return new VoteResponse(myTerm, false, myAddress);
            }

        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Answer a PreVote request (#306): "would I grant this candidate a vote if it started a
     * real election now?" - evaluated with the same checks a real vote would face (term
     * recency, log recency, priority) plus leader stickiness, but strictly READ-ONLY: no term
     * adoption, no votedFor, no election timer reset. Called under stateLock (from
     * {@link #handleVoteRequest}).
     *
     * <p>Note the term semantics: the candidate sends its CURRENT term (it would campaign at
     * term+1), chosen so old nodes can misread the request as a harmless same-term real vote
     * request - see {@link VoteRequest#isPreVote()} for the rolling-upgrade rationale.
     */
    private VoteResponse handlePreVoteRequest(VoteRequest request) {
        long requestTerm = request.getTerm();
        long myTerm = currentTerm.get();
        int candidatePriority = request.getCandidatePriority();
        int myPriority = config.getElectionPriority();

        log.debug("{} received PreVote request from {} at term {} (my term={}, candidate priority={}, my priority={})",
                myAddress, request.getCandidateId(), requestTerm, myTerm, candidatePriority, myPriority);

        // Candidate's term is behind ours - a real election started from it could not win.
        // Returning our term lets the candidate catch up (as follower, without campaigning).
        if (requestTerm < myTerm) {
            log.debug("{} denying PreVote to {} (term {} < my term {})",
                    myAddress, request.getCandidateId(), requestTerm, myTerm);
            return new VoteResponse(myTerm, false, myAddress);
        }

        // Leader stickiness (#306 point 2): a working leader exists - either us, or one we
        // heard from within the last minimum election timeout. Pre-denying here is the whole
        // point of PreVote: the disruptive candidate never gets to bump any term.
        if (state == ElectionState.LEADER) {
            log.debug("{} denying PreVote to {} (I am the leader)", myAddress, request.getCandidateId());
            return new VoteResponse(myTerm, false, myAddress);
        }

        if (heardFromLeaderRecently()) {
            log.debug("{} denying PreVote to {} (heard from leader {} within the election timeout window)",
                    myAddress, request.getCandidateId(), currentLeader);
            return new VoteResponse(myTerm, false, myAddress);
        }

        // Same log-recency veto as the real vote (this is what makes PreVote effective against
        // the empty-restarted-node case). Operator-visible at INFO like the real veto.
        if (!isLogAtLeastAsUpToDate(request.getLastLogTerm(), request.getLastLogIndex())) {
            log.info("{} denied PreVote to {} (candidate log behind: candidateIndex={} < myIndex={})",
                    myAddress, request.getCandidateId(), request.getLastLogIndex(), lastLogIndex.get());
            return new VoteResponse(myTerm, false, myAddress);
        }

        // Mirror of the real vote's priority rule: at the (fresh) term the candidate would
        // campaign at, votedFor is reset on every voter, so the real election's priority check
        // reduces to exactly this comparison - a higher-priority node that can lead itself
        // would not vote for a lower-priority candidate.
        if (config.canBecomeLeaderByPriority() && myPriority > candidatePriority) {
            log.debug("{} (priority {}) denying PreVote to lower priority candidate {} (priority {})",
                    myAddress, myPriority, request.getCandidateId(), candidatePriority);
            return new VoteResponse(myTerm, false, myAddress);
        }

        log.debug("{} pre-granting vote to {} at term {}", myAddress, request.getCandidateId(), requestTerm);
        return new VoteResponse(myTerm, true, myAddress);
    }

    /**
     * Whether a heartbeat from a current leader arrived within the last minimum election
     * timeout - the leader-stickiness window used to pre-deny/deny disruptive (Pre)Vote
     * requests. Deliberately the MINIMUM timeout: any legitimate candidate only campaigns
     * after its own randomized timeout (>= minimum) of leader silence, and by then this window
     * has expired on every correctly-functioning voter too (worst case a marginal race delays
     * the election by one more timeout cycle - it cannot deadlock).
     */
    private boolean heardFromLeaderRecently() {
        long last = lastHeartbeatTime;
        return last > 0 && (System.currentTimeMillis() - last) < config.getElectionTimeoutMinMs();
    }

    /**
     * Handle vote response from a peer.
     */
    public void handleVoteResponse(String peer, VoteResponse response) {
        stateLock.lock();
        try {
            // Responses arriving while a PreVote round is open belong to that round: we are
            // still FOLLOWER (PreVote never leaves it), so the CANDIDATE-only real-vote logic
            // below could never see them. A stale PreVote grant can conversely never leak into
            // a later real election: the real election runs at term+1, and the term filter
            // below discards the PreVote-era response terms as stale.
            if (preVoteInProgress && state != ElectionState.CANDIDATE) {
                long responseTerm = response.getTerm();
                long myTerm = currentTerm.get();

                if (responseTerm > myTerm) {
                    // We are behind - catch up (no timer reset, no campaigning) and abort the round.
                    log.info("{} discovered higher term {} from PreVote response, updating from {}",
                            myAddress, responseTerm, myTerm);
                    becomeFollower(responseTerm, null, false);
                    return;
                }

                if (response.isVoteGranted()) {
                    preVotesReceived.add(peer);
                    log.debug("{} received PreVote grant from {} (total: {})", myAddress, peer, preVotesReceived.size());
                    checkPreVoteMajority();
                } else {
                    log.debug("{} PreVote denied by {}", myAddress, peer);
                }
                return;
            }

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
     * Check if the open PreVote round reached a majority - only then is a real election
     * (with its term increment) started. Called under stateLock.
     */
    private void checkPreVoteMajority() {
        int totalNodes = peerAddresses.size() + 1;  // +1 for self
        int majority = (totalNodes / 2) + 1;
        int votes = preVotesReceived.size();

        log.debug("{} checking PreVote majority: {} pre-granted, need {} (total {})",
                myAddress, votes, majority, totalNodes);

        if (votes >= majority && preVoteInProgress) {
            log.info("{} won PreVote round with {}/{} pre-granted votes, starting real election",
                    myAddress, votes, totalNodes);
            preVoteInProgress = false;
            preVotesReceived.clear();
            becomeCandidate();
        }
    }

    /**
     * Deliberately NOT a Raft &sect;5.4.1 log comparison: {@code ReplicationManager}'s change
     * stream sequences are primary-local, and {@code lastLogTerm} is a stand-in fed from
     * {@code currentTerm} at uncorrelated moments (leader heartbeat vs. follower batch-apply,
     * on different nodes, with no synchronization between the two feeds' timing) - so ordering
     * nodes by {@code (term, index)} the way Raft does is not meaningful here: a node that was
     * elected once while still empty can carry a high, stale {@code lastLogTerm} at {@code
     * index 0}, which a naive term-first comparison would prefer over a real data-holding voter.
     *
     * <p>The one invariant this can honestly enforce: a node that has applied/produced no data
     * since process start ({@code index 0}) must never win against a voter that has ({@code
     * index > 0}) - directly closes the empty-node-wipe bug this method exists for. A stale but
     * non-empty candidate (index &gt; 0 but behind) is intentionally NOT denied here; that case
     * is handled elsewhere (fail-closed resync refusing sequence regression, and candidacy
     * restraint keeping behind nodes from campaigning in the first place - see the other D-tasks
     * in this bug's task list).
     */
    private boolean isLogAtLeastAsUpToDate(long candidateLastTerm, long candidateLastIndex) {
        return !(candidateLastIndex == 0 && lastLogIndex.get() > 0);
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

        // Keep our own log index fed from real replication progress while we lead - this is
        // the leader-side half of the log-recency check's data source (the follower half is
        // ReplicationManager's onLogIndexUpdate, wired in PoppyDB). Piggybacked on the existing
        // heartbeat cadence rather than a new timer. currentTerm is still passed through as the
        // log term for bookkeeping/future use, but isLogAtLeastAsUpToDate no longer reads it -
        // see that method's javadoc for why term ordering across nodes isn't meaningful here.
        updateLogIndex(localSequenceSupplier.getAsLong(), currentTerm.get());

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
            // capture before any mutation below - becomeFollower() and the heartbeat
            // handling both overwrite currentLeader, which would hide the change
            String previousLeader = currentLeader;

            log.trace("{} received appendEntries from {} (term={}, myTerm={})",
                    myAddress, request.getLeaderId(), requestTerm, myTerm);

            // If request term is higher, update our term and become follower
            if (requestTerm > myTerm) {
                log.info("{} discovered higher term {} from leader {}", myAddress, requestTerm, request.getLeaderId());
                becomeFollower(requestTerm, request.getLeaderId());
                myTerm = currentTerm.get();
            }

            // Candidacy restraint (D3, #306 point 3): the sender's index, advertised as
            // prevLogIndex on every heartbeat (see sendHeartbeats), tells us whether the
            // cluster has real data even while our own lastLogIndex is still 0. Recorded even
            // for heartbeats we reject below as stale-term: the data behind them is real, and
            // an inflated own term (the pre-#306 livelock) must not blind this node to the
            // fact that a data-bearing peer exists.
            recordPeerLogIndex(request.getPrevLogIndex());

            // Reject if term is lower
            if (requestTerm < myTerm) {
                log.debug("{} rejecting appendEntries from {} (term {} < {})",
                        myAddress, request.getLeaderId(), requestTerm, myTerm);
                return new AppendEntriesResponse(myTerm, false, lastLogIndex.get())
                        .setFollowerId(myAddress).setPriority(takeoverPriority());
            }

            // Valid heartbeat from current leader
            lastHeartbeatTime = System.currentTimeMillis();
            currentLeader = request.getLeaderId();

            // A live leader ends any PreVote round we had open - no point finishing it.
            preVoteInProgress = false;
            preVotesReceived.clear();

            // If we were a candidate, step down
            if (state == ElectionState.CANDIDATE) {
                log.info("{} stepping down from candidate (received heartbeat from leader {})",
                        myAddress, request.getLeaderId());
                becomeFollower(requestTerm, request.getLeaderId());
            }

            // Reset election timer
            resetElectionTimer();

            // Notify of leader discovery — only on actual change to prevent flapping.
            // Compare against the leader known BEFORE this request: currentLeader has
            // already been updated above, comparing against it never fired the callback,
            // so election-mode followers never started replication.
            if (onLeaderDiscovered != null) {
                String leader = request.getLeaderId();
                if (leader != null && !leader.equals(previousLeader)) {
                    scheduler.execute(() -> onLeaderDiscovered.accept(leader));
                }
            }

            // For now, just acknowledge (log replication will be added later).
            // The priority lets the leader detect that we are a better candidate (priority takeover).
            return new AppendEntriesResponse(myTerm, true, lastLogIndex.get())
                    .setFollowerId(myAddress).setPriority(takeoverPriority());

        } finally {
            stateLock.unlock();
        }
    }

    /**
     * The priority we advertise to a leader looking for a successor: a node that must never
     * lead (arbiter, canBecomeLeader=false) reports -1, so nobody hands leadership to it.
     */
    private int takeoverPriority() {
        return config.canBecomeLeaderByPriority() ? config.getElectionPriority() : -1;
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
                peerLastContact.put(peer, System.currentTimeMillis());

                // Candidacy restraint (D3): a follower's matchIndex tells us it holds real data,
                // relevant if we ever step down and end up empty ourselves (e.g. after a resync).
                recordPeerLogIndex(response.getMatchIndex());

                // Nodes older than priority takeover omit the field and report -1
                if (response.getPriority() >= 0) {
                    peerPriorities.put(peer, response.getPriority());
                }

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

    // ==================== Priority Takeover ====================

    /**
     * Start looking for a higher-priority successor while we are leader.
     */
    private void startPriorityTakeoverCheck() {
        if (priorityTakeoverTask != null) {
            priorityTakeoverTask.cancel(false);
        }

        if (!config.isPriorityTakeoverEnabled() || peerAddresses.isEmpty()) {
            return;
        }

        int interval = config.getPriorityTakeoverCheckIntervalMs();
        priorityTakeoverTask = scheduler.scheduleAtFixedRate(
                this::checkPriorityTakeover,
                interval,
                interval,
                TimeUnit.MILLISECONDS
        );

        log.debug("{} started priority takeover check every {}ms (my priority: {})",
                myAddress, interval, config.getElectionPriority());
    }

    /**
     * Voluntarily hand leadership to a peer with higher priority, mirroring MongoDB's
     * priority takeover: without this, a failover to a lower-priority node would be permanent.
     *
     * A peer only qualifies if it is
     * <ul>
     *   <li>configured with a higher priority than ours,</li>
     *   <li>still answering our heartbeats, and</li>
     *   <li>caught up with our replication stream (see {@link #isCaughtUp}).</li>
     * </ul>
     * We only yield once we have been leader for {@code priorityTakeoverMinStabilityMs},
     * so a settling cluster does not flap.
     */
    private void checkPriorityTakeover() {
        if (!running || state != ElectionState.LEADER || !config.isPriorityTakeoverEnabled()) {
            return;
        }

        long now = System.currentTimeMillis();
        long stableFor = now - leaderSince;

        if (stableFor < config.getPriorityTakeoverMinStabilityMs()) {
            log.trace("{} leader for only {}ms, not yet eligible to yield", myAddress, stableFor);
            return;
        }

        int myPriority = config.getElectionPriority();
        long localSequence = localSequenceSupplier.getAsLong();

        // A peer that stopped answering heartbeats must not inherit leadership
        long freshnessMs = Math.max(3L * config.getHeartbeatIntervalMs(), 2000L);

        String successor = null;
        int successorPriority = myPriority;

        for (Map.Entry<String, Integer> entry : peerPriorities.entrySet()) {
            String peer = entry.getKey();
            int peerPriority = entry.getValue();

            if (peerPriority <= successorPriority) {
                continue;
            }

            Long lastContact = peerLastContact.get(peer);

            if (lastContact == null || now - lastContact > freshnessMs) {
                log.debug("{} skipping higher-priority peer {} - no heartbeat response for {}ms",
                        myAddress, peer, lastContact == null ? -1 : now - lastContact);
                continue;
            }

            if (!isCaughtUp(peer, localSequence)) {
                continue;
            }

            successor = peer;
            successorPriority = peerPriority;
        }

        if (successor == null) {
            return;
        }

        log.info("{} (priority {}) yielding leadership: {} has higher priority {} and is caught up",
                myAddress, myPriority, successor, successorPriority);

        // Refusing re-election for a while gives the successor time to win the election
        // its own (shorter, priority-adjusted) election timeout triggers.
        stepDown(config.getPriorityTakeoverStepDownSecs(), 0, true);
    }

    /**
     * Whether the peer has replicated far enough to take over without data loss.
     * Sequences are change stream tokens issued by this leader, reported back by the
     * secondaries; a peer we have no progress report for is never considered caught up.
     */
    private boolean isCaughtUp(String peer, long localSequence) {
        if (localSequence <= leaderStartSequence) {
            return true;  // we replicated nothing during our term - any peer is as up-to-date as we are
        }

        long peerSequence = peerSequenceSupplier.applyAsLong(peer);

        if (peerSequence < 0) {
            log.debug("{} skipping higher-priority peer {} - no replication progress reported", myAddress, peer);
            return false;
        }

        long lag = localSequence - peerSequence;

        if (lag > config.getPriorityTakeoverMaxLag()) {
            log.debug("{} skipping higher-priority peer {} - lagging {} events behind", myAddress, peer, lag);
            return false;
        }

        return true;
    }

    // ==================== State Persistence (#306 point 5) ====================

    /**
     * Whether election state (currentTerm, votedFor) is persisted across restarts. Requires
     * both {@link ElectionConfig#isPersistState()} and a configured path (see the config's
     * javadoc - a path given via the {@code morphiumserver.electionStatePath} system property
     * enables persistence on its own).
     */
    private boolean isPersistenceEnabled() {
        return config.isPersistState() && config.getStatePersistencePath() != null;
    }

    /**
     * Restore currentTerm/votedFor from the persistence file, if configured and present. A
     * node without a state file (first start, or persistence newly enabled) starts clean at
     * term 0 - that must keep working, so a missing or unreadable file is never fatal.
     */
    private void loadPersistedState() {
        if (!isPersistenceEnabled()) {
            return;
        }

        Path path = Path.of(config.getStatePersistencePath());

        if (!Files.exists(path)) {
            log.info("{} no persisted election state at {} - starting clean at term 0", myAddress, path);
            return;
        }

        try (InputStream in = Files.newInputStream(path)) {
            Properties props = new Properties();
            props.load(in);
            long term = Long.parseLong(props.getProperty("currentTerm", "0"));
            String voted = props.getProperty("votedFor");
            currentTerm.set(term);
            votedFor = (voted == null || voted.isEmpty()) ? null : voted;
            log.info("{} restored persisted election state from {}: term={}, votedFor={}",
                    myAddress, path, term, votedFor);
        } catch (Exception e) {
            // A corrupt state file must not keep the node from starting - Raft-wise a clean
            // term-0 restart is safe here because PreVote keeps a term-behind node from
            // disrupting the cluster while it catches its term back up.
            log.warn("{} failed to load persisted election state from {} - starting clean at term 0 ({})",
                    myAddress, path, e.toString());
            currentTerm.set(0);
            votedFor = null;
        }
    }

    /**
     * Persist currentTerm/votedFor, as Raft requires, at every point they change (term
     * adoption, self-vote on candidacy, granting a vote). Called under stateLock. Written
     * atomically (tmp file + move) so a crash mid-write leaves the previous state intact; an
     * I/O failure is logged but never takes the node down - a node that later restarts at a
     * stale/zero term is exactly what PreVote makes non-disruptive.
     */
    private void persistElectionState() {
        if (!isPersistenceEnabled()) {
            return;
        }

        Path path = Path.of(config.getStatePersistencePath());
        Properties props = new Properties();
        props.setProperty("currentTerm", Long.toString(currentTerm.get()));

        if (votedFor != null) {
            props.setProperty("votedFor", votedFor);
        }

        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }

            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");

            try (OutputStream out = Files.newOutputStream(tmp)) {
                props.store(out, "PoppyDB election state (Raft currentTerm/votedFor) - managed by ElectionManager");
            }

            try {
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (Exception e) {
            log.error("{} failed to persist election state to {}: {}", myAddress, path, e.toString());
        }
    }

    // ==================== Callbacks ====================

    /**
     * Supplies this node's current replication sequence (change stream token) while it is leader.
     * Used by the priority takeover check to measure how far a peer lags behind.
     */
    public void setLocalSequenceSupplier(LongSupplier supplier) {
        this.localSequenceSupplier = supplier != null ? supplier : () -> 0L;
    }

    /**
     * Supplies the last replication sequence a peer acknowledged, or a negative value if unknown.
     */
    public void setPeerSequenceSupplier(ToLongFunction<String> supplier) {
        this.peerSequenceSupplier = supplier != null ? supplier : peer -> -1L;
    }

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

    /**
     * Atomic snapshot of leader state — prevents inconsistent reads when
     * isLeader() and getCurrentLeader() are called separately.
     * Returns [isLeader, currentLeader] under the state lock.
     */
    public Object[] getLeaderSnapshot() {
        stateLock.lock();
        try {
            return new Object[]{state == ElectionState.LEADER, currentLeader};
        } finally {
            stateLock.unlock();
        }
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * Records the highest lastLogIndex we have observed reported by ANY peer via
     * AppendEntries/heartbeat traffic (see {@link #highestPeerLogIndexSeen}). Monotonic (max),
     * same rationale as {@link #updateLogIndex}: this is used purely as a "have we ever seen a
     * data-bearing peer" signal for the candidacy-restraint guard in {@link #becomeCandidate()},
     * so a peer's index momentarily appearing lower (e.g. it just restarted itself) must not
     * make this node newly eligible to race for an election it would still lose against that
     * same peer once it resyncs.
     */
    private void recordPeerLogIndex(long peerIndex) {
        highestPeerLogIndexSeen.updateAndGet(current -> Math.max(current, peerIndex));
    }

    /**
     * Update log index/term. Three production callers keep this fed with the real replication
     * sequence:
     * <ul>
     *   <li><b>Leader:</b> {@link #sendHeartbeats()} calls this every heartbeat with
     *       {@code localSequenceSupplier}'s current value (wired by PoppyDB to
     *       {@code driver::getChangeStreamSequence}) and {@code currentTerm} - the same supplier
     *       already used for priority-takeover catch-up checks.</li>
     *   <li><b>Follower, live events:</b> {@code ReplicationManager}'s {@code onLogIndexUpdate}
     *       hook, wired by PoppyDB in {@code startReplicationToLeader}, calls this after every
     *       applied batch with {@code lastAppliedSequence} and this node's own {@code
     *       currentTerm} (substituted for the term {@code ReplicationManager} passes, which it
     *       has no way to know).</li>
     *   <li><b>Follower, initial sync:</b> the same hook is also called once, immediately after
     *       an initial sync (full snapshot or consistency-shortcut) completes, with the sequence
     *       seeded at watch registration ({@code recordPrimarySequenceAtRegistration}). Without
     *       this a freshly-synced node that then applies zero live events would never reach the
     *       live-event call above and would keep reporting index {@code 0} to this class despite
     *       holding real data - see that call site's comment in {@code ReplicationManager} for
     *       the full mechanism.</li>
     * </ul>
     *
     * <p>Term is still passed through as {@code currentTerm} and stored in {@code lastLogTerm}
     * (harmless bookkeeping, and may serve a genuine per-log-entry term if PoppyDB ever gets a
     * real replicated log), but {@link #isLogAtLeastAsUpToDate} deliberately does NOT read it
     * for the vote decision any more - see that method's javadoc. An earlier version of this
     * comment argued the two nodes' terms were "the same basis by construction" at comparison
     * time; that argument does not hold across the actual comparison, which reads whatever
     * {@code lastLogTerm} was last written by this node's own feed (possibly stale, from a term
     * this node held before a later election it did not participate in) against the candidate's
     * {@code lastLogTerm} (same staleness problem on their side) - i.e. two independently stale
     * snapshots, not a fresh pair. Relying on term ordering there reopened the exact bug this
     * method exists to close (a once-elected, now-empty node carrying a high stale term at index
     * 0 outranking a real data-holding voter). Index-only comparison side-steps this entirely.
     *
     * <p><b>Monotonic (max) index:</b> {@code index} is only ever raised, never lowered - a call
     * with an {@code index} lower than the current value is a no-op. This is required, not just
     * defensive: the initial-sync seed above can set a real, non-zero index before this node has
     * applied or produced any live event of its own; the leader-side heartbeat feed
     * ({@link #sendHeartbeats()}) reads the LOCAL driver's change-stream sequence, which initial
     * sync deliberately runs under {@code suppressChangeStreamEvents()} and therefore never
     * advances for synced data. Without monotonic semantics, the very next heartbeat after
     * becoming leader (or the next call from either feed racing the other) would silently
     * overwrite the seeded value back down to {@code 0}, reopening the empty-node-wipe bug for
     * exactly the freshly-synced node the seed exists to protect.
     *
     * <p>Thread-safety: called from two independent, unsynchronized threads - the leader's
     * heartbeat scheduler ({@link #sendHeartbeats()}) and the follower's replication batch
     * processor (via the {@code onLogIndexUpdate} hook wired in PoppyDB). Takes {@code
     * stateLock} for the duration of the read-compare-write so it can never interleave with
     * itself or with {@link #handleVoteRequest}'s read of both fields (which already runs under
     * the same lock).
     *
     * <p>Because both process state and this in-memory field reset to {@code 0} on restart, a
     * node whose local database was just cleared for a resync (e.g. mid-{@code
     * clearLocalDatabases}) still starts back at {@code 0} - that is intentional (see the
     * users-file version gate's documented mid-resync caveat,
     * {@code docs/poppydb.md#bootstrapping-users---users-file}); it is exactly why {@link
     * #isLogAtLeastAsUpToDate} now has a real value on the other side to compare against.
     */
    public void updateLogIndex(long index, long term) {
        stateLock.lock();
        try {
            if (index >= lastLogIndex.get()) {
                lastLogIndex.set(index);
                lastLogTerm.set(term);
            }
        } finally {
            stateLock.unlock();
        }
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
     * Whether we (as leader) have heard a heartbeat ack from this peer recently enough to
     * consider it reachable - reuses the same freshness window as priority-takeover
     * eligibility (see {@link #checkPriorityTakeover} and its use of {@code peerLastContact}).
     * Only meaningful when we ARE the leader: the leader is the only role
     * that actively heartbeats every peer and tracks acks, so a follower has no independent
     * way to know whether some OTHER follower is up - it returns true (optimistic/unknown) in
     * that case.
     *
     * <p>A peer with NO contact entry at all gets a grace period of the same freshness window,
     * measured from {@code leaderSince} ({@code becomeLeader()} clears {@code peerLastContact},
     * so every peer starts entry-less on each new leadership). Within the window it is treated
     * as reachable, so a healthy peer is never falsely flagged DOWN by the startup race (first
     * heartbeat round-trip still in flight). Beyond it, no-entry means the peer has not acked a
     * single heartbeat since we became leader - the typical shape of the ex-primary that died
     * WITH the failover, which an optimistic-forever null-check would report SECONDARY for the
     * rest of this leadership (2026-08-06 review finding).
     */
    public boolean isPeerReachable(String peer) {
        if (state != ElectionState.LEADER) {
            return true;
        }

        long freshnessMs = Math.max(3L * config.getHeartbeatIntervalMs(), 2000L);
        Long lastContact = peerLastContact.get(peer);

        if (lastContact == null) {
            return System.currentTimeMillis() - leaderSince <= freshnessMs;
        }

        return System.currentTimeMillis() - lastContact <= freshnessMs;
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
        stats.put("priority", config.getElectionPriority());
        stats.put("canBecomeLeader", config.canBecomeLeaderByPriority());
        stats.put("priorityTakeoverEnabled", config.isPriorityTakeoverEnabled());
        stats.put("preVoteInProgress", preVoteInProgress);
        stats.put("statePersistenceEnabled", isPersistenceEnabled());
        if (state == ElectionState.LEADER) {
            stats.put("leaseExpiryMs", Math.max(0, leaseExpiryTime - System.currentTimeMillis()));
            stats.put("leaderSinceMs", System.currentTimeMillis() - leaderSince);
            stats.put("peerPriorities", new LinkedHashMap<>(peerPriorities));
        }
        return stats;
    }

    /**
     * Get this node's election priority.
     */
    public int getPriority() {
        return config.getElectionPriority();
    }
}
