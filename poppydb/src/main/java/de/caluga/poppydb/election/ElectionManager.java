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

    // Set when a persisted election state file EXISTS but cannot be read (#306 P1-1). This is
    // fundamentally different from a missing file: a missing file proves the node never voted
    // (first start), while an unreadable one means the node may have voted at ANY term -
    // restarting it at term 0 reopens the double-voting hole persistence exists to close
    // (PreVote protects against term inflation, not against a second vote in the same term).
    // While set, the node neither votes nor campaigns; the operator resolves it by restoring
    // the file or deliberately deleting it (accepting a clean start) and restarting.
    private volatile boolean stateFileUnreadable = false;

    // Set to false while this node's local data is known to be incomplete (a partial restore,
    // see PoppyDBCLI). Restoring does not advance the change-stream sequence, so after a full
    // cluster restart every node reports index 0 and the index-based restraint below cannot
    // tell an intact node from a gutted one - this flag can. Cleared again once an
    // authoritative initial sync has given us a complete copy.
    private volatile boolean dataComplete = true;

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

    // Round correlation for vote traffic (#306 P1-3): every batch of outgoing (Pre)Vote
    // requests - one PreVote round, or one real election - gets the next id stamped onto its
    // requests (sender-local only, never serialized; see VoteRequest#roundId). A response is
    // only credited to the tally if the request it answers carries the CURRENT id, so a grant
    // straggling in from an earlier round can never combine with the self-vote into a fake
    // majority. Guarded by stateLock.
    private long voteRoundId = 0;

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

        // #306 P1-1: an existing but unreadable state file means our own currentTerm/votedFor
        // are unknown - campaigning would run on made-up state (see the field's comment).
        if (stateFileUnreadable) {
            log.warn("{} holding back candidacy: persisted election state exists but is unreadable - "
                    + "restore or delete the state file and restart to re-enable elections", myAddress);
            resetElectionTimer();
            return false;
        }

        // A partially restored node must never win: restoring does not advance the
        // change-stream sequence, so after a cluster-wide restart every node - intact or not -
        // reports index 0, and the index comparison below cannot separate them. Such a node
        // becoming primary would push its incomplete state onto the intact nodes through their
        // initial sync. It still VOTES (see handleVoteRequest), because the intact nodes need
        // its vote to reach a majority; it only refrains from winning itself.
        //
        // EXCEPT with no peers (#306 review round 2): the guard's only release path is a
        // completed initial sync FROM a primary - a peer-less node can never have one, so the
        // hold-back would be a permanent deadlock (a single-node RS with one broken dump file
        // never serves again, and no runtime command lifts the guard). There is also nothing
        // the guard protects there: no intact peer whose data a partial primary could wipe.
        if (!dataComplete && !peerAddresses.isEmpty()) {
            log.warn("{} holding back candidacy: local data is incomplete (partial restore) - "
                    + "waiting for an authoritative sync before taking part in elections", myAddress);
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
                    config.getElectionPriority()).setPreVote(true).setRoundId(++voteRoundId);

            log.debug("{} starting PreVote round {} at term {} ({} peers)",
                    myAddress, voteRoundId, term, peerAddresses.size());

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
            String previousVotedFor = votedFor;
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

            // The self-vote is a vote like any other (#306 P1-1): (newTerm, votedFor=self)
            // must be durable BEFORE the first RequestVote leaves, or a crashed candidate can
            // forget it campaigned and grant its vote to somebody else in the very same term.
            // On failure the whole candidacy is rolled back - term included, since an
            // unpersisted term increment would be forgotten by the same crash - and retried on
            // the next election timeout like any other failed round.
            if (!persistElectionState()) {
                log.error("{} aborting candidacy at term {}: election state could not be persisted - "
                        + "an unpersisted self-vote could be cast twice after a crash", myAddress, newTerm);
                currentTerm.decrementAndGet();
                state = ElectionState.FOLLOWER;
                votedFor = previousVotedFor;
                electionInProgress = false;
                votesReceived.clear();
                resetElectionTimer();
                return;
            }

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

        // Fresh round id for the real election too (called under stateLock from
        // becomeCandidate): any response still in flight for the PreVote round that led here
        // must not be credited to this election's tally.
        VoteRequest request = new VoteRequest(term, myAddress, lastLogIndex.get(), lastLogTerm.get(),
                config.getElectionPriority()).setRoundId(++voteRoundId);

        log.debug("{} requesting votes for term {} from {} peers (round {})",
                myAddress, term, peerAddresses.size(), voteRoundId);

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
            // #306 P1-1: our own currentTerm/votedFor are unknown (existing but unreadable
            // state file) - we may already have voted in this or any term, so granting
            // anything (a real vote, or a PreVote that leads to one) risks double voting.
            // Denied before the PreVote dispatch so both request kinds are covered.
            if (stateFileUnreadable) {
                log.warn("{} denying {} to {}: persisted election state exists but is unreadable - "
                        + "this node cannot know whether it already voted (restore or delete the "
                        + "state file and restart to re-enable elections)",
                        myAddress, request.isPreVote() ? "PreVote" : "vote", request.getCandidateId());
                return new VoteResponse(currentTerm.get(), false, myAddress);
            }

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
                String previousVotedFor = votedFor;
                votedFor = request.getCandidateId();

                // Raft: votedFor must be durable before the response leaves - and that is only
                // true if the write actually succeeded (#306 P1-1). A grant backed by nothing
                // is forgotten on crash, and the restarted node votes again in the same term:
                // two leaders in one term. So a failed persist turns the grant into a denial;
                // votedFor is rolled back to its PREVIOUS value, not to null (#306 review
                // round 2): on a RETRY from the candidate we already durably voted for
                // (response lost, candidate re-asks - canVote is true via votedFor.equals), a
                // null rollback would forget that earlier, durable vote in memory and let a
                // SECOND candidate be granted the same term next - the exact double vote the
                // durability rule exists to prevent. Same pattern as becomeCandidate's
                // previousVotedFor rollback.
                if (!persistElectionState()) {
                    votedFor = previousVotedFor;
                    log.error("{} denying vote to {} for term {}: votedFor could not be persisted, "
                            + "an unpersisted vote could be cast twice after a crash",
                            myAddress, request.getCandidateId(), requestTerm);
                    return new VoteResponse(myTerm, false, myAddress);
                }

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
     *
     * @param peer        the peer that answered
     * @param sentRequest the request this response answers - held by the sending side
     *     (ElectionNetworkClient) and passed back in for round/type correlation (#306 P1-3);
     *     its {@code roundId}/{@code preVote} tell which round and which kind of round the
     *     response belongs to
     * @param response    the peer's answer
     */
    public void handleVoteResponse(String peer, VoteRequest sentRequest, VoteResponse response) {
        stateLock.lock();
        try {
            long responseTerm = response.getTerm();
            long myTerm = currentTerm.get();

            // A higher term in ANY response - current round or stale - is authoritative news
            // about the cluster and is honored BEFORE round correlation: Raft demands falling
            // back to follower on discovering a higher term, whichever RPC delivered it. Timer
            // handling mirrors the request side: an actual campaigning candidate re-arms its
            // timer here, while a follower catching its term up mid-PreVote must not defer its
            // own next candidacy (see becomeFollower's resetTimer javadoc). A demoted LEADER
            // must re-arm too (#306 review round 2): becomeLeader() cancelled its election
            // timer, and the timer is one-shot - demoting it with resetTimer=false leaves the
            // node with neither heartbeats nor a timer, so it would never campaign again (a
            // dead node if the higher-term peer never makes contact). Every other
            // leader-demotion path (handleAppendEntries, handleAppendEntriesResponse, lease
            // expiry) re-arms; this straggler-response path must not be the exception.
            if (responseTerm > myTerm) {
                log.info("{} discovered higher term {} from vote response, updating from {}",
                        myAddress, responseTerm, myTerm);
                becomeFollower(responseTerm, null,
                        state == ElectionState.CANDIDATE || state == ElectionState.LEADER);
                return;
            }

            // Round correlation (#306 P1-3): only a response answering a request of the
            // CURRENT round may be tallied. Without this, every incoming grant used to be
            // credited to whatever round happened to be open - in a three-node set the
            // self-vote plus ONE grant straggling in from an earlier round is already a
            // "majority", starting a real election (term bump included) that no current peer
            // agreed to. sentRequest also carries the request TYPE, so a late answer to a real
            // vote request can no longer masquerade as a PreVote confirmation (and vice versa
            // - PreVote and real rounds never share a round id).
            if (sentRequest == null || sentRequest.getRoundId() != voteRoundId) {
                log.debug("{} discarding stale vote response from {} (answers round {}, current round is {})",
                        myAddress, peer, sentRequest == null ? "unknown" : sentRequest.getRoundId(), voteRoundId);
                return;
            }

            if (sentRequest.isPreVote()) {
                if (!preVoteInProgress || state == ElectionState.CANDIDATE) {
                    // The round is already decided (majority reached while later probes of the
                    // same batch were still being sent/answered) - nothing left to tally.
                    log.trace("{} ignoring PreVote response from {} (round already closed)", myAddress, peer);
                    return;
                }

                // NOTE deliberately NO lower-term discard here: a voter whose term is behind
                // ours may legitimately pre-grant (PreVote adopts no terms on either side), so
                // within the current round its response term being lower means nothing.
                // Staleness is exactly what the round check above already rules out.
                if (response.isVoteGranted()) {
                    preVotesReceived.add(peer);
                    log.debug("{} received PreVote grant from {} (total: {})", myAddress, peer, preVotesReceived.size());
                    checkPreVoteMajority();
                } else {
                    log.debug("{} PreVote denied by {}", myAddress, peer);
                }
                return;
            }

            // Real vote response - only meaningful while the election it belongs to is live
            if (state != ElectionState.CANDIDATE) {
                log.trace("{} ignoring vote response (not a candidate)", myAddress);
                return;
            }

            // Ignore votes from old terms (e.g. the network client's synthesized term-0 denial
            // for an unreachable peer): a real grant echoes the campaign term.
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
     * Restore currentTerm/votedFor from the persistence file, if configured and present. Two
     * very different "no usable state" cases (#306 P1-1):
     * <ul>
     *   <li><b>File missing</b> - first start, or persistence newly enabled. The node has
     *       provably never voted, so a clean start at term 0 is correct and harmless.</li>
     *   <li><b>File exists but is unreadable</b> - the node HAS persisted state at some point
     *       and may have voted at any term. Resetting to term 0 here would let it vote a
     *       second time in a term it already voted in (double voting - PreVote only guards
     *       against term inflation, not against that). The node still starts and serves data,
     *       but {@link #stateFileUnreadable} keeps it out of elections entirely until an
     *       operator restores the file or deliberately deletes it (accepting a clean start).</li>
     * </ul>
     *
     * <p>A third case is neither: a <b>legacy pre-checksum file</b> (no {@code checksum} key)
     * is a complete write of an older build and is accepted and immediately rewritten in the
     * current format - see the inline comment for why it is distinguishable from truncation.
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

            // Mandatory schema (#306 P1-1 follow-up): every checksum-era write of ours contains
            // ALL THREE keys - currentTerm, votedFor (explicitly empty when null) and a
            // checksum over both. Properties.load() parses an empty or truncated file without
            // complaint, and a getProperty default would quietly turn "file lost its content"
            // into "term 0, never voted" - the exact reset the quarantine exists to prevent. A
            // file missing any key, or failing the checksum, is not a complete write of ours
            // and is quarantined like an unparsable one.
            String termProp = props.getProperty("currentTerm");
            String votedProp = props.getProperty("votedFor");
            String checksumProp = props.getProperty("checksum");

            // currentTerm is mandatory in EVERY format generation - a file without it (e.g. an
            // empty one) is never a complete write of ours.
            if (termProp == null) {
                throw new IllegalStateException("incomplete state file: currentTerm missing");
            }

            long term = Long.parseLong(termProp.trim());

            if (checksumProp == null) {
                // Legacy pre-checksum write (up to e26d5ad98): currentTerm always present,
                // votedFor only when non-null, no checksum. A missing checksum KEY is the
                // legacy signature, not a truncation - checksum-era files are written
                // atomically (tmp+move) and cannot lose single lines short of filesystem-level
                // corruption, which their checksum then catches. Quarantining legacy files
                // bricked a whole RS on upgrade (2026-08-17 testrunner incident: all three
                // nodes with intact legacy state "holding back candidacy" -> no primary).
                String legacyVoted = (votedProp == null || votedProp.isEmpty()) ? null : votedProp;
                currentTerm.set(term);
                votedFor = legacyVoted;
                log.warn("{} restored LEGACY (pre-checksum) election state from {}: term={}, "
                        + "votedFor={} - rewriting it in the current three-key format",
                        myAddress, path, term, legacyVoted);

                // One-time migration: rewrite immediately, or the file stays outside the
                // checksum protection until the next term increase (becomeFollower only
                // persists when the term actually changes). Safe without stateLock: start()
                // runs single-threaded, the scheduler does not exist yet. Best-effort - the
                // in-memory state is correct either way, and the vote-granting paths enforce
                // durability themselves.
                if (!persistElectionState()) {
                    log.warn("{} could not rewrite the legacy election state file {} - it stays in "
                            + "the legacy format and will be migrated on the next successful persist",
                            myAddress, path);
                }
                return;
            }

            if (votedProp == null) {
                throw new IllegalStateException(
                        "incomplete state file: votedFor missing from a checksum-era write");
            }

            String voted = votedProp.isEmpty() ? null : votedProp;

            if (!checksumProp.equals(stateChecksum(term, voted))) {
                throw new IllegalStateException("state file checksum mismatch (stored " + checksumProp
                        + ", computed " + stateChecksum(term, voted) + ")");
            }

            currentTerm.set(term);
            votedFor = voted;
            log.info("{} restored persisted election state from {}: term={}, votedFor={}",
                    myAddress, path, term, votedFor);
        } catch (Exception e) {
            stateFileUnreadable = true;
            log.error("{} persisted election state at {} EXISTS but cannot be read ({}) - this node "
                    + "will neither vote nor stand for election, because it cannot know whether it "
                    + "already voted (a term-0 restart could double-vote). The node still starts and "
                    + "serves data. To re-enable elections: restore the file from a backup, or delete "
                    + "it to deliberately start clean at term 0, then restart this node.",
                    myAddress, path, e.toString());
        }
    }

    /**
     * Integrity checksum over the two persisted values (CRC32 of {@code term|votedFor}).
     * Detects truncated or tampered state files that still parse as valid Properties - see
     * the mandatory-schema comment in {@link #loadPersistedState()}.
     */
    private static String stateChecksum(long term, String votedFor) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update((term + "|" + (votedFor == null ? "" : votedFor))
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return Long.toHexString(crc.getValue());
    }

    /**
     * Persist currentTerm/votedFor, as Raft requires, at every point they change (term
     * adoption, self-vote on candidacy, granting a vote). Called under stateLock. Written
     * atomically (tmp file + move) so a crash mid-write leaves the previous state intact.
     *
     * <p>#306 P1-1: the result is not advisory. The two callers for whom durability is the
     * whole point - granting a vote ({@link #handleVoteRequest}) and the self-vote of a
     * candidacy ({@link #becomeCandidate}) - roll their vote back and answer/abort with a
     * denial when this returns {@code false}: an unpersisted vote is forgotten by a crash and
     * can then be cast a second time in the same term (two leaders in one term). Bare term
     * adoption ({@link #becomeFollower}) stays best-effort: losing an adopted term to a crash
     * costs no safety (votedFor is null for the new term until a grant, and every grant
     * re-persists both values or is denied), and PreVote keeps the term-behind restart
     * non-disruptive.
     *
     * @return true if the state is durable (or persistence is not configured - nothing was
     *         promised), false if the write failed
     */
    private boolean persistElectionState() {
        if (!isPersistenceEnabled()) {
            return true;
        }

        // Quarantine guard (#306 P1-1 follow-up): while the existing state file is unreadable,
        // NOTHING may write it. A higher-term heartbeat runs becomeFollower() ->
        // persistElectionState(), and writing here would replace the broken file with the
        // made-up in-memory state - perfectly readable on the next restart, silently lifting
        // the quarantine while the unknown earlier vote stays lost. The broken file is also
        // the operator's evidence of what happened; it must survive untouched until the
        // operator restores or deliberately deletes it.
        if (stateFileUnreadable) {
            log.debug("{} not persisting election state: the existing state file is unreadable and is "
                    + "preserved untouched for the operator (see the startup ERROR)", myAddress);
            return false;
        }

        Path path = Path.of(config.getStatePersistencePath());
        Properties props = new Properties();
        // Mandatory schema, mirrored by loadPersistedState(): all three keys are ALWAYS
        // written - votedFor explicitly empty (not omitted) when null - so a file missing any
        // of them is provably not a complete write of ours and gets quarantined on load.
        props.setProperty("currentTerm", Long.toString(currentTerm.get()));
        props.setProperty("votedFor", votedFor == null ? "" : votedFor);
        props.setProperty("checksum", stateChecksum(currentTerm.get(), votedFor));

        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }

            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");

            try (OutputStream out = Files.newOutputStream(tmp)) {
                props.store(out, "PoppyDB election state (Raft currentTerm/votedFor) - managed by ElectionManager");
            }

            // "Durable" has to hold across power/kernel failures too, not just JVM crashes
            // (#306 P2): force the tmp file's bytes to storage BEFORE the rename makes them the
            // authoritative state, and force the parent directory afterwards so the rename
            // itself (a directory-metadata operation) is on disk as well. The directory fsync
            // is best-effort: opening a directory for fsync is not supported on every platform
            // (e.g. Windows), and a failed directory sync must not turn an otherwise-persisted
            // vote into a denial on those platforms.
            try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(tmp,
                    java.nio.file.StandardOpenOption.WRITE)) {
                ch.force(true);
            }

            try {
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
            }

            if (path.getParent() != null) {
                try (java.nio.channels.FileChannel dir = java.nio.channels.FileChannel.open(path.getParent(),
                        java.nio.file.StandardOpenOption.READ)) {
                    dir.force(true);
                } catch (Exception e) {
                    log.debug("{} could not fsync election state directory {} ({}) - rename durability "
                            + "depends on the platform's rename semantics", myAddress, path.getParent(), e.toString());
                }
            }

            return true;
        } catch (Exception e) {
            log.error("{} failed to persist election state to {}: {}", myAddress, path, e.toString());
            return false;
        }
    }

    // ==================== Callbacks ====================

    /**
     * Marks whether this node's local data is complete. A node that restored only part of its
     * databases must not become primary (it would overwrite intact peers via their initial
     * sync), so it stays out of candidacy until an authoritative sync completes. Voting is
     * unaffected - a cluster-wide restart still needs this node's vote to reach a majority.
     */
    public void setDataComplete(boolean complete) {
        boolean was = this.dataComplete;
        this.dataComplete = complete;

        if (was != complete) {
            log.info("{} data completeness changed to {}", myAddress, complete ? "COMPLETE" : "INCOMPLETE");
        }
    }

    /** Whether this node considers its local data complete enough to stand for election. */
    public boolean isDataComplete() {
        return dataComplete;
    }

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
        stats.put("stateFileUnreadable", stateFileUnreadable);
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
