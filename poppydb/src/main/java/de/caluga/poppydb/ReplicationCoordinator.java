package de.caluga.poppydb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Coordinates replication between primary and secondary PoppyDB nodes.
 * Tracks write sequence numbers and waits for replication acknowledgment.
 *
 * <p>Waiting for a write concern does not occupy a thread: {@link #awaitReplication} registers a
 * waiter and returns, a progress report or the shared timer completes it. Before, every
 * unacknowledged w&gt;=2 write held one of the few command-pool threads for its full timeout and
 * the answers of all later writes queued behind those waits. The number of waiting writes is now
 * bounded by memory, and one wait never delays the answer of another.
 */
public class ReplicationCoordinator {

    /** How a write-concern wait ended. See {@link #awaitReplication}. */
    public enum WaitOutcome {
        /** Enough live secondaries had reported the write's sequence. */
        ACKNOWLEDGED,
        /** The wait's timeout elapsed first. */
        TIMEOUT,
        /** No secondary had ever reported to this node - the concern cannot be satisfied at all. */
        NO_SECONDARIES,
        /** This node stopped being primary: its secondaries report to another node now. */
        STEPPED_DOWN
    }

    private static final Logger log = LoggerFactory.getLogger(ReplicationCoordinator.class);

    /** A secondary counts as live while its last report is younger than this. */
    private static final long SECONDARY_STALE_MS = 30000;

    /** Grace before a wait started with no secondary in sight is given up as NO_SECONDARIES. */
    private static final long NO_SECONDARIES_GRACE_MS = 100;

    /** At most one summary WARN in this interval while write-concern timeouts continue. */
    private static final long TIMEOUT_SUMMARY_INTERVAL_MS = 10000;

    /** A failed wait after this long without one opens a new episode (a detailed WARN again). */
    private static final long TIMEOUT_EPISODE_QUIET_MS = 60000;

    /**
     * One daemon thread drives every write-concern deadline in the process - a wait is a
     * registered waiter plus a scheduled task, never a blocked thread. Shared (static) rather than
     * per instance because coordinators come and go with every leadership change: nothing to stop
     * in {@link #close()}, nothing to leak. Its tasks only complete waiters; the callbacks they
     * run must not block (the server's callback hands the answer to the channel's event loop).
     * Cancelled tasks are removed at once, so acknowledged waits do not pile up in the queue
     * until their deadline.
     */
    private static final ScheduledThreadPoolExecutor WAIT_TIMER = createWaitTimer();

    private static ScheduledThreadPoolExecutor createWaitTimer() {
        ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "PoppyDB-wc-waits");
            t.setDaemon(true);
            return t;
        });
        timer.setRemoveOnCancelPolicy(true);
        return timer;
    }

    // Current write sequence number (incremented on each write)
    private final AtomicLong writeSequence = new AtomicLong(0);

    // Map of secondary address -> last acknowledged sequence number
    private final Map<String, Long> secondaryProgress = new ConcurrentHashMap<>();

    // Map of secondary address -> last heartbeat time
    private final Map<String, Long> secondaryHeartbeats = new ConcurrentHashMap<>();

    // Registered write-concern waits, ordered by the sequence they wait for: a report only has to
    // look at the waiters up to the sequence it covers, and the coverage only ever shrinks with a
    // growing sequence (a longer wait needs at least the secondaries of a shorter one).
    private final ReentrantLock waitersLock = new ReentrantLock();
    private final TreeMap<Long, List<Waiter>> waiters = new TreeMap<>();
    // How many registered waiters need how many secondaries (guarded by waitersLock): its lowest
    // key ends a report's scan as soon as the covering count drops below it.
    private final TreeMap<Integer, Integer> registeredByRequired = new TreeMap<>();

    // Configuration
    private final int replicaSetSize;
    private final long defaultWriteConcernTimeoutMs;

    // Cumulative counters for serverStatus; shared across coordinator instances when handed in by
    // PoppyDB, so they survive the coordinator replacement on a leadership change.
    private final WriteConcernWaitStats waitStats;

    // The primary's current sequence for the timeout WARN: the driver's change-stream sequence
    // when handed in by PoppyDB (writeSequence is not advanced by the server's write path).
    private final LongSupplier primarySequence;

    // Set when this node stops being primary. No secondary reports to a closed coordinator any
    // more, so nothing waiting on it can ever be acknowledged - see close().
    private volatile boolean closed = false;

    // Write-concern timeout logging state (guarded by timeoutLogLock, never by waitersLock).
    private final Object timeoutLogLock = new Object();
    private volatile long timeoutEpisodeStartedAtMs = 0; // 0 = no open timeout episode
    private long lastTimeoutSummaryAtMs = 0;
    private long lastFailedWaitAtMs = 0;
    private long timeoutsSinceLastSummary = 0;
    private long noSecondariesSinceLastSummary = 0;
    private long failedWaitsInEpisode = 0;

    public ReplicationCoordinator(int replicaSetSize) {
        this(replicaSetSize, 10000); // Default 10 second timeout
    }

    public ReplicationCoordinator(int replicaSetSize, long defaultWriteConcernTimeoutMs) {
        this(replicaSetSize, defaultWriteConcernTimeoutMs, null, null);
    }

    /**
     * @param waitStats       cumulative counters to record into - hand in the one owned by the
     *                        server, so they survive the replacement of the coordinator on a
     *                        leadership change; null = private counters
     * @param primarySequence the primary's current sequence, for the timeout log; null = this
     *                        coordinator's own {@link #getCurrentSequence()}
     */
    public ReplicationCoordinator(int replicaSetSize, long defaultWriteConcernTimeoutMs,
                                  WriteConcernWaitStats waitStats, LongSupplier primarySequence) {
        this.replicaSetSize = replicaSetSize;
        this.defaultWriteConcernTimeoutMs = defaultWriteConcernTimeoutMs;
        this.waitStats = waitStats == null ? new WriteConcernWaitStats() : waitStats;
        this.primarySequence = primarySequence == null ? writeSequence::get : primarySequence;
    }

    /**
     * Called after each write operation on the primary.
     * Returns the sequence number for this write.
     */
    public long recordWrite() {
        return writeSequence.incrementAndGet();
    }

    /**
     * Get the current write sequence number.
     */
    public long getCurrentSequence() {
        return writeSequence.get();
    }

    /**
     * Last sequence number the given secondary reported, or -1 if it never reported progress.
     */
    public long getAcknowledgedSequence(String secondaryAddress) {
        Long seq = secondaryProgress.get(secondaryAddress);
        return seq == null ? -1L : seq;
    }

    /**
     * Called by secondaries to report their replication progress.
     */
    public void reportProgress(String secondaryAddress, long sequenceNumber) {
        reportProgressAt(secondaryAddress, sequenceNumber, System.currentTimeMillis());
    }

    /**
     * Same report, with the heartbeat it refreshes stated explicitly. Package-visible for tests
     * that need a secondary whose last report is old (the liveness rule is a 30s window).
     */
    void reportProgressAt(String secondaryAddress, long sequenceNumber, long reportedAtMs) {
        Long previous = secondaryProgress.put(secondaryAddress, sequenceNumber);
        secondaryHeartbeats.put(secondaryAddress, reportedAtMs);

        if (previous == null || sequenceNumber > previous) {
            log.debug("Secondary {} reported progress: seq={}", secondaryAddress, sequenceNumber);
        }

        completeAcknowledgedWaiters();
    }

    /**
     * Wait for a write to be replicated to the specified number of nodes, without blocking the
     * caller's thread. The callback runs exactly once - whatever races between progress report,
     * timeout and {@link #close()} - and always outside the coordinator's lock.
     *
     * <p>{@code w <= 1} and a sequence the live secondaries already cover complete synchronously,
     * on the calling thread.
     *
     * @param sequenceNumber the sequence number of the write to wait for
     * @param w              number of nodes that must acknowledge (including the primary)
     * @param timeoutMs      maximum time to wait in milliseconds (0 = use the default)
     * @param startedAtMs    {@link System#currentTimeMillis()} at the time of the write - the
     *                       timeout counts from there, not from the moment this method is entered
     * @param callback       receives the outcome; may run on the calling thread, on the timer
     *                       thread, or on the thread of a reporting secondary
     */
    public void awaitReplication(long sequenceNumber, int w, long timeoutMs, long startedAtMs,
                                 Consumer<WaitOutcome> callback) {
        if (callback == null) {
            return;
        }

        if (w <= 1) {
            // w=1 means primary only, no need to wait
            callback.accept(WaitOutcome.ACKNOWLEDGED);
            return;
        }

        long timeout = timeoutMs > 0 ? timeoutMs : defaultWriteConcernTimeoutMs;
        long deadline = startedAtMs + timeout;
        Waiter waiter = new Waiter(sequenceNumber, w, w - 1, System.currentTimeMillis(), callback);

        boolean registered = false;
        WaitOutcome immediate = null;
        boolean armGrace = false;

        waitersLock.lock();
        try {
            int acknowledgedCount = countAcknowledgedSecondaries(sequenceNumber);

            if (closed) {
                log.debug("Write seq={} not acknowledged: this node is no longer primary", sequenceNumber);
                immediate = WaitOutcome.STEPPED_DOWN;
            } else if (acknowledgedCount >= waiter.requiredSecondaries) {
                log.debug("Write seq={} acknowledged by {} secondaries", sequenceNumber, acknowledgedCount);
                immediate = WaitOutcome.ACKNOWLEDGED;
            } else if (deadline - System.currentTimeMillis() <= 0) {
                // The timeout was spent before this wait was even registered.
                immediate = WaitOutcome.TIMEOUT;
            } else {
                // Fail fast if no secondaries have ever reported and the timeout is long - this
                // used to be a 100ms sleep on a pool thread, it is a grace timer now.
                armGrace = secondaryHeartbeats.isEmpty() && timeout > NO_SECONDARIES_GRACE_MS;
                register(waiter);
                registered = true;
            }
        } finally {
            waitersLock.unlock();
        }

        if (!registered) {
            waiter.done.set(true);
            finish(waiter, immediate);
            return;
        }

        // Timers are armed after registration, outside the lock. A report or close() that
        // completes the waiter in between is not undone by them (expire() is CAS-guarded); the
        // re-check below only spares the timer queue tasks that have nothing left to do.
        if (!waiter.done.get()) {
            waiter.deadlineTask = WAIT_TIMER.schedule(() -> expire(waiter, WaitOutcome.TIMEOUT),
                    Math.max(0, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
            if (armGrace) {
                waiter.graceTask = WAIT_TIMER.schedule(() -> {
                    if (secondaryHeartbeats.isEmpty()) {
                        expire(waiter, WaitOutcome.NO_SECONDARIES);
                    }
                }, NO_SECONDARIES_GRACE_MS, TimeUnit.MILLISECONDS);
            }
            if (waiter.done.get()) {
                cancelTimers(waiter);
            }
        }
    }

    /**
     * Wait for a write to be replicated to the specified number of nodes.
     *
     * @param sequenceNumber The sequence number of the write to wait for
     * @param w              Number of nodes that must acknowledge (including primary)
     * @param timeoutMs      Maximum time to wait in milliseconds (0 = use default)
     * @return true if replication was acknowledged, false if timeout
     */
    public boolean waitForReplication(long sequenceNumber, int w, long timeoutMs) {
        return waitForReplication(sequenceNumber, w, timeoutMs, System.currentTimeMillis());
    }

    /**
     * Same wait, with the timeout counted from {@code startedAtMs} (the moment the write was
     * executed) instead of from the moment this method is entered: a wait that already spent part
     * - or all - of its timeout elsewhere must not wait a full timeout on top of it.
     *
     * <p>Thin blocking wrapper around {@link #awaitReplication} for embedded callers and tests;
     * the server itself uses the non-blocking entry point.
     *
     * @param startedAtMs {@link System#currentTimeMillis()} at the time of the write
     */
    public boolean waitForReplication(long sequenceNumber, int w, long timeoutMs, long startedAtMs) {
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<WaitOutcome> outcome = new AtomicReference<>(WaitOutcome.TIMEOUT);

        awaitReplication(sequenceNumber, w, timeoutMs, startedAtMs, result -> {
            outcome.set(result);
            completed.countDown();
        });

        try {
            completed.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

        return outcome.get() == WaitOutcome.ACKNOWLEDGED;
    }

    /**
     * Ends every wait on this coordinator, running or still to come, as {@code STEPPED_DOWN}.
     * Called when the node stops being primary: its secondaries then follow another primary and
     * report their progress to that node's coordinator (or, after a failback, to the NEW
     * coordinator of this node), so a wait on this instance could only run into its timeout. On
     * the fixed command pool those dead waits blocked every thread for a full timeout each and
     * held up the answers of all writes queued behind them (prod, 2026-11-20 and 2026-11-23).
     */
    public void close() {
        List<Waiter> released;
        waitersLock.lock();
        try {
            closed = true;
            released = new ArrayList<>();
            for (List<Waiter> waiting : waiters.values()) {
                for (Waiter waiter : waiting) {
                    if (waiter.done.compareAndSet(false, true)) {
                        released.add(waiter);
                    }
                }
            }
            waiters.clear();
            registeredByRequired.clear();
        } finally {
            waitersLock.unlock();
        }

        for (Waiter waiter : released) {
            finish(waiter, WaitOutcome.STEPPED_DOWN);
        }

        log.info("Replication coordinator closed: released {} pending write-concern wait(s) as PrimarySteppedDown",
                released.size());
    }

    /**
     * Wait for "majority" write concern.
     */
    public boolean waitForMajority(long sequenceNumber, long timeoutMs) {
        int majority = (replicaSetSize / 2) + 1;
        return waitForReplication(sequenceNumber, majority, timeoutMs);
    }

    /**
     * The {@code serverStatus.writeConcernWaits} section: waits registered right now, how long
     * the oldest one has been waiting, and the cumulative outcomes since process start.
     */
    public Map<String, Object> getWaitStats() {
        int waiting = 0;
        long oldestWaitMs = 0;
        long now = System.currentTimeMillis();

        waitersLock.lock();
        try {
            for (List<Waiter> waitingList : waiters.values()) {
                for (Waiter waiter : waitingList) {
                    waiting++;
                    oldestWaitMs = Math.max(oldestWaitMs, now - waiter.registeredAtMs);
                }
            }
        } finally {
            waitersLock.unlock();
        }

        return waitStats.snapshot(waiting, oldestWaitMs);
    }

    /** Test hook: deadline/grace tasks queued in the shared timer (all coordinators). */
    static int pendingTimerTasksForTest() {
        return WAIT_TIMER.getQueue().size();
    }

    /** Test hook: whether the calling thread holds the waiter registry's lock. */
    boolean isWaitersLockHeldByCurrentThread() {
        return waitersLock.isHeldByCurrentThread();
    }

    // ---- waiter bookkeeping ----

    private void register(Waiter waiter) {
        waiters.computeIfAbsent(waiter.sequenceNumber, seq -> new ArrayList<>(1)).add(waiter);
        registeredByRequired.merge(waiter.requiredSecondaries, 1, Integer::sum);
    }

    /** Bookkeeping for a waiter that left the registry, under {@link #waitersLock}. */
    private void forgetRequired(Waiter waiter) {
        registeredByRequired.computeIfPresent(waiter.requiredSecondaries, (k, n) -> n > 1 ? n - 1 : null);
    }

    /**
     * Removes a waiter from the registry, under the caller's hold of {@link #waitersLock}.
     */
    private void unregister(Waiter waiter) {
        List<Waiter> waiting = waiters.get(waiter.sequenceNumber);
        if (waiting == null) {
            return;
        }

        if (waiting.remove(waiter)) {
            forgetRequired(waiter);
        }
        if (waiting.isEmpty()) {
            waiters.remove(waiter.sequenceNumber);
        }
    }

    /**
     * Completes every registered waiter whose sequence the live secondaries now cover.
     */
    private void completeAcknowledgedWaiters() {
        List<Waiter> released = null;
        waitersLock.lock();
        try {
            if (!waiters.isEmpty()) {
                released = collectAcknowledgedWaiters();
            }
        } finally {
            waitersLock.unlock();
        }

        if (released == null || released.isEmpty()) {
            return;
        }

        for (Waiter waiter : released) {
            finish(waiter, WaitOutcome.ACKNOWLEDGED);
        }
    }

    /**
     * Under the lock: takes every waiter the current progress covers out of the registry and
     * marks it done, so a concurrent timeout cannot complete it as well.
     */
    private List<Waiter> collectAcknowledgedWaiters() {
        List<Waiter> released = new ArrayList<>();

        Iterator<Map.Entry<Long, List<Waiter>>> it = waiters.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, List<Waiter>> entry = it.next();
            // Coverage only shrinks with a growing sequence: once it is below what the least
            // demanding registered waiter needs, no later sequence can complete anything either.
            int acknowledgedCount = countAcknowledgedSecondaries(entry.getKey());
            if (registeredByRequired.isEmpty() || acknowledgedCount < registeredByRequired.firstKey()) {
                break;
            }

            // A waiter a timer already marked done stays until that timer unregisters it.
            entry.getValue().removeIf(waiter -> {
                if (acknowledgedCount >= waiter.requiredSecondaries
                        && waiter.done.compareAndSet(false, true)) {
                    released.add(waiter);
                    forgetRequired(waiter);
                    return true;
                }
                return false;
            });

            if (entry.getValue().isEmpty()) {
                it.remove();
            }
        }

        return released;
    }

    /**
     * Completes a registered waiter from a timer: whoever marks it done first - this, a progress
     * report or {@link #close()} - is the one that runs its callback.
     */
    private void expire(Waiter waiter, WaitOutcome outcome) {
        if (!waiter.done.compareAndSet(false, true)) {
            return;
        }

        waitersLock.lock();
        try {
            unregister(waiter);
        } finally {
            waitersLock.unlock();
        }

        finish(waiter, outcome);
    }

    /**
     * Records the outcome, logs it and runs the callback - never under the lock.
     */
    private void finish(Waiter waiter, WaitOutcome outcome) {
        cancelTimers(waiter);

        waitStats.record(outcome);

        switch (outcome) {
            case TIMEOUT:
            case NO_SECONDARIES:
                // Both are answered as WriteConcernFailed and both come in storms when no
                // secondary acknowledges - one rate-limited log for the two.
                logTimeout(waiter, outcome);
                break;
            case ACKNOWLEDGED:
                logAcknowledgedAgain();
                break;
            default:
                break;
        }

        // Throwable, not Exception: close() and a report complete many waiters in one loop, and
        // those are already marked done - one failing callback must not cost the rest their answer.
        try {
            waiter.callback.accept(outcome);
        } catch (Throwable e) {
            log.warn("Write-concern callback failed for seq={}: {}", waiter.sequenceNumber, e.getMessage(), e);
        }
    }

    private static void cancelTimers(Waiter waiter) {
        ScheduledFuture<?> deadlineTask = waiter.deadlineTask;
        if (deadlineTask != null) {
            deadlineTask.cancel(false);
        }
        ScheduledFuture<?> graceTask = waiter.graceTask;
        if (graceTask != null) {
            graceTask.cancel(false);
        }
    }

    /**
     * One detailed WARN when failed waits (timeout or no secondaries) start after a clean period -
     * an acknowledgement, or {@link #TIMEOUT_EPISODE_QUIET_MS} without a failure - then at most one
     * summary WARN per {@link #TIMEOUT_SUMMARY_INTERVAL_MS}: a storm must not flood the log, but
     * the next incident has to be readable from it.
     */
    private void logTimeout(Waiter waiter, WaitOutcome outcome) {
        long now = System.currentTimeMillis();
        boolean firstInEpisode = false;
        boolean summary = false;
        long summaryTimeouts = 0;
        long summaryNoSecondaries = 0;
        long summaryWindowMs = 0;

        synchronized (timeoutLogLock) {
            if (timeoutEpisodeStartedAtMs == 0 || now - lastFailedWaitAtMs >= TIMEOUT_EPISODE_QUIET_MS) {
                timeoutEpisodeStartedAtMs = now;
                lastTimeoutSummaryAtMs = now;
                timeoutsSinceLastSummary = 0;
                noSecondariesSinceLastSummary = 0;
                failedWaitsInEpisode = 1;
                firstInEpisode = true;
            } else {
                failedWaitsInEpisode++;
                if (outcome == WaitOutcome.NO_SECONDARIES) {
                    noSecondariesSinceLastSummary++;
                } else {
                    timeoutsSinceLastSummary++;
                }

                if (now - lastTimeoutSummaryAtMs >= TIMEOUT_SUMMARY_INTERVAL_MS) {
                    summary = true;
                    summaryTimeouts = timeoutsSinceLastSummary;
                    summaryNoSecondaries = noSecondariesSinceLastSummary;
                    summaryWindowMs = now - lastTimeoutSummaryAtMs;
                    lastTimeoutSummaryAtMs = now;
                    timeoutsSinceLastSummary = 0;
                    noSecondariesSinceLastSummary = 0;
                }
            }
            lastFailedWaitAtMs = now;
        }

        if (firstInEpisode) {
            log.warn("Write concern {}: seq={}, w={}, primary sequence={}, secondaries={}",
                    outcome == WaitOutcome.NO_SECONDARIES ? "cannot be satisfied (no secondaries available)" : "timeout",
                    waiter.sequenceNumber, waiter.w, primarySequence.getAsLong(), describeSecondaries(now));
        } else if (summary) {
            log.warn("Write concern still not acknowledged: {} more timeout(s) and {} without secondaries in the last {} ms "
                            + "(latest: seq={}, w={}, primary sequence={}, secondaries={})",
                    summaryTimeouts, summaryNoSecondaries, summaryWindowMs, waiter.sequenceNumber, waiter.w,
                    primarySequence.getAsLong(), describeSecondaries(now));
        }
    }

    /** The secondaries and their last report, for the timeout WARN. */
    private String describeSecondaries(long now) {
        if (secondaryProgress.isEmpty()) {
            return "none";
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Long> entry : secondaryProgress.entrySet()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }

            Long heartbeat = secondaryHeartbeats.get(entry.getKey());
            sb.append(entry.getKey()).append(" seq=").append(entry.getValue())
                    .append(" (last report ")
                    .append(heartbeat == null ? "never" : (now - heartbeat) + " ms ago")
                    .append(')');
        }
        return sb.toString();
    }

    /** Closes a timeout episode: one INFO, once waits are acknowledged again. */
    private void logAcknowledgedAgain() {
        if (timeoutEpisodeStartedAtMs == 0) {
            return; // the common case, without taking the lock on every acknowledged write
        }

        long timeouts;
        long startedAtMs;

        synchronized (timeoutLogLock) {
            if (timeoutEpisodeStartedAtMs == 0) {
                return;
            }

            timeouts = failedWaitsInEpisode;
            startedAtMs = timeoutEpisodeStartedAtMs;
            timeoutEpisodeStartedAtMs = 0;
            failedWaitsInEpisode = 0;
            timeoutsSinceLastSummary = 0;
            noSecondariesSinceLastSummary = 0;
        }

        log.info("Write-concern waits acknowledged again after {} failed wait(s) (timeout or no secondaries) in {} s",
                timeouts, (System.currentTimeMillis() - startedAtMs) / 1000);
    }

    /**
     * Count how many secondaries have acknowledged up to the given sequence.
     */
    private int countAcknowledgedSecondaries(long sequenceNumber) {
        int count = 0;
        long now = System.currentTimeMillis();

        for (Map.Entry<String, Long> entry : secondaryProgress.entrySet()) {
            String secondary = entry.getKey();
            Long lastSeq = entry.getValue();
            Long lastHeartbeat = secondaryHeartbeats.get(secondary);

            // Check if secondary is still alive
            if (lastHeartbeat != null && (now - lastHeartbeat) < SECONDARY_STALE_MS) {
                if (lastSeq != null && lastSeq >= sequenceNumber) {
                    count++;
                }
            }
        }

        return count;
    }

    /**
     * Get the number of active secondaries.
     */
    public int getActiveSecondaryCount() {
        long now = System.currentTimeMillis();
        int count = 0;

        for (Map.Entry<String, Long> entry : secondaryHeartbeats.entrySet()) {
            if ((now - entry.getValue()) < SECONDARY_STALE_MS) {
                count++;
            }
        }

        return count;
    }

    /**
     * Remove a secondary from tracking (e.g., when it disconnects).
     */
    public void removeSecondary(String secondaryAddress) {
        secondaryProgress.remove(secondaryAddress);
        secondaryHeartbeats.remove(secondaryAddress);
        log.info("Removed secondary from tracking: {}", secondaryAddress);
    }

    /**
     * Get replication statistics.
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new ConcurrentHashMap<>();
        stats.put("currentSequence", writeSequence.get());
        stats.put("activeSecondaries", getActiveSecondaryCount());
        stats.put("replicaSetSize", replicaSetSize);

        Map<String, Object> secondaryStats = new ConcurrentHashMap<>();
        for (Map.Entry<String, Long> entry : secondaryProgress.entrySet()) {
            String secondary = entry.getKey();
            Long seq = entry.getValue();
            Long heartbeat = secondaryHeartbeats.get(secondary);
            long lag = writeSequence.get() - (seq != null ? seq : 0);

            Map<String, Object> secInfo = new ConcurrentHashMap<>();
            secInfo.put("lastSequence", seq);
            secInfo.put("lastHeartbeat", heartbeat);
            secInfo.put("lagSequences", lag);
            secondaryStats.put(secondary, secInfo);
        }
        stats.put("secondaries", secondaryStats);

        return stats;
    }

    /**
     * One write-concern wait. Mutable only in the two task handles and the done flag, which is
     * what makes "exactly one callback, whatever races" hold.
     */
    private static final class Waiter {
        private final long sequenceNumber;
        private final int w;
        private final int requiredSecondaries;
        private final long registeredAtMs;
        private final Consumer<WaitOutcome> callback;
        private final AtomicBoolean done = new AtomicBoolean(false);
        private volatile ScheduledFuture<?> deadlineTask;
        private volatile ScheduledFuture<?> graceTask;

        private Waiter(long sequenceNumber, int w, int requiredSecondaries, long registeredAtMs,
                       Consumer<WaitOutcome> callback) {
            this.sequenceNumber = sequenceNumber;
            this.w = w;
            this.requiredSecondaries = requiredSecondaries;
            this.registeredAtMs = registeredAtMs;
            this.callback = callback;
        }
    }
}
