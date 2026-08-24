package de.caluga.poppydb;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.commands.DropIndexesCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.ListIndexesCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.MongoConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.SSLContext;

/**
 * Handles replication from primary to secondary PoppyDB nodes.
 *
 * Secondaries connect to the primary and watch for changes via change streams.
 * When change events arrive, they are applied to the local InMemoryDriver.
 */
public class ReplicationManager {

    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);

    private final InMemoryDriver localDriver;
    private final String primaryHost;
    private final int primaryPort;
    // Package-visible so ReplicationEventQueueByteBudgetTest can exercise the byte-budget
    // backpressure without a live replication connection - the wait loop keys off this flag.
    final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicLong eventsApplied = new AtomicLong(0);
    private final AtomicLong lastEventTime = new AtomicLong(0);
    private final AtomicLong lastAppliedSequence = new AtomicLong(0);
    private final AtomicLong lastReportedSequence = new AtomicLong(0);
    // The primary's own change-stream sequence as observed at the most recent watch
    // registration (piggybacked on the wire as "poppyPrimarySequence", mirroring how
    // "poppyResumeSequence" rides the resumeAfter token in the other direction - see
    // watchForChanges()). Updated on every successful registration, independent of whether any
    // events are ever applied during that session. Two purposes: (1) seeds lastAppliedSequence
    // when it is still 0 so an idle session (zero applied events) still has a correct resume
    // point on the next reconnect instead of silently starting "from now"; (2) exposed via
    // getLastKnownPrimarySequence() so callers (e.g. a replicationLagEvents metric) can compare
    // it against lastAppliedSequence without needing their own copy of the wire plumbing.
    private final AtomicLong lastKnownPrimarySequence = new AtomicLong(0);
    // Number of times the primary signalled "resume window lost" and we fell back to a full re-sync.
    // Exposed for tests/metrics to distinguish a clean resume (0) from a re-sync fallback.
    private final AtomicLong resyncCount = new AtomicLong(0);
    // True while the initial-sync retry loop is refusing a destructive full re-sync (clear +
    // snapshot, or a shortcut-driven equivalent) because the primary's reported sequence at the
    // most recent watch registration is BEHIND the sequence our local data was last known to
    // reflect - see the guard in startInitialSyncOnce(). Cleared as soon as an attempt's primary
    // sequence catches back up (>= local), whether that attempt then takes the shortcut or a full
    // sync. Exposed via getStats() so operators/tests can see a node that is deliberately holding
    // onto its data rather than idly "still syncing".
    private final AtomicBoolean refusingDestructiveResync = new AtomicBoolean(false);
    // Number of times a destructive full re-sync was refused for the reason above. Monotonic
    // counter, never reset - distinguishes "never needed to refuse" from "refused N times" in
    // stats/tests, independent of the current (possibly already-cleared) refusingDestructiveResync
    // flag.
    private final AtomicLong refusedResyncCount = new AtomicLong(0);
    // Wall-clock time (System.currentTimeMillis()) of the previous resync, used to detect resyncs
    // repeating faster than the buffer can absorb (see triggerResync()). 0 = no resync yet.
    private final AtomicLong lastResyncTimestamp = new AtomicLong(0);
    // If a second resync happens within this window of the previous one, the replay buffer is not
    // keeping up with the sustained write rate x sync duration - log a WARN so an operator can size
    // the buffer (see docs/poppydb.md "Replication buffer sizing").
    private static final long RESYNC_WARN_WINDOW_MS = TimeUnit.MINUTES.toMillis(10);
    // Test hook: when true the replication loop severs its connection and stops reconnecting,
    // simulating a network partition between this secondary and the primary.
    private final AtomicBoolean pausedForTest = new AtomicBoolean(false);

    // Secondary's address for reporting back to primary
    private String myAddress;

    // volatile: written by the replication-loop thread (connect/reconnect) and read by the
    // separate initial-sync snapshot thread.
    private volatile Morphium primaryMorphium;
    private ExecutorService replicationExecutor;
    private ScheduledExecutorService progressReporter;
    // Periodic index diff (#258): change streams carry no index DDL, so createIndexes/dropIndexes
    // on the primary are picked up by diffing listIndexes at this interval (and once as part of
    // every initial sync). 30s of index lag is acceptable - the data plane is not affected.
    private static final long INDEX_SYNC_INTERVAL_MS = TimeUnit.SECONDS.toMillis(30);
    private ScheduledExecutorService indexSyncer;
    private volatile long watchCursorId = -1;

    // Initial sync state
    // Package-visible so InitialSyncElectionSeedTest can put the manager into the exact
    // post-sync state the losing interleaving produces (sync done, lastAppliedSequence still 0).
    final AtomicBoolean initialSyncComplete = new AtomicBoolean(false);
    private final CountDownLatch initialSyncLatch = new CountDownLatch(1);
    // True when the most recently COMPLETED initial sync was satisfied by the consistency
    // shortcut (dbHash comparison against the primary, see tryConsistencyShortcut()) instead of
    // a full clear + snapshot. Only meaningful once initialSyncComplete is true; reset to false
    // whenever a full snapshot completes. Package-private observable (wasLastSyncShortcut()) so
    // tests can assert the path taken without parsing logs.
    private final AtomicBoolean lastSyncWasShortcut = new AtomicBoolean(false);

    // True once clearLocalDatabases() has run during the CURRENT sync cycle (a "cycle" is one
    // invocation of startInitialSyncOnce()'s thread body, which may internally retry several
    // times - see the watchInvalidatedDuringSnapshot branch below). Reset to false at the start of
    // every fresh cycle. Guards against a misreport: if an attempt runs clearLocalDatabases() +
    // performInitialSync() but is then discarded because the watch died/re-registered mid-copy,
    // the NEXT retry iteration must NOT call tryConsistencyShortcut() again - the local data now
    // holds (at least partially) this very cycle's own full copy, so a dbHash comparison would
    // very plausibly match the primary not because the node was already consistent BEFORE this
    // cycle started, but only because this cycle's own discarded full sync made it so. That would
    // set lastSyncWasShortcut(true) despite a full clear + copy having actually happened, and risks
    // flaking a test that asserts on the shortcut/full-sync distinction (e.g.
    // FastResyncTest#fallbackOnDivergence). Once set, every subsequent retry within the same cycle
    // skips the shortcut attempt and goes straight to a full sync.
    private final AtomicBoolean wipedThisSyncCycle = new AtomicBoolean(false);

    // Test-only observables for the abort-must-never-wipe hardening (stop() racing the sync
    // retry loop): counts of how many times tryConsistencyShortcut() was entered and
    // clearLocalDatabases() actually ran. A seam test uses the former to poll for "the shortcut
    // attempt against the (unreachable/slow) primary has begun" before racing stop() against it,
    // instead of a blind sleep; the latter is the actual assertion - it must stay 0 for a cycle
    // that stop() interrupted, even though tryConsistencyShortcut()'s own InterruptedException
    // handling only restores the interrupt flag and converts the exception to `false`, which by
    // itself does NOT stop the caller from wiping local data (see the running/interrupted guard
    // in startInitialSyncOnce() immediately before the clearLocalDatabases() call). Same seam
    // pattern as wasLastSyncShortcut()/setWatchLiveForTest().
    private final AtomicInteger consistencyShortcutAttempts = new AtomicInteger(0);
    private final AtomicInteger clearLocalDatabasesInvocations = new AtomicInteger(0);

    /** Test hook: number of times tryConsistencyShortcut() has been entered. */
    int getConsistencyShortcutAttemptsForTest() {
        return consistencyShortcutAttempts.get();
    }

    /** Test hook: number of times clearLocalDatabases() has actually run. */
    int getClearLocalDatabasesInvocationsForTest() {
        return clearLocalDatabasesInvocations.get();
    }

    /**
     * Test hook: the currently running initial-sync snapshot thread, or {@code null}. Must be
     * captured by a test BEFORE calling {@link #stop()} - stop() clears the field once it has
     * interrupted (and, per this hardening fix, joined) the thread.
     */
    Thread getInitialSyncThreadForTest() {
        return initialSyncThread;
    }

    // Test-only synchronization point, armed only when a test calls armTestPauseInShortcutForTest().
    // A no-op (null) in production. Lets a test deterministically block the sync thread INSIDE
    // tryConsistencyShortcut() - past the consistencyShortcutAttempts counter, at the exact spot a
    // real blocking primary-side driver call would sit - so it can race stop() against that precise
    // window instead of depending on real network timing (which, against a merely unreachable port,
    // resolves in single-digit milliseconds and gives no usable window at all).
    private volatile CountDownLatch testPauseInShortcut;

    private void awaitTestPauseIfArmed() throws InterruptedException {
        CountDownLatch latch = testPauseInShortcut;
        if (latch != null) {
            latch.await();
        }
    }

    /**
     * Test hook: arm the pause point inside {@code tryConsistencyShortcut()} (see
     * {@link #testPauseInShortcut}). A test polls {@link #getConsistencyShortcutAttemptsForTest()}
     * to know the sync thread has reached (and is now blocked at) the pause, then calls
     * {@link #stop()} - whose interrupt() throws {@code InterruptedException} out of the latch
     * await, reproducing exactly the "stop() interrupts a still-attempting shortcut" race the
     * hardening fix guards against.
     */
    void armTestPauseInShortcutForTest() {
        testPauseInShortcut = new CountDownLatch(1);
    }

    // #323 test seam: parks the sync thread INSIDE syncCollection - between taking the read
    // connection and the local insert - deliberately IGNORING interrupts, mirroring a socket
    // read blocked on a slow primary (which Thread.interrupt() cannot unblock either). null in
    // production.
    private volatile AtomicBoolean testSyncReadHold;
    private volatile CountDownLatch testSyncReadPauseReached;

    void armSyncReadPauseForTest() {
        testSyncReadPauseReached = new CountDownLatch(1);
        testSyncReadHold = new AtomicBoolean(true);
    }

    void releaseSyncReadPauseForTest() {
        AtomicBoolean hold = testSyncReadHold;

        if (hold != null) {
            hold.set(false);
        }
    }

    boolean syncReadPauseReachedForTest() {
        CountDownLatch reached = testSyncReadPauseReached;
        return reached != null && reached.getCount() == 0;
    }

    // #323: the sync connection currently blocked (or about to block) in a collection read.
    // stop() closes it so a socket read on a slow primary aborts instead of running out its
    // 60s read timeout - interrupts cannot unblock a socket read.
    private volatile MongoConnection inFlightSyncConnection;

    MongoConnection getInFlightSyncConnectionForTest() {
        return inFlightSyncConnection;
    }

    private void awaitSyncReadPauseIfArmed() {
        AtomicBoolean hold = testSyncReadHold;

        if (hold == null) {
            return;
        }

        testSyncReadPauseReached.countDown();

        while (hold.get()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // Deliberately keep holding AND swallow the flag: this simulates the thread
                // being parked deep inside a socket read when stop()'s interrupt lands - the
                // read neither aborts nor leaves an interruptible point between its return and
                // the local insert, so nothing downstream would ever see the interrupt. The
                // cooperative `running` check is what has to catch this case.
            }
        }
    }

    /** Test hook: release a pause armed with {@link #armTestPauseInShortcutForTest()} without
     * stopping the manager - the parked sync thread resumes its cycle normally. */
    void releaseTestPauseInShortcutForTest() {
        CountDownLatch latch = testPauseInShortcut;

        if (latch != null) {
            latch.countDown();
        }
    }

    // #322: snapshots discarded because the primary reported this cycle's watch cursor dead
    // (see the dead-watch guard in startInitialSyncOnce). Package-visible getter for tests.
    private final AtomicLong snapshotsDiscardedDeadWatch = new AtomicLong();

    long getSnapshotsDiscardedDeadWatchForTest() {
        return snapshotsDiscardedDeadWatch.get();
    }

    // Lossless initial sync (watch-first, buffer, snapshot, replay):
    //   applying              - gate for the batch processor. While false, replication events
    //                           keep accumulating in eventQueue but are NOT applied. It is
    //                           opened once the initial-sync snapshot is done, so events that
    //                           arrived during the snapshot are replayed on top of it.
    //   watchLive             - true while the change-stream watch cursor is established on the
    //                           primary (set by the WatchCommand registration callback, cleared
    //                           when the watch ends). While it is true the watch is guaranteed to
    //                           capture every subsequent write, so the snapshot may start without
    //                           a lost-write gap. It is resettable (unlike a one-shot latch) so a
    //                           reconnect during the initial sync makes the snapshot wait for the
    //                           new watch to re-establish rather than racing ahead.
    //   initialSyncStarted    - guards against launching more than one snapshot thread.
    private final AtomicBoolean applying = new AtomicBoolean(false);
    private final AtomicBoolean watchLive = new AtomicBoolean(false);
    // The current watch session's command, for the #322 dead-watch guard (wire cursor id via
    // its "cursor" metadata). null while no session is between construction and its finally.
    private volatile WatchCommand activeWatchCommand;
    private final AtomicBoolean initialSyncStarted = new AtomicBoolean(false);
    private volatile Thread initialSyncThread;
    //   watchGeneration       - bumped every time a watch cursor registers on the primary (the
    //                           registration callback). The snapshot captures it before the copy;
    //                           if it changes (or watchLive drops) before the copy finishes, the
    //                           watch died and was re-established "from now" (no resumeAfter while
    //                           initial sync is incomplete) mid-copy, so writes in the gap between
    //                           the old watch's death and the new watch's registration are lost and
    //                           the snapshot must be redone. Package-private for the seam test.
    final AtomicLong watchGeneration = new AtomicLong(0);

    // Progress reporting interval - balanced for good throughput and write concern latency
    // 50ms gives good responsiveness while not overwhelming the primary with reports
    private static final long PROGRESS_REPORT_INTERVAL_MS = 50;

    // Batching configuration for efficient replication
    // Using reasonable batch interval for good throughput
    private static final int BATCH_SIZE = 100;
    private static final long BATCH_FLUSH_INTERVAL_MS = 5;
    // Bounded so a stalled batch processor applies backpressure to the watch callback
    // (via put()) instead of buffering replication events until OOM.
    private static final int EVENT_QUEUE_CAPACITY = 100_000;
    private final BlockingQueue<QueuedEvent> eventQueue = new LinkedBlockingQueue<>(EVENT_QUEUE_CAPACITY);

    /**
     * A queued replication event plus its estimated size, measured exactly once at enqueue time
     * (see {@link InMemoryDriver#estimateBsonSize}) so the byte accounting adds and subtracts
     * the very same value at every queue mutation site.
     */
    private record QueuedEvent(Map<String, Object> event, long estimatedBytes) {}

    // Byte budget for eventQueue (estimated bytes; 0 = off - PoppyDB opts in with a 256m
    // default, --event-queue-budget). The count capacity alone does not bound memory: every
    // queued event retains its full document, so 100k ~300KB bulk-export events blow any heap
    // (same failure family as the replay-buffer incident, commit 88acb76b0). Unlike the replay
    // buffer, overflow must NOT evict here - queued events are not yet applied, dropping one is
    // silent data loss on this secondary. Instead the budget extends the existing count
    // backpressure to bytes: the producer (watch callback) blocks in enqueueReplicationEvent()
    // until the consumer's drain frees budget.
    private volatile long eventQueueByteBudget = 0;
    // Estimated bytes currently queued (sum of QueuedEvent.estimatedBytes). Maintained at every
    // queue mutation site: enqueue adds after a successful put, every drain subtracts the exact
    // per-event values it removed - so the counter converges to the queue's true content even
    // across a resync discard racing the producer.
    private final AtomicLong eventQueueBytes = new AtomicLong();
    // Monotonic count of enqueues that had to wait on the byte budget (diagnostic, in getStats()).
    private final AtomicLong eventQueueBytePressureCount = new AtomicLong();
    // Rate limit for the byte-backpressure WARN log (at most one per minute).
    private volatile long lastBytePressureWarnAt = 0;
    // Monitor for byte-budget waits: the producer waits here, releaseEventQueueBytes() notifies.
    private final Object eventQueueByteLock = new Object();
    // volatile: written by start()/stop(), read by the watch-callback thread in
    // requestFlush() with no happens-before edge between them
    private volatile ScheduledExecutorService batchProcessor;
    /** at most one on-demand flush queued at a time - see requestFlush() */
    private final java.util.concurrent.atomic.AtomicBoolean flushPending = new java.util.concurrent.atomic.AtomicBoolean();

    // Flag to enable immediate progress reporting after each batch
    private volatile boolean immediateProgressReporting = true;

    // Staleness detection - track last response time to detect broken connections
    private final AtomicLong lastWatchResponseTime = new AtomicLong(0);
    private static final long STALENESS_THRESHOLD_MS = 30000; // 30 seconds without response = stale

    // How long isContinued() sleeps before ending the watch while refusingDestructiveResync is
    // true (2026-08-14 task-3 review fix). Paces the register/teardown cycle that refreshes
    // lastKnownPrimarySequence - see the pacing comment at that isContinued() check for why this
    // is load-bearing, not cosmetic. A fixed interval rather than mirroring the initial-sync
    // thread's own growing 1s->30s backoff: that state lives on a different thread and this is a
    // different loop (the watch's own getMore cadence, not the sync-decision retry cadence) -
    // a fixed value in the same 1-5s ballpark is simpler and avoids coupling the two.
    private static final long REFUSAL_WATCH_PACE_MS = 2000;

    // Callback to notify when log index is updated (for election consistency)
    private java.util.function.BiConsumer<Long, Long> onLogIndexUpdate;

    // Fired every time an initial sync COMPLETES successfully (the gate-opening moment where
    // initialSyncComplete flips true) - i.e. this node now holds an authoritative copy of the
    // primary's dataset. Wired by PoppyDB to release the partial-restore candidacy guard
    // (#306 P1-2 follow-up): the release must hang off actual sync COMPLETION, not off
    // replication merely starting, and it must exist in the election path
    // (startReplicationToLeader), which - unlike static-mode startReplication() - has no
    // synchronous waitForInitialSync it could hook. May fire more than once per
    // ReplicationManager lifetime (a resync closes and re-opens the gate); receivers must be
    // idempotent.
    private volatile Runnable onInitialSyncComplete;

    // Arms the completion notification (#306 review follow-up): the gate-opening moment only
    // means "snapshot copied, apply gate open" - the events buffered in eventQueue during the
    // snapshot (up to 100k) are NOT applied yet, and a guard released before they are would be
    // released onto a node that is still measurably behind. So the sync success block only
    // ARMS this flag; the batch processor fires the callback once the queue has actually
    // drained (and only while running - a stopped manager's late sync thread can arm it, but
    // nothing will ever fire it, which is exactly right for a superseded manager).
    // Package-visible for tests.
    final AtomicBoolean syncCompleteNotifyPending = new AtomicBoolean(false);

    // RS-internal connection security, set once via setInternalConnectionSecurity() before
    // start() - see docs/superpowers/specs/2026-08-05-poppydb-rs-internal-auth-tls-design.md.
    // Defaults (auth off, no SSL context) reproduce today's plaintext/unauthenticated behavior.
    private volatile boolean authEnabled = false;
    private volatile String authUser = null;
    private volatile String authPassword = null;
    private volatile SSLContext internalSslContext = null;

    public ReplicationManager(InMemoryDriver localDriver, String primaryHost, int primaryPort) {
        this.localDriver = localDriver;
        this.primaryHost = primaryHost;
        this.primaryPort = primaryPort;
    }

    /**
     * Set this secondary's address for reporting to primary.
     */
    public void setMyAddress(String myAddress) {
        this.myAddress = myAddress;
    }

    /**
     * Set callback to be notified when log index is updated.
     * This is used to keep ElectionManager's log indices in sync with replication.
     * The callback receives (logIndex, logTerm).
     */
    public void setOnLogIndexUpdate(java.util.function.BiConsumer<Long, Long> callback) {
        this.onLogIndexUpdate = callback;
    }

    /**
     * Set the callback fired on every successful initial-sync completion (see the field's
     * comment). Call before {@link #start()}; the callback must be idempotent.
     */
    public void setOnInitialSyncComplete(Runnable callback) {
        this.onInitialSyncComplete = callback;
    }

    /**
     * Configure how the connection to the primary authenticates/encrypts itself. Call before
     * {@link #start()}. {@code internalSslContext} of {@code null} means the connection stays
     * plaintext even if {@code authEnabled} is true.
     */
    public void setInternalConnectionSecurity(boolean authEnabled, String authUser, String authPassword,
            SSLContext internalSslContext) {
        this.authEnabled = authEnabled;
        this.authUser = authUser;
        this.authPassword = authPassword;
        this.internalSslContext = internalSslContext;
    }

    /**
     * Byte budget for the replication event queue (estimated bytes, see
     * {@link InMemoryDriver#estimateBsonSize}; 0 = off, only the count capacity applies).
     * Enforced as backpressure on the producer, never by dropping queued events - see
     * {@link #enqueueReplicationEvent}. Safe to call on a running manager: raising or
     * disabling the budget wakes a currently blocked producer.
     */
    public void setEventQueueByteBudget(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("eventQueueByteBudget must be >= 0 (0 = disabled)");
        }

        this.eventQueueByteBudget = bytes;

        synchronized (eventQueueByteLock) {
            eventQueueByteLock.notifyAll();
        }
    }

    /** Current event-queue byte budget; 0 = byte bound disabled. */
    long getEventQueueByteBudget() {
        return eventQueueByteBudget;
    }

    /** Estimated bytes currently held by the event queue (diagnostic). */
    long getEventQueueBytes() {
        return eventQueueBytes.get();
    }

    /** Number of enqueues that had to wait on the byte budget (diagnostic). */
    long getEventQueueBytePressureCount() {
        return eventQueueBytePressureCount.get();
    }

    /**
     * Start the replication process.
     * This will:
     * 1. Connect to the primary
     * 2. Perform initial sync (copy all data)
     * 3. Start watching for changes
     * 4. Start reporting progress to primary
     */
    public void start() throws Exception {
        if (running.getAndSet(true)) {
            throw new IllegalStateException("Replication already running");
        }

        log.info("Starting replication from primary {}:{}", primaryHost, primaryPort);

        replicationExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "PoppyDB-Replication");
            t.setDaemon(true);
            return t;
        });

        // Connect to primary
        connectToPrimary();

        // Start replication in background
        replicationExecutor.submit(this::replicationLoop);

        // Start batch processor for efficient event application
        startBatchProcessor();

        // Start progress reporter
        startProgressReporter();

        // Start periodic index replication (#258)
        startIndexSyncer();
    }

    private void startIndexSyncer() {
        indexSyncer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PoppyDB-IndexSyncer");
            t.setDaemon(true);
            return t;
        });

        indexSyncer.scheduleWithFixedDelay(this::periodicIndexSync,
                INDEX_SYNC_INTERVAL_MS, INDEX_SYNC_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void periodicIndexSync() {
        // The initial sync replicates indexes itself; until it is done (and while partitioned)
        // there is nothing sensible to diff against.
        if (!connected.get() || pausedForTest.get() || !initialSyncComplete.get()) {
            return;
        }

        Morphium pm = primaryMorphium;

        if (pm == null) {
            return;
        }

        try {
            syncIndexesFrom(pm.getDriver());
        } catch (Exception e) {
            log.warn("Periodic index sync failed (will retry in {}ms): {}", INDEX_SYNC_INTERVAL_MS, e.getMessage());
        }
    }

    /**
     * Start the batch processor that efficiently applies change events.
     */
    private void startBatchProcessor() {
        // A task accepted by execute() but discarded by a later shutdownNow() would leave the
        // flag stuck true, silently disabling every on-demand flush for this instance - the
        // regression this whole mechanism exists to prevent, and invisible except in latency.
        flushPending.set(false);
        batchProcessor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PoppyDB-BatchProcessor");
            t.setDaemon(true);
            return t;
        });

        // The fixed schedule stays as the safety net (catches anything enqueued while the gate
        // was still closed, or a missed wake-up); requestFlush() is what makes a single write
        // replicate immediately rather than on the next tick.
        batchProcessor.scheduleAtFixedRate(this::processBatch,
                BATCH_FLUSH_INTERVAL_MS, BATCH_FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Asks the batch processor to run now. Submitted to the SAME single-threaded executor the
     * periodic flush uses, so an on-demand run can never overlap a scheduled one - {@code
     * processBatch()} keeps its single-threaded contract without any locking of its own.
     *
     * <p>{@code flushPending} collapses a burst into one extra run: while a flush is queued or
     * in flight, further events do not pile up additional tasks. The flag is cleared BEFORE
     * {@code processBatch()} runs, so an event arriving during that run schedules the next one
     * and nothing is left sitting in the queue until the timer comes round.
     */
    private void requestFlush() {
        ScheduledExecutorService bp = batchProcessor;

        if (bp == null || bp.isShutdown() || !applying.get()) {
            return;
        }

        if (flushPending.compareAndSet(false, true)) {
            try {
                bp.execute(() -> {
                    flushPending.set(false);
                    processBatch();
                });
            } catch (RejectedExecutionException e) {
                // shutting down - the periodic task (if any) or the next start handles it
                flushPending.set(false);
            }
        }
    }

    /**
     * Enqueue a replication event from the watch callback. Blocks (backpressure to the watch
     * reader, never to the initial-sync snapshot - see the gate comment in processBatch())
     * while the queue is at its count capacity ({@code LinkedBlockingQueue.put}) or over its
     * byte budget. Two deliberate properties of the byte wait:
     * <ul>
     * <li>an event larger than the whole budget is admitted whenever the queue holds no bytes,
     *     so it can never block forever (the byte analogue of the replay buffer's
     *     {@code size > 1} guard);</li>
     * <li>the wait is a timed loop re-checking {@code running}, so a {@link #stop()} that never
     *     interrupts this thread still gets out - and an interrupt propagates exactly like the
     *     count path's {@code put()}.</li>
     * </ul>
     * Package-visible for tests.
     */
    void enqueueReplicationEvent(Map<String, Object> data) throws InterruptedException {
        enqueueReplicationEvent(data, replicationSessionEpoch.get());
    }

    // #322: retired watch sessions. Bumped whenever the buffered events of the CURRENT watch
    // session are discarded (dead-watch guard, triggerResync). A producer whose captured epoch
    // is no longer current drops its event instead of enqueueing it: those events belong to a
    // watch the node has already decided to abandon, and applying them after the next snapshot
    // would overwrite freshly-copied documents with stale state - the #319 inversion class,
    // reintroduced through the back door. Epoch check and offer happen under
    // eventQueueByteLock, the same lock the discard drains under, so an event is either
    // discarded by the drain or refused by the epoch check - never silently retained.
    private final AtomicLong replicationSessionEpoch = new AtomicLong();
    // Count-capacity backpressure entries (the byte analogue is eventQueueBytePressureCount).
    private final AtomicLong eventQueueCountPressureCount = new AtomicLong();

    void enqueueReplicationEvent(Map<String, Object> data, long sessionEpoch) throws InterruptedException {
        long size = InMemoryDriver.estimateBsonSize(data);

        if (eventQueueByteBudget > 0) {
            boolean waited = false;

            synchronized (eventQueueByteLock) {
                // Wait while admitting would exceed the budget AND the queue still holds bytes
                // - a queue at 0 bytes always admits, even an event above the whole budget.
                // Re-read the budget each round so setEventQueueByteBudget() takes effect on a
                // waiting producer. A session-epoch change ends the wait: the events of this
                // producer's watch session were discarded, so is this one (checked again below).
                while (running.get() && replicationSessionEpoch.get() == sessionEpoch
                        && eventQueueByteBudget > 0 && eventQueueBytes.get() > 0
                        && eventQueueBytes.get() + size > eventQueueByteBudget) {
                    if (!waited) {
                        waited = true;
                        eventQueueBytePressureCount.incrementAndGet();
                        long now = System.currentTimeMillis();

                        if (now - lastBytePressureWarnAt > 60_000) {
                            lastBytePressureWarnAt = now;
                            log.warn("Replication event queue byte budget ({} bytes) exhausted - blocking the "
                                    + "watch reader until the apply side frees budget (bulk writes of large "
                                    + "documents, or initial sync still running?)", eventQueueByteBudget);
                        }
                    }

                    eventQueueByteLock.wait(100);
                }
            }
        }

        // Count-capacity backpressure, epoch-atomic: offer under the same lock the dead-watch
        // discard drains under. The former blocking put() could re-admit an event AFTER a
        // discard had drained the queue, resurrecting a retired session's event.
        boolean countWaited = false;

        while (true) {
            synchronized (eventQueueByteLock) {
                if (replicationSessionEpoch.get() != sessionEpoch) {
                    return; // session retired (#322) - the event belongs to a dead watch
                }

                if (eventQueue.offer(new QueuedEvent(data, size))) {
                    // Adding after the offer keeps the invariant that the counter only ever
                    // accounts events that actually made it into the queue; any drain racing us
                    // subtracts this event's exact size, so the counter converges.
                    eventQueueBytes.addAndGet(size);
                    return;
                }
            }

            if (!running.get()) {
                return; // shutting down - the queue is discarded anyway (see stop())
            }

            if (!countWaited) {
                countWaited = true;
                eventQueueCountPressureCount.incrementAndGet();
            }

            Thread.sleep(50); // count capacity full - same backpressure the old put() applied
        }
    }

    /**
     * Drain up to {@code max} queued events, releasing their bytes from the budget (waking a
     * producer blocked in {@link #enqueueReplicationEvent}). The single consumer-side drain
     * point, used by processBatch(). Package-visible for tests.
     */
    List<Map<String, Object>> drainBatch(int max) {
        List<QueuedEvent> drained = new ArrayList<>(max);
        eventQueue.drainTo(drained, max);
        List<Map<String, Object>> batch = new ArrayList<>(drained.size());

        if (!drained.isEmpty()) {
            releaseEventQueueBytes(drained);

            for (QueuedEvent qe : drained) {
                batch.add(qe.event());
            }
        }

        return batch;
    }

    /** Subtract the removed events' bytes from the accounting and wake a blocked producer. */
    private void releaseEventQueueBytes(List<QueuedEvent> removed) {
        long freed = 0;

        for (QueuedEvent qe : removed) {
            freed += qe.estimatedBytes();
        }

        if (freed != 0) {
            eventQueueBytes.addAndGet(-freed);

            synchronized (eventQueueByteLock) {
                eventQueueByteLock.notifyAll();
            }
        }
    }

    /**
     * Process queued events in batches for better performance.
     */
    private void processBatch() {
        // Gate: do not apply events until the initial-sync snapshot has completed. Events keep
        // accumulating in the (bounded) eventQueue; once the snapshot is done the gate opens and
        // the buffered events are drained as an idempotent replay on top of the snapshot. If the
        // snapshot outlasts the queue capacity the watch callback's blocking put() applies
        // backpressure to the watch reader (never to the snapshot, which runs on its own thread
        // and uses its own connections), so this cannot deadlock the snapshot.
        if (!applying.get()) {
            return;
        }

        if (eventQueue.isEmpty()) {
            // Periodic catch-up half of the election log-index feed: even with nothing to
            // drain, reconcile the election layer's view on every flush tick. A replication
            // position that became known WITHOUT a live event to carry it (e.g. the
            // registration seed landing only after the initial sync's own one-shot report
            // already ran - see recordPrimarySequenceAtRegistration) then still reaches
            // ElectionManager within one tick instead of never. updateLogIndex is
            // monotonic-max, so re-reporting the same value every tick is a harmless no-op.
            reportLogIndexToElection();
            maybeFireSyncCompleteNotify();
            return;
        }

        List<Map<String, Object>> batch = drainBatch(BATCH_SIZE);

        if (batch.isEmpty()) {
            reportLogIndexToElection();
            maybeFireSyncCompleteNotify();
            return;
        }

        applyEventsInOrder(batch);

        // Notify about log index update for election consistency
        reportLogIndexToElection();
        maybeFireSyncCompleteNotify();

        // Immediately report progress after processing batch for faster write concern acknowledgment
        if (immediateProgressReporting) {
            reportProgressToPrimary();
        }
    }

    /**
     * Fires the armed initial-sync-completion notification once it is actually TRUE end to
     * end - see {@link #syncCompleteNotifyPending}. Called from the batch processor thread
     * only. Package-visible for tests.
     */
    void maybeFireSyncCompleteNotify() {
        if (!syncCompleteNotifyPending.get()) {
            return;
        }

        // All three must hold before the armed notification may fire:
        // - running: a stopped (superseded) manager's late sync thread can still ARM the flag,
        //   but its sync ran against a primary that may no longer lead - nothing may fire it;
        // - initialSyncComplete: a resync in between closed the gate again - wait for it;
        // - empty queue: the backlog buffered during the snapshot must actually be APPLIED,
        //   or the "authoritative copy" the receiver acts on is still measurably behind.
        if (!running.get() || !initialSyncComplete.get() || !eventQueue.isEmpty()) {
            return;
        }

        if (!syncCompleteNotifyPending.compareAndSet(true, false)) {
            return;
        }

        Runnable hook = onInitialSyncComplete;

        if (hook != null) {
            try {
                hook.run();
            } catch (Exception e) {
                log.warn("onInitialSyncComplete callback failed: {}", e.getMessage(), e);
            }
        }
    }

    /**
     * Test seam: puts an event into the apply queue exactly like the watch callback does,
     * without a live change stream.
     */
    void enqueueEventForTest(Map<String, Object> event) throws InterruptedException {
        eventQueue.put(new QueuedEvent(event, 0));
    }

    /** Test seam: drops all buffered events, as if the batch processor had applied them. */
    void clearEventQueueForTest() {
        eventQueue.clear();
    }

    /**
     * Push the current replication position to the election layer (wired by PoppyDB to
     * {@link de.caluga.poppydb.election.ElectionManager#updateLogIndex}, whose monotonic-max
     * semantics make repeated or out-of-date reports harmless no-ops). That monotonicity is
     * what this method's design leans on: instead of reporting the position exactly ONCE at a
     * place where it may not be known yet (the seed race between the initial-sync success
     * block and the watch-registration seed, both racing on other threads), it is reported
     * from every place the position can become known, plus periodically from the batch
     * processor's flush tick as a net over the whole race class.
     *
     * <p>Gated on {@code initialSyncComplete}: before the sync gate opens this node does not
     * hold the primary's dataset, and claiming a non-zero replication position then would
     * recreate - from the other side - exactly the hazard the #306 empty-candidate restraint
     * closed (an effectively-empty node taking part in elections as if it had the data).
     */
    private void reportLogIndexToElection() {
        if (!initialSyncComplete.get()) {
            return;
        }
        long currentSeq = lastAppliedSequence.get();
        if (onLogIndexUpdate != null && currentSeq > 0) {
            // Term is 0 for now - will be updated when we receive term from leader
            onLogIndexUpdate.accept(currentSeq, 0L);
        }
    }

    /**
     * Apply a batch of change events to the local driver, preserving global event order.
     *
     * Only *contiguous* runs of insert events for the same collection are bundled into a
     * single bulk insert; any non-insert event, or an insert for a different collection,
     * flushes the pending run first (in order) before the next event is handled. This keeps
     * the effective application order identical to sequential (one-event-at-a-time)
     * application, while still batching same-collection inserts that happen to be adjacent
     * for throughput.
     *
     * Package-visible (rather than private) so tests can exercise the ordering/grouping
     * logic directly, without a live replication connection.
     */
    @SuppressWarnings("unchecked")
    void applyEventsInOrder(List<Map<String, Object>> batch) {
        // Replication applies must never be rejected by the memory watermark: the primary is
        // the gate, and a secondary refusing what the primary accepted would silently diverge.
        try (var ignored = localDriver.bypassMemoryGuard()) {
            applyEventsInOrderGuarded(batch);
        }
    }

    private void applyEventsInOrderGuarded(List<Map<String, Object>> batch) {
        List<Map<String, Object>> run = new ArrayList<>();
        String runCollectionKey = null;

        for (Map<String, Object> event : batch) {
            String operationType = (String) event.get("operationType");
            Map<String, Object> ns = (Map<String, Object>) event.get("ns");
            boolean isInsert = ns != null && "insert".equals(operationType);
            String collKey = isInsert ? ns.get("db") + "." + ns.get("coll") : null;

            if (isInsert && (runCollectionKey == null || runCollectionKey.equals(collKey))) {
                run.add(event);
                runCollectionKey = collKey;
                continue;
            }

            // Non-insert event, or insert for a different collection: flush the pending run
            // first so it is applied before this event, preserving global order.
            if (!run.isEmpty()) {
                applyBulkInserts(runCollectionKey, run);
                run = new ArrayList<>();
            }
            runCollectionKey = null;

            if (isInsert) {
                run.add(event);
                runCollectionKey = collKey;
            } else {
                applyChangeEvent(event);
            }
        }

        // Flush any trailing run.
        if (!run.isEmpty()) {
            applyBulkInserts(runCollectionKey, run);
        }
    }

    /**
     * Which (db, collection) pairs replicate. Normal user data does; internal databases and
     * system collections do not - with exactly two exceptions: admin.system.users, so that
     * logins survive failovers and initial sync (users would otherwise be node-local), and
     * admin.system.version, which carries the users-file version-gate meta doc ({_id:
     * "poppydb.usersFile", appliedVersion: N}) so a newly-elected primary sees the version a
     * prior primary already applied instead of silently re-applying (or skipping) the file.
     *
     * Central predicate for every skip decision in this class (live apply, initial-sync
     * enumeration, resync clearing, index diff) - do not add per-site variations.
     */
    static boolean isReplicated(String db, String collection) {
        if ("admin".equals(db)) {
            return "system.users".equals(collection) || "system.version".equals(collection);
        }
        if ("local".equals(db) || "config".equals(db)) {
            return false;
        }
        return collection == null || !collection.startsWith("system.");
    }

    /**
     * Apply multiple insert events as a single bulk insert.
     */
    @SuppressWarnings("unchecked")
    private void applyBulkInserts(String collKey, List<Map<String, Object>> events) {
        if (events.isEmpty()) return;

        String[] parts = collKey.split("\\.", 2);
        String db = parts[0];
        String coll = parts[1];

        // Skip everything outside the replicated namespace set (system databases and
        // system.* collections - except the replicated admin system collections, see isReplicated)
        if (!isReplicated(db, coll)) {
            // Still update sequence for skipped events
            for (Map<String, Object> event : events) {
                long seq = extractSequenceFromEvent(event);
                if (seq > 0) {
                    lastAppliedSequence.updateAndGet(current -> Math.max(current, seq));
                }
            }
            return;
        }

        List<Map<String, Object>> documents = new ArrayList<>(events.size());
        long maxSeq = 0;

        for (Map<String, Object> event : events) {
            Map<String, Object> fullDoc = (Map<String, Object>) event.get("fullDocument");
            if (fullDoc != null) {
                documents.add(fullDoc);
            }
            long seq = extractSequenceFromEvent(event);
            if (seq > maxSeq) {
                maxSeq = seq;
            }
        }

        final long finalMaxSeq = maxSeq;

        if (!documents.isEmpty()) {
            try {
                GenericCommand cmd = new GenericCommand(localDriver);
                cmd.setDb(db);
                cmd.setColl(coll);
                cmd.setCmdData(Doc.of(
                    "insert", coll,
                    "$db", db,
                    "documents", documents
                ));
                int msgId = localDriver.runCommand(cmd);
                Map<String, Object> result = localDriver.readSingleAnswer(msgId);
                Object writeErrors = (result != null) ? result.get("writeErrors") : null;

                if (writeErrors instanceof List<?> errors && !errors.isEmpty()) {
                    // InMemoryDriver does not throw for unique-secondary-index
                    // violations (only an ordered _id duplicate throws); it silently
                    // commits the non-conflicting documents from this very call and
                    // reports the rest as writeErrors in the result. Treat that as a
                    // failure of the bulk as a whole so it goes through the same
                    // fallback below, instead of being mistaken for full success.
                    throw new MorphiumDriverException(
                        "Bulk insert into " + db + "." + coll + " reported writeErrors: " + errors, null);
                }

                eventsApplied.addAndGet(documents.size());
                log.debug("Bulk inserted {} documents into {}.{}", documents.size(), db, coll);

                // Whole bulk command reported success: safe to advance to the run's max
                // sequence.
                if (finalMaxSeq > 0) {
                    lastAppliedSequence.updateAndGet(current -> Math.max(current, finalMaxSeq));
                }
            } catch (Exception e) {
                // The bulk insert failed as a whole, or partially (writeErrors above).
                // Its atomicity is *not* guaranteed in general: an ordered _id-duplicate
                // throws before any document is written, but a unique-secondary-index
                // writeErrors result (or a failure raised later, e.g. during index
                // maintenance) can leave some of this run's documents already
                // committed. So we cannot just retry every event with a plain insert --
                // that would spuriously fail (and permanently stall the sequence) on
                // whatever already landed.
                //
                // Instead, fall back to applying each event in the run individually via
                // applyChangeEvent in "replay" mode, which applies inserts as an
                // idempotent full-document upsert-by-key (applyInsertIdempotent) rather
                // than a strict insert -- the same replay-idempotency rule the
                // initial-sync path needs (see task 8). Documents that already landed
                // are harmlessly re-written to the same content; documents that didn't
                // land yet get created. applyChangeEvent advances lastAppliedSequence per
                // event and only on success, so it acts as a poison-skip watermark: a
                // genuinely poison event (e.g. a real, still-unresolved unique-index
                // conflict) fails on its own without blocking the rest of the run, and the
                // events that follow it in the run still apply and advance the watermark.
                // NOTE this means a poison event that is NOT the trailing (highest-sequence)
                // event of the run does get skipped over: a later successful event pushes
                // lastAppliedSequence past the poison's sequence, so the poison is
                // effectively dropped rather than retried. The "no false advance" guarantee
                // therefore only holds for a trailing conflict; a mid-run poison is skipped.
                // That is the intended trade-off -- we prefer forward progress and
                // eventual convergence (the primary is the source of truth) over stalling
                // the whole stream on one unresolved conflict.
                //
                // Log level for the bulk failure itself is decided AFTER the fallback runs, not
                // before: the common case here is a benign ordered _id-duplicate from a sync race
                // (e.g. a document the initial-sync snapshot and a buffered replay both bring in),
                // which the idempotent replay below fully resolves -- that is expected noise, not
                // an operational problem, so it logs at WARN. Only when the per-document fallback
                // ALSO fails for at least one event (a genuine, still-unresolved conflict) does
                // this stay at ERROR.
                boolean fallbackHadFailure = false;
                for (Map<String, Object> event : events) {
                    if (!applyChangeEvent(event, true)) {
                        fallbackHadFailure = true;
                    }
                }
                if (fallbackHadFailure) {
                    log.error("Error applying bulk insert to {}.{}: {} (per-document fallback also "
                            + "failed for at least one event -- see individual event errors above)",
                            db, coll, e.getMessage());
                } else {
                    log.warn("Bulk insert to {}.{} hit {} (expected during sync races, e.g. a "
                            + "document already present from initial sync or a concurrent replay; "
                            + "auto-resolved via idempotent per-document replay)",
                            db, coll, e.getMessage());
                }
            }
        } else {
            // No documents to insert (e.g. all events lacked fullDocument) -- nothing was
            // attempted, so it's safe to advance to the run's max sequence.
            if (finalMaxSeq > 0) {
                lastAppliedSequence.updateAndGet(current -> Math.max(current, finalMaxSeq));
            }
        }
    }

    /**
     * Start a background task to periodically report replication progress to primary.
     */
    private void startProgressReporter() {
        progressReporter = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PoppyDB-ProgressReporter");
            t.setDaemon(true);
            return t;
        });

        progressReporter.scheduleAtFixedRate(this::reportProgressToPrimary,
                PROGRESS_REPORT_INTERVAL_MS, PROGRESS_REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Report current replication progress to the primary.
     * Uses synchronous communication to ensure acknowledgment is received.
     */
    private void reportProgressToPrimary() {
        if (!connected.get() || primaryMorphium == null || myAddress == null) {
            return;
        }

        long currentSeq = lastAppliedSequence.get();
        long lastReported = lastReportedSequence.get();

        // Only report if there's new progress
        if (currentSeq <= lastReported) {
            return;
        }

        MongoConnection con = null;
        try {
            // Get a connection and send replSetProgress command to primary
            con = primaryMorphium.getDriver().getPrimaryConnection(null);
            GenericCommand cmd = new GenericCommand(con);
            cmd.setDb("admin");
            cmd.setCmdData(Doc.of(
                "replSetProgress", 1,
                "secondaryAddress", myAddress,
                "sequenceNumber", currentSeq
            ));

            // Use synchronous execution to ensure the progress report is acknowledged
            int msgId = cmd.executeAsync();
            Map<String, Object> result = con.readSingleAnswer(msgId);
            if (result != null && Double.valueOf(1.0).equals(result.get("ok"))) {
                lastReportedSequence.set(currentSeq);
                log.debug("Reported progress to primary: seq={}", currentSeq);
            } else {
                log.warn("Progress report not acknowledged by primary: seq={}, result={}", currentSeq, result);
            }
        } catch (Exception e) {
            // Don't update lastReportedSequence - will retry on next interval
            log.debug("Failed to report progress (will retry): {}", e.getMessage());
        } finally {
            if (con != null) {
                try {
                    primaryMorphium.getDriver().releaseConnection(con);
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Stop the replication process.
     */
    public void stop() {
        if (!running.getAndSet(false)) {
            return;
        }

        log.info("Stopping replication...");

        // Interrupt an in-flight initial-sync snapshot thread (if any) so it exits promptly, then
        // join it with a bounded wait before proceeding. Without the join, an old, just-stopped
        // ReplicationManager's sync thread could still be mid-cycle (see the running/interrupted
        // guard added to the retry loop above) at the moment PoppyDB installs its replacement RM,
        // letting the old thread's clearLocalDatabases()/performInitialSync() race the new RM's
        // own sync on the same local database.
        //
        // Bounded rather than unbounded: stop() is called from PoppyDB's synchronized leadership/
        // probe paths (startReplicationToLeader, probeReplicationLiveness,
        // onLeadershipChangeSynchronized) and, for the liveness probe, on PoppyDB's single
        // retry-scheduler thread - an unbounded join here could stall those indefinitely on a
        // wedged sync thread. The joined thread only ever touches this ReplicationManager's own
        // state and its own driver connections; it never calls back into PoppyDB, so it can never
        // itself need the monitor stop() is running under - this join cannot deadlock against it.
        // 5s is generous versus the sync loop's own bounded per-attempt work (a dbHash comparison
        // or per-collection copy against a healthy primary, or a fast failure against an
        // unreachable one) while still capping the worst case for the callers above.
        Thread syncThread = initialSyncThread;
        initialSyncThread = null;
        if (syncThread != null) {
            syncThread.interrupt();

            // #323: the interrupt cannot unblock a socket read, and the sync connection has a
            // 60s read timeout - far beyond the bounded join below. Closing the tracked
            // in-flight connection ends a blocked collection read with an IO error instead,
            // so the join actually has a chance. Rough on a mid-flight healthy request, but
            // this sync is being abandoned either way and every successor re-syncs.
            MongoConnection blockedRead = inFlightSyncConnection;

            if (blockedRead != null) {
                try {
                    blockedRead.close();
                } catch (Exception e) {
                    log.debug("Closing the in-flight sync connection failed: {}", e.toString());
                }
            }

            try {
                syncThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (syncThread.isAlive()) {
                log.warn("Initial-sync thread did not terminate within 5s of stop(); it may still "
                        + "be running concurrently with a replacement ReplicationManager");
            }
        }

        // Stop batch processor first to flush remaining events
        if (batchProcessor != null) {
            // Flush the remainder ON the batch thread, not on the caller's. Calling
            // processBatch() directly here raced a concurrently running scheduled (or
            // on-demand) flush: two interleaved drainTo() calls can apply events out of
            // order, and it is the one place that broke processBatch()'s single-threaded
            // contract. Submitting it keeps every invocation on the same thread; if the
            // executor is already gone or the flush does not finish in time, shutdownNow()
            // below takes over exactly as before.
            try {
                batchProcessor.submit(this::processBatch).get(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.debug("Final replication flush did not complete before shutdown: {}", e.toString());
            }

            batchProcessor.shutdownNow();
            try {
                batchProcessor.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            batchProcessor = null;
        }

        // Stop progress reporter
        if (progressReporter != null) {
            progressReporter.shutdownNow();
            try {
                progressReporter.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            progressReporter = null;
        }

        // Stop periodic index replication
        if (indexSyncer != null) {
            indexSyncer.shutdownNow();
            try {
                indexSyncer.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            indexSyncer = null;
        }

        if (replicationExecutor != null) {
            replicationExecutor.shutdownNow();
        }

        disconnectFromPrimary();

        log.info("Replication stopped. Events applied: {}, lastSequence: {}",
                eventsApplied.get(), lastAppliedSequence.get());
    }

    private void connectToPrimary() throws Exception {
        log.info("Connecting to primary at {}:{}", primaryHost, primaryPort);

        try {
            MorphiumConfig config = new MorphiumConfig();
            config.connectionSettings().setDatabase("admin");  // Default db for admin operations
            config.clusterSettings().setHostSeed(primaryHost + ":" + primaryPort);
            // Increase connection pool for handling watch + progress reporting under load
            config.connectionSettings().setMaxConnections(10);
            config.connectionSettings().setMinConnections(2);
            config.connectionSettings().setConnectionTimeout(10000);  // 10s connection timeout
            config.driverSettings().setReadTimeout(60000);  // 60s read timeout for long-running watch
            config.connectionSettings().setMaxWaitTime(5000);  // 5s max wait for connection from pool
            config.driverSettings().setRetryReads(true);  // Retry on transient failures
            config.driverSettings().setRetryWrites(true);
            config.connectionSettings().setRetriesOnNetworkError(3);
            config.connectionSettings().setSleepBetweenNetworkErrorRetries(500);

            if (authEnabled) {
                config.authSettings().setMongoLogin(authUser).setMongoPassword(authPassword)
                        .setMongoAuthDb("admin");
            }
            if (internalSslContext != null) {
                config.connectionSettings().setUseSSL(true).setSslContext(internalSslContext)
                        .setSslInvalidHostNameAllowed(true);
            }

            primaryMorphium = new Morphium(config);
            primaryMorphium.getDriver();  // Force connection

            connected.set(true);
            log.info("Connected to primary with enhanced connection pool");
        } catch (Exception e) {
            log.error("Failed to connect to primary: {}", e.getMessage());
            connected.set(false);
            throw e;
        }
    }

    private void disconnectFromPrimary() {
        if (primaryMorphium != null) {
            try {
                primaryMorphium.close();
            } catch (Exception e) {
                log.warn("Error closing primary connection: {}", e.getMessage());
            }
            primaryMorphium = null;
        }
        connected.set(false);
    }

    private void replicationLoop() {
        while (running.get()) {
            try {
                // Test hook: simulate a partition — stay severed and do not reconnect until resumed.
                if (pausedForTest.get()) {
                    Thread.sleep(100);
                    continue;
                }

                if (!connected.get()) {
                    log.info("Not connected to primary, attempting reconnect...");
                    try {
                        disconnectFromPrimary(); // clean up previous connection before reconnecting
                        connectToPrimary();
                    } catch (Exception e) {
                        log.warn("Reconnect failed, will retry in 5s: {}", e.getMessage());
                        Thread.sleep(5000);
                        continue;
                    }
                }

                // Lossless initial sync: start the change-stream watch FIRST (below, on this
                // thread) so events flow into eventQueue, while a background thread performs the
                // snapshot copy. The snapshot waits for the watch to be live (watchLive, set by the
                // WatchCommand registration callback) before copying, so no write is lost in the
                // gap between snapshot and watch. The batch processor stays gated (applying=false)
                // until the snapshot completes, then drains the buffered events as an idempotent
                // replay.
                if (!initialSyncComplete.get()) {
                    startInitialSyncOnce();
                }

                // Watch for changes (blocks; produces events into eventQueue). During the initial
                // sync this is the producer that fills the buffer while the snapshot runs.
                watchForChanges();

                // Check if watch ended due to staleness (no response for too long)
                long lastResponse = lastWatchResponseTime.get();
                long now = System.currentTimeMillis();
                if (lastResponse > 0 && (now - lastResponse) > STALENESS_THRESHOLD_MS) {
                    log.warn("Watch ended due to staleness, forcing reconnection to primary");
                    disconnectFromPrimary();
                    connected.set(false);
                    // Will reconnect on next iteration
                }

            } catch (InterruptedException e) {
                log.debug("Replication loop interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                connected.set(false);
                // A partition simulated by the test hook severs the connection on purpose; the watch
                // throwing is expected, so don't log it as an error or sleep the 5s backoff.
                if (pausedForTest.get()) {
                    continue;
                }
                log.error("Error in replication loop: {}", e.getMessage(), e);

                if (running.get()) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
    }

    /**
     * Launch the initial-sync snapshot on a dedicated background thread, exactly once.
     *
     * The snapshot runs concurrently with the change-stream watch (which is driven on the
     * replication loop thread). It first waits for the watch to be live ({@code watchLive}, set by
     * the WatchCommand registration callback) so that every write happening during the copy is
     * already being captured into {@code eventQueue}; only then does it copy the data. When the
     * copy is done it opens the {@code applying} gate, which lets the batch processor drain the
     * events that were buffered during the copy -- an idempotent replay on top of the snapshot
     * (see the idempotency note below) -- followed by all subsequent live events.
     *
     * Failure/retry semantics: the snapshot uses its own pool connections, so it can fail
     * transiently (e.g. a per-collection read error) while the change-stream watch is perfectly
     * healthy. Because the replication loop thread is parked inside {@code watchForChanges()} for
     * the whole life of a healthy watch, it will NOT come back around to relaunch the snapshot.
     * So this thread retries the snapshot itself, with exponential backoff, keeping the gate
     * closed (events keep buffering) until a copy succeeds -- rather than resetting state and
     * relying on the loop to retry, which would leave the node permanently ungated (and eventually
     * fill the bounded queue, blocking the watch reader) whenever the watch stays up. Each retry
     * first drops any partially-copied local data ({@link #clearLocalDatabases()}) so that
     * {@code performInitialSync}'s strict inserts start from a clean slate instead of failing on
     * documents left behind by a previous, partially-successful attempt.
     *
     * Idempotency of the replay: the buffered events may cover documents the snapshot already
     * copied (a document inserted during the copy can appear both in the snapshot's find() and as
     * a buffered insert event). Update/replace events are already applied as full-document
     * upserts-by-key, so they are naturally idempotent; a delete of a document the snapshot never
     * contained is a no-op. Buffered *inserts* that collide with an already-copied _id are handled
     * by the existing bulk-insert path: an ordered _id-duplicate makes the bulk command fail, and
     * {@code applyBulkInserts} then falls back to a per-event idempotent replay
     * ({@link #applyInsertIdempotent}, a {@code {q: _id, u: doc, upsert: true}} upsert), which
     * converges the colliding document instead of stalling on a duplicate key. We deliberately do
     * NOT route every replicated insert through the per-document idempotent path permanently:
     * that would defeat the contiguous-insert bulk batching and, because InMemoryDriver's
     * upsert-replace path skips unique-secondary-index enforcement, would be strictly weaker than
     * the bulk path for genuine unique-index conflicts. The bulk-with-idempotent-fallback already
     * makes the replay window lossless and convergent, which is all the initial sync needs.
     */
    private void startInitialSyncOnce() {
        if (!initialSyncStarted.compareAndSet(false, true)) {
            return; // snapshot already launched (or completed)
        }

        initialSyncThread = new Thread(() -> {
            long backoffMs = 1000;
            wipedThisSyncCycle.set(false); // fresh cycle: no wipe has happened yet, shortcut is fair game
            try {
                while (running.get()) {
                    // Wait until the watch is live before copying, so every write that happens
                    // during the snapshot is already being captured into eventQueue. The watch may
                    // establish on this or a later (reconnect) attempt; poll running so a stop()
                    // during this wait exits promptly.
                    while (running.get() && !watchLive.get()) {
                        Thread.sleep(50);
                    }
                    if (!running.get()) {
                        return;
                    }
                    // Capture the generation of the watch we are about to copy under. If it changes
                    // (or watchLive drops) before the copy finishes, the watch died mid-copy and a
                    // replacement started "from now" with no resume point, losing the writes in the
                    // gap -- we must redo the snapshot under the new watch.
                    long watchGen = watchGeneration.get();

                    try {
                        // Consistency shortcut (leader change with identical data): the watch on
                        // the (possibly new) primary is live at this point, so every subsequent
                        // primary write is already being buffered. If the local state matches the
                        // primary byte-for-byte per dbHash, the clear + full snapshot below is
                        // pure waste - skip it. Any mismatch, error, or watch death falls through
                        // to today's full path; correctness beats speed.
                        //
                        // shouldAttemptConsistencyShortcut() is false once this cycle has already
                        // wiped local data via clearLocalDatabases() (see wipedThisSyncCycle's
                        // javadoc) - a retry in that state must not re-run the shortcut, it would
                        // be comparing the primary against data this very cycle just copied.
                        boolean shortcut = shouldAttemptConsistencyShortcut() && tryConsistencyShortcut();

                        // stop() may have interrupted this thread while tryConsistencyShortcut()'s
                        // blocking IO was in flight (or at any point up to here). Its
                        // InterruptedException handling restores the interrupt flag but converts the
                        // exception itself into `false` (a correctness fallback -> full sync), which by
                        // itself would let a STOPPED cycle fall straight into clearLocalDatabases()
                        // below and wipe local data - possibly while an already-running replacement
                        // ReplicationManager (PoppyDB replaces RMs on leader change) is populating the
                        // very same local database. running.get() alone already catches the stop()
                        // case (it flips false before the interrupt is even sent), and the interrupt
                        // check is the belt-and-suspenders half for any other source of interruption.
                        if (!running.get() || Thread.currentThread().isInterrupted()) {
                            break;
                        }

                        if (!shortcut) {
                            // Fail-closed destructive-resync guard (D2, 2026-08-14 empty-node-wipe
                            // fix): a legitimate primary NEVER regresses its own change-stream
                            // sequence counter - not even a real, replicated dropDatabase, which is
                            // itself an event and therefore ADVANCES the counter. A primary whose
                            // sequence at THIS watch registration is BEHIND the sequence our local
                            // data was last known to reflect can therefore only be a freshly
                            // restarted/stale process that reset its counter to 0 (or an older
                            // build's "resume window lost" chain that lost the original data's
                            // provenance) - not a trustworthy source of "the real current state".
                            // Wiping local data to match it would be the exact kill chain this fix
                            // closes: a restarted, empty node winning re-election (or simply coming
                            // back up on the same address) and every follower dropping its real
                            // data to match it. Refuse instead: keep the data, keep retrying with
                            // backoff - a later, genuinely caught-up primary (sequence >= ours)
                            // un-sticks this on its own, no manual intervention needed.
                            long primarySeqAtRegistration = lastKnownPrimarySequence.get();
                            long localSeqBeforeWipe = lastAppliedSequence.get();

                            if (primarySeqAtRegistration < localSeqBeforeWipe) {
                                log.error("refusing full re-sync: primary sequence {} is behind local {} - "
                                        + "possible restarted/stale primary, keeping local data",
                                        primarySeqAtRegistration, localSeqBeforeWipe);
                                refusingDestructiveResync.set(true);
                                refusedResyncCount.incrementAndGet();
                                Thread.sleep(backoffMs);
                                backoffMs = Math.min(backoffMs * 2, 30_000);
                                continue;
                            }

                            refusingDestructiveResync.set(false);

                            // Start each attempt from a clean local slate so a retry after a
                            // partially-successful copy doesn't fail on already-copied documents.
                            // The flag is set BEFORE the clear: even a clear that throws partway
                            // leaves the local state partially wiped, and a later retry must not
                            // run the consistency shortcut against that.
                            wipedThisSyncCycle.set(true);
                            // Initial-sync writes are never observable via the local change
                            // stream (MongoDB: initial sync is not oplogged). Without this, the
                            // wipe below is broadcast as live "drop" events - and during a
                            // leadership transition the other nodes' still-running OLD
                            // ReplicationManagers (watching this demoted ex-primary) apply those
                            // drops to their own data, destroying admin.system.users
                            // cluster-wide (the StepdownReplicationTest flake: even the freshly
                            // promoted primary applied the demoted node's wipe-drop).
                            try (var ignored = localDriver.suppressChangeStreamEvents()) {
                                clearLocalDatabases();
                                performInitialSync();
                            }
                        }

                        // Guard: if the watch died or was re-established during the copy (or the
                        // shortcut's hash comparison), the result may be missing writes that fell
                        // into the gap. Discard it and retry under the new watch instead of
                        // opening the gate on a lossy snapshot / stale match.
                        if (watchInvalidatedDuringSnapshot(watchGen)) {
                            log.warn("Watch changed during initial sync (captured gen {}, now {}, live {}); "
                                    + "redoing snapshot in {}ms to avoid a lost-write gap",
                                    watchGen, watchGeneration.get(), watchLive.get(), backoffMs);
                            Thread.sleep(backoffMs);
                            backoffMs = Math.min(backoffMs * 2, 30_000);
                            continue;
                        }

                        // #322: the pair above is owned by the watch READER thread - which may be
                        // parked in byte-budget backpressure (the apply gate is still closed, so
                        // nothing drains) and therefore unable to notice that the PRIMARY killed
                        // its cursor for buffer overflow. watchLive/watchGeneration then look
                        // healthy over a provably dead stream with a real event gap. Ask the
                        // primary itself - the one liveness signal the blocked reader cannot make
                        // stale - and discard the snapshot instead of opening the gate over it.
                        if (!primaryConfirmsWatchCursorAlive()) {
                            long discards = snapshotsDiscardedDeadWatch.incrementAndGet();
                            log.warn("Primary reports this cycle's watch cursor dead (killed while the "
                                    + "reader was blocked in backpressure); discarding snapshot #{} and "
                                    + "redoing it under a fresh watch in {}ms", discards, backoffMs);
                            discardDeadWatchSession();
                            Thread.sleep(backoffMs);
                            backoffMs = Math.min(backoffMs * 2, 30_000);
                            continue;
                        }

                        // Not (or no longer) refusing: this attempt is about to declare success,
                        // whether via the shortcut or a full copy, both of which require the guard
                        // above to have passed (or never triggered - shortcut skips it entirely,
                        // but a matching dbHash on non-trivial data is itself strong evidence of a
                        // legitimate, caught-up primary).
                        refusingDestructiveResync.set(false);

                        // Adopt this attempt's confirmed primary sequence as our new base now that
                        // we are declaring success (I-1, 2026-08-14 final review fix). A plain
                        // set(), NOT Math.max(current, ...): change-stream sequences are
                        // PRIMARY-LOCAL (see tryConsistencyShortcut's own javadoc on this) - the
                        // OLD lastAppliedSequence (from whatever primary we last successfully
                        // tracked, possibly a dead one with a much HIGHER counter than this brand
                        // new/still-quiet primary) lives in a completely different, incomparable
                        // number space from THIS primary's. Taking the max of two unrelated
                        // counters is not "the safer of two options", it is meaningless - and
                        // concretely harmful: it left this node believing it needed to resume
                        // after a sequence number the new primary's own history could never
                        // contain, so the very next reconnect always hit "resume window lost" ->
                        // a dbHash mismatch (as soon as one real write happened) -> the D2 guard
                        // above comparing the new primary's still-low counter against that stale
                        // inherited high-water mark -> refusing an entirely LEGITIMATE resync,
                        // unbounded on a quiet cluster (the new primary would need N more writes
                        // before its counter ever caught up to the old primary's abandoned one).
                        // "Having successfully synced against THIS primary, its base is my base."
                        //
                        // Two compositions this set() must not break, both verified safe:
                        //
                        // (1) The election feed just below must not regress. It doesn't:
                        // ElectionManager#updateLogIndex is ITSELF monotonic-max internally
                        // (`if (index >= lastLogIndex.get())`, a lower index is silently a no-op)
                        // - so adopting a LOWER base here can at most make the value THIS method
                        // reports go down, never the election's own recorded lastLogIndex. The
                        // monotonic guarantee Task 1 relies on lives in ElectionManager, by
                        // design, precisely so a primary-local counter reset on THIS side can
                        // never regress it - see updateLogIndex's own javadoc.
                        //
                        // (2) Events buffered during the sync window are not lost. Every event
                        // sitting in eventQueue right now was captured by the watch AFTER this
                        // same registration (recordPrimarySequenceAtRegistration ran, and hence
                        // lastKnownPrimarySequence was captured, at the START of this sync cycle -
                        // strictly before any of those events could have arrived), so every
                        // buffered event's own sequence number is >= lastKnownPrimarySequence.
                        // Setting lastAppliedSequence to that lower bound now and then draining
                        // the gate is safe: applyChangeEvent/applyBulkInserts advance it further
                        // via their own per-event Math.max as each buffered (and all subsequent
                        // live) event is applied - nothing regresses, nothing is skipped.
                        lastAppliedSequence.set(lastKnownPrimarySequence.get());

                        // Success: open the gate. The batch processor now drains the events
                        // buffered during the snapshot (idempotent replay) and all subsequent live
                        // events, in order.
                        lastSyncWasShortcut.set(shortcut);
                        applying.set(true);
                        initialSyncComplete.set(true);
                        initialSyncLatch.countDown();

                        // Arm the completion notification - do NOT fire it here (#306 review
                        // follow-up): "sync complete" at this point only means the snapshot is
                        // copied and the apply gate is open; the events buffered during the
                        // snapshot are still in eventQueue. The batch processor fires the
                        // callback once that backlog has drained (see
                        // maybeFireSyncCompleteNotify), and only while this manager is still
                        // running - firing from THIS thread would also break stop()'s
                        // documented invariant that the sync thread never calls back into
                        // PoppyDB (whose monitor stop() holds while joining this thread).
                        syncCompleteNotifyPending.set(true);

                        // Seed the election layer's view of our replication position now that we
                        // hold the primary's dataset (either path: full snapshot or consistency
                        // shortcut both land here). lastAppliedSequence is already correct at this
                        // point (either seeded at registration when it started at 0, or reseeded
                        // just above when it did not) - see the reseed comment above for the full
                        // picture. Without this, a freshly-synced node that then applies zero LIVE
                        // events would never reach
                        // processBatch()'s onLogIndexUpdate call (it only fires when there is
                        // something in eventQueue to drain) and would keep reporting index 0 to
                        // ElectionManager despite actually holding real data - wrongly granting
                        // votes to genuinely empty candidates as voter, and wrongly denied as
                        // candidate. updateLogIndex()'s monotonic (max) semantics make this safe to
                        // call unconditionally: it can only raise ElectionManager's view, never
                        // regress it.
                        //
                        // This report is deliberately NOT the only one: on a loaded host this
                        // thread can get here BEFORE the watch thread's registration callback has
                        // seeded lastAppliedSequence (the sync is released by watchLive alone,
                        // which that callback flips before recording the seed - and a fast
                        // shortcut/tiny-dataset sync can outrun the gap). The read below then
                        // sees 0 and reports nothing - which is fine, because the late-landing
                        // seed reports itself (see recordPrimarySequenceAtRegistration) and the
                        // batch processor's flush tick reconciles periodically as a net.
                        reportLogIndexToElection();
                        return;
                    } catch (Exception e) {
                        // #323: a stopped manager's cycle ends here quietly - retrying (or even
                        // logging an ERROR) for a sync that stop() abandoned is noise, and the
                        // cooperative checks above deliberately funnel that case into this catch.
                        if (!running.get()) {
                            log.info("Initial sync abandoned: replication manager was stopped");
                            return;
                        }

                        // Snapshot failed while the watch may still be healthy. Retry from within
                        // this thread with backoff, keeping the gate closed, so the node cannot get
                        // stuck permanently ungated when watchForChanges() is parked on a healthy
                        // watch and never returns to drive the loop's retry.
                        log.error("Initial sync failed, retrying in {}ms (replication gate stays closed): {}",
                                backoffMs, e.getMessage(), e);
                        Thread.sleep(backoffMs);
                        backoffMs = Math.min(backoffMs * 2, 30_000);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "PoppyDB-InitialSync");
        initialSyncThread.setDaemon(true);
        initialSyncThread.start();
    }

    /**
     * True when the change-stream watch that the snapshot started copying under is no longer the
     * live watch: either it dropped ({@code watchLive} is false) or a new watch has since registered
     * ({@code watchGeneration} advanced past {@code capturedGeneration}). Either way the snapshot may
     * be missing writes from the gap and must be redone. Package-private so the seam test can drive
     * the predicate without a live primary.
     */
    boolean watchInvalidatedDuringSnapshot(long capturedGeneration) {
        return !watchLive.get() || watchGeneration.get() != capturedGeneration;
    }

    /**
     * #322: asks the PRIMARY whether the current watch session's cursor still exists
     * ({@code poppyCursorAlive}, answered from its {@code WatchCursorManager}). Called by the
     * initial-sync guard after the snapshot, because the reader-owned {@code watchLive}/
     * {@code watchGeneration} pair goes stale exactly when the reader is parked in byte-budget
     * backpressure - the very situation in which the primary kills the cursor.
     *
     * <p>Fail-open cases (returns {@code true} - the guard behaves as before this fix):
     * <ul>
     * <li>no wire cursor id available (tests driving the sync loop without a real watch, or a
     *     watch established through a path that does not stash the "cursor" metadata);</li>
     * <li>the primary answers but does not understand the command (older PoppyDB during a
     *     rolling upgrade - it answered, so it is reachable; the pre-fix behaviour is the best
     *     available).</li>
     * </ul>
     * Fail-closed on a transport error: a primary that cannot even be asked cannot be serving
     * the watch either - discarding the snapshot is the only safe answer.
     */
    private boolean primaryConfirmsWatchCursorAlive() {
        WatchCommand session = activeWatchCommand;
        Object cursorId = session == null ? null : session.getMetaData().get("cursor");

        if (!(cursorId instanceof Number cursorIdNum) || cursorIdNum.longValue() == 0) {
            return true; // nothing to validate against - fail open
        }

        MongoConnection con = null;

        try {
            con = primaryMorphium.getDriver().getPrimaryConnection(null);
            GenericCommand cmd = new GenericCommand(con);
            cmd.setDb("admin");
            cmd.setCmdData(Doc.of("poppyCursorAlive", cursorIdNum.longValue()));
            int msgId = cmd.executeAsync();
            Map<String, Object> result = con.readSingleAnswer(msgId);

            if (result != null && Double.valueOf(1.0).equals(result.get("ok"))) {
                return Boolean.TRUE.equals(result.get("alive"));
            }

            log.warn("Primary did not understand poppyCursorAlive (rolling upgrade?) - "
                    + "cannot validate the watch cursor, proceeding as before: {}", result);
            return true;
        } catch (Exception e) {
            log.warn("Could not ask the primary about watch cursor {} - treating the watch as "
                    + "dead (a primary that cannot be reached cannot serve the watch either): {}",
                    cursorIdNum, e.getMessage());
            return false;
        } finally {
            if (con != null) {
                try {
                    primaryMorphium.getDriver().releaseConnection(con);
                } catch (Exception ignored) {
                }
            }
        }
    }

    /**
     * #322: retire the current watch session after its cursor died on the primary. Three things,
     * in one atomic step under {@code eventQueueByteLock}:
     * <ul>
     * <li>bump {@code replicationSessionEpoch}, so events of the dead session that the (possibly
     *     still blocked) reader enqueues from now on are dropped instead of being applied as
     *     stale upserts over the next snapshot;</li>
     * <li>drain-and-release the buffered events (exact byte accounting, wakes a producer blocked
     *     on the byte budget - the wake is what lets the reader run into the dead cursor's
     *     getMore error and reconnect);</li>
     * <li>flip {@code watchLive}, so the retry loop waits for the NEXT watch registration
     *     instead of re-snapshotting under the dead one (the reader's own finally confirms this
     *     shortly after).</li>
     * </ul>
     */
    private void discardDeadWatchSession() {
        synchronized (eventQueueByteLock) {
            replicationSessionEpoch.incrementAndGet();
            List<QueuedEvent> discarded = new ArrayList<>();
            eventQueue.drainTo(discarded);
            releaseEventQueueBytes(discarded);
            eventQueueByteLock.notifyAll(); // wake byte-waiters even when nothing was buffered
        }

        watchLive.set(false);
    }

    /** Test hook: force the watchLive flag. */
    void setWatchLiveForTest(boolean live) {
        watchLive.set(live);
    }


    /**
     * True once the change-stream watch has registered with the primary (see {@code watchLive}'s
     * field javadoc above for the design: watch registration is the FIRST step of a sync cycle,
     * before any snapshot, so on a healthy connection this goes true within seconds of
     * {@link #start()}). Package-private accessor backing PoppyDB's post-start liveness probe
     * (Finding: for an unreachable leader, {@code PooledDriver.connect()} swallows the failure and
     * {@code start()} returns normally instead of throwing, so the exception-based retry never
     * fires - watchLive staying false long after start is the reliable signal that the connection
     * never actually came up).
     */
    boolean isWatchLive() {
        return watchLive.get();
    }

    /**
     * True once the change-stream watch has registered with the primary AT LEAST ONCE since
     * {@link #start()} ({@code watchGeneration} only ever advances, one bump per registration).
     * This - not the instantaneous {@link #isWatchLive()} - is what PoppyDB's one-shot
     * post-start liveness probe must check: {@code watchLive} deliberately drops to false in
     * the watch loop's finally block between every two watch sessions, so a probe sampling
     * {@code isWatchLive()} during such a routine reconnect gap would tear down a
     * ReplicationManager whose connection DID come up (2026-08-06 review finding). A watch that
     * registered once and later died is the watch-retry loop's job to repair, not the probe's.
     */
    boolean hasWatchEverRegistered() {
        return watchGeneration.get() > 0;
    }

    /**
     * True when the initial-sync retry loop should attempt the consistency shortcut for the
     * current iteration; false once {@link #wipedThisSyncCycle} has been set by a
     * {@code clearLocalDatabases()} call earlier in the same sync cycle. Package-private (the
     * same seam pattern as {@link #watchInvalidatedDuringSnapshot}) so a test can drive the guard
     * without a live primary.
     */
    boolean shouldAttemptConsistencyShortcut() {
        return !wipedThisSyncCycle.get();
    }

    /** Test hook: force the wipedThisSyncCycle guard (see its javadoc) without running a real sync. */
    void setWipedThisSyncCycleForTest(boolean wiped) {
        wipedThisSyncCycle.set(wiped);
    }

    /** Test hook: simulate a watch (re-)registration bumping the generation. */
    void bumpWatchGenerationForTest() {
        watchGeneration.incrementAndGet();
    }

    /**
     * Consistency shortcut for the initial sync: decide, via dbHash, whether the local state
     * already matches the primary - in which case the clear + full snapshot can be skipped
     * entirely. Returns {@code true} on a verified full match (and has then already converged
     * the index definitions, see below); {@code false} on ANY mismatch, error, or timeout, in
     * which case the caller runs today's full clear + snapshot. Never throws.
     *
     * <p><b>Why hashes and not a sequence resume:</b> change-stream sequences are primary-local.
     * Each node's InMemoryDriver numbers events from its own private {@code changeStreamSequence}
     * counter (reset to 0 on restart, advanced by arbitrary jumps on drops), and a follower
     * applying replicated writes generates its OWN local numbers - nothing propagates the
     * primary's numbering into the follower's counter. So the {@code lastAppliedSequence} this
     * node accumulated against the OLD primary is meaningless in a NEW primary's sequence space
     * (InMemoryDriver's resume check explicitly treats foreign-sequence-space tokens as never
     * resumable); "resuming" there could silently skip or replay the wrong events. Comparing the
     * actual data is the only sound cheap path.
     *
     * <p><b>Soundness of match-then-replay (the hash-vs-buffer window):</b> this runs in the
     * same retry-loop slot as the snapshot, i.e. strictly AFTER the watch on the primary is
     * registered and live - from that moment every primary write is captured into
     * {@code eventQueue} (the gate is still closed, so nothing is applied locally; and this
     * node, being a follower, accepts no local data-plane writes either, so the local state is
     * frozen throughout the comparison). Each per-collection hash the primary answers is a
     * read-locked snapshot of that collection at some instant t &gt;= watch registration. If the
     * hashes match, the frozen local collection equals the primary's state at t - which already
     * INCLUDES every buffered event on that collection with an effect before t. Replaying those
     * buffered events after the gate opens is therefore a pure idempotent overlap (update/replace
     * are upserts-by-key, deletes are no-ops, colliding inserts go through applyBulkInserts'
     * idempotent per-event fallback - expected "Duplicate _id" noise, not corruption), and events
     * after t apply exactly as in steady-state replication. If instead a write landed between
     * registration and the hash read, the hashes differ and we take the full path - a spurious
     * fallback is possible, a spurious match is not. Watch death during the comparison is caught
     * by the caller's watchInvalidatedDuringSnapshot guard, same as for a real snapshot.
     *
     * <p><b>What is compared:</b> exactly the replicated namespace set per {@link #isReplicated}:
     * every non-system database's non-system collections, plus the replicated admin system
     * collections admin.system.users and admin.system.version (users must match too - a stale
     * user set is divergence like any other; a follower legitimately holding the SAME users it
     * replicated earlier is precisely the match case). Databases whose
     * replicated-collection set is empty count as absent on both sides. Collection sets must be
     * equal AND every per-collection dbHash must agree.
     *
     * <p><b>Indexes:</b> dbHash covers documents, not index definitions. The full path replicates
     * indexes right after its snapshot (#258: never report "synced" while missing the primary's
     * unique/TTL constraints); the shortcut upholds the same invariant by running the same
     * {@link #syncIndexesFrom} diff after the data match. If that fails, the shortcut is
     * abandoned and the full path (which redoes the index sync) runs.
     */
    private boolean tryConsistencyShortcut() {
        consistencyShortcutAttempts.incrementAndGet();
        try {
            awaitTestPauseIfArmed();
            MorphiumDriver primaryDriver = primaryMorphium.getDriver();
            SortedMap<String, SortedSet<String>> primaryNs =
                replicatedNamespaces(primaryDriver.listDatabases(), db -> primaryDriver.listCollections(db, null));
            SortedMap<String, SortedSet<String>> localNs =
                replicatedNamespaces(localDriver.listDatabases(), db -> localDriver.listCollections(db, null));

            if (!primaryNs.equals(localNs)) {
                log.info("Falling back to full sync: replicated namespace sets differ (primary: {}, local: {})",
                        primaryNs, localNs);
                return false;
            }

            int verified = 0;

            for (Map.Entry<String, SortedSet<String>> e : primaryNs.entrySet()) {
                String db = e.getKey();
                List<String> colls = new ArrayList<>(e.getValue());
                Map<String, Object> primaryHashes = collectionHashesOnPrimary(db, colls);
                Map<String, Object> localHashes = collectionHashesLocal(db, colls);

                for (String coll : colls) {
                    Object p = primaryHashes.get(coll);
                    Object l = localHashes.get(coll);

                    if (p == null || !p.equals(l)) {
                        log.info("Falling back to full sync: dbHash mismatch on {}.{} (primary: {}, local: {})",
                                db, coll, p, l);
                        return false;
                    }

                    verified++;
                }
            }

            // Data matches; converge the index definitions too before declaring victory (see
            // javadoc). A failure lands in the catch below -> full path.
            syncIndexesFrom(primaryDriver);

            log.info("Consistency shortcut taken ({} collections verified): local state matches primary, "
                    + "skipping clear + full snapshot", verified);
            return true;
        } catch (InterruptedException e) {
            // stop() interrupting the initial-sync thread must not be swallowed: restore the flag
            // so the caller's next blocking call (full sync or retry backoff) exits promptly.
            Thread.currentThread().interrupt();
            log.info("Falling back to full sync: consistency check interrupted");
            return false;
        } catch (Exception e) {
            log.info("Falling back to full sync: consistency check failed: {}", e.getMessage());
            return false;
        }
    }

    /** A function that may throw a driver exception - listCollections in both driver flavors. */
    private interface CollectionLister {
        List<String> collectionsOf(String db) throws Exception;
    }

    /**
     * The replicated namespace map of one side: database -> sorted set of its replicated
     * collections (per {@link #isReplicated} - so for admin at most system.users and
     * system.version survive, and local/config never contribute). Databases with no replicated
     * collection are omitted, so a
     * db existing on one side only as an empty shell (or with nothing but system collections)
     * does not count as divergence.
     */
    private SortedMap<String, SortedSet<String>> replicatedNamespaces(List<String> databases,
            CollectionLister lister) throws Exception {
        SortedMap<String, SortedSet<String>> result = new TreeMap<>();

        for (String db : databases) {
            SortedSet<String> colls = new TreeSet<>();

            for (String coll : lister.collectionsOf(db)) {
                if (isReplicated(db, coll)) {
                    colls.add(coll);
                }
            }

            if (!colls.isEmpty()) {
                result.put(db, colls);
            }
        }

        return result;
    }

    /** Per-collection dbHash of one database on the primary, over the existing connection pool. */
    private Map<String, Object> collectionHashesOnPrimary(String db, List<String> colls) throws Exception {
        MongoConnection con = primaryMorphium.getDriver().getReadConnection(null);

        try {
            GenericCommand cmd = new GenericCommand(con);
            cmd.setDb(db);
            cmd.setCmdData(Doc.of("dbHash", 1, "collections", colls, "$db", db));
            int msgId = cmd.executeAsync();
            Map<String, Object> result = con.readSingleAnswer(msgId);
            return extractCollectionHashes(db, result);
        } finally {
            primaryMorphium.getDriver().releaseConnection(con);
        }
    }

    /** Per-collection dbHash of one database on the local driver. */
    private Map<String, Object> collectionHashesLocal(String db, List<String> colls) throws Exception {
        GenericCommand cmd = new GenericCommand(localDriver);
        cmd.setDb(db);
        cmd.setCmdData(Doc.of("dbHash", 1, "collections", colls, "$db", db));
        int msgId = localDriver.runCommand(cmd);
        Map<String, Object> result = localDriver.readSingleAnswer(msgId);
        return extractCollectionHashes(db, result);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> extractCollectionHashes(String db, Map<String, Object> dbHashResult) {
        if (dbHashResult == null || !(dbHashResult.get("ok") instanceof Number ok) || ok.doubleValue() != 1.0
                || !(dbHashResult.get("collections") instanceof Map)) {
            throw new IllegalStateException("dbHash on " + db + " did not answer ok: " + dbHashResult);
        }

        return (Map<String, Object>) dbHashResult.get("collections");
    }

    /**
     * Drop all non-system databases from the local driver.
     *
     * Used before each (re)try of the initial snapshot so {@code performInitialSync}'s strict
     * inserts start from a clean slate and don't fail on documents left behind by a previous,
     * partially-successful copy. Safe during the buffer phase: buffered change events are not
     * applied until the gate opens ({@code applying == true}), so only snapshot data lives locally
     * at this point -- dropping and re-copying it just rebuilds the snapshot, and the buffered
     * events are still replayed on top of it once the gate opens.
     */
    /**
     * Replicate the index definitions of every user database/collection from the given source
     * driver to the local one (#258). Change streams do not carry index DDL (neither MongoDB's
     * nor ours), so this runs after the initial-sync snapshot and periodically from
     * {@code indexSyncLoop} - the periodic diff also picks up createIndexes/dropIndexes that
     * happened on the primary while this node was disconnected.
     */
    void syncIndexesFrom(MorphiumDriver source) throws Exception {
        for (String dbName : source.listDatabases()) {
            for (String collName : source.listCollections(dbName, null)) {
                // Same namespace set as the data plane: only replicated collections get their
                // indexes converged. That includes admin.system.users - its documents arrive on
                // secondaries via replication (never via a local createUser), so any index the
                // primary keeps on it must be carried over here as well.
                if (!isReplicated(dbName, collName)) {
                    continue;
                }

                applyIndexDiff(dbName, collName, listIndexesOf(source, dbName, collName));
            }
        }
    }

    /**
     * Diff the primary's index list for one collection against the local one and converge:
     * create missing indexes (full spec - unique/TTL/partial/sparse/... survive), drop local
     * ones the primary no longer has. The {@code _id} index is never touched. The diff is
     * name-based; MongoDB refuses to change an existing index's options under the same name
     * anyway (IndexOptionsConflict), so a name match means the spec matches.
     */
    void applyIndexDiff(String db, String coll, List<IndexDescription> primaryIndexes) throws Exception {
        List<IndexDescription> localIndexes = listIndexesOf(localDriver, db, coll);
        Set<String> localNames = new HashSet<>();
        Set<String> primaryNames = new HashSet<>();

        for (IndexDescription l : localIndexes) {
            localNames.add(l.getName());
        }

        List<Map<String, Object>> toCreate = new ArrayList<>();

        for (IndexDescription p : primaryIndexes) {
            if (isIdIndex(p)) {
                continue;
            }

            primaryNames.add(p.getName());

            if (!localNames.contains(p.getName())) {
                toCreate.add(p.asMap());
            }
        }

        if (!toCreate.isEmpty()) {
            CreateIndexesCommand createCmd = null;

            try {
                createCmd = new CreateIndexesCommand(localDriver.getPrimaryConnection(null))
                .setDb(db).setColl(coll).setIndexes(toCreate);
                createCmd.execute();
                log.info("Index replication: created {} index(es) on {}.{}", toCreate.size(), db, coll);
            } finally {
                if (createCmd != null) {
                    createCmd.releaseConnection();
                }
            }
        }

        for (IndexDescription l : localIndexes) {
            if (isIdIndex(l) || primaryNames.contains(l.getName())) {
                continue;
            }

            DropIndexesCommand dropCmd = null;

            try {
                dropCmd = new DropIndexesCommand(localDriver.getPrimaryConnection(null))
                .setDb(db).setColl(coll).setIndex(l.getName());
                dropCmd.execute();
                log.info("Index replication: dropped stale index {} on {}.{}", l.getName(), db, coll);
            } finally {
                if (dropCmd != null) {
                    dropCmd.releaseConnection();
                }
            }
        }
    }

    private boolean isIdIndex(IndexDescription idx) {
        return idx.getKey() != null && idx.getKey().size() == 1 && idx.getKey().containsKey("_id");
    }

    /** listIndexes against any driver; a missing collection reports no indexes (mongod: code 26) */
    private List<IndexDescription> listIndexesOf(MorphiumDriver drv, String db, String coll) throws MorphiumDriverException {
        MongoConnection con = null;
        ListIndexesCommand cmd = null;

        try {
            con = drv.getReadConnection(null);
            cmd = new ListIndexesCommand(con).setDb(db).setColl(coll);
            return cmd.execute();
        } catch (MorphiumDriverException e) {
            if (e.getMessage() != null && e.getMessage().contains("Error: 26")) {
                return new ArrayList<>();
            }

            throw e;
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            } else if (con != null) {
                drv.releaseConnection(con);
            }
        }
    }

    private void clearLocalDatabases() throws Exception {
        clearLocalDatabasesInvocations.incrementAndGet();
        for (String dbName : localDriver.listDatabases()) {
            // admin/local/config are never dropped wholesale: they hold node-local state beyond
            // the replicated admin system collections (admin.system.users and
            // admin.system.version, both cleared separately below).
            if ("admin".equals(dbName) || "local".equals(dbName) || "config".equals(dbName)) {
                continue;
            }
            GenericCommand cmd = new GenericCommand(localDriver);
            cmd.setDb(dbName);
            cmd.setColl(null);
            cmd.setCmdData(Doc.of("dropDatabase", 1, "$db", dbName));
            runLocalApplyCommand(cmd, "pre-sync dropDatabase of " + dbName);
        }

        // admin.system.users DOES replicate, so the snapshot copy must fully define its
        // content: clear it here (right before the snapshot begins) so users deleted on the
        // primary while this node was disconnected cannot survive a resync - and so the
        // snapshot's strict inserts cannot collide with leftovers of a previous partial copy.
        // Only the collection is removed, never the admin database itself. drop() rather than
        // an empty-filter delete for the same auto-vivification reason documented for
        // system.version below: on a cluster that never ran createUser (no root user
        // configured), a delete's internal find() would phantom-create an empty system.users
        // on every resyncing secondary but not on the primary, asymmetrically diverging the
        // namespace set the consistency shortcut compares.
        localDriver.drop("admin", "system.users", null);

        // admin.system.version DOES replicate too (the users-file version-gate meta doc), and is
        // subject to the exact same staleness risk as admin.system.users above: without this
        // clear, a meta doc left over from BEFORE this node dropped out of the cluster would
        // survive the resync untouched (admin is never dropped wholesale), and the snapshot copy
        // would then land its own fresh appliedVersion doc alongside it via strict insert - either
        // colliding on _id (harmless, same doc) or, if the primary's meta doc genuinely changed
        // underneath, leaving stale data around long enough to wrongly gate a future users-file
        // apply on this node.
        //
        // Deliberately NOT the same "delete" GenericCommand idiom clearUsers above uses: an empty
        // delete against a collection that does not locally exist yet still runs a find() to
        // determine the (empty) match set, and InMemoryDriver's find() auto-vivifies the target
        // collection as a side effect (getCollection() lazily creates it, complete with its
        // implicit _id index) even when nothing is deleted. Since system.version has no writer
        // before the users-file feature (task 4) ever runs createUser/updateUser-style traffic
        // against it, that phantom empty collection would otherwise get created HERE, on every
        // secondary that ever completes a full sync - but never on a primary that has not
        // separately gone through this same path - permanently and asymmetrically diverging the
        // replicated-namespace set the initial-sync consistency shortcut compares
        // (tryConsistencyShortcut's replicatedNamespaces()), which would then always fall back to
        // a full sync instead of taking the shortcut. drop() is the safe idempotent primitive:
        // it removes the map entry outright (a no-op if the collection was never created) and
        // never conjures one into existence.
        localDriver.drop("admin", "system.version", null);
    }

    /**
     * Perform initial sync - copy all data from primary to secondary.
     */
    private void performInitialSync() throws Exception {
        // Same as applyEventsInOrder: the snapshot copy must not be refused by the local
        // memory watermark, or a secondary could never sync a near-watermark primary.
        try (var ignored = localDriver.bypassMemoryGuard()) {
            performInitialSyncGuarded();
        }
    }

    private void performInitialSyncGuarded() throws Exception {
        log.info("Starting initial sync from primary...");
        long startTime = System.currentTimeMillis();

        // List all databases on primary using the driver
        List<String> databases = primaryMorphium.getDriver().listDatabases();

        int totalDocs = 0;
        for (String dbName : databases) {
            throwIfSyncAbandoned(); // #323: no work for a manager that was stopped mid-copy
            // No db-level skip here: the per-collection isReplicated filter in syncDatabase
            // decides. admin must be enumerated (its system.users and system.version replicate);
            // for local and config every collection is filtered out there.
            totalDocs += syncDatabase(dbName);
        }

        // Replicate index definitions after the data copy (mongod also builds indexes after
        // cloning). A failure here fails the initial sync on purpose: the snapshot retry loop
        // redoes the whole sync, so the node never reports "synced" while missing the primary's
        // unique/TTL constraints (#258).
        syncIndexesFrom(primaryMorphium.getDriver());

        long duration = System.currentTimeMillis() - startTime;
        log.info("Initial sync complete: {} documents synced in {}ms", totalDocs, duration);
    }

    /**
     * Sync a single database from primary.
     */
    private int syncDatabase(String dbName) throws Exception {
        log.debug("Syncing database: {}", dbName);

        // List collections in database
        List<String> collections = primaryMorphium.getDriver().listCollections(dbName, null);

        int totalDocs = 0;
        for (String collName : collections) {
            throwIfSyncAbandoned(); // #323
            // Copy exactly the replicated namespace set - which includes admin.system.users
            // (copied verbatim by syncCollection: the documents carry credential material and
            // must arrive bit-identical for SCRAM to verify on this node).
            if (!isReplicated(dbName, collName)) {
                continue;
            }

            totalDocs += syncCollection(dbName, collName);
        }

        return totalDocs;
    }

    /**
     * Sync a single collection from primary.
     */
    private int syncCollection(String dbName, String collName) throws Exception {
        log.debug("Syncing collection: {}.{}", dbName, collName);

        // Use FindCommand to get all documents
        MongoConnection con = primaryMorphium.getDriver().getReadConnection(null);
        // #323: track the connection while the read is in flight, so stop() can close it - a
        // socket read blocked on a slow primary ignores Thread.interrupt() and would otherwise
        // run out its full 60s read timeout, long after stop()'s bounded join gave up.
        inFlightSyncConnection = con;
        try {
            FindCommand findCmd = new FindCommand(con)
                .setDb(dbName)
                .setColl(collName)
                .setFilter(Doc.of())
                .setBatchSize(1000);

            List<Map<String, Object>> documents = findCmd.execute();
            // Test seam (#323): parks the thread with the read RESULT already in hand - the
            // position a straggler is in when a slow read finally returns after stop() came
            // and went. The local insert below needs no network, so nothing else stops it.
            awaitSyncReadPauseIfArmed();
            // #323: a read that returns AFTER stop() must not write its result into the local
            // driver - by then the data belongs to this manager's successor, and a stale
            // foreign document that lands after the successor's copy is silent divergence.
            throwIfSyncAbandoned();

            if (documents == null || documents.isEmpty()) {
                return 0;
            }

            // Insert documents into local driver
            GenericCommand insertCmd = new GenericCommand(localDriver);
            insertCmd.setDb(dbName);
            insertCmd.setColl(collName);
            insertCmd.setCmdData(Doc.of(
                "insert", collName,
                "$db", dbName,
                "documents", documents
            ));

            runLocalApplyCommand(insertCmd, "initial-sync insert into " + dbName + "." + collName);

            log.debug("Synced {} documents to {}.{}", documents.size(), dbName, collName);
            return documents.size();
        } finally {
            inFlightSyncConnection = null;
            primaryMorphium.getDriver().releaseConnection(con);
        }
    }

    /**
     * #323: cooperative cancellation for the initial-sync copy. Checked between databases,
     * between collections and - the case that matters most - between a completed collection
     * read and its local insert: socket reads are not interruptible, so a sync thread that
     * stop() abandoned can resurface with a full read result long after its 5s join expired,
     * while a replacement ReplicationManager already owns the local data.
     */
    private void throwIfSyncAbandoned() throws InterruptedException {
        if (!running.get() || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("replication manager stopped - abandoning initial sync");
        }
    }

    /**
     * Watch for changes from primary and apply them locally.
     */
    private void watchForChanges() throws Exception {
        log.info("Starting change stream watch on primary...");

        // Initialize staleness tracker
        lastWatchResponseTime.set(System.currentTimeMillis());

        MongoConnection con = primaryMorphium.getDriver().getPrimaryConnection(null);
        WatchCommand cmd = null;
        // #322: every event this session enqueues carries this epoch; discarding the session
        // (dead-watch guard, triggerResync) bumps the epoch, so late events of this session are
        // dropped instead of applied over the next snapshot.
        final long sessionEpoch = replicationSessionEpoch.get();
        try {
            // Built as its own effectively-final local (rather than assigned straight into the
            // outer `cmd`) so the registration callback below can close over it and read back the
            // "poppyPrimarySequence" metadata that SingleMongoConnection.watch() stashes on it the
            // moment the cursor is established (see that class for the wire read). `cmd` still
            // gets assigned the same instance right after, for the finally block's release.
            final WatchCommand watchCmd = new WatchCommand(con)
                .setDb("admin")  // Watch at cluster level
                .setMaxTimeMS(500)  // 500ms timeout - low latency for messaging tests
                .setFullDocument(WatchCommand.FullDocumentEnum.updateLookup)
                .setPipeline(List.of());  // Empty = watch everything
            // Fires once the watch cursor is established on the primary. From that point the
            // stream captures every subsequent write, so the initial-sync snapshot can safely
            // start copying without losing writes that happen during the copy. Bump the
            // generation FIRST so a snapshot that captures the generation the instant it sees
            // watchLive observes the value belonging to this watch (not a stale one). Record
            // the primary's sequence BEFORE flipping watchLive: watchLive is what releases the
            // initial-sync thread, and recording after the flip opened a race window in which
            // a fast (shortcut/tiny-dataset) sync could run its ENTIRE cycle against the
            // still-stale pre-registration sequence - reading 0 for the destructive-resync
            // guard's primary side, re-basing lastAppliedSequence to 0 at success, and
            // skipping its election seed (the CI-flaky InitialSyncElectionSeedTest leg; the
            // catch-up report in recordPrimarySequenceAtRegistration is the net for any
            // ordering this reorder cannot guarantee, e.g. a primary that never sends the
            // sequence on the first watch). SingleMongoConnection.watch() stashes the
            // "poppyPrimarySequence" metadata before invoking this callback, so the read is
            // safe here.
            watchCmd.setRegistrationCallback(() -> {
                watchGeneration.incrementAndGet();
                recordPrimarySequenceAtRegistration(watchCmd);
                watchLive.set(true);
            });
            // #322: expose this session's command so the dead-watch guard can read the wire
            // cursor id ("cursor" metadata, stashed by SingleMongoConnection.watch()) and ask
            // the primary whether that cursor still exists - the one liveness signal a reader
            // parked in byte-budget backpressure cannot make stale.
            activeWatchCommand = watchCmd;
            cmd = watchCmd
                .setCb(new DriverTailableIterationCallback() {
                    @Override
                    public void incomingData(Map<String, Object> data, long cursorId) {
                        if (!running.get()) {
                            return;
                        }
                        // Update staleness tracker - we received a response
                        lastWatchResponseTime.set(System.currentTimeMillis());
                        // Queue for batch processing instead of immediate application.
                        // enqueueReplicationEvent() blocks the watch callback (backpressure)
                        // when the queue is at its count capacity or over its byte budget,
                        // rather than dropping events or growing without bound.
                        try {
                            enqueueReplicationEvent(data, sessionEpoch);
                            // Apply it now instead of waiting out the flush tick. Without this the
                            // batch processor only ran on its fixed BATCH_FLUSH_INTERVAL_MS
                            // schedule, so a single write that a write concern waits on paid the
                            // full interval - measured in-process (no network): 5.04 ms per
                            // individual store() against a 3-node replica set vs 0.31 ms against a
                            // single node, with p50 landing exactly on the 5 ms tick. Batched
                            // writes never showed it because one tick covers a whole batch.
                            requestFlush();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            log.warn("Interrupted while enqueuing replication event; dropping event");
                        }
                    }

                    @Override
                    public boolean isContinued() {
                        if (!running.get()) {
                            return false;
                        }
                        // Check for staleness - if no response for too long, assume connection is broken
                        long lastResponse = lastWatchResponseTime.get();
                        long now = System.currentTimeMillis();
                        if (lastResponse > 0 && (now - lastResponse) > STALENESS_THRESHOLD_MS) {
                            log.warn("Watch connection appears stale (no response for {}ms), forcing reconnection",
                                    now - lastResponse);
                            return false;
                        }
                        // While refusing a destructive resync (see the guard in
                        // startInitialSyncOnce()), recordPrimarySequenceAtRegistration() only ever
                        // refreshes lastKnownPrimarySequence at watch REGISTRATION - a live watch
                        // session registers exactly once, so without this, a refusal would freeze
                        // on the primary sequence observed at that one registration forever, never
                        // discovering that the primary has since caught up ("a later caught-up
                        // leader syncs normally" would then require some UNRELATED event, e.g. a
                        // real disconnect, to ever re-check). Ending the watch here lets the
                        // replication loop's own retry re-establish it, which re-registers and
                        // refreshes the primary-sequence signal the destructive-resync guard reads
                        // on its next attempt.
                        //
                        // PACING (2026-08-14 task-3 review fix): this is NOT reached only after a
                        // maxTimeMS getMore wait as the earlier version of this comment assumed -
                        // isContinued() is also checked immediately after the very first reply that
                        // establishes the watch cursor (SingleMongoConnection.watch()'s post-
                        // establishment check, before any getMore is ever issued), and
                        // replicationLoop() calls watchForChanges() again with no sleep of its own
                        // once it returns. Without an explicit sleep here those two facts combine
                        // into an unbounded register/teardown spin against a possibly-troubled
                        // primary - measured at ~1400 registrations/s in review, not the "~500ms,
                        // bounded, self-limiting" cadence this comment used to (wrongly) claim. The
                        // sleep paces every refusal retry, not just conceptually the first.
                        if (refusingDestructiveResync.get()) {
                            log.debug("Watch cycling while refusing a destructive resync, to refresh the "
                                    + "primary-sequence signal");
                            try {
                                Thread.sleep(REFUSAL_WATCH_PACE_MS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            return false;
                        }
                        return true;
                    }
                });

            // Resume-after-disconnect: once the initial sync is complete and we have applied events,
            // ask the primary to resume the stream right after our last-applied sequence instead of
            // starting "now" (which would silently drop every event that occurred while we were
            // disconnected). The token carries the standard change-stream _data (so the primary's
            // replay buffer delivers the gap) plus a "poppyResumeSequence" marker that tells the
            // primary this is a replication resume and to answer with an explicit "resume window lost"
            // error (rather than a truncated replay) when the buffer can no longer cover the gap.
            long resumeSeq = lastAppliedSequence.get();
            if (initialSyncComplete.get() && resumeSeq > 0) {
                cmd.setResumeAfter(Doc.of(
                    "_data", String.format(Locale.ROOT, "%016x", resumeSeq),
                    "poppyResumeSequence", resumeSeq));
                log.info("Resuming change stream after sequence {}", resumeSeq);
            }

            try {
                cmd.watch();
            } catch (MorphiumDriverException e) {
                if (isResumeWindowLost(e)) {
                    // Primary can no longer replay from our last-applied sequence — fall back to a
                    // full re-initial-sync via the Task 8 machinery.
                    triggerResync(resumeSeq);
                    return;
                }
                throw e;
            }
        } finally {
            // Watch is no longer live: a snapshot still waiting to start must wait for the next
            // watch attempt to re-establish before copying.
            watchLive.set(false);
            activeWatchCommand = null;
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }

        // If we get here, the watch ended - loop will restart it
        log.debug("Change stream watch ended");
    }

    /**
     * Records the primary's change-stream sequence as observed at watch registration (see the
     * "poppyPrimarySequence" wire field written by MongoCommandHandler.processChangeStream and read
     * back by SingleMongoConnection.watch() into this command's metadata).
     *
     * <p>Fixes the idle-window resume hole: after a completed initial sync during which ZERO
     * events were applied, {@code lastAppliedSequence} stays at its initial 0, so a later
     * reconnect's {@code resumeSeq > 0} check (see {@link #watchForChanges()}) fails and the new
     * watch silently starts "from now" - silently dropping every write that happened during the
     * gap. Seeding {@code lastAppliedSequence} here with the primary's sequence at THIS
     * registration closes that hole: any write after this point necessarily gets a token greater
     * than the seeded value (the primary's sequence counter only increases), so it is never missed
     * by a subsequent resumeAfter built from this seed, and nothing before this point needs
     * replaying (there is nothing this secondary hasn't already covered - either the initial-sync
     * snapshot captured it, or the secondary didn't exist yet).
     *
     * <p>Guarded with compareAndSet(0, ...) rather than an unconditional set: once any real event
     * is applied, {@code lastAppliedSequence} advances past this seed via the normal
     * updateAndGet(Math::max) path (see applyChangeEvent) and must never be pulled back down or
     * jumped forward past events that are still buffered/pending application - only a still-virgin
     * (0) value is safe to seed.
     *
     * <p>{@code lastKnownPrimarySequence} is updated unconditionally on every registration
     * (independent of the guard above) so it always reflects the most recently observed primary
     * sequence - exposed via {@link #getLastKnownPrimarySequence()} for callers such as a
     * replication-lag metric that need it regardless of whether it was actually used to seed the
     * resume point.
     *
     * <p>Package-private so the seed-race regression test ({@code InitialSyncElectionSeedTest})
     * can drive the exact losing interleaving (initial sync declares success BEFORE this seed
     * lands) deterministically, without depending on CI-load scheduling to hit it.
     */
    void recordPrimarySequenceAtRegistration(WatchCommand watchCmd) {
        Map<String, Object> metaData = watchCmd.getMetaData();
        Object raw = metaData == null ? null : metaData.get("poppyPrimarySequence");
        if (!(raw instanceof Number n)) {
            // Not a PoppyDB primary (or an older one without this field) - nothing to seed.
            return;
        }
        long primarySeq = n.longValue();
        lastKnownPrimarySequence.set(primarySeq);
        if (lastAppliedSequence.compareAndSet(0, primarySeq)) {
            log.debug("Seeded lastAppliedSequence with primary sequence {} at watch registration "
                    + "(idle-window resume point)", primarySeq);
            // Catch-up half of the initial-sync election seed (#306 follow-up): if the sync
            // loop already declared success, its own one-shot report over there read a still-0
            // lastAppliedSequence and was skipped - this seed landing is the moment the
            // position actually becomes known, so report it from here. In the ordinary,
            // non-racy order (seed lands at the START of a sync cycle, gate still closed)
            // reportLogIndexToElection()'s initialSyncComplete gate keeps this silent - a node
            // that does not hold the primary's dataset yet must not claim a position.
            reportLogIndexToElection();
        }
    }

    /**
     * Recognise the primary's explicit "resume window lost" signal (ChangeStreamHistoryLost, code
     * 286) sent when its replay buffer can no longer cover the gap after our last-applied sequence.
     */
    private boolean isResumeWindowLost(MorphiumDriverException e) {
        // Prefer the structured error code (286 = ChangeStreamHistoryLost); fall back to the message
        // text when the code did not survive the driver surface.
        if (e.getMongoCode() instanceof Number code && code.intValue() == 286) {
            return true;
        }
        String msg = e.getMessage();
        return msg != null && (msg.contains("resume window lost") || msg.contains("ChangeStreamHistoryLost"));
    }

    /**
     * Fall back to a full re-initial-sync after the primary signalled that our resume point is no
     * longer replayable. Rearms the Task 8 initial-sync machinery: closes the apply gate, resets the
     * sync flags so {@link #startInitialSyncOnce()} launches a fresh snapshot, and drops the events
     * left over from the lost window. The replication loop then re-runs initial sync + watch on its
     * next iteration.
     *
     * <p>Deliberately does NOT reset {@code lastAppliedSequence} to 0 (unlike before the 2026-08-14
     * empty-node-wipe fix). {@code initialSyncComplete} is already false at this point, which alone
     * already suppresses the next watch's {@code resumeAfter} (see the {@code initialSyncComplete.get()
     * && resumeSeq > 0} guard in {@link #watchForChanges()}) - zeroing the sequence was never load-
     * bearing for that. It WAS, however, load-bearing for a hazard: zeroing it here made
     * {@code recordPrimarySequenceAtRegistration()}'s reseed ({@code compareAndSet(0, primarySeq)})
     * fire unconditionally on the very next registration, silently replacing our real local data's
     * last-known-good sequence with whatever the new/possibly-empty primary reports - which is
     * exactly what let {@link #startInitialSyncOnce()}'s destructive-resync guard be defeated: by the
     * time that guard ran, the honest "how far behind is this primary" signal was already gone.
     * Preserving the value here is what lets that guard compare the primary's regressed sequence
     * against our data's true position instead of a freshly-overwritten 0. The now-stale value is
     * reseeded explicitly, and correctly, once a sync attempt actually succeeds (or is legitimately
     * allowed to proceed) - see the reseed at the "not (or no longer) refusing" point in
     * {@link #startInitialSyncOnce()}.
     */
    void triggerResync(long fromSequence) {
        long n = resyncCount.incrementAndGet();
        long now = System.currentTimeMillis();
        long previous = lastResyncTimestamp.getAndSet(now);
        if (previous != 0 && (now - previous) <= RESYNC_WARN_WINDOW_MS) {
            log.warn("replication cannot keep up — buffer sizes bound write rate × sync duration "
                    + "(resync #{} came {} ms after the previous one, within the {}-minute window)",
                    n, now - previous, TimeUnit.MILLISECONDS.toMinutes(RESYNC_WARN_WINDOW_MS));
        }
        log.warn("Primary signalled resume window lost at sequence {} — falling back to full re-sync (#{})",
                fromSequence, n);
        applying.set(false);            // close the apply gate until the new snapshot completes
        initialSyncComplete.set(false);
        initialSyncStarted.set(false);  // allow startInitialSyncOnce() to launch a new snapshot
        watchLive.set(false);
        lastReportedSequence.set(0);
        // Discard events buffered for the lost window. Drain-and-release instead of clear() so
        // the byte accounting stays exact (each queued event's bytes are subtracted
        // individually, converging with a producer that adds its bytes only after a successful
        // offer) and a producer blocked on the byte budget is woken. The epoch bump (under the
        // same lock the producer offers under, #322) retires the session: events the woken
        // reader still enqueues afterwards are dropped instead of surviving into the re-sync.
        synchronized (eventQueueByteLock) {
            replicationSessionEpoch.incrementAndGet();
            List<QueuedEvent> discarded = new ArrayList<>();
            eventQueue.drainTo(discarded);
            releaseEventQueueBytes(discarded);
            eventQueueByteLock.notifyAll();
        }
    }

    /**
     * Test hook: sever the replication connection and stop reconnecting, simulating a network
     * partition between this secondary and the primary. Writes on the primary during the pause are
     * not seen until {@link #resumeReplicationForTest()} is called.
     */
    void pauseReplicationForTest() {
        pausedForTest.set(true);
        connected.set(false);
        disconnectFromPrimary();
    }

    /** Test hook: heal the simulated partition; the replication loop reconnects and resumes. */
    void resumeReplicationForTest() {
        pausedForTest.set(false);
    }

    /** Number of times replication fell back to a full re-sync because the resume window was lost. */
    long getResyncCount() {
        return resyncCount.get();
    }

    /**
     * True while this node is currently refusing a destructive full re-sync because the primary's
     * sequence regressed below our local data's (see {@link #getStats()}'s
     * {@code refusingDestructiveResync}).
     */
    boolean isRefusingDestructiveResync() {
        return refusingDestructiveResync.get();
    }

    /** Lifetime count of destructive-resync refusals (see {@link #isRefusingDestructiveResync()}). */
    long getRefusedResyncCount() {
        return refusedResyncCount.get();
    }

    /**
     * True when the most recently completed initial sync was satisfied by the consistency
     * shortcut (local data already matched the primary per dbHash - no clear, no snapshot)
     * instead of a full copy. Only meaningful once {@link #isInitialSyncComplete()} is true.
     */
    boolean wasLastSyncShortcut() {
        return lastSyncWasShortcut.get();
    }

    /**
     * Apply a change event to the local driver.
     */
    private void applyChangeEvent(Map<String, Object> event) {
        applyChangeEvent(event, false);
    }

    /**
     * Apply a change event to the local driver.
     *
     * @param asReplay when {@code true}, an "insert" event is applied as an idempotent
     *                  full-document upsert-by-key (see {@link #applyInsertIdempotent})
     *                  instead of a strict insert. Used by {@code applyBulkInserts}'
     *                  per-event fallback after a failed/partially-failed bulk insert,
     *                  where some of the run's documents may already have been
     *                  committed -- a plain re-insert of those would spuriously fail on
     *                  a duplicate key. Other operation types are already applied
     *                  idempotently regardless of this flag (update/replace as an
     *                  upsert, delete/drop/dropDatabase are naturally safe to repeat).
     * @return {@code true} if the event applied without error; {@code false} if it threw (the
     *         exception is caught and logged internally either way, as before -- the return value
     *         is additive, used by {@code applyBulkInserts}' per-event fallback loop to decide the
     *         bulk-failure log level without otherwise changing this method's behavior).
     */
    @SuppressWarnings("unchecked")
    private boolean applyChangeEvent(Map<String, Object> event, boolean asReplay) {
        try {
            // Extract sequence number from resume token
            long sequenceNumber = extractSequenceFromEvent(event);

            String operationType = (String) event.get("operationType");
            Map<String, Object> ns = (Map<String, Object>) event.get("ns");

            if (ns == null) {
                log.debug("Ignoring event without namespace: {}", operationType);
                // Still update sequence for non-namespace events
                if (sequenceNumber > 0) {
                    lastAppliedSequence.updateAndGet(current -> Math.max(current, sequenceNumber));
                }
                return true;
            }

            String db = (String) ns.get("db");
            String coll = (String) ns.get("coll");

            // Skip everything outside the replicated namespace set (system databases and
            // system.* collections - except the replicated admin system collections, see isReplicated)
            if (!isReplicated(db, coll)) {
                // Still update sequence for skipped events
                if (sequenceNumber > 0) {
                    lastAppliedSequence.updateAndGet(current -> Math.max(current, sequenceNumber));
                }
                return true;
            }

            log.debug("Applying change event: {} on {}.{} seq={}", operationType, db, coll, sequenceNumber);

            switch (operationType) {
                case "insert": {
                    Map<String, Object> fullDoc = (Map<String, Object>) event.get("fullDocument");
                    Map<String, Object> docKey = (Map<String, Object>) event.get("documentKey");
                    if (fullDoc != null) {
                        if (asReplay && docKey != null) {
                            applyInsertIdempotent(db, coll, docKey, fullDoc);
                        } else {
                            GenericCommand cmd = new GenericCommand(localDriver);
                            cmd.setDb(db);
                            cmd.setColl(coll);
                            cmd.setCmdData(Doc.of(
                                "insert", coll,
                                "$db", db,
                                "documents", List.of(fullDoc)
                            ));
                            runLocalApplyCommand(cmd, "insert into " + db + "." + coll);
                        }
                    }
                    break;
                }

                case "update":
                case "replace": {
                    Map<String, Object> docKey = (Map<String, Object>) event.get("documentKey");
                    Map<String, Object> fullDoc = (Map<String, Object>) event.get("fullDocument");

                    if (fullDoc != null && docKey != null) {
                        // Replace the document
                        GenericCommand cmd = new GenericCommand(localDriver);
                        cmd.setDb(db);
                        cmd.setColl(coll);
                        cmd.setCmdData(Doc.of(
                            "update", coll,
                            "$db", db,
                            "updates", List.of(Doc.of(
                                "q", docKey,
                                "u", fullDoc,
                                "upsert", true
                            ))
                        ));
                        runLocalApplyCommand(cmd, "update of " + db + "." + coll);
                    }
                    break;
                }

                case "delete": {
                    Map<String, Object> docKey = (Map<String, Object>) event.get("documentKey");
                    if (docKey != null) {
                        GenericCommand cmd = new GenericCommand(localDriver);
                        cmd.setDb(db);
                        cmd.setColl(coll);
                        cmd.setCmdData(Doc.of(
                            "delete", coll,
                            "$db", db,
                            "deletes", List.of(Doc.of(
                                "q", docKey,
                                "limit", 1
                            ))
                        ));
                        runLocalApplyCommand(cmd, "delete from " + db + "." + coll);
                    }
                    break;
                }

                case "drop": {
                    GenericCommand cmd = new GenericCommand(localDriver);
                    cmd.setDb(db);
                    cmd.setColl(coll);
                    cmd.setCmdData(Doc.of("drop", coll, "$db", db));
                    runLocalApplyCommand(cmd, "drop of " + db + "." + coll);
                    break;
                }

                case "dropDatabase": {
                    GenericCommand cmd = new GenericCommand(localDriver);
                    cmd.setDb(db);
                    cmd.setColl(null);
                    cmd.setCmdData(Doc.of("dropDatabase", 1, "$db", db));
                    runLocalApplyCommand(cmd, "dropDatabase of " + db);
                    break;
                }

                case "invalidate": {
                    log.warn("Received invalidate event, change stream will be restarted");
                    break;
                }

                default:
                    log.debug("Ignoring event type: {}", operationType);
            }

            eventsApplied.incrementAndGet();
            lastEventTime.set(System.currentTimeMillis());

            // Update last applied sequence after successful application. NEVER a plain set: the
            // batch paths already advance this with Math.max, and an event can arrive out of
            // sequence (a replayed event racing live dispatch on a resume). A plain set would
            // then move the watermark BACKWARDS, and a reconnect would ask the primary to resume
            // from a point whose events this node has already applied - re-applying stale full
            // documents over newer ones.
            if (sequenceNumber > 0) {
                lastAppliedSequence.updateAndGet(current -> Math.max(current, sequenceNumber));
            }

            return true;
        } catch (Exception e) {
            log.error("Error applying change event: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Applies an insert event as an idempotent full-document upsert-by-key rather than a
     * strict insert.
     *
     * This is the replay-safe counterpart to the strict insert path above: it is used
     * when an insert event might be re-applied after already having landed (see the
     * per-event fallback in {@code applyBulkInserts}, and the upcoming initial-sync
     * replay in task 8). A strict insert of a document whose key already exists fails
     * with a duplicate-key error even when the replayed content is identical to what's
     * already there, which would incorrectly treat a harmless replay as a real conflict
     * and stall replication. Using {@code {q: documentKey, u: fullDocument, upsert:
     * true}} -- the exact same technique already used for replicated update/replace
     * events -- makes replay a no-op when the document already matches, and creates it
     * when it doesn't exist yet.
     *
     * A genuine unique-index conflict (a *different* document already owning a
     * unique-indexed value the replayed document also wants) still surfaces as an
     * exception when the replayed document doesn't exist yet (InMemoryDriver enforces
     * uniqueness for the upsert-creates-a-new-document case). Note this is currently
     * NOT enforced by InMemoryDriver when the upsert instead replaces an
     * already-existing document -- a pre-existing driver characteristic (its
     * full-document-replacement path skips the uniqueness check that its
     * partial-update path runs), not something introduced or relied upon here.
     */
    private void applyInsertIdempotent(String db, String coll, Map<String, Object> docKey,
                                        Map<String, Object> fullDoc) throws MorphiumDriverException {
        GenericCommand cmd = new GenericCommand(localDriver);
        cmd.setDb(db);
        cmd.setColl(coll);
        cmd.setCmdData(Doc.of(
            "update", coll,
            "$db", db,
            "updates", List.of(Doc.of(
                "q", docKey,
                "u", fullDoc,
                "upsert", true
            ))
        ));
        runLocalApplyCommand(cmd, "idempotent insert replay into " + db + "." + coll);
    }

    /**
     * Runs a command against the local driver AND fetches its result.
     *
     * Fetching is not optional: every {@code InMemoryDriver.runCommand()} stores its result in
     * an internal by-id map that is only ever cleared by the matching read
     * ({@code readSingleAnswer} et al.). On the primary the netty handler reads every answer;
     * here on the apply path nobody did, so every replicated non-bulk operation (update,
     * delete, drop, ...) leaked one result entry forever -- ~800 bytes per replicated event of
     * unbounded secondary heap growth, verified via GC class histograms on a live replica set.
     *
     * Fetching the result also surfaces errors that used to be swallowed silently. They are
     * logged, never thrown: an error reported inside an otherwise-delivered result must not
     * make the apply path fail harder than it did before this fix (exceptions thrown by
     * {@code runCommand} itself still propagate exactly as they always did). A
     * NamespaceNotFound (code 26) logs at debug only -- a replicated drop of a collection that
     * never materialized locally (it never held replicated documents on this node) is a normal
     * occurrence, not an operational problem.
     */
    private Map<String, Object> runLocalApplyCommand(GenericCommand cmd, String opDescription)
            throws MorphiumDriverException {
        int msgId = localDriver.runCommand(cmd);
        Map<String, Object> result = localDriver.readSingleAnswer(msgId);

        if (result == null) {
            log.warn("Local {} produced no result", opDescription);
            return null;
        }

        if (result.get("ok") instanceof Number ok && ok.doubleValue() == 0.0) {
            if (result.get("code") instanceof Number code && code.intValue() == 26) {
                log.debug("Local {}: namespace not found ({})", opDescription, result.get("errmsg"));
            } else {
                log.warn("Local {} failed: {}", opDescription, result.get("errmsg"));
            }
        } else if (result.get("writeErrors") instanceof List<?> errors && !errors.isEmpty()) {
            log.warn("Local {} reported writeErrors: {}", opDescription, errors);
        }

        return result;
    }

    /**
     * Extract the sequence number from a change event's resume token.
     * The InMemoryDriver uses format: {_id: {_data: "hex-encoded-sequence"}}
     */
    @SuppressWarnings("unchecked")
    private long extractSequenceFromEvent(Map<String, Object> event) {
        try {
            Object idObj = event.get("_id");
            if (idObj instanceof Map) {
                Map<String, Object> idMap = (Map<String, Object>) idObj;
                Object dataObj = idMap.get("_data");
                if (dataObj instanceof String) {
                    String hexData = (String) dataObj;
                    return Long.parseLong(hexData, 16);
                }
            }
        } catch (Exception e) {
            log.debug("Could not extract sequence from event: {}", e.getMessage());
        }
        return 0;
    }

    /**
     * Wait for initial sync to complete.
     */
    public boolean waitForInitialSync(long timeout, TimeUnit unit) throws InterruptedException {
        return initialSyncLatch.await(timeout, unit);
    }

    /**
     * Check if replication is running and connected.
     */
    public boolean isConnected() {
        return running.get() && connected.get();
    }

    /**
     * Check if initial sync is complete.
     */
    public boolean isInitialSyncComplete() {
        return initialSyncComplete.get();
    }

    /**
     * True while this secondary is (re-)running its initial sync and therefore may hold a
     * half-cleared / partial local database ({@link #clearLocalDatabases()} runs at the start of the
     * snapshot and again on a {@link #triggerResync}). A node in this state is the PoppyDB equivalent
     * of MongoDB's RECOVERING member: it must not serve data-plane reads or writes. Returns false
     * once the initial sync has completed and the local database is a consistent replica, and false
     * after {@link #stop()} (running == false).
     *
     * <p>Also true while {@link #isRefusingDestructiveResync()} holds - a node refusing a
     * destructive resync has NOT re-completed initial sync against the (currently untrusted) primary,
     * even though, unlike the ordinary half-cleared case this javadoc otherwise describes, its local
     * database is fully intact and deliberately left untouched. It is still treated as RECOVERING
     * here (conservative: correctness over availability) rather than carved out as a distinct state.
     */
    public boolean isSyncing() {
        return running.get() && !initialSyncComplete.get();
    }

    /**
     * Get the number of change events applied.
     */
    public long getEventsApplied() {
        return eventsApplied.get();
    }

    /**
     * Get replication statistics.
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("running", running.get());
        stats.put("connected", connected.get());
        stats.put("initialSyncComplete", initialSyncComplete.get());
        stats.put("lastSyncWasShortcut", lastSyncWasShortcut.get());
        stats.put("eventsApplied", eventsApplied.get());
        stats.put("lastEventTime", lastEventTime.get());
        stats.put("lastAppliedSequence", lastAppliedSequence.get());
        stats.put("lastReportedSequence", lastReportedSequence.get());
        stats.put("lastKnownPrimarySequence", lastKnownPrimarySequence.get());
        stats.put("resyncCount", resyncCount.get());
        // D2 (2026-08-14 empty-node-wipe fix): true while this node is deliberately refusing a
        // destructive full re-sync because the primary's sequence regressed below our local data's
        // - see the guard in startInitialSyncOnce(). refusedResyncCount is the monotonic lifetime
        // count of such refusals, independent of whether the flag is currently set.
        stats.put("refusingDestructiveResync", refusingDestructiveResync.get());
        stats.put("refusedResyncCount", refusedResyncCount.get());
        stats.put("primaryHost", primaryHost + ":" + primaryPort);
        stats.put("myAddress", myAddress);
        stats.put("eventQueueSize", eventQueue.size());
        stats.put("eventQueueCapacity", EVENT_QUEUE_CAPACITY);
        stats.put("eventQueueBytes", eventQueueBytes.get());
        stats.put("eventQueueByteBudget", eventQueueByteBudget);
        stats.put("eventQueueBytePressureCount", eventQueueBytePressureCount.get());
        // How many events behind the secondary is, based on the primary's sequence at the most
        // recent watch registration (Task 2b's exchange - see getLastKnownPrimarySequence()).
        // Clamped to 0: once live events keep flowing past that registration-time snapshot,
        // lastAppliedSequence naturally overtakes it between registrations, which is progress, not
        // negative lag.
        stats.put("replicationLagEvents",
                Math.max(0, getLastKnownPrimarySequence() - getLastAppliedSequence()));
        stats.put("watchGeneration", watchGeneration.get());
        return stats;
    }

    /**
     * Get the last applied sequence number.
     */
    public long getLastAppliedSequence() {
        return lastAppliedSequence.get();
    }

    /**
     * This instance's own replication target, in {@code "host:port"} form - the identity a
     * carried sequence must match to be comparable (see the two-arg
     * {@link #carryOverLastAppliedSequence(long, String)} overload). {@code primaryHost}/
     * {@code primaryPort} are final, set once at construction and never updated for the life of
     * this instance (see the field javadocs) - a leader change always replaces the whole
     * {@code ReplicationManager}, it never repoints an existing one.
     */
    String getLeaderAddress() {
        return primaryHost + ":" + primaryPort;
    }

    /**
     * Seeds {@link #lastAppliedSequence} from a predecessor {@code ReplicationManager}'s value,
     * carried across a leader-change instance replacement (2026-08-14 task-3 review fix, D2
     * defense-in-depth). {@code PoppyDB#startReplicationToLeader} constructs a brand-new
     * {@code ReplicationManager} on every leader change; a fresh instance's
     * {@code lastAppliedSequence} starts at 0, and
     * {@code recordPrimarySequenceAtRegistration()}'s own seed
     * ({@code compareAndSet(0, primarySeq)}) then unconditionally adopts whatever the new
     * leader reports - making {@code localSeqBeforeWipe == primarySeqAtRegistration} by
     * construction and the destructive-resync guard in {@link #startInitialSyncOnce()} vacuously
     * pass every time on this path (a primary can never be "behind" a local sequence it just
     * supplied itself). Without carrying the predecessor's real position forward, this path was
     * protected only by the election-layer empty-vs-data invariant (Tasks 1/2/4), not by this
     * task's own guard.
     *
     * <p><b>Superseded by the two-arg overload below for production use</b> (2026-08-14
     * production-CI fix, I-2): calling this single-arg form unconditionally is only correct when
     * the caller has already established that the carried sequence was earned against THIS SAME
     * primary - see that overload's javadoc for why blindly carrying a value across a genuine
     * leader change caused a real incident (82 refusal loops on poppydb.fritz.box). Kept
     * package-private (not deleted) because it is still exactly right for that one case - the
     * two-arg overload delegates to it - and because tests exercise it directly to seed a
     * {@code ReplicationManager} without a live connection.
     *
     * <p>Must be called before {@link #start()}, while {@code lastAppliedSequence} is still its
     * untouched 0 default - enforced with the same {@code compareAndSet(0, ...)} idiom every
     * other seed of this field uses (see {@link #recordPrimarySequenceAtRegistration}), so a
     * second/late call, or one that races an already-started sync, is a safe no-op rather than a
     * regression. A predecessor sequence of 0 (cold-boot / never-synced predecessor, or no
     * predecessor at all) is intentionally a no-op - 0 is exactly the legitimate default for a
     * genuinely fresh node with nothing to protect.
     */
    void carryOverLastAppliedSequence(long predecessorSequence) {
        if (predecessorSequence > 0) {
            lastAppliedSequence.compareAndSet(0, predecessorSequence);
        }
    }

    /**
     * Primary-identity-aware carry-over (2026-08-14 production-CI fix, I-2): the single-arg
     * overload above blindly arms the destructive-resync guard with the predecessor's carried
     * sequence, which is only sound when that sequence was earned against THIS SAME primary.
     * Change-stream sequences are PRIMARY-LOCAL (see {@code tryConsistencyShortcut}'s own
     * javadoc), and a LEADER CHANGE - the very reason a carry-over happens at all - is the NORMAL
     * case that makes two RMs' sequence spaces incomparable, not a rare edge case. Production
     * evidence (poppydb.fritz.box CI, branch fix/poppydb-empty-node-wipe): after a real leader
     * change under messaging load, a follower carried {@code lastAppliedSequence} 227951 from the
     * old leader's space; the new leader's own (entirely unrelated) counter was 213896. Every
     * reconnect logged "refusing full re-sync: primary sequence 213896 is behind local 227951"
     * every 1-2s for 40+ minutes - the node stuck RECOVERING, three messaging test classes timed
     * out. The commit that made a successful sync ADOPT the synced primary's base
     * ({@code lastAppliedSequence.set(lastKnownPrimarySequence.get())}, see the reseed comment in
     * {@link #startInitialSyncOnce()}) could not help: adoption only runs AFTER a successful sync,
     * and the guard - armed with the foreign 227951 - was exactly what blocked that sync from ever
     * succeeding. Hen-and-egg.
     *
     * <p>{@code predecessorSourceAddress} is the {@code "host:port"} the carried sequence was
     * actually earned against (see {@link #getLeaderAddress()}), or {@code null} if there was no
     * live predecessor at all. Two cases:
     * <ul>
     *   <li><b>Matches this instance's own {@link #getLeaderAddress()}</b> - the true kill chain:
     *       the SAME node (address-wise) restarted empty/stale, or - the other route into this
     *       state - the intra-RM {@code triggerResync()} retry path, where the primary literally
     *       cannot have changed (one {@code ReplicationManager}'s {@code primaryHost}/
     *       {@code primaryPort} are final). Arm the guard exactly as before, via
     *       {@link #carryOverLastAppliedSequence(long)}.</li>
     *   <li><b>Any other address, including {@code null}</b> - a genuinely different primary (or
     *       no predecessor at all). The carried sequence must NOT arm the guard - it lives in an
     *       unrelated number space. Deliberately a no-op here: {@code lastAppliedSequence} is left
     *       at its 0 default, so {@code recordPrimarySequenceAtRegistration()}'s EXISTING
     *       {@code compareAndSet(0, primarySeq)} seed (unconditionally live for every instance,
     *       not something this method needs to duplicate) adopts THIS primary's own base the
     *       moment it is first learned at watch registration - "let dbHash/the consistency
     *       shortcut decide whether a resync is actually needed", exactly as a genuinely fresh
     *       node would. This is a deliberate scope boundary, not a gap: a wrongly-elected empty
     *       primary that has itself taken on enough fresh writes could still pass a subsequent
     *       resync decision - the actual barrier against that is the election-layer invariant
     *       (Tasks 1/2/4, "an empty node must never win against a data-bearing voter"), not this
     *       guard.</li>
     * </ul>
     */
    void carryOverLastAppliedSequence(long predecessorSequence, String predecessorSourceAddress) {
        if (getLeaderAddress().equals(predecessorSourceAddress)) {
            carryOverLastAppliedSequence(predecessorSequence);
        }
        // else: different primary (or no predecessor) - see javadoc; intentionally not armed.
    }

    /**
     * The primary's change-stream sequence as observed at the most recent watch registration (see
     * {@link #recordPrimarySequenceAtRegistration(WatchCommand)}). Updated on every successful
     * registration regardless of whether it was actually used to seed {@link #lastAppliedSequence}.
     * Intended for a replication-lag metric ({@code lastKnownPrimarySequence - getLastAppliedSequence()}
     * approximates how many sequence numbers this secondary is behind); 0 until the first
     * registration completes.
     */
    public long getLastKnownPrimarySequence() {
        return lastKnownPrimarySequence.get();
    }
}
