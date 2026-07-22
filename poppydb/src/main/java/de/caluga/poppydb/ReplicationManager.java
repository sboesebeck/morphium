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
import java.util.concurrent.atomic.AtomicLong;

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
    private final AtomicBoolean running = new AtomicBoolean(false);
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
    private final AtomicBoolean initialSyncComplete = new AtomicBoolean(false);
    private final CountDownLatch initialSyncLatch = new CountDownLatch(1);

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
    private final BlockingQueue<Map<String, Object>> eventQueue = new LinkedBlockingQueue<>(EVENT_QUEUE_CAPACITY);
    private ScheduledExecutorService batchProcessor;

    // Flag to enable immediate progress reporting after each batch
    private volatile boolean immediateProgressReporting = true;

    // Staleness detection - track last response time to detect broken connections
    private final AtomicLong lastWatchResponseTime = new AtomicLong(0);
    private static final long STALENESS_THRESHOLD_MS = 30000; // 30 seconds without response = stale

    // Callback to notify when log index is updated (for election consistency)
    private java.util.function.BiConsumer<Long, Long> onLogIndexUpdate;

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
        batchProcessor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PoppyDB-BatchProcessor");
            t.setDaemon(true);
            return t;
        });

        batchProcessor.scheduleAtFixedRate(this::processBatch,
                BATCH_FLUSH_INTERVAL_MS, BATCH_FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
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
            return;
        }

        List<Map<String, Object>> batch = new ArrayList<>(BATCH_SIZE);
        eventQueue.drainTo(batch, BATCH_SIZE);

        if (batch.isEmpty()) {
            return;
        }

        applyEventsInOrder(batch);

        // Notify about log index update for election consistency
        long currentSeq = lastAppliedSequence.get();
        if (onLogIndexUpdate != null && currentSeq > 0) {
            // Term is 0 for now - will be updated when we receive term from leader
            onLogIndexUpdate.accept(currentSeq, 0L);
        }

        // Immediately report progress after processing batch for faster write concern acknowledgment
        if (immediateProgressReporting) {
            reportProgressToPrimary();
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
     * Apply multiple insert events as a single bulk insert.
     */
    @SuppressWarnings("unchecked")
    private void applyBulkInserts(String collKey, List<Map<String, Object>> events) {
        if (events.isEmpty()) return;

        String[] parts = collKey.split("\\.", 2);
        String db = parts[0];
        String coll = parts[1];

        // Skip system databases
        if ("admin".equals(db) || "local".equals(db) || "config".equals(db)) {
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
                log.error("Error applying bulk insert to {}.{}: {}", db, coll, e.getMessage());
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
                for (Map<String, Object> event : events) {
                    applyChangeEvent(event, true);
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

        // Interrupt an in-flight initial-sync snapshot thread (if any) so it exits promptly.
        Thread syncThread = initialSyncThread;
        if (syncThread != null) {
            syncThread.interrupt();
            initialSyncThread = null;
        }

        // Stop batch processor first to flush remaining events
        if (batchProcessor != null) {
            // Process any remaining events
            processBatch();
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
                        // Start each attempt from a clean local slate so a retry after a
                        // partially-successful copy doesn't fail on already-copied documents.
                        clearLocalDatabases();
                        performInitialSync();

                        // Guard: if the watch died or was re-established during the copy, the snapshot
                        // may be missing writes that fell into the gap. Discard it and retry under the
                        // new watch instead of opening the gate on a lossy snapshot.
                        if (watchInvalidatedDuringSnapshot(watchGen)) {
                            log.warn("Watch changed during initial sync (captured gen {}, now {}, live {}); "
                                    + "redoing snapshot in {}ms to avoid a lost-write gap",
                                    watchGen, watchGeneration.get(), watchLive.get(), backoffMs);
                            Thread.sleep(backoffMs);
                            backoffMs = Math.min(backoffMs * 2, 30_000);
                            continue;
                        }

                        // Success: open the gate. The batch processor now drains the events
                        // buffered during the snapshot (idempotent replay) and all subsequent live
                        // events, in order.
                        applying.set(true);
                        initialSyncComplete.set(true);
                        initialSyncLatch.countDown();
                        return;
                    } catch (Exception e) {
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

    /** Test hook: force the watchLive flag. */
    void setWatchLiveForTest(boolean live) {
        watchLive.set(live);
    }

    /** Test hook: simulate a watch (re-)registration bumping the generation. */
    void bumpWatchGenerationForTest() {
        watchGeneration.incrementAndGet();
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
            if ("admin".equals(dbName) || "local".equals(dbName) || "config".equals(dbName)) {
                continue;
            }

            for (String collName : source.listCollections(dbName, null)) {
                if (collName.startsWith("system.")) {
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
        for (String dbName : localDriver.listDatabases()) {
            if ("admin".equals(dbName) || "local".equals(dbName) || "config".equals(dbName)) {
                continue;
            }
            GenericCommand cmd = new GenericCommand(localDriver);
            cmd.setDb(dbName);
            cmd.setColl(null);
            cmd.setCmdData(Doc.of("dropDatabase", 1, "$db", dbName));
            localDriver.runCommand(cmd);
        }
    }

    /**
     * Perform initial sync - copy all data from primary to secondary.
     */
    private void performInitialSync() throws Exception {
        log.info("Starting initial sync from primary...");
        long startTime = System.currentTimeMillis();

        // List all databases on primary using the driver
        List<String> databases = primaryMorphium.getDriver().listDatabases();

        int totalDocs = 0;
        for (String dbName : databases) {
            // Skip system databases
            if ("admin".equals(dbName) || "local".equals(dbName) || "config".equals(dbName)) {
                continue;
            }

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
            // Skip system collections
            if (collName.startsWith("system.")) {
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
        try {
            FindCommand findCmd = new FindCommand(con)
                .setDb(dbName)
                .setColl(collName)
                .setFilter(Doc.of())
                .setBatchSize(1000);

            List<Map<String, Object>> documents = findCmd.execute();
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

            localDriver.runCommand(insertCmd);

            log.debug("Synced {} documents to {}.{}", documents.size(), dbName, collName);
            return documents.size();
        } finally {
            primaryMorphium.getDriver().releaseConnection(con);
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
            // watchLive observes the value belonging to this watch (not a stale one).
            watchCmd.setRegistrationCallback(() -> {
                watchGeneration.incrementAndGet();
                watchLive.set(true);
                recordPrimarySequenceAtRegistration(watchCmd);
            });
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
                        // Use put() so a full queue blocks the watch callback (backpressure)
                        // rather than dropping events or growing without bound.
                        try {
                            eventQueue.put(data);
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
     */
    private void recordPrimarySequenceAtRegistration(WatchCommand watchCmd) {
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
     * sync flags so {@link #startInitialSyncOnce()} launches a fresh snapshot, drops the events left
     * over from the lost window, and resets the sequence so the next watch starts fresh (no
     * resumeAfter) instead of re-requesting the same lost window in a loop. The replication loop then
     * re-runs initial sync + watch on its next iteration.
     */
    private void triggerResync(long fromSequence) {
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
        lastAppliedSequence.set(0);     // resume fresh; next watch sends no resumeAfter
        lastReportedSequence.set(0);
        eventQueue.clear();             // discard events buffered for the lost window
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
     */
    @SuppressWarnings("unchecked")
    private void applyChangeEvent(Map<String, Object> event, boolean asReplay) {
        try {
            // Extract sequence number from resume token
            long sequenceNumber = extractSequenceFromEvent(event);

            String operationType = (String) event.get("operationType");
            Map<String, Object> ns = (Map<String, Object>) event.get("ns");

            if (ns == null) {
                log.debug("Ignoring event without namespace: {}", operationType);
                // Still update sequence for non-namespace events
                if (sequenceNumber > 0) {
                    lastAppliedSequence.set(sequenceNumber);
                }
                return;
            }

            String db = (String) ns.get("db");
            String coll = (String) ns.get("coll");

            // Skip system databases
            if ("admin".equals(db) || "local".equals(db) || "config".equals(db)) {
                // Still update sequence for skipped events
                if (sequenceNumber > 0) {
                    lastAppliedSequence.set(sequenceNumber);
                }
                return;
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
                            localDriver.runCommand(cmd);
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
                        localDriver.runCommand(cmd);
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
                        localDriver.runCommand(cmd);
                    }
                    break;
                }

                case "drop": {
                    GenericCommand cmd = new GenericCommand(localDriver);
                    cmd.setDb(db);
                    cmd.setColl(coll);
                    cmd.setCmdData(Doc.of("drop", coll, "$db", db));
                    localDriver.runCommand(cmd);
                    break;
                }

                case "dropDatabase": {
                    GenericCommand cmd = new GenericCommand(localDriver);
                    cmd.setDb(db);
                    cmd.setColl(null);
                    cmd.setCmdData(Doc.of("dropDatabase", 1, "$db", db));
                    localDriver.runCommand(cmd);
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

            // Update last applied sequence after successful application
            if (sequenceNumber > 0) {
                lastAppliedSequence.set(sequenceNumber);
            }

        } catch (Exception e) {
            log.error("Error applying change event: {}", e.getMessage(), e);
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
                                        Map<String, Object> fullDoc) {
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
        localDriver.runCommand(cmd);
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
        stats.put("eventsApplied", eventsApplied.get());
        stats.put("lastEventTime", lastEventTime.get());
        stats.put("lastAppliedSequence", lastAppliedSequence.get());
        stats.put("lastReportedSequence", lastReportedSequence.get());
        stats.put("lastKnownPrimarySequence", lastKnownPrimarySequence.get());
        stats.put("resyncCount", resyncCount.get());
        stats.put("primaryHost", primaryHost + ":" + primaryPort);
        stats.put("myAddress", myAddress);
        stats.put("eventQueueSize", eventQueue.size());
        stats.put("eventQueueCapacity", EVENT_QUEUE_CAPACITY);
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
