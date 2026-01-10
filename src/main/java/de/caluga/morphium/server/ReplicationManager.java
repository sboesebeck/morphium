package de.caluga.morphium.server;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.GenericCommand;
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
 * Handles replication from primary to secondary MorphiumServer nodes.
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

    // Secondary's address for reporting back to primary
    private String myAddress;

    private Morphium primaryMorphium;
    private ExecutorService replicationExecutor;
    private ScheduledExecutorService progressReporter;
    private volatile long watchCursorId = -1;

    // Initial sync state
    private final AtomicBoolean initialSyncComplete = new AtomicBoolean(false);
    private final CountDownLatch initialSyncLatch = new CountDownLatch(1);

    // Progress reporting interval - balanced for good throughput and write concern latency
    // 50ms gives good responsiveness while not overwhelming the primary with reports
    private static final long PROGRESS_REPORT_INTERVAL_MS = 50;

    // Batching configuration for efficient replication
    // Using reasonable batch interval for good throughput
    private static final int BATCH_SIZE = 100;
    private static final long BATCH_FLUSH_INTERVAL_MS = 5;
    private final BlockingQueue<Map<String, Object>> eventQueue = new LinkedBlockingQueue<>();
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
            Thread t = new Thread(r, "MorphiumServer-Replication");
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
    }

    /**
     * Start the batch processor that efficiently applies change events.
     */
    private void startBatchProcessor() {
        batchProcessor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "MorphiumServer-BatchProcessor");
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
        if (eventQueue.isEmpty()) {
            return;
        }

        List<Map<String, Object>> batch = new ArrayList<>(BATCH_SIZE);
        eventQueue.drainTo(batch, BATCH_SIZE);

        if (batch.isEmpty()) {
            return;
        }

        // Group events by type and collection for bulk operations
        Map<String, List<Map<String, Object>>> insertsByCollection = new HashMap<>();
        List<Map<String, Object>> otherEvents = new ArrayList<>();

        for (Map<String, Object> event : batch) {
            String operationType = (String) event.get("operationType");
            @SuppressWarnings("unchecked")
            Map<String, Object> ns = (Map<String, Object>) event.get("ns");

            if (ns == null || !"insert".equals(operationType)) {
                otherEvents.add(event);
                continue;
            }

            String db = (String) ns.get("db");
            String coll = (String) ns.get("coll");
            String key = db + "." + coll;

            insertsByCollection.computeIfAbsent(key, k -> new ArrayList<>()).add(event);
        }

        // Apply bulk inserts
        for (Map.Entry<String, List<Map<String, Object>>> entry : insertsByCollection.entrySet()) {
            applyBulkInserts(entry.getKey(), entry.getValue());
        }

        // Apply other events individually
        for (Map<String, Object> event : otherEvents) {
            applyChangeEvent(event);
        }

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
                localDriver.runCommand(cmd);
                eventsApplied.addAndGet(documents.size());
                log.debug("Bulk inserted {} documents into {}.{}", documents.size(), db, coll);
            } catch (Exception e) {
                log.error("Error applying bulk insert to {}.{}: {}", db, coll, e.getMessage());
            }
        }

        // Update sequence
        final long finalMaxSeq = maxSeq;
        if (finalMaxSeq > 0) {
            lastAppliedSequence.updateAndGet(current -> Math.max(current, finalMaxSeq));
        }
    }

    /**
     * Start a background task to periodically report replication progress to primary.
     */
    private void startProgressReporter() {
        progressReporter = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "MorphiumServer-ProgressReporter");
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
            config.setDatabase("admin");  // Default db for admin operations
            config.setHostSeed(primaryHost + ":" + primaryPort);
            // Increase connection pool for handling watch + progress reporting under load
            config.setMaxConnections(10);
            config.setMinConnections(2);
            config.setConnectionTimeout(10000);  // 10s connection timeout
            config.setReadTimeout(60000);  // 60s read timeout for long-running watch
            config.setMaxWaitTime(5000);  // 5s max wait for connection from pool
            config.setRetryReads(true);  // Retry on transient failures
            config.setRetryWrites(true);
            config.setRetriesOnNetworkError(3);
            config.setSleepBetweenNetworkErrorRetries(500);

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
                if (!connected.get()) {
                    log.info("Not connected to primary, attempting reconnect...");
                    try {
                        connectToPrimary();
                    } catch (Exception e) {
                        log.warn("Reconnect failed, will retry in 5s: {}", e.getMessage());
                        Thread.sleep(5000);
                        continue;
                    }
                }

                // Perform initial sync if not done
                if (!initialSyncComplete.get()) {
                    performInitialSync();
                    initialSyncComplete.set(true);
                    initialSyncLatch.countDown();
                }

                // Watch for changes
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
                log.error("Error in replication loop: {}", e.getMessage(), e);
                connected.set(false);

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
            cmd = new WatchCommand(con)
                .setDb("admin")  // Watch at cluster level
                .setMaxTimeMS(500)  // 500ms timeout - low latency for messaging tests
                .setFullDocument(WatchCommand.FullDocumentEnum.updateLookup)
                .setPipeline(List.of())  // Empty = watch everything
                .setCb(new DriverTailableIterationCallback() {
                    @Override
                    public void incomingData(Map<String, Object> data, long cursorId) {
                        if (!running.get()) {
                            return;
                        }
                        // Update staleness tracker - we received a response
                        lastWatchResponseTime.set(System.currentTimeMillis());
                        // Queue for batch processing instead of immediate application
                        eventQueue.offer(data);
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

            cmd.watch();
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }

        // If we get here, the watch ended - loop will restart it
        log.debug("Change stream watch ended");
    }

    /**
     * Apply a change event to the local driver.
     */
    @SuppressWarnings("unchecked")
    private void applyChangeEvent(Map<String, Object> event) {
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
                    if (fullDoc != null) {
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
        stats.put("primaryHost", primaryHost + ":" + primaryPort);
        stats.put("myAddress", myAddress);
        return stats;
    }

    /**
     * Get the last applied sequence number.
     */
    public long getLastAppliedSequence() {
        return lastAppliedSequence.get();
    }
}
