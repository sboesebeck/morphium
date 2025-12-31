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

    // Progress reporting interval
    private static final long PROGRESS_REPORT_INTERVAL_MS = 100;

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

        // Start progress reporter
        startProgressReporter();
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

            cmd.executeAsync();  // Fire and forget - we don't need to wait for response
            lastReportedSequence.set(currentSeq);
            log.debug("Reported progress to primary: seq={}", currentSeq);
        } catch (Exception e) {
            log.debug("Failed to report progress: {}", e.getMessage());
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
            config.setMaxConnections(3);
            config.setMinConnections(1);
            config.setConnectionTimeout(5000);
            config.setReadTimeout(30000);
            config.setRetryReads(false);  // Don't retry on replication client
            config.setRetryWrites(false);

            primaryMorphium = new Morphium(config);
            primaryMorphium.getDriver();  // Force connection

            connected.set(true);
            log.info("Connected to primary");
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

        MongoConnection con = primaryMorphium.getDriver().getPrimaryConnection(null);
        WatchCommand cmd = null;
        try {
            cmd = new WatchCommand(con)
                .setDb("admin")  // Watch at cluster level
                .setMaxTimeMS(5000)
                .setFullDocument(WatchCommand.FullDocumentEnum.updateLookup)
                .setPipeline(List.of())  // Empty = watch everything
                .setCb(new DriverTailableIterationCallback() {
                    @Override
                    public void incomingData(Map<String, Object> data, long cursorId) {
                        if (!running.get()) {
                            return;
                        }
                        applyChangeEvent(data);
                    }

                    @Override
                    public boolean isContinued() {
                        return running.get();
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
