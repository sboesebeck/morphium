package de.caluga.morphium.changestream;

import de.caluga.morphium.*;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.ConnectionType;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by stephan on 15.11.16.
 */
@SuppressWarnings("BusyWait")
public class ChangeStreamMonitor implements Runnable, ShutdownListener {
    private final Collection<ChangeStreamListener> listeners;
    private final Morphium morphium;
    private final Logger log = LoggerFactory.getLogger(ChangeStreamMonitor.class);
    private final String collectionName;
    private final boolean fullDocument;
    private final int maxWait;
    private volatile boolean running = true;
    private Thread changeStreamThread;
    private final MorphiumObjectMapper mapper;
    private boolean dbOnly = false;
    private final List<Map<String, Object >> pipeline;
    private MorphiumDriver dedicatedConnection;
    private final CountDownLatch watchStartedLatch = new CountDownLatch(1);
    private final AtomicBoolean watchStartedSignaled = new AtomicBoolean(false);
    private volatile WatchCommand activeWatch;
    private volatile de.caluga.morphium.driver.wire.MongoConnection activeConnection;
    // Resume token tracking to prevent duplicate events on watch restart
    private volatile Map<String, Object> lastResumeToken = null;

    public ChangeStreamMonitor(Morphium m) {
        this(m, null, false, null);
        dbOnly = true;
    }

    public ChangeStreamMonitor(Morphium m, List<Map<String, Object >> pipeline) {
        this(m, null, false, pipeline);
        dbOnly = true;
    }


    public ChangeStreamMonitor(Morphium m, Class<?> entity) {
        this(m, m.getMapper().getCollectionName(entity), false, null);
    }

    public ChangeStreamMonitor(Morphium m, Class<?> entity, List<Map<String, Object >> pipeline) {
        this(m, m.getMapper().getCollectionName(entity), false, null);
    }

    public ChangeStreamMonitor(Morphium m, String collectionName, boolean fullDocument) {
        this(m, collectionName, fullDocument, null);
    }

    public ChangeStreamMonitor(Morphium m, String collectionName, boolean fullDocument, List<Map<String, Object >> pipeline) {
        this(m, collectionName, fullDocument, m.getConfig().connectionSettings().getMaxWaitTime(), pipeline);
    }

    public ChangeStreamMonitor(Morphium m, String collectionName, boolean fullDocument, int maxWait, List<Map<String, Object >> pipeline) {
        morphium = m;
        try {
            dedicatedConnection = m.getDriver();
        } catch (Exception e) {
            if (!e.getMessage().contains("sleep interrupted")) {
                throw new RuntimeException(e);
            }
        }
        listeners = new ConcurrentLinkedDeque<>();
        morphium.addShutdownListener(this);
        this.pipeline = pipeline;
        this.collectionName = collectionName;
        this.fullDocument = fullDocument;
        dbOnly = (collectionName == null);

        if (maxWait != 0) {
            this.maxWait = maxWait;
        } else {
            this.maxWait = m.getConfig().connectionSettings().getMaxWaitTime();
        }

        mapper = new ObjectMapperImpl();
        AnnotationAndReflectionHelper hlp = new AnnotationAndReflectionHelper(false);
        mapper.setAnnotationHelper(hlp);
    }

    public void addListener(ChangeStreamListener lst) {
        listeners.add(lst);
    }

    public void removeListener(ChangeStreamListener lst) {
        listeners.remove(lst);
    }

    public boolean isFullDocument() {
        return fullDocument;
    }

    /**
     * Start the change stream monitor and block until the watch is established (up to 2s).
     * For parallel startup of multiple monitors, use {@link #startAsync()} + {@link #awaitReady(long, TimeUnit)}.
     */
    public void start() {
        startAsync();
        awaitReady(2, TimeUnit.SECONDS);
    }

    /**
     * Start the change stream monitor thread without waiting for watch establishment.
     * Call {@link #awaitReady(long, TimeUnit)} afterwards if you need to ensure the watch is active.
     */
    public void startAsync() {
        if (changeStreamThread != null) {
            throw new RuntimeException("Already running!");
        }

        changeStreamThread = Thread.ofPlatform()
                             .name("changeStream")
                             .start(this);
        running = true;
    }

    /**
     * Wait for the change stream watch to be established.
     * @return true if ready, false if timeout elapsed
     */
    public boolean awaitReady(long timeout, TimeUnit unit) {
        try {
            return watchStartedLatch.await(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public boolean isRunning() {
        return running;
    }

    @SuppressWarnings("deprecation")
    public void terminate() {
        running = false;
        signalWatchStarted(); // ensure `start()` doesn't wait if we are terminating

        try {
            long start = System.currentTimeMillis();
            dedicatedConnection = null;

            // Actively break out of blocking watch operations.
            // Closing the underlying connection is the most reliable way to stop watch loops.
            try {
                var con = activeConnection;
                if (con != null) {
                    con.close();
                }
            } catch (Exception ignore) {
            } finally {
                activeConnection = null;
            }

            try {
                var w = activeWatch;
                if (w != null) {
                    w.releaseConnection();
                }
            } catch (Exception ignore) {
            } finally {
                activeWatch = null;
            }

            try {
                if (changeStreamThread != null) {
                    changeStreamThread.interrupt();
                }
            } catch (Exception ignore) {
            }

            //while (changeStreamThread != null && changeStreamThread.isAlive()) {
            //    try {
            //        Thread.sleep(100);
            //    } catch (InterruptedException e) {
            //        //ignoring it
            //    }

            //    if (System.currentTimeMillis() - start > morphium.getConfig().getReadTimeout()) {
            //        log.debug("Changestream monitor did not finish before max wait time is over! Interrupting");
            //        changeStreamThread.interrupt();

            //        try {
            //            Thread.sleep(100);
            //        } catch (InterruptedException e) {
            //            // e.printStackTrace();
            //        }

            //        break;
            //    }
            //}

            changeStreamThread = null;
        } catch (Exception e1) {
            log.warn("Exception when closing changestreamMonitor", e1.getMessage());
        } finally {
            listeners.clear();
            morphium.removeShutdownListener(this);
        }
    }

    public String getcollectionName() {
        return collectionName;
    }

    @Override
    public void run() {
        WatchCommand watch = null;

        while (running && morphium.getConfig() != null) {
            try {
                DriverTailableIterationCallback callback = new DriverTailableIterationCallback() {
                    @Override
                    public void incomingData(Map<String, Object> data, long dur) {
                        if (!running) {
                            return;
                        }

                        // Capture resume token for use when restarting the watch
                        // The _id field in change stream events IS the resume token
                        @SuppressWarnings("unchecked")
                        Map<String, Object> resumeToken = (Map<String, Object>) data.get("_id");
                        if (resumeToken != null) {
                            lastResumeToken = resumeToken;
                        }

                        @SuppressWarnings("unchecked")
                        Map<String, Object> obj = (Map<String, Object>) data.get("fullDocument");
                        data.remove("fullDocument");
                        @SuppressWarnings("unchecked")
                        Map<String, Object> before = (Map<String, Object>) data.get("fullDocumentBeforeChange");
                        data.remove("fullDocumentBeforeChange");

                        if (data.get("documentKey") instanceof MorphiumId || data.get("documentKey") instanceof ObjectId) {
                            data.put("documentKey", Doc.of("_id", data.get("documentKey")));
                        }

                        ChangeStreamEvent evt = mapper.deserialize(ChangeStreamEvent.class, data);
                        evt.setFullDocument(obj);
                        evt.setFullDocumentBeforeChange(before);
                        @SuppressWarnings("unchecked")
                        Map<String, String> ns = (Map<String, String>) data.get("ns");
                        evt.setDbName(ns.get("db"));
                        evt.setCollectionName(ns.get("coll"));
                        List<ChangeStreamListener> toRemove = new ArrayList<>();

                        for (ChangeStreamListener lst : listeners) {
                            try {
                                if (!lst.incomingData(evt)) {
                                    toRemove.add(lst);
                                }
                            } catch (Exception e) {
                                log.error("listener threw exception", e);
                            }
                        }

                        listeners.removeAll(toRemove);
                    }
                    @Override
                    public boolean isContinued() {
                        return ChangeStreamMonitor.this.running && morphium.getConfig() != null;
                    }
                };

                if (dedicatedConnection == null || !dedicatedConnection.isConnected()) {
                    log.debug("Driver not available, will retry");
                    // Wait before retrying to avoid tight loop
                    try {
                        Thread.sleep(morphium.getConfig().connectionSettings().getSleepBetweenNetworkErrorRetries());
                    } catch (InterruptedException e) {
                        if (!running) break;
                    }
                    continue;  // Retry getting connection
                }

                var con = dedicatedConnection.getPrimaryConnection(null);
                activeConnection = con;

                if (!con.isConnected()) {
                    log.error("Could not connect - will retry");
                    // Release connection back to pool before retrying
                    try {
                        dedicatedConnection.releaseConnection(con);
                    } catch (Exception ignore) {
                    }
                    activeConnection = null;
                    // Wait before retrying to avoid tight loop
                    try {
                        Thread.sleep(morphium.getConfig().connectionSettings().getSleepBetweenNetworkErrorRetries());
                    } catch (InterruptedException e) {
                        if (!running) break;
                    }
                    continue;  // Retry connection
                }

                watch = new WatchCommand(con).setCb(callback).setDb(morphium.getDatabase()).setBatchSize(1).setMaxTimeMS(maxWait)
                .setFullDocument(fullDocument ? WatchCommand.FullDocumentEnum.updateLookup : WatchCommand.FullDocumentEnum.defaultValue).setPipeline(pipeline);

                // Use resume token to continue from where we left off (prevents duplicate events)
                if (lastResumeToken != null) {
                    watch.setResumeAfter(lastResumeToken);
                    log.debug("Resuming change stream from token: {}", lastResumeToken);
                }

                activeWatch = watch;

                if (!dbOnly) {
                    watch.setColl(collectionName);
                }

                if (watch.getConnection().isConnected()) {
                    // Set a registration callback to be called when subscription is registered
                    // This ensures signalWatchStarted() is called AFTER the subscription is active
                    watch.setRegistrationCallback(this::signalWatchStarted);
                    log.info("CSM: Starting watch on collection '{}' with maxWait={}ms", collectionName, maxWait);
                    watch.watch();
                    log.info("CSM: watch() returned normally for collection '{}'", collectionName);
                }
            } catch (Exception e) {
                // Check if we should stop before handling errors
                if (!running || morphium.getConfig() == null) {
                    log.debug("ChangeStreamMonitor stopping due to shutdown");
                    break;
                }

                if (e.getMessage() == null) {
                    log.warn("Restarting changestream", e);
                } else if (e.getMessage().contains("reply is null")) {
                    log.warn("Reply is null - cannot watch - retrying");
                } else if (e.getMessage().contains("cursor is null")) {
                    log.warn("Cursor is null - cannot watch - retrying");
                } else if (e.getMessage().contains("ChangeStreamHistoryLost") || e.getMessage().contains("resume point may no longer be in the oplog")) {
                    // Oplog has rolled past our resume point - discard token and start fresh
                    log.warn("Oplog rolled past resume point for changestream '{}' - discarding resume token and restarting fresh", collectionName);
                    lastResumeToken = null;
                    try {
                        Thread.sleep(morphium.getConfig().connectionSettings().getSleepBetweenNetworkErrorRetries());
                    } catch (InterruptedException ex) {
                        if (!running) break;
                    }
                } else if (e.getMessage().contains("Network error error: state should be: open")) {
                    log.warn("Changstream connection broke - restarting");
                } else if (e.getMessage().contains("Did not receive OpMsg-Reply in time") || e.getMessage().contains("Read timed out")) {
                    log.debug("changestream iteration");
                } else if (morphium.getConfig() == null) {
                    log.warn("Morphium config is null, stopping changestream monitor for '{}'", collectionName);
                    break;
                } else if (e.getMessage().contains("closed")) {
                    // Connection closed is often transient (network issues, failover) - retry instead of giving up
                    log.warn("Connection closed for changestream '{}' - will retry", collectionName);
                    try {
                        Thread.sleep(morphium.getConfig().connectionSettings().getSleepBetweenNetworkErrorRetries());
                    } catch (InterruptedException ex) {
                        if (!running) break;
                    }
                } else if (e.getMessage().contains("No such host")) {
                    // Server was shut down, stop trying to reconnect
                    log.warn("Server no longer available (No such host), stopping changestream monitor for collection '{}'", collectionName);
                    break;
                } else {
                    if (running) {
                        log.warn("Error in changestream monitor - restarting", e);

                        try {
                            Thread.sleep(morphium.getConfig().connectionSettings().getSleepBetweenNetworkErrorRetries());
                        } catch (InterruptedException ex) {
                        }
                    } else {
                        break;
                    }
                }
            } finally {
                boolean connectionReleased = false;
                if (watch != null && watch.getConnection() != null) {
                    watch.releaseConnection();
                    connectionReleased = true;
                }
                // Fallback: release activeConnection directly if watch didn't have a connection
                // (can happen if watch was created but connection was lost/nulled before release)
                if (!connectionReleased && activeConnection != null && dedicatedConnection != null) {
                    try {
                        dedicatedConnection.releaseConnection(activeConnection);
                    } catch (Exception ignore) {
                        // Best effort - connection may already be closed
                    }
                }
                activeWatch = null;
                activeConnection = null;
            }
        }

        if (!running) {
            log.info("ChangeStreamMonitor for '{}' terminated (running=false)", collectionName);
        } else if (morphium.getConfig() == null) {
            log.warn("ChangeStreamMonitor for '{}' terminated because morphium config is null!", collectionName);
        } else {
            log.warn("ChangeStreamMonitor for '{}' exited unexpectedly!", collectionName);
        }
    }

    private void signalWatchStarted() {
        if (watchStartedSignaled.compareAndSet(false, true)) {
            watchStartedLatch.countDown();
        }
    }

    @Override
    public void onShutdown(Morphium m) {
        terminate();
    }
}
