package de.caluga.morphium.changestream;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.ShutdownListener;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.ConnectionType;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.morphium.ObjectMapperImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

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
    private final List<Map<String, Object>> pipeline;
    private MorphiumDriver dedicatedConnection;

    public ChangeStreamMonitor(Morphium m) {
        this(m, null, false, null);
        dbOnly = true;
    }

    public ChangeStreamMonitor(Morphium m, List<Map<String, Object>> pipeline) {
        this(m, null, false, pipeline);
        dbOnly = true;
    }


    public ChangeStreamMonitor(Morphium m, Class<?> entity) {
        this(m, m.getMapper().getCollectionName(entity), false, null);
    }

    public ChangeStreamMonitor(Morphium m, Class<?> entity, List<Map<String, Object>> pipeline) {
        this(m, m.getMapper().getCollectionName(entity), false, null);
    }

    public ChangeStreamMonitor(Morphium m, String collectionName, boolean fullDocument) {
        this(m, collectionName, fullDocument, null);
    }

    public ChangeStreamMonitor(Morphium m, String collectionName, boolean fullDocument, List<Map<String, Object>> pipeline) {
        this(m, collectionName, fullDocument, m.getConfig().getMaxWaitTime(), pipeline);
    }

    public ChangeStreamMonitor(Morphium m, String collectionName, boolean fullDocument, int maxWait, List<Map<String, Object>> pipeline) {
        morphium = m;

        //dedicated connection
        try {
            if (m.getDriver() instanceof InMemoryDriver) {
                dedicatedConnection = m.getDriver();
            } else {
                dedicatedConnection = new SingleMongoConnectDriver().setConnectionType(ConnectionType.PRIMARY);
                dedicatedConnection.setDefaultBatchSize(morphium.getConfig().getCursorBatchSize());
                dedicatedConnection.setMaxWaitTime(morphium.getConfig().getMaxWaitTime());
                dedicatedConnection.setHostSeed(morphium.getConfig().getHostSeed());
                dedicatedConnection.setMinConnections(1);
                dedicatedConnection.setMaxConnections(3);
                dedicatedConnection.setCredentials(morphium.getConfig().decryptAuthDb(), morphium.getConfig().decryptMongoLogin(), morphium.getConfig().decryptMongoPassword());
                dedicatedConnection.connect();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        listeners = new ConcurrentLinkedDeque<>();
        morphium.addShutdownListener(this);
        this.pipeline = pipeline;
        this.collectionName = collectionName;
        this.fullDocument = fullDocument;

        if (maxWait != 0) {
            this.maxWait = maxWait;
        } else {
            this.maxWait = m.getConfig().getMaxWaitTime();
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

    public void start() {
        if (changeStreamThread != null) {
            throw new RuntimeException("Already running!");
        }

        changeStreamThread = new Thread(this);
        changeStreamThread.setDaemon(true);
        changeStreamThread.setName("changeStream");
        changeStreamThread.start();
        running = true;
    }

    public boolean isRunning() {
        return running;
    }

    @SuppressWarnings("deprecation")
    public void terminate() {
        running = false;

        try {
            long start = System.currentTimeMillis();

            while (changeStreamThread != null && changeStreamThread.isAlive()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    //ignoring it
                }

                if (System.currentTimeMillis() - start > morphium.getConfig().getReadTimeout()) {
                    log.debug("Changestream monitor did not finish before max wait time is over! Interrupting");
                    changeStreamThread.interrupt();

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    if (changeStreamThread.isAlive()) {
                        try {
                            changeStreamThread.stop();
                        } catch (Throwable e) {
                            //swallow
                            // e.printStackTrace();
                        }
                    }

                    break;
                }
            }

            changeStreamThread = null;
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
        MongoConnection con = null;

        while (running) {
            try {
                DriverTailableIterationCallback callback = new DriverTailableIterationCallback() {
                    @Override
                    public void incomingData(Map<String, Object> data, long dur) {
                        if (!ChangeStreamMonitor.this.running) {
                            return;
                        }

                        @SuppressWarnings("unchecked") Map<String, Object> obj = (Map<String, Object>) data.get("fullDocument");
                        data.remove("fullDocument");
                        ChangeStreamEvent evt = mapper.deserialize(ChangeStreamEvent.class, data);
                        evt.setFullDocument(obj);
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
                        return ChangeStreamMonitor.this.running;
                    }
                };
                con = dedicatedConnection.getPrimaryConnection(null);
                WatchCommand watchCommand = new WatchCommand(con)
                    .setCb(callback)
                    .setDb(morphium.getDatabase())
                    .setBatchSize(1)
                    .setMaxTimeMS(morphium.getConfig().getMaxWaitTime())
                    .setFullDocument(fullDocument ? WatchCommand.FullDocumentEnum.updateLookup : WatchCommand.FullDocumentEnum.defaultValue)
                    .setPipeline(pipeline);

                if (!dbOnly) {
                    watchCommand.setColl(collectionName);
                    //                    morphium.getDriver().watch(morphium.getConfig().getDatabase(), maxWait, fullDocument, pipeline, callback);
                }

                watchCommand.watch();
            } catch (Exception e) {
                if (e.getMessage()==null){
                    log.warn("Restarting changestream",e);
                } else if (e.getMessage().contains("Network error error: state should be: open")) {
                    log.warn("Changstream connection broke - restarting");
                } else if (e.getMessage().contains("Did not receive OpMsg-Reply in time")) {
                    log.debug("changestream iteration");
                } else {
                    log.warn("Error in changestream monitor - restarting", e);
                }
            }
        }

        if (con != null) {
            con.release();
        }

        try {
            if (!(dedicatedConnection instanceof InMemoryDriver)) {
                dedicatedConnection.close();
            }
        } catch (IOException e) {
            //Swallow
        }

        log.debug("ChangeStreamMonitor finished gracefully!");
    }

    @Override
    public void onShutdown(Morphium m) {
        terminate();
    }
}
