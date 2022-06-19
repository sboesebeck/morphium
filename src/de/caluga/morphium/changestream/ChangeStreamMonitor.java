package de.caluga.morphium.changestream;

import de.caluga.morphium.*;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.morphium.objectmapping.ObjectMapperImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                if (System.currentTimeMillis() - start > morphium.getConfig().getMaxWaitTime()) {
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
                        } catch (Exception e) {
                            e.printStackTrace();
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

//                if (dbOnly) {
//                    morphium.getDriver().watch(morphium.getConfig().getDatabase(), maxWait, fullDocument, pipeline, callback);
//                } else {
//                    morphium.getDriver().watch(morphium.getConfig().getDatabase(), collectionName, maxWait, fullDocument, pipeline, callback);
//                }
            } catch (Exception e) {
                if (e.getMessage().contains("Network error error: state should be: open")) {
                    log.warn("Changstream connection broke - restarting");
                } else {
                    log.warn("Error in changestream monitor - restarting", e);
                }
            }

            log.debug("ChangeStreamMonitor finished gracefully!!!!!!!!!!!");
        }
    }

    @Override
    public void onShutdown(Morphium m) {
        terminate();
    }
}
