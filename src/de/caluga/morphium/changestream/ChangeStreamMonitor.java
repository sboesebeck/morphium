package de.caluga.morphium.changestream;

import de.caluga.morphium.*;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by stephan on 15.11.16.
 */
public class ChangeStreamMonitor implements Runnable, ShutdownListener {
    private final Collection<ChangeStreamListener> listeners;
    private final Morphium morphium;
    private final Logger log = LoggerFactory.getLogger(ChangeStreamMonitor.class);
    private final String collectionName;
    private final boolean fullDocument;
    private boolean running = true;
    private long timestamp;
    private Thread changeStreamThread;
    private ObjectMapper mapper;
    private boolean dbOnly = false;


    public ChangeStreamMonitor(Morphium m) {
        this(m, null, false);
        dbOnly = true;
    }

    public ChangeStreamMonitor(Morphium m, Class<?> entity) {
        this(m, m.getMapper().getCollectionName(entity), false);
    }

    public ChangeStreamMonitor(Morphium m, String collectionName, boolean fullDocument) {
        morphium = m;
        listeners = new ConcurrentLinkedDeque<>();
        timestamp = System.currentTimeMillis() / 1000;
        morphium.addShutdownListener(this);
        this.collectionName = collectionName;
        this.fullDocument = fullDocument;

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
    }

    public boolean isRunning() {
        return running;
    }

    public void stop() {
        running = false;
        long start = System.currentTimeMillis();
        while (changeStreamThread.isAlive()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                //ignoring it
            }
            if (System.currentTimeMillis() - start > 1000) {
                break;
            }
        }
        if (changeStreamThread.isAlive()) {
            changeStreamThread.interrupt();
        }
        changeStreamThread = null;
        listeners.clear();
        morphium.removeShutdownListener(this);
    }

    public String getcollectionName() {
        return collectionName;
    }

    @Override
    public void run() {

        while (running) {
            try {
                DriverTailableIterationCallback callback = (data, dur) -> {
                    if (!running) {
                        return false;
                    }
                    Map<String, Object> obj = (Map<String, Object>) data.get("fullDocument");
                    data.put("fullDocument", null);
                    ChangeStreamEvent evt = mapper.unmarshall(ChangeStreamEvent.class, data);

                    evt.setFullDocument(obj);
                    for (ChangeStreamListener lst : listeners) {
                        try {
                            lst.incomingData(evt);
                        } catch (Exception e) {
                            log.error("listener threw exception", e);
                        }
                    }
                    return running;
                };
                if (dbOnly) {
                    morphium.getDriver().watch(morphium.getConfig().getDatabase(), morphium.getConfig().getMaxWaitTime(), fullDocument, callback);
                } else {
                    morphium.getDriver().watch(morphium.getConfig().getDatabase(), collectionName, morphium.getConfig().getMaxWaitTime(), fullDocument, callback);
                }
            } catch (MorphiumDriverException e) {
                log.warn("Error in oplogmonitor - restarting", e);
            }
        }
    }

    @Override
    public void onShutdown(Morphium m) {
        stop();
    }
}
