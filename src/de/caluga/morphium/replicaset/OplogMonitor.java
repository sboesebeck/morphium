package de.caluga.morphium.replicaset;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.ShutdownListener;
import de.caluga.morphium.driver.MorphiumDriverException;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.regex.Pattern;

/**
 * Created by stephan on 15.11.16.
 */
public class OplogMonitor implements Runnable, ShutdownListener {
    private final Collection<OplogListener> listeners;
    private final Morphium morphium;
    private final Logger log = LoggerFactory.getLogger(OplogMonitor.class);
    private final String nameSpace;
    private final boolean useRegex;
    private boolean running = true;
    private long timestamp;
    private Thread oplogMonitorThread;


    public OplogMonitor(Morphium m) {
        this(m, null, false);
    }

    public OplogMonitor(Morphium m, Class<?> entity) {
        this(m, m.getConfig().getDatabase() + "." + m.getMapper().getCollectionName(entity), false);
    }

    public OplogMonitor(Morphium m, String nameSpace, boolean regex) {
        morphium = m;
        listeners = new ConcurrentLinkedDeque<>();
        timestamp = System.currentTimeMillis() / 1000;
        morphium.addShutdownListener(this);
        this.nameSpace = nameSpace;
        this.useRegex = regex;
    }

    public void addListener(OplogListener lst) {
        listeners.add(lst);
    }

    public void removeListener(OplogListener lst) {
        listeners.remove(lst);
    }

    public boolean isUseRegex() {
        return useRegex;
    }

    public void start() {
        if (oplogMonitorThread != null) {
            throw new RuntimeException("Already running!");
        }
        oplogMonitorThread = new Thread(this);
        oplogMonitorThread.setDaemon(true);
        oplogMonitorThread.setName("oplogmonitor");
        oplogMonitorThread.start();
    }

    public boolean isRunning() {
        return running;
    }

    public void stop() {
        running = false;
        long start = System.currentTimeMillis();
        while (oplogMonitorThread.isAlive()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                //ignoring it
            }
            if (System.currentTimeMillis() - start > 1000) {
                break;
            }
        }
        if (oplogMonitorThread.isAlive()) {
            oplogMonitorThread.interrupt();
        }
        oplogMonitorThread = null;
        listeners.clear();
        morphium.removeShutdownListener(this);
    }

    public String getNameSpace() {
        return nameSpace;
    }

    @Override
    public void run() {
        Map<String, Object> q = new LinkedHashMap<>();
        Map<String, Object> q2 = new HashMap<>();
        q2.put("$gt", new BsonTimestamp((int) timestamp, 0));
        String ns;


        if (nameSpace != null) {
            ns = morphium.getConfig().getDatabase() + "." + nameSpace;
            if (nameSpace.contains(".") && !useRegex) {
                ns = nameSpace; //assuming you specify DB
            }
            if (useRegex) {
                q.put("ns", Pattern.compile(ns));
            } else {
                q.put("ns", ns);
            }
        }
        q.put("ts", q2);
        while (running) {
            try {
                morphium.getDriver().tailableIteration("local", "oplog.rs", q, null, null, 0, 0, 1000, null, 60000, (data, dur) -> {
                    if (!running){
                        return false;
                    }
                    timestamp = (Integer) data.get("ts");
                    for (OplogListener lst : listeners) {
                        try {
                            lst.incomingData(data);
                        } catch (Exception e) {
                            log.error("listener threw exception", e);
                        }
                    }
                    return running;
                });
            } catch (MorphiumDriverException e) {
                e.printStackTrace();
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e1) {
                    //swallowing

                }
            }
        }
    }

    @Override
    public void onShutdown(Morphium m) {
        stop();
    }
}
