package de.caluga.morphium.replicaset;

import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.ShutdownListener;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import org.bson.types.BSONTimestamp;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by stephan on 15.11.16.
 */
public class OplogMonitor implements Runnable, ShutdownListener {
    private Collection<OplogListener> listeners;
    private Morphium morphium;
    private boolean running = true;
    private Logger log = new Logger(OplogMonitor.class);
    private long timestamp;
    private Thread oplogMonitorThread;

    public OplogMonitor(Morphium m) {
        morphium = m;
        listeners = new ConcurrentLinkedDeque<OplogListener>();
        timestamp = System.currentTimeMillis() / 1000;
        morphium.addShutdownListener(this);
    }

    public void addListener(OplogListener lst) {
        listeners.add(lst);
    }

    public void removeListener(OplogListener lst) {
        listeners.remove(lst);
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
        morphium.removeShutdownListener(this);
    }

    @Override
    public void run() {
        while (running) {
            try {
                Map<String, Object> q = new HashMap<>();
                Map<String, Object> q2 = new HashMap<>();
                q2.put("$gt", new BSONTimestamp((int) timestamp, 0));
                q.put("ts", q2);

                morphium.getDriver().tailableIteration("local", "oplog.rs", q, null, null, 0, 0, 1000, null, 1000, new DriverTailableIterationCallback() {
                    @Override
                    public boolean incomingData(Map<String, Object> data, long dur) {
                        timestamp = (Integer) data.get("ts");
                        for (OplogListener lst : listeners) {
                            try {
                                lst.incomingData(data);
                            } catch (Exception e) {
                                log.error("listener threw exception", e);
                            }
                        }
                        return true;
                    }
                });
            } catch (MorphiumDriverException e) {
                e.printStackTrace();
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e1) {
                }
            }
        }
    }

    @Override
    public void onShutdown(Morphium m) {
        stop();
    }
}
