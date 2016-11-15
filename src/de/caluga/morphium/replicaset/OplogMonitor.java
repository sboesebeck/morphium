package de.caluga.morphium.replicaset;

import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
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
public class OplogMonitor implements Runnable {
    private Collection<OplogListener> listeners;
    private Morphium morphium;
    private boolean running = true;
    private Logger log = new Logger(OplogMonitor.class);
    private long timestamp;

    public OplogMonitor(Morphium m) {
        morphium = m;
        listeners = new ConcurrentLinkedDeque<OplogListener>();
        timestamp = System.currentTimeMillis() / 1000;
    }

    public void terminate() {
        running = false;
    }

    public void addListener(OplogListener lst) {
        listeners.add(lst);
    }

    public void removeListener(OplogListener lst) {
        listeners.remove(lst);
    }

    @Override
    public void run() {
        while (true) {
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

}
