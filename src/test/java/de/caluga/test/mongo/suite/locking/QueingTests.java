
package de.caluga.test.mongo.suite.locking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.annotations.CreationTime;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.locking.Lockable;
import de.caluga.morphium.annotations.locking.LockedAt;
import de.caluga.morphium.annotations.locking.LockedBy;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MessageRejectedException;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;

public class QueingTests extends MorphiumTestBase {

    @Test
    public void queingWithMessagingTest() throws Exception {
        Messaging producer = new Messaging(morphium);
        producer.start();
        Vector<MorphiumId> processed = new Vector<>();
        List<Messaging> consumers = new ArrayList<>();
        AtomicBoolean running = new AtomicBoolean(true);
        Vector<String[]> notParallel = new Vector<>();
        notParallel.add(new String[] {"exec_2", "exec_5"});
        notParallel.add(new String[] {"exec_1", "exec_7"});

        for (int i = 0; i < 10; i++) {
            Messaging m = new Messaging(morphium, 100, false, true, 1);
            m.setSenderId("M" + i);

            for (int x = 0; x < 10; x++) {
                int j = x;
                m.addListenerForMessageNamed("exec_" + j, new MessageListener<Msg>() {
                    @Override
                    public boolean markAsProcessedBeforeExec() {
                        return true;
                    }
                    @Override
                    public Msg onMessage(Messaging m, Msg msg)  {
                        // log.info("incoming!");
                        if (!running.get()) { return null; }

                        var q = morphium.createQueryFor(JobCounts.class).f("_id").eq("exec_" + j);
                        var current = q.get();

                        //aktueller wert
                        while (true) {
                            var currentCount=0;
                            if (current!=null) currentCount=current.count;
                            var res = q.q().f("_id").eq("exec_" + j).f("count").eq(currentCount).inc("count", 1, true, false);

                            if (((Integer)res.get("nModified")) > 0) { break; }

                            current = q.q().f("_id").eq("exec_" + j).get();
                            if (current.count > 3) {
                                m.pauseProcessingOfMessagesNamed(msg.getName());
                                throw new MessageRejectedException("max parallel reached!");
                            }
                            //retry
                        }

                        try {
                            for (var pairs : notParallel) {
                                if (msg.getName().equals(pairs[0]) || msg.getName().equals(pairs[1])) {
                                    //not parallel check
                                    var idx = 0;

                                    if (msg.getName().equals(pairs[0])) { idx = 1; }

                                    var jc = morphium.createQueryFor(JobCounts.class).f("_id").eq(pairs[idx]).get();

                                    if (jc != null && jc.count > 0) {
                                        m.pauseProcessingOfMessagesNamed(msg.getName());
                                        throw new MessageRejectedException(pairs[0] + " not parallel with " + pairs[1]);
                                    }

                                    break;
                                }
                            }

                            if (q.get() != null && q.get().count > 3) {
                                log.info("Maxparallel reached");
                                m.pauseProcessingOfMessagesNamed("exec_" + j);
                                throw new MessageRejectedException("Max Parallel reached!");
                            }

                            if (processed.contains(msg.getMsgId())) {
                                throw new RuntimeException("duplicate processing! " + j);
                            }

                            processed.add(msg.getMsgId());

                            if (Math.random() < 0.01) {
                                throw new RuntimeException("simulating an error in a runner!");
                            }

                            try {
                                Thread.sleep((int)(1000.0 * Math.random()) + 500);
                            } catch (Exception e) {
                            }
                        } finally {
                            q.dec("count", 1);
                            log.info("Running count for " + j + " is: " + q.get().count);
                        }

                        return null;
                    }
                });
            }

            m.start();
            consumers.add(m);
        }

        //janitor threads
        var janitor = new Thread(()->{
            while (running.get()) {
                for (var m : consumers) {
                    //get paused messages - uAnpausenpause if counter is ok
MESSAGES:

                    for (String msg : m.getPausedMessageNames()) {
                        //check for not_parallel_with functionality
                        for (var pairs : notParallel) {
                            if (msg.equals(pairs[0]) || msg.equals(pairs[1])) {
                                //not-parallel candidate...
                                var idx = 0;

                                if (msg.equals(pairs[0])) { idx = 1; }

                                var jc = morphium.createQueryFor(JobCounts.class).f("_id").eq(pairs[idx]).get();

                                if (jc != null && jc.count != 0) {
                                    log.info("Cannot continue: " + msg + " - " + pairs[idx] + " is still running " + jc.count);
                                    continue MESSAGES; //still duplicate
                                }
                            }
                        }

                        var cnts = morphium.createQueryFor(JobCounts.class).f("_id").eq(msg).get();

                        if (cnts == null || cnts.count < 3) {
                            //unpausing
                            log.info(msg + " can be processed again");
                            m.unpauseProcessingOfMessagesNamed(msg);
                        }
                    }
                }

                var lst = morphium.createQueryFor(JobCounts.class).asList();

                for (String[] pair : notParallel) {
                    int found = 0;

                    for (var jc : lst) {
                        assertTrue(jc.count < 3);

                        if (jc.jobId.equals(pair[0]) || jc.jobId.equals(pair[1])) { found++; }
                    }

                    assertTrue(found <= 1);
                }

                //check counter, if max is reached, pause all listeners.
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                }
            }
        });
        janitor.start();

        for (int i = 0; i < 1000; i++) {
            String execId = "exec_" + (((int)(Math.random() * 10000)) % 10);
            Msg m = new Msg(execId, "execute job", "some params").setExclusive(true);//.setTimingOut(false).setDeleteAfterProcessingTime(0).setDeleteAfterProcessing(true);
            producer.queueMessage(m);

            if (i % 100 == 0) {
                log.info("Sending msg..." + i + "/" + m.getName());
            }
        }

        while (processed.size() < 100) {
            log.info("Waiting for messages to be processed..." + processed.size());
            Thread.sleep(1000);
        }

        running.set(false);
        log.info("Shutting down");
        producer.terminate();

        for (var msg : consumers) { msg.terminate(); }
    }

    @Test
    public void queueTest() throws Exception {
        AtomicInteger threads = new AtomicInteger();
        AtomicBoolean running                = new AtomicBoolean(true);
        Vector<MorphiumId> idsProcessed = new Vector<>();
        Map<String, AtomicInteger> runningById = new ConcurrentHashMap<>();
        Thread producer = new Thread(()->{
            try {
                threads.incrementAndGet();

                while (running.get()) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                    }

                    String v = "v" + ((int)(Math.random() * 10));
                    QueueElement qe = new QueueElement();
                    qe.value = v;
                    morphium.store(qe);
                }
            } finally {
                threads.decrementAndGet();
            }
        });
        producer.start();
        Runnable consumer = ()->{
            threads.incrementAndGet();
            String thrId = "Thread" + threads.get();

            try {
                while (running.get()) {
                    var q = morphium.createQueryFor(QueueElement.class).sort("priority", "createdAt");
                    q.limit(1);

                    try {
                        //erstes element in der liste OHNE LOCK!
                        //lock collection für MaxParallel, ID:JOBName 5 Einträge,
                        //lock not parallel with
                        List<QueueElement> lst = morphium.lockEntities(q, thrId, 1000);

                        if (lst == null || lst.isEmpty()) {
                            //nothing got, pausing
                            Thread.sleep(100);
                            //locks wieder freigeben, von vorne anfangen
                        } else {
                            if (lst.size() > 1) {
                                throw new RuntimeException("Too many objects locked!");
                            }

                            log.info(thrId + " got qe " + lst.get(0).id);
                            runningById.putIfAbsent(lst.get(0).value, new AtomicInteger());
                            runningById.get(lst.get(0).value).incrementAndGet();

                            if (idsProcessed.contains(lst.get(0).id)) {
                                throw new RuntimeException("Id was already processed!");
                            }

                            idsProcessed.add(lst.get(0).id);
                            morphium.delete(lst.get(0));
                            runningById.get(lst.get(0).value).decrementAndGet();
                        }
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        running.set(false);
                    }

                    Thread.sleep(200);
                }
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            } finally {
                threads.decrementAndGet();
            }
        };

        for (int i = 0; i < 10; i++) {
            new Thread(consumer).start();
        }

        Thread.sleep(10000);
        running.set(false);

        while (threads.get() > 0) {
            log.info("Waiting...");
            Thread.sleep(500);
        }

        log.info("Processed: " + idsProcessed.size());

        for (var e : runningById.entrySet()) {
            assertEquals(0, e.getValue().get());
        }
    }

    @Lockable @Entity @CreationTime
    public static class QueueElement {
        @Id
        public MorphiumId id;
        public int priority = 500;
        @CreationTime
        public long createdAt;
        @LockedBy
        public String lockedBy;
        @LockedAt
        public long lockedAt;
        public String value;

    }
    @Entity
    public static class JobCounts {
        @Id
        public String jobId;
        public int count;
    }

}
