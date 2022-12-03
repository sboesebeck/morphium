package de.caluga.test.mongo.suite.messaging;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.annotations.locking.Lockable;
import de.caluga.morphium.annotations.locking.LockedAt;
import de.caluga.morphium.annotations.locking.LockedBy;
import de.caluga.morphium.changestream.ChangeStreamEvent;
import de.caluga.morphium.changestream.ChangeStreamListener;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MessageRejectedException;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
public class ComplexMessageQueueTests extends MorphiumTestBase {

    @Test
    public void releaseLockonMessage() throws Exception {
        Messaging sender = new Messaging(morphium);
        sender.setSenderId("sender");
        sender.start();
        Vector<Messaging> clients = new Vector<>();
        Vector<MorphiumId> processedMessages = new Vector<>();

        try {
            MessageListener<Msg> lst = new MessageListener<Msg>() {
                @Override
                public boolean markAsProcessedBeforeExec() {
                    return true;
                }
                @Override
                public Msg onMessage(Messaging msg, Msg m) {
                    if (processedMessages.contains(m.getMsgId())) {
                        log.error("Duplicate processing!");
                    }

                    processedMessages.add(m.getMsgId());
                    return null;
                }
            };

            for (int i = 0; i < 10; i++) {
                Messaging cl = new Messaging(morphium);
                cl.setSenderId("cl" + i);
                cl.addMessageListener(lst);
                cl.start();
                clients.add(cl);
            }

            // //sending messages
            // for (int i = 0; i < 10; i++) {
            //     Msg m = new Msg("test", "msg", "value", 1000, true);
            //     sender.sendMessage(m);
            // }
            //
            // Thread.sleep(1000);
            // assertEquals(10, processedMessages.size());
            // processedMessages.clear();
            // //releasing lock _after_ sending
            Msg m = new Msg("test", "msg", "value", 1000, true);
            m.setLockedBy("someone");
            m.setTimingOut(false);
            sender.sendMessage(m);
            Thread.sleep(1000);
            assertEquals(0, processedMessages.size());

            var q=morphium.createQueryFor(Msg.class).f("_id").eq(m.getMsgId());
            q.set("locked_by",null);

            Thread.sleep(1000);
            assertEquals(1, processedMessages.size(), "not processed");
        } finally {
            for (Messaging m : clients) { m.terminate(); }

            sender.terminate();
        }
    }
    @Test
    public void rejectPauseReplayTest() throws Exception {
        Messaging sender = new Messaging(morphium);
        sender.setSenderId("sender");
        sender.start();
        Vector<Messaging> clients = new Vector<>();
        Map<String, Integer> maxParallel = Map.of("msg1", 1, "msg3", 3, "msg4", 2);
        AtomicInteger counts = new AtomicInteger(0);
        List<MorphiumId> processedIds = new Vector<>();
        GlobalLock gl = new GlobalLock();
        gl.scope = "global";
        morphium.insert(gl);

        try {
            var msgL = new MessageListener<Msg>() {
                @Override
                public boolean markAsProcessedBeforeExec() {
                    return true;
                }
                @Override
                public Msg onMessage(Messaging msg, Msg m) {
                    // var q = morphium.createQueryFor(GlobalLock.class).f("_id").eq("global");
                    // q.limit(1);
                    //
                    // if (q.countAll() == 0) {
                    //     GlobalLock gl = new GlobalLock();
                    //     gl.scope = "global";
                    //     morphium.store(gl);
                    // }
                    //
                    try {
                        // var ent = morphium.lockEntities(q, msg.getSenderId(), 5000);
                        // if (ent != null && !ent.isEmpty()) {
                        // log.info(msg.getSenderId() + " - got lock!");
                        //check for maxParallel
                        log.info("OnMessage " + m.getMsgId());
                        var cnt = morphium.createQueryFor(MessageCount.class).f("name").eq(m.getName()).countAll();

                        if (maxParallel.containsKey(m.getName()) && maxParallel.get(m.getName()) != null) {
                            if (cnt >= maxParallel.get(m.getName())) {
                                log.info("MaxParallel exceeded! - pausing!");
                                msg.pauseProcessingOfMessagesNamed(m.getName());
                                throw new MessageRejectedException("MaxPArallel exceeded", true);
                            }
                        }

                        // }

                        //no maxParallel - can do our magic
                        if (processedIds.contains(m.getMsgId())) {
                            log.error("Duplicate Message processing for ID " + m.getMsgId());
                        }

                        processedIds.add(m.getMsgId());
                        MessageCount mcnt = new MessageCount();
                        mcnt.id = m.getMsgId().toString();
                        mcnt.name = m.getName();
                        morphium.insert(mcnt);
                        //simulate to process message
                        counts.incrementAndGet();
                        Thread.sleep(((int)(Math.random() * 10000.0)));
                        //reset message count
                        morphium.delete(mcnt);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    return null;
                }
            };

            for (int i = 0; i < 10; i++) {
                log.info("Creating client " + i);
                Messaging rec1 = new Messaging(morphium, 100, true, true, 1);
                rec1.setSenderId("rec" + i);
                rec1.addListenerForMessageNamed("msg1", msgL);
                rec1.addListenerForMessageNamed("msg2", msgL);
                rec1.addListenerForMessageNamed("msg3", msgL);
                rec1.addListenerForMessageNamed("msg4", msgL);
                rec1.addListenerForMessageNamed("msg5", msgL);
                rec1.start();
                new Thread() {
                    public void run() {
                        morphium.watch(MessageCount.class, true, new ChangeStreamListener() {
                            @Override
                            public boolean incomingData(ChangeStreamEvent evt) {
                                if (evt.getOperationType().equals("delete")) {
                                    log.info("got delete!");
                                } else if (evt.getOperationType().equals("insert")) {
                                    log.info("got insert");
                                    Msg obj = morphium.getMapper().deserialize(Msg.class, evt.getFullDocument());
                                    var cnt = morphium.createQueryFor(MessageCount.class).f("name").eq(obj.getName()).countAll();

                                    if (maxParallel.containsKey(obj.getName()) && maxParallel.get(obj.getName()) != null && maxParallel.get(obj.getName()) <= cnt) {
                                        log.info("MaxParrallel reached - pausing for " + obj.getName());
                                        rec1.pauseProcessingOfMessagesNamed(obj.getName());
                                    }
                                }

                                return rec1.isRunning();
                            }
                        });
                    }
                }.start();
                new Thread() {
                    public void run() {
                        morphium.watch(Msg.class, true, new ChangeStreamListener() {
                            @Override
                            public boolean incomingData(ChangeStreamEvent evt) {
                                if (evt.getOperationType().equals("insert")) {
                                    log.info("new Message!");
                                    Msg obj = morphium.getMapper().deserialize(Msg.class, evt.getFullDocument());

                                    if (obj.getLockedBy() != null && obj.getLockedBy().equals("Planner")) {
                                        log.info("For planning....");
                                        //for me!
                                        //Planning ahead
                                        var q = morphium.createQueryFor(GlobalLock.class).f("_id").eq("global");
                                        q.limit(1);

                                        try {
                                            var lock = morphium.lockEntities(q, "global", 5000);

                                            if (lock != null && lock.size() != 0) {
                                                log.info("Got lock! processing " + obj.getMsgId());
                                                obj.setLockedBy(null);
                                                morphium.save(obj);
                                                morphium.releaseLock(lock.get(0));
                                            }
                                        } catch (Exception e) {
                                            log.error("Error ", e);
                                        }
                                    }
                                }

                                return rec1.isRunning();
                            }
                        });
                    }
                }
                .start();
                clients.add(rec1);
            }

            //sending some messages
            int amount = 1000;

            for (int i = 0; i < amount; i++) {
                Msg m = new Msg("msg" + (i % 5 + 1), "msg", "value");
                m.setDeleteAfterProcessing(true);
                // m.setLockedBy("Planner");
                // m.setLocked(System.currentTimeMillis());
                // assertTrue(m.isExclusive());
                m.setExclusive(true);
                sender.sendMessage(m);
                log.info("Inserted " + m.getMsgId());
                Thread.sleep(100);
            }

            TestUtils.waitForConditionToBecomeTrue(5000, "Not all processed?", ()->counts.get() >= amount);
            assertEquals(amount, counts.get());
        } finally {
            for (Messaging m : clients) { m.terminate(); }

            sender.terminate();
        }
    }

    @Lockable
    @Entity
    public static class GlobalLock {
        @Id
        public String scope;
        @LockedBy
        public String lockedBy;
        @LockedAt
        public long lockedAt;
    }

    @Entity
    @Index("id,name")
    public static class MessageCount {
        @Id
        public String id;
        @Index
        public String name;
        @Index(options = "expireAfterSeconds:0")
        public Date delAt;
    }
}
