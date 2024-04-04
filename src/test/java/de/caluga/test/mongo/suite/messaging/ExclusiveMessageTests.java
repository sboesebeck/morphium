package de.caluga.test.mongo.suite.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MsgLock;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

public class ExclusiveMessageTests extends MorphiumTestBase {
    private boolean gotMessage1 = false;
    private boolean gotMessage2 = false;
    private boolean gotMessage3 = false;
    private boolean gotMessage4 = false;

    @Test
    public void ignoringExclusiveMessagesTest() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(100);
        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        m1.setSenderId("m1");
        Messaging m2 = new Messaging(morphium, 10, false, true, 10);
        m2.setSenderId("m2");
        Messaging m3 = new Messaging(morphium, 10, false, true, 10);
        m3.setSenderId("m3");
        m3.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) {
                return null;
            }
        });

        try {
            m1.start();
            m2.start();
            m3.start();
            Thread.sleep(1250);

            for (int i = 0; i < 10; i++) {
                Msg m = new Msg("test", "ignore me please", "value", 20000, true);
                m1.sendMessage(m);

                while (true) {
                    Thread.sleep(1000);
                    m = morphium.reread(m);
                    assertNotNull(m);

                    if (m.getProcessedBy() != null && m.getProcessedBy().size() != 0) {
                        break;
                    }
                }

                assertEquals(1, m.getProcessedBy().size());
                assertTrue(m.getProcessedBy().contains("m3"));
            }
        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
        }
    }

    @Test
    @Disabled
    public void deleteAfterProcessingTest() throws Exception {
        morphium.dropCollection(Msg.class);
        TestUtils.waitForConditionToBecomeTrue(1000, "Collection did not drop", ()->!morphium.exists(Msg.class));
        Messaging sender = new Messaging(morphium, 100, false);
        sender.setQueueName("t1");
        sender.start();
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;
        Messaging m1 = new Messaging(morphium, 100, false);
        m1.setQueueName("t1");
        m1.addMessageListener((msg, m)-> {
            gotMessage1 = true;
            return null;
        });
        Messaging m2 = new Messaging(morphium, 100, false);
        m2.setQueueName("t1");
        m2.addMessageListener((msg, m)-> {
            // gotMessage2 = true;
            return null;
        });
        Messaging m3 = new Messaging(morphium, 100, false);
        m3.setQueueName("t1");
        m3.addMessageListener((msg, m)-> {
            gotMessage3 = true;
            return null;
        });
        m1.start();
        m2.start();
        m3.start();

        try {
            Thread.sleep(2000);
            Msg m = new Msg();
            m.setExclusive(true);
            m.setDeleteAfterProcessing(true);
            m.setDeleteAfterProcessingTime(0);
            m.setName("A message");
            m.setProcessedBy(Arrays.asList("someone_else"));
            sender.sendMessage(m);
            Thread.sleep(1000);
            assertFalse(gotMessage1 || gotMessage2 || gotMessage3 || gotMessage4);
            morphium.setInEntity(m, m1.getCollectionName(), Map.of(Msg.Fields.processedBy, new ArrayList<String>()));
            Thread.sleep(100);
            long s = System.currentTimeMillis();

            while (true) {
                int rec = 0;

                if (gotMessage1) {
                    rec++;
                }

                if (gotMessage2) {
                    rec++;
                }

                if (gotMessage3) {
                    rec++;
                }

                if (rec == 1) {
                    break;
                }

                assertThat(rec).isLessThanOrEqualTo(1);
                Thread.sleep(50);
                assertThat(System.currentTimeMillis() - s).isLessThan(morphium.getConfig().getMaxWaitTime());
            }

            Thread.sleep(5100);
            assertEquals(0, m1.getNumberOfMessages());
            assertEquals(0, morphium.createQueryFor(Msg.class, m1.getCollectionName()).countAll());
            assertEquals(0, morphium.createQueryFor(MsgLock.class, m1.getLockCollectionName()).countAll());
        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
            sender.terminate();
        }
    }

    @Test
    @Disabled
    public void exclusiveMessageTest() throws Exception {
        morphium.dropCollection(Msg.class);
        TestUtils.waitForConditionToBecomeTrue(1000, "Collection did not drop", ()->!morphium.exists(Msg.class));
        Messaging sender = new Messaging(morphium, 100, false);
        sender.start();
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;
        Messaging m1 = new Messaging(morphium, 100, false);
        m1.addMessageListener((msg, m)-> {
            gotMessage1 = true;
            return null;
        });
        Messaging m2 = new Messaging(morphium, 100, false);
        m2.addMessageListener((msg, m)-> {
            // gotMessage2 = true;
            return null;
        });
        Messaging m3 = new Messaging(morphium, 100, false);
        m3.addMessageListener((msg, m)-> {
            gotMessage3 = true;
            return null;
        });
        m1.start();
        m2.start();
        m3.start();

        try {
            Thread.sleep(2000);
            Msg m = new Msg();
            m.setExclusive(true);
            m.setName("A message");
            sender.sendMessage(m);
            long s = System.currentTimeMillis();

            while (true) {
                int rec = 0;

                if (gotMessage1) {
                    rec++;
                }

                if (gotMessage2) {
                    rec++;
                }

                if (gotMessage3) {
                    rec++;
                }

                if (rec == 1) {
                    break;
                }

                assertThat(rec).isLessThanOrEqualTo(1);
                Thread.sleep(50);
                assertThat(System.currentTimeMillis() - s).isLessThan(2 * morphium.getConfig().getMaxWaitTime());
            }

            Thread.sleep(100);
            assertEquals(0, m1.getNumberOfMessages());
        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
            sender.terminate();
        }
    }

    @Test
    public void exclusiveMessageCustomQueueTest() throws Exception {
        Messaging sender = null;
        Messaging sender2 = null;
        Messaging m1 = null;
        Messaging m2 = null;
        Messaging m3 = null;
        Messaging m4 = null;

        try {
            morphium.dropCollection(Msg.class);
            sender = new Messaging(morphium, "test", 100, false);
            sender.setSenderId("sender1");
            morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
            sender.start();
            sender2 = new Messaging(morphium, "test2", 100, false);
            sender2.setSenderId("sender2");
            morphium.dropCollection(Msg.class, sender2.getCollectionName(), null);
            sender2.start();
            Thread.sleep(200);
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;
            m1 = new Messaging(morphium, "test", 100, false);
            m1.setSenderId("m1");
            m1.addMessageListener((msg, m)-> {
                gotMessage1 = true;
                log.info("Got message m1");
                return null;
            });
            m2 = new Messaging(morphium, "test", 100, false);
            m2.setSenderId("m2");
            m2.addMessageListener((msg, m)-> {
                gotMessage2 = true;
                log.info("Got message m2");
                return null;
            });
            m3 = new Messaging(morphium, "test2", 100, false);
            m3.setSenderId("m3");
            m3.addMessageListener((msg, m)-> {
                gotMessage3 = true;
                log.info("Got message m3");
                return null;
            });
            m4 = new Messaging(morphium, "test2", 100, false);
            m4.setSenderId("m4");
            m4.addMessageListener((msg, m)-> {
                gotMessage4 = true;
                log.info("Got message m4");
                return null;
            });
            m1.start();
            m2.start();
            m3.start();
            m4.start();
            Thread.sleep(2200);
            //Sending exclusive Message
            Msg m = new Msg();
            m.setExclusive(true);
            m.setTtl(3000000);
            m.setMsgId(new MorphiumId());
            m.setName("A message");
            log.info("Sending: " + m.getMsgId().toString());
            sender.sendMessage(m);
            assert(!gotMessage3);
            assert(!gotMessage4);
            long s = System.currentTimeMillis();

            while (!gotMessage1 && !gotMessage2) {
                Thread.sleep(200);
                log.info("Still did not get all messages: m1=" + gotMessage1 + " m2=" + gotMessage2);
                assertThat(System.currentTimeMillis() - s).isLessThan(morphium.getConfig().getMaxWaitTime());
            }

            int rec = 0;

            if (gotMessage1) {
                rec++;
            }

            if (gotMessage2) {
                rec++;
            }

            assert(rec == 1) : "rec is " + rec;
            gotMessage1 = false;
            gotMessage2 = false;
            m = new Msg();
            m.setExclusive(true);
            m.setName("A message");
            m.setTtl(3000000);
            sender2.sendMessage(m);
            Thread.sleep(500);
            assert(!gotMessage1);
            assert(!gotMessage2);
            rec = 0;
            s = System.currentTimeMillis();

            while (rec == 0) {
                if (gotMessage3) {
                    rec++;
                }

                if (gotMessage4) {
                    rec++;
                }

                Thread.sleep(100);
                assert(System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
            }

            assert(rec == 1) : "rec is " + rec;
            Thread.sleep(2500);

            for (Messaging ms : Arrays.asList(m1, m2, m3)) {
                if (ms.getNumberOfMessages() > 0) {
                    Query<Msg> q1 = morphium.createQueryFor(Msg.class, ms.getCollectionName());
                    q1.f(Msg.Fields.sender).ne(ms.getSenderId());
                    q1.f(Msg.Fields.processedBy).ne(ms.getSenderId());
                    List<Msg> ret = q1.asList();

                    for (Msg f : ret) {
                        log.info("Found elements for " + ms.getSenderId() + ": " + f.toString());
                    }
                }
            }

            for (Messaging ms : Arrays.asList(m1, m2, m3)) {
                assert(ms.getNumberOfMessages() == 0) : "Number of messages " + ms.getSenderId() + " is " + ms.getNumberOfMessages();
            }
        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
            m4.terminate();
            sender.terminate();
            sender2.terminate();
        }
    }


    @Test
    public void exclusivityTest() throws Exception {
        Messaging sender = new Messaging(morphium, 100, false);
        sender.setSenderId("sender");
        morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
        morphium.dropCollection(MsgLock.class, sender.getLockCollectionName(), null);
        Thread.sleep(2000);
        sender.start();
        Morphium morphium2 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        morphium2.getConfig().setThreadPoolMessagingMaxSize(10);
        morphium2.getConfig().setThreadPoolMessagingCoreSize(5);
        morphium2.getConfig().setThreadPoolAsyncOpMaxSize(10);
        Messaging receiver = new Messaging(morphium2, 10, true, true, 15);
        receiver.setSenderId("r1");
        receiver.start();
        Morphium morphium3 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        morphium3.getConfig().setThreadPoolMessagingMaxSize(10);
        morphium3.getConfig().setThreadPoolMessagingCoreSize(5);
        morphium3.getConfig().setThreadPoolAsyncOpMaxSize(10);
        Messaging receiver2 = new Messaging(morphium3, 10, false, false, 15);
        receiver2.setSenderId("r2");
        receiver2.start();
        Morphium morphium4 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        morphium4.getConfig().setThreadPoolMessagingMaxSize(10);
        morphium4.getConfig().setThreadPoolMessagingCoreSize(5);
        morphium4.getConfig().setThreadPoolAsyncOpMaxSize(10);
        Messaging receiver3 = new Messaging(morphium4, 10, true, false, 15);
        receiver3.setSenderId("r3");
        receiver3.start();
        Morphium morphium5 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        morphium5.getConfig().setThreadPoolMessagingMaxSize(10);
        morphium5.getConfig().setThreadPoolMessagingCoreSize(5);
        morphium5.getConfig().setThreadPoolAsyncOpMaxSize(10);
        Messaging receiver4 = new Messaging(morphium5, 10, false, true, 15);
        receiver4.setSenderId("r4");
        receiver4.start();
        final AtomicInteger received = new AtomicInteger();
        final AtomicInteger dups = new AtomicInteger();
        final Map<String, Long> ids = new ConcurrentHashMap<>();
        final Map<String, String> recById = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> recieveCount = new ConcurrentHashMap<>();
        final Map<String, List<MorphiumId>> recIdsByReceiver = new ConcurrentHashMap<>();
        log.info("All receivers initialized... starting");
        Thread.sleep(2000);

        try {
            MessageListener messageListener = (msg, m)-> {
                try {
                    Thread.sleep((long)(500 * Math.random()));
                } catch (InterruptedException e) {
                }

                received.incrementAndGet();
                recieveCount.putIfAbsent(msg.getSenderId(), new AtomicInteger());
                recieveCount.get(msg.getSenderId()).incrementAndGet();

                if (ids.containsKey(m.getMsgId().toString()) && m.isExclusive()) {
                    log.error("Duplicate recieved message " + msg.getSenderId() + " " + (System.currentTimeMillis() - ids.get(m.getMsgId().toString())) + "ms ago");

                    if (recById.get(m.getMsgId().toString()).equals(msg.getSenderId())) {
                        log.error("--- duplicate was processed before by me!");
                    } else {
                        log.error("--- duplicate processed by someone else");
                    }

                    dups.incrementAndGet();
                } else if (!m.isExclusive()) {
                    recIdsByReceiver.putIfAbsent(msg.getSenderId(), new ArrayList<>());

                    if (recIdsByReceiver.get(msg.getSenderId()).contains(m.getMsgId())) {
                        log.error(msg.getSenderId() + ": Duplicate processing of broadcast message of same receiver! ");
                        m = msg.getMorphium().reread(m);

                        if (m == null) {
                            log.error("... is deleted???");
                        } else if (m.getProcessedBy().contains(msg.getSenderId())) {
                            log.error("... but is properly marked!");
                        }

                        dups.incrementAndGet();
                    } else {
                        recIdsByReceiver.get(msg.getSenderId()).add(m.getMsgId());
                    }
                }

                // log.info("Processed msg excl: " + m.isExclusive());
                ids.put(m.getMsgId().toString(), System.currentTimeMillis());
                recById.put(m.getMsgId().toString(), msg.getSenderId());
                return null;
            };
            receiver.addListenerForMessageNamed("m", messageListener);
            receiver2.addListenerForMessageNamed("m", messageListener);
            receiver3.addListenerForMessageNamed("m", messageListener);
            receiver4.addListenerForMessageNamed("m", messageListener);
            int amount = 150;
            int broadcastAmount = 50;

            for (int i = 0; i < amount; i++) {
                int rec = received.get();
                long messageCount = receiver.getPendingMessagesCount();

                if (i % 10 == 0) {
                    log.info("Send " + i + " recieved: " + rec + " queue: " + messageCount);
                }

                Msg m = new Msg("m", "m", "v" + i, 30000, true);
                m.setExclusive(true);
                sender.sendMessage(m);
            }

            for (int i = 0; i < broadcastAmount; i++) {
                int rec = received.get();
                long messageCount = receiver.getPendingMessagesCount();

                if (i % 100 == 0) {
                    log.info("Send broadcast " + i + " recieved: " + rec + " queue: " + messageCount);
                }

                Msg m = new Msg("m", "m", "v" + i, 30000, false);
                sender.sendMessage(m);
            }

            long waitUntil = System.currentTimeMillis() + 60000;

            while (received.get() != amount + broadcastAmount * 4) {
                int rec = received.get();
                long messageCount = receiver.getPendingMessagesCount();
                log.info(String.format("Send excl: %d  brodadcast: %d recieved: %d queue: %d currently processing: %d", amount, broadcastAmount, rec, messageCount,
                        (amount + broadcastAmount * 4 - rec - messageCount)));
                log.info(String.format("Number of ids: %d", ids.size()));
                assert(dups.get() == 0) : "got duplicate message";

                for (Messaging m : Arrays.asList(receiver, receiver2, receiver3, receiver4)) {
                    log.info(m.getSenderId() + " active Tasks: " + m.getRunningTasks());
                }

                Thread.sleep(1000);
                assertTrue(System.currentTimeMillis() < waitUntil, "Took too long!");
            }

            int rec = received.get();
            long messageCount = sender.getPendingMessagesCount();
            log.info("Send " + amount + " recieved: " + rec + " queue: " + messageCount);
            assert(received.get() == amount + broadcastAmount * 4) : "should have received " + (amount + broadcastAmount * 4) + " but actually got " + received.get();

            for (String id : recieveCount.keySet()) {
                log.info("Reciever " + id + " message count: " + recieveCount.get(id).get());
            }

            log.info("R1 active: " + receiver.getRunningTasks());
            log.info("R2 active: " + receiver2.getRunningTasks());
            log.info("R3 active: " + receiver3.getRunningTasks());
            log.info("R4 active: " + receiver4.getRunningTasks());
        } finally {
            sender.terminate();
            receiver.terminate();
            receiver2.terminate();
            receiver3.terminate();
            receiver4.terminate();
            morphium2.close();
            morphium3.close();
            morphium4.close();
            morphium5.close();
        }
    }

    @Test
    public void exclusiveMessageStartupTests() throws Exception {
        Messaging sender = new Messaging(morphium, 100, false);
        Messaging receiverNoListener = new Messaging(morphium, 100, true);

        try {
            sender.setSenderId("sender");
            morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
            morphium.dropCollection(MsgLock.class, sender.getLockCollectionName(), null);
            Thread.sleep(2000);
            sender.start();
            sender.sendMessage(new Msg("test", "test", "test", 30000, true));
            sender.sendMessage(new Msg("test", "test", "test", 30000, true));
            sender.sendMessage(new Msg("test", "test", "test", 30000, true));
            Thread.sleep(1000);
            receiverNoListener.setSenderId("recNL");
            receiverNoListener.start();
            assert(morphium.createQueryFor(Msg.class, sender.getCollectionName()).countAll() == 3);
        } finally {
            sender.terminate();
            receiverNoListener.terminate();
        }
    }

    @Test
    public void exclusiveMessageDelAfterProcessingTimeOffsetTest() throws Exception {
        Messaging sender = new Messaging(morphium, 100, false);
        sender.setSenderId("Sender");
        // sender.start();
        Msg m = new Msg("test", "msg", "value");
        m.setExclusive(true);
        m.setTimingOut(false);
        m.setDeleteAfterProcessing(true);
        m.setDeleteAfterProcessingTime(10);
        sender.sendMessage(m);
        Thread.sleep(200);
        AtomicInteger recCount = new AtomicInteger(0);
        Messaging receiver = new Messaging(morphium, 100, true);
        receiver.addListenerForMessageNamed("test2", (messaging, msg)-> {
            recCount.incrementAndGet();
            return null;
        });
        receiver.addListenerForMessageNamed("test", (messaging, msg)-> {
            recCount.incrementAndGet();
            return null;
        });
        receiver.start();

        try {
            Thread.sleep(5000);
            assertEquals(1, recCount.get());
            log.info("waiting for mongo to delete...");
            long start = System.currentTimeMillis();

            while (true) {
                var cnt = morphium.createQueryFor(Msg.class, sender.getCollectionName()).countAll();
                log.info("... still " + cnt);

                if (cnt == 0) {
                    break;
                }

                Thread.sleep(4000);
                assertTrue(System.currentTimeMillis() - start < 90000);
            }

            log.info("Deleted after: " + (System.currentTimeMillis() - start));
            assertEquals(0, morphium.createQueryFor(MsgLock.class, sender.getLockCollectionName()).countAll());;
        } finally {
            sender.terminate();
            receiver.terminate();
        }
    }

    @Test
    public void exclusiveMessageCheckOnStartTest() throws Exception {
        Messaging sender = new Messaging(morphium, 100, false);
        sender.setSenderId("Sender");
        // sender.start();
        Msg m = new Msg("test", "msg", "value");
        m.setExclusive(true);
        m.setTimingOut(false);
        m.setDeleteAfterProcessing(true);
        m.setDeleteAfterProcessingTime(0);
        sender.sendMessage(m);
        Thread.sleep(200);
        AtomicInteger recCount = new AtomicInteger(0);
        Messaging receiver = new Messaging(morphium, 100, true);
        receiver.addListenerForMessageNamed("test2", (messaging, msg)-> {
            recCount.incrementAndGet();
            return null;
        });
        receiver.addListenerForMessageNamed("test", (messaging, msg)-> {
            recCount.incrementAndGet();
            return null;
        });
        receiver.start();

        try {
            Thread.sleep(3000);
            assertEquals(1, recCount.get());
        } finally {
            sender.terminate();
            receiver.terminate();
        }
    }

    @Test
    public void exclusiveTest() throws Exception {
        morphium.dropCollection(Msg.class);
        Messaging sender;
        List<Messaging> recs;
        sender = new Messaging(morphium, 1000, false);
        sender.setSenderId("sender");
        //sender.start();
        final AtomicInteger counts = new AtomicInteger();
        recs = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            Messaging r = new Messaging(morphium, 100, false);
            r.setSenderId("r" + i);
            recs.add(r);
            r.start();
            r.addMessageListener((m, msg)-> {
                counts.incrementAndGet();
                return null;
            });
        }

        try {
            for (int i = 0; i < 50; i++) {
                if (i % 10 == 0) {
                    log.info("Msg sent");
                }

                sender.sendMessage(new Msg("excl_name", "msg", "value", 20000000, true).setDeleteAfterProcessing(true).setDeleteAfterProcessingTime(0));
            }

            while (counts.get() < 50) {
                log.info("Still waiting for incoming messages: " + counts.get());
                Thread.sleep(1000);
            }

            Thread.sleep(2000);
            assertThat(counts.get()).describedAs("Dig get too many {}", counts.get()).isEqualTo(50);
            counts.set(0);

            for (int i = 0; i < 10; i++) {
                log.info("Msg sent");
                sender.sendMessage(new Msg("excl_name", "msg", "value", 20000000, false));
            }

            while (counts.get() < 10 * recs.size()) {
                log.info("Still waiting for incoming messages: " + counts.get());
                Thread.sleep(1000);
            }

            Thread.sleep(2000);
            assert(counts.get() == 10 * recs.size()) : "Did get too many? " + counts.get();
        } finally {
            sender.terminate();

            for (Messaging r : recs) {
                r.terminate();
            }
        }

        for (Messaging r : recs) {
            assert(!r.isRunning());
        }
    }

}
