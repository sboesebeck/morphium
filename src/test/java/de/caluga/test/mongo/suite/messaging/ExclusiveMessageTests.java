package de.caluga.test.mongo.suite.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;


public class ExclusiveMessageTests extends MorphiumTestBase{
    private boolean gotMessage1=false;
    private boolean gotMessage2=false;
    private boolean gotMessage3=false;
    private boolean gotMessage4=false;


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
            public Msg onMessage(Messaging msg, Msg m)  {
                return null;
            }
        });
        try {
            m1.start();
            m2.start();
            m3.start();
            Thread.sleep(1250);
            for (int i = 0; i < 10; i++) {
                Msg m = new Msg("test", "ignore me please", "value", 2000, true);
                m1.sendMessage(m);
                while (true) {
                    Thread.sleep(1000);
                    m = morphium.reread(m);
                    assertNotNull(m);
                    if (m.getProcessedBy() != null && m.getProcessedBy().size() != 0) break;
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
        m1.addMessageListener((msg, m) -> {
            gotMessage1 = true;
            return null;
        });
        Messaging m2 = new Messaging(morphium, 100, false);
        m2.addMessageListener((msg, m) -> {
            gotMessage2 = true;
            return null;
        });
        Messaging m3 = new Messaging(morphium, 100, false);
        m3.addMessageListener((msg, m) -> {
            gotMessage3 = true;
            return null;
        });

        m1.start();
        m2.start();
        m3.start();
        try {
            Thread.sleep(100);


            Msg m = new Msg();
            m.setExclusive(true);
            m.setName("A message");

            sender.queueMessage(m);
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
                if (rec == 1) break;
                assertThat(rec).isLessThanOrEqualTo(1);
                Thread.sleep(50);
                assertThat(System.currentTimeMillis() - s).isLessThan(morphium.getConfig().getMaxWaitTime());
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
            m1.addMessageListener((msg, m) -> {
                gotMessage1 = true;
                log.info("Got message m1");
                return null;
            });
            m2 = new Messaging(morphium, "test", 100, false);
            m2.setSenderId("m2");
            m2.addMessageListener((msg, m) -> {
                gotMessage2 = true;
                log.info("Got message m2");
                return null;
            });
            m3 = new Messaging(morphium, "test2", 100, false);
            m3.setSenderId("m3");
            m3.addMessageListener((msg, m) -> {
                gotMessage3 = true;
                log.info("Got message m3");
                return null;
            });
            m4 = new Messaging(morphium, "test2", 100, false);
            m4.setSenderId("m4");
            m4.addMessageListener((msg, m) -> {
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

            assert (!gotMessage3);
            assert (!gotMessage4);
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
            assert (rec == 1) : "rec is " + rec;

            gotMessage1 = false;
            gotMessage2 = false;

            m = new Msg();
            m.setExclusive(true);
            m.setName("A message");
            m.setTtl(3000000);
            sender2.sendMessage(m);
            Thread.sleep(500);
            assert (!gotMessage1);
            assert (!gotMessage2);

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
                assert (System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
            }
            assert (rec == 1) : "rec is " + rec;
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
                assert (ms.getNumberOfMessages() == 0) : "Number of messages " + ms.getSenderId() + " is " + ms.getNumberOfMessages();
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
    public void markExclusiveMessageTest() throws Exception {

        Messaging sender = new Messaging(morphium, 100, false);
        morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
        sender.start();
        Messaging receiver = new Messaging(morphium, 100, false, true, 10);
        receiver.start();
        Messaging receiver2 = new Messaging(morphium, 100, false, true, 10);
        receiver2.start();

        final AtomicInteger pausedReciever = new AtomicInteger(0);
        final AtomicInteger messageCount=new AtomicInteger();

        try {
            Thread.sleep(100);
            receiver.addMessageListener((msg, m) -> {
//                log.info("R1: Incoming message");
                messageCount.incrementAndGet();
                assertThat(pausedReciever.get()).describedAs("Should not get message when paused").isNotEqualTo(1);

                return null;
            });

            receiver2.addMessageListener((msg, m) -> {
//                log.info("R2: Incoming message");
                messageCount.incrementAndGet();
                assertThat(pausedReciever.get()).describedAs("Should not get message when paused").isNotEqualTo(2);
                return null;
            });


            for (int i = 0; i < 200; i++) {
                Msg m = new Msg("test", "test", "value", 3000000, true);
                sender.sendMessage(m);
                if (i == 100) {
                    receiver2.pauseProcessingOfMessagesNamed("test");
                    Thread.sleep(50);
                    pausedReciever.set(2);
                } else if (i == 120) {
                    receiver.pauseProcessingOfMessagesNamed("test");
                    Thread.sleep(50);
                    pausedReciever.set(1);
                } else if (i == 160) {
                    pausedReciever.set(0);
                    receiver.unpauseProcessingOfMessagesNamed("test");
                    receiver.findAndProcessPendingMessages("test");
                    receiver2.unpauseProcessingOfMessagesNamed("test");
                    receiver2.findAndProcessPendingMessages("test");
                }

            }

            long start = System.currentTimeMillis();
            Query<Msg> q = morphium.createQueryFor(Msg.class).f(Msg.Fields.name).eq("test").f(Msg.Fields.processedBy).eq(null);
            while (q.countAll() > 0) {
                log.info("Count is still: " + q.countAll()+ " received: "+messageCount.get());
                Thread.sleep(500);
                receiver.triggerCheck();
                receiver2.triggerCheck();
                // assertThat(System.currentTimeMillis()-start).describedAs("Messages should be processed by now!").isLessThan(15000);
            }
            assert (q.countAll() == 0) : "Count is wrong: " + q.countAll();
//
        } finally {
            receiver.terminate();
            receiver2.terminate();
            sender.terminate();
        }

    }

    @Test
    public void exclusivityTest() throws Exception {
        Messaging sender = new Messaging(morphium, 100, false);
        sender.setSenderId("sender");
        morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
        Thread.sleep(100);
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
        Thread.sleep(100);

        try {
            MessageListener messageListener = (msg, m) -> {
                try {
                    Thread.sleep((long) (500 * Math.random()));
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
                }
                ids.put(m.getMsgId().toString(), System.currentTimeMillis());
                recById.put(m.getMsgId().toString(), msg.getSenderId());
                return null;
            };
            receiver.addListenerForMessageNamed("m", messageListener);
            receiver2.addListenerForMessageNamed("m", messageListener);
            receiver3.addListenerForMessageNamed("m", messageListener);
            receiver4.addListenerForMessageNamed("m", messageListener);
            int amount = 200;
            int broadcastAmount = 50;
            for (int i = 0; i < amount; i++) {
                int rec = received.get();
                long messageCount = 0;
                messageCount += receiver.getPendingMessagesCount();
                if (i % 100 == 0) log.info("Send " + i + " recieved: " + rec + " queue: " + messageCount);
                Msg m = new Msg("m", "m", "v" + i, 3000000, true);
                m.setExclusive(true);
                sender.sendMessage(m);
            }
            for (int i = 0; i < broadcastAmount; i++) {
                int rec = received.get();
                long messageCount = receiver.getPendingMessagesCount();
                if (i % 100 == 0) log.info("Send broadcast" + i + " recieved: " + rec + " queue: " + messageCount);
                Msg m = new Msg("m", "m", "v" + i, 3000000, false);
                sender.sendMessage(m);
            }

            while (received.get() != amount + broadcastAmount * 4) {
                int rec = received.get();
                long messageCount = sender.getPendingMessagesCount();
                log.info("Send excl: " + amount + "  brodadcast: " + broadcastAmount + " recieved: " + rec + " queue: " + messageCount + " currently processing: " + (amount + broadcastAmount * 4 - rec - messageCount));
                assert (dups.get() == 0) : "got duplicate message";
                for (Messaging m : Arrays.asList(receiver, receiver2, receiver3, receiver4)) {
                    log.info(m.getSenderId() + " active Tasks: " + m.getRunningTasks());
                }
                Thread.sleep(1000);
            }
            int rec = received.get();
            long messageCount = sender.getPendingMessagesCount();
            log.info("Send " + amount + " recieved: " + rec + " queue: " + messageCount);
            assert (received.get() == amount + broadcastAmount * 4) : "should have received " + (amount + broadcastAmount * 4) + " but actually got " + received.get();

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
            Thread.sleep(100);
            sender.start();

            sender.sendMessage(new Msg("test", "test", "test", 30000, true));
            sender.sendMessage(new Msg("test", "test", "test", 30000, true));
            sender.sendMessage(new Msg("test", "test", "test", 30000, true));
            Thread.sleep(1000);
            receiverNoListener.setSenderId("recNL");
            receiverNoListener.start();

            assert (morphium.createQueryFor(Msg.class, sender.getCollectionName()).countAll() == 3);
        } finally {
            sender.terminate();
            receiverNoListener.terminate();
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

            r.addMessageListener((m, msg) -> {
                counts.incrementAndGet();
                return null;
            });
        }
        try {

            for (int i = 0; i < 50; i++) {
                if (i % 10 == 0) log.info("Msg sent");
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
            assert (counts.get() == 10 * recs.size()) : "Did get too many? " + counts.get();

        } finally {
            sender.terminate();
            for (Messaging r : recs) r.terminate();


        }

        for (Messaging r : recs) {
            assert (!r.isRunning());
        }

    }


}
