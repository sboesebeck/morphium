package de.caluga.test.mongo.suite.messaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;

public class PausingUnpausingTests extends MorphiumTestBase {
    public boolean gotMessage = false;

    public boolean gotMessage1 = false;
    public boolean gotMessage2 = false;
    public boolean gotMessage3 = false;
    public boolean gotMessage4 = false;

    public boolean error = false;

    public MorphiumId lastMsgId;

    public AtomicInteger procCounter = new AtomicInteger(0);

    private final List<Msg> list = new ArrayList<>();

    private final AtomicInteger queueCount = new AtomicInteger(1000);


    @Test
    public void pauseUnpauseProcessingTest() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(1000);
        Messaging sender = new Messaging(morphium, 100, false, false, 1);
        sender.start();
        Thread.sleep(2500);
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        Messaging m1 = new Messaging(morphium, 100, false, false, 1);
        m1.addMessageListener((msg, m) -> {
            gotMessage1 = true;
            return new Msg(m.getName(), "got message", "value", 5000);
        });

        try {
            m1.start();
            Thread.sleep(2500);
            m1.pauseProcessingOfMessagesNamed("tst1");

            sender.sendMessage(new Msg("test", "a message", "the value"));
            Thread.sleep(1200);
            assert (gotMessage1);

            gotMessage1 = false;

            sender.sendMessage(new Msg("tst1", "a message", "the value"));
            Thread.sleep(1200);
            assert (!gotMessage1);

            Long l = m1.unpauseProcessingOfMessagesNamed("tst1");
            log.info("Processing was paused for ms " + l);
            //m1.findAndProcessPendingMessages("tst1");
            Thread.sleep(500);

            assert (gotMessage1);
            gotMessage1 = false;
            Thread.sleep(500);
            assert (!gotMessage1);

            gotMessage1 = false;
            sender.sendMessage(new Msg("tst1", "a message", "the value"));
            Thread.sleep(1200);
            assert (gotMessage1);
        } finally {
            m1.terminate();
            sender.terminate();
        }


    }

    @Test
    @Disabled
    public void priorityPausedMessagingTest() throws Exception {
        Messaging sender = new Messaging(morphium, 100, false);
        sender.start();
        Thread.sleep(2500);
        final AtomicInteger count = new AtomicInteger();
        final AtomicLong lastTS = new AtomicLong(0);

        list.clear();
        Messaging receiver = new Messaging(morphium, 10, false, true, 100);

        receiver.addListenerForMessageNamed("pause", (msg, m) -> {
            msg.pauseProcessingOfMessagesNamed(m.getName());
            String lst = "";
            if (lastTS.get() != 0) {
                lst = ("Last msg " + (System.currentTimeMillis() - lastTS.get()) + "ms ago");
            }
            lastTS.set(System.currentTimeMillis());
            log.info("Incoming paused message: prio " + m.getPriority() + "  timestamp: " + m.getTimestamp() + " " + lst);
            try {
                Thread.sleep(550);
            } catch (InterruptedException e) {
            }
            list.add(m);
            msg.unpauseProcessingOfMessagesNamed(m.getName());
            return null;
        });

        receiver.addListenerForMessageNamed("now", (msg, m) -> {
            log.info("incoming now-msg");
            count.incrementAndGet();
            return null;
        });

        for (int i = 0; i < 20; i++) {
            Msg m = new Msg("pause", "pause", "pause");
            m.setPriority((int) (Math.random() * 100.0));
            m.setExclusive(true);
            sender.sendMessage(m);
            //Throtteling for first message to wait for pausing
            if (i == 0) Thread.sleep(25);
            if (i % 2 == 0) {
                sender.sendMessage(new Msg("now", "now", "now"));
            }
        }
        //this can only work, if the receiver is started _after_ all messages are sent
        receiver.start();
        Thread.sleep(2000);
        long s = System.currentTimeMillis();
        while (count.get() < 6) {
            Thread.sleep(500);

            assert (System.currentTimeMillis() - s < 5*morphium.getConfig().getMaxWaitTime());
        }
        assert (count.get() > 5 && count.get() <= 10) : "Count wrong " + count.get();
        assert (list.size() < 5);
        s = System.currentTimeMillis();
        while (list.size() != 20) {
            Thread.sleep(1000);
            assert (System.currentTimeMillis() - s < 25000);
        }
        assert (list.size() == 20) : "Size wrong " + list.size();

        list.remove(0); //prio of first  is random

        int lastPrio = -1;

        for (Msg m : list) {
            log.info("Msg: " + m.getPriority());
            assert (m.getPriority() >= lastPrio);
            lastPrio = m.getPriority();
        }

    }


    @Test
    public void unpausingTest() throws Exception {
        list.clear();
        final AtomicInteger cnt = new AtomicInteger(0);
        Messaging sender = new Messaging(morphium, 100, false);
        sender.start();

        Messaging receiver = new Messaging(morphium, 10, false, true, 10);
        receiver.start();

        Thread.sleep(2000);
        receiver.addListenerForMessageNamed("pause", (msg, m) -> {
            msg.pauseProcessingOfMessagesNamed("pause");
            log.info("Processing pause  message");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
            cnt.incrementAndGet();
            msg.unpauseProcessingOfMessagesNamed("pause");

            return null;
        });

        receiver.addListenerForMessageNamed("now", (msg, m) -> {
            msg.pauseProcessingOfMessagesNamed("now");
            list.add(m);
            //log.info("Incoming msg..."+m.getMsgId());
            msg.unpauseProcessingOfMessagesNamed("now");
            return null;
        });
        sender.sendMessage(new Msg("now", "now", "now"));
        Thread.sleep(2500);
        assert (list.size() == 1);

        sender.sendMessage(new Msg("pause", "pause", "pause"));
        sender.sendMessage(new Msg("now", "now", "now"));
        Thread.sleep(1000);
        assert (list.size() == 2);

        sender.sendMessage(new Msg("pause", "pause", "pause"));
        sender.sendMessage(new Msg("pause", "pause", "pause"));
        sender.sendMessage(new Msg("pause", "pause", "pause"));
        assert (cnt.get() == 0) : "Count wrong " + cnt.get();
        Thread.sleep(2000);
        assert (cnt.get() == 1);
        //1st message processed
        Thread.sleep(2500);
        //Message after unpausing:
        assert (cnt.get() == 2) : "Count wrong: " + cnt.get();
        sender.sendMessage(new Msg("now", "now", "now"));
        Thread.sleep(500);
        assert (list.size() == 3);
        Thread.sleep(4000);
        //Message after unpausing:
        assert (cnt.get() == 4) : "Count wrong: " + cnt.get();
    }


    @Test
    public void testPausingUnpausingInListenerMultithreadded() throws Exception {
        testPausingUnpausingInListener(true);
    }

    @Test
    public void testPausingUnpausingInListenerSinglethreadded() throws Exception {
        testPausingUnpausingInListener(false);
    }

    private void testPausingUnpausingInListener(boolean multithreadded) throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(1000);
        Messaging sender = new Messaging(morphium, 100, false);
        sender.start();
        Thread.sleep(2500);
        log.info("Sender ID: " + sender.getSenderId());

        gotMessage1 = false;
        gotMessage2 = false;

        Messaging m1 = new Messaging(morphium, 10, false, multithreadded, 10);
        m1.addListenerForMessageNamed("test", (msg, m) -> {
            msg.pauseProcessingOfMessagesNamed("test");
            try {
                log.info("Incoming message " + m.getMsgId() + "/" + m.getMsg() + " from " + m.getSender() + " my id: " + msg.getSenderId());
                Thread.sleep(1000);
                if (m.getMsg().equals("test1")) {
                    gotMessage1 = true;
                }
                if (m.getMsg().equals("test2")) {
                    gotMessage2 = true;
                }
            } catch (InterruptedException e) {
            }
            msg.unpauseProcessingOfMessagesNamed("test");
            return null;
        });
        m1.start();
        Thread.sleep(1000);
        try {
            log.info("receiver id: " + m1.getSenderId());

            log.info("Testing with non-exclusive messages");
            Msg m = new Msg("test", "test1", "test", 3000000);
            m.setExclusive(false);
            sender.sendMessage(m);

            m = new Msg("test", "test2", "test", 3000000);
            m.setExclusive(false);
            sender.sendMessage(m);

            Thread.sleep(200);
            assert (!gotMessage1);
            assert (!gotMessage2);

            Thread.sleep(5200);
            assert (gotMessage1);
            assert (gotMessage2);

            log.info("... done!");
            log.info("Testing with exclusive messages...");


            gotMessage1 = gotMessage2 = false;

            m = new Msg("test", "test1", "test", 3000000);
            m.setExclusive(true);
            sender.sendMessage(m);

            m = new Msg("test", "test2", "test", 3000000);
            m.setExclusive(true);
            sender.sendMessage(m);
            Thread.sleep(200);
            assert (!gotMessage1);
            assert (!gotMessage2);

            Thread.sleep(5000);
            assert (gotMessage1);
            assert (gotMessage2);
        } finally {
            sender.terminate();
            m1.terminate();
        }


    }


    @Test
    public void testPausingUnpausingInListenerExclusiveMultithreadded() throws Exception {
        testPausingUnpausingInListenerExclusive(true);
    }


    @Test
    public void testPausingUnpausingInListenerExclusiveSinglethreadded() throws Exception {
        testPausingUnpausingInListenerExclusive(false);
    }


    private void testPausingUnpausingInListenerExclusive(boolean multithreadded) throws Exception {
        Messaging sender = null;
        Messaging m1 = null;
        try {
            morphium.dropCollection(Msg.class);
            Thread.sleep(1000);
            sender = new Messaging(morphium, 100, false);
            sender.setSenderId("Sender");
            // sender.start();
            log.info("Sender ID: " + sender.getSenderId());

            gotMessage1 = false;
            gotMessage2 = false;
            boolean[] fail = {false};
            m1 = new Messaging(morphium, 100, true, multithreadded, 1);
            m1.setSenderId("m1");
            m1.addListenerForMessageNamed("test", (msg, m) -> {
                msg.pauseProcessingOfMessagesNamed("test");

                try {
                    assert (m.isExclusive());
                    //                assert (m.getReceivedBy().contains(msg.getSenderId()));
                    log.info("Incoming message " + m.getMsgId() + "/" + m.getMsg() + " from " + m.getSender() + " my id: " + msg.getSenderId());
                    Thread.sleep(500);
                    if (m.getMsg().equals("test1")) {
                        if (gotMessage1) fail[0] = true;
                        assert (!gotMessage1);
                        gotMessage1 = true;
                    }
                    if (m.getMsg().equals("test2")) {
                        if (gotMessage2) fail[0] = true;
                        assert (!gotMessage2);

                        gotMessage2 = true;
                    }
                } catch (InterruptedException e) {
                }
                msg.unpauseProcessingOfMessagesNamed("test");
                return null;
            });
            m1.start();
            Thread.sleep(2000);
            log.info("receiver id: " + m1.getSenderId());


            log.info("Testing with exclusive messages...");


            gotMessage1 = gotMessage2 = false;
            assert (!fail[0]);
            Msg m = new Msg("test", "test1", "test", 3000000);
            m.setExclusive(true);
            sender.sendMessage(m);
            assert (!fail[0]);

            m = new Msg("test", "test2", "test", 3000000);
            m.setExclusive(true);
            sender.sendMessage(m);
            Thread.sleep(500);
            assert (!gotMessage1);
            assert (!gotMessage2);
            assert (!fail[0]);

            Thread.sleep(8500);
            assert (gotMessage1);
            assert (gotMessage2);
            assert (!fail[0]);
        } finally {
            sender.terminate();
            m1.terminate();

        }


    }
    @Test
    public void exclusivityPausedUnpausingTest() throws Exception {
        Messaging sender = new Messaging(morphium, 1000, false);
        sender.setSenderId("sender");
        morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
        Thread.sleep(100);
        sender.start();
        MorphiumConfig cfg = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
        cfg.setCredentialsEncryptionKey(morphium.getConfig().getCredentialsEncryptionKey());
        cfg.setCredentialsDecryptionKey(morphium.getConfig().getCredentialsDecryptionKey());
        cfg.setThreadPoolMessagingMaxSize(10);
        cfg.setThreadPoolMessagingCoreSize(5);
        cfg.setThreadPoolAsyncOpMaxSize(10);
        Morphium morphium2 = new Morphium(cfg);

        Messaging receiver = new Messaging(morphium2, (int) (50 + 100 * Math.random()), true, true, 15);
        receiver.setSenderId("r1");
        receiver.start();

        cfg = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
        cfg.setCredentialsEncryptionKey(morphium.getConfig().getCredentialsEncryptionKey());
        cfg.setCredentialsDecryptionKey(morphium.getConfig().getCredentialsDecryptionKey());
        cfg.setThreadPoolMessagingMaxSize(10);
        cfg.setThreadPoolMessagingCoreSize(5);
        cfg.setThreadPoolAsyncOpMaxSize(10);
        Morphium morphium3 = new Morphium(cfg);

        Messaging receiver2 = new Messaging(morphium3, (int) (50 + 100 * Math.random()), false, false, 15);
        receiver2.setSenderId("r2");
        receiver2.start();

        cfg = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
        cfg.setCredentialsEncryptionKey(morphium.getConfig().getCredentialsEncryptionKey());
        cfg.setCredentialsDecryptionKey(morphium.getConfig().getCredentialsDecryptionKey());
        cfg.setThreadPoolMessagingMaxSize(10);
        cfg.setThreadPoolMessagingCoreSize(5);
        cfg.setThreadPoolAsyncOpMaxSize(10);
        Morphium morphium4 = new Morphium(cfg);
        Messaging receiver3 = new Messaging(morphium4, (int) (50 + 100 * Math.random()), true, false, 15);
        receiver3.setSenderId("r3");
        receiver3.start();

        cfg = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
        cfg.setCredentialsEncryptionKey(morphium.getConfig().getCredentialsEncryptionKey());
        cfg.setCredentialsDecryptionKey(morphium.getConfig().getCredentialsDecryptionKey());
        cfg.setThreadPoolMessagingMaxSize(10);
        cfg.setThreadPoolMessagingCoreSize(5);
        cfg.setThreadPoolAsyncOpMaxSize(10);
        Morphium morphium5 = new Morphium(cfg);
        morphium5.getConfig().setThreadPoolMessagingMaxSize(10);
        morphium5.getConfig().setThreadPoolMessagingCoreSize(5);
        morphium5.getConfig().setThreadPoolAsyncOpMaxSize(10);
        Messaging receiver4 = new Messaging(morphium5, (int) (50 + 100 * Math.random()), false, true, 15);
        receiver4.setSenderId("r4");
        receiver4.start();
        Thread.sleep(2000);

        final AtomicInteger received = new AtomicInteger();
        final AtomicInteger dups = new AtomicInteger();
        final Map<String, Long> ids = new ConcurrentHashMap<>();
        final Map<String, String> recById = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> recieveCount = new ConcurrentHashMap<>();
        Thread.sleep(100);
        try {
            MessageListener messageListener = (msg, m) -> {
                msg.pauseProcessingOfMessagesNamed("m");
                try {
                    Thread.sleep((long) (300 * Math.random()));
                } catch (InterruptedException e) {
                }
                //log.info("R1: Incoming message "+m.getValue());
                received.incrementAndGet();
                recieveCount.putIfAbsent(msg.getSenderId(), new AtomicInteger());
                recieveCount.get(msg.getSenderId()).incrementAndGet();
                if (ids.containsKey(m.getMsgId().toString())) {
                    if (m.isExclusive()) {
                        log.error("Duplicate recieved message " + msg.getSenderId() + " " + (System.currentTimeMillis() - ids.get(m.getMsgId().toString())) + "ms ago");
                        if (recById.get(m.getMsgId().toString()).equals(msg.getSenderId())) {
                            log.error("--- duplicate was processed before by me!");
                        } else {
                            log.error("--- duplicate processed by someone else");
                        }
                        dups.incrementAndGet();
                    }
                }
                ids.put(m.getMsgId().toString(), System.currentTimeMillis());
                recById.put(m.getMsgId().toString(), msg.getSenderId());
                msg.unpauseProcessingOfMessagesNamed("m");
                return null;
            };
            receiver.addListenerForMessageNamed("m", messageListener);
            receiver2.addListenerForMessageNamed("m", messageListener);
            receiver3.addListenerForMessageNamed("m", messageListener);
            receiver4.addListenerForMessageNamed("m", messageListener);
            int exclusiveAmount = 50;
            int broadcastAmount = 100;
            Thread.sleep(2000);
            for (int i = 0; i < exclusiveAmount; i++) {
                int rec = received.get();
                long messageCount = receiver.getPendingMessagesCount();
                if (i % 100 == 0) log.info("Send " + i + " recieved: " + rec + " queue: " + messageCount);
                Msg m = new Msg("m", "m", "v" + i, 3300000, true);
                m.setExclusive(true);
                sender.sendMessage(m);
            }
            for (int i = 0; i < broadcastAmount; i++) {
                int rec = received.get();
                long messageCount = receiver.getPendingMessagesCount();
                if (i % 100 == 0) log.info("Send boadcast" + i + " recieved: " + rec + " queue: " + messageCount);
                Msg m = new Msg("m", "m", "v" + i, 3300000, false);
                sender.sendMessage(m);
            }

            while (received.get() != exclusiveAmount + broadcastAmount * 4) {
                int rec = received.get();
                long messageCount = sender.getPendingMessagesCount();

                log.info("Send excl: " + exclusiveAmount + "  brodadcast: " + broadcastAmount + " recieved: " + rec + " queue: " + messageCount + " currently processing: " + (exclusiveAmount + broadcastAmount * 4 - rec - messageCount));
                for (Messaging m : Arrays.asList(receiver, receiver2, receiver3, receiver4)) {
                    assert (m.getRunningTasks() <= 10) : m.getSenderId() + " runs too many tasks! " + m.getRunningTasks();
                    m.triggerCheck();
                }
                assert (dups.get() == 0) : "got duplicate message";

                Thread.sleep(1000);
            }
            int rec = received.get();
            long messageCount = sender.getPendingMessagesCount();
            log.info("Send " + exclusiveAmount + " recieved: " + rec + " queue: " + messageCount);
            assert (received.get() == exclusiveAmount + broadcastAmount * 4) : "should have received " + (exclusiveAmount + broadcastAmount * 4) + " but actually got " + received.get();

            for (String id : recieveCount.keySet()) {
                log.info("Reciever " + id + " message count: " + recieveCount.get(id).get());
            }
            log.info("R1 active: " + receiver.getRunningTasks());
            log.info("R2 active: " + receiver2.getRunningTasks());
            log.info("R3 active: " + receiver3.getRunningTasks());
            log.info("R4 active: " + receiver4.getRunningTasks());

            logStats(morphium);
            logStats(morphium2);
            logStats(morphium3);
            logStats(morphium4);
            logStats(morphium5);
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



    private long logmem(long startFree, long startTotal, long startMax) {
        System.gc();
        log.info("==== Memory consumption: =======================");
        Runtime runtime = Runtime.getRuntime();
        long free = runtime.freeMemory();
        long total = runtime.totalMemory();
        long max = runtime.maxMemory();
//
//        log.info("Free Memory  : "+(free/1024/1024)+"mb");
//        log.info("Total Memory : "+(total/1024/1024)+"mb");
//        log.info("Max Memory   : "+(max/1024/1024)+"mb");
//        log.info("diff Free Memory  : "+((free-startFree)/1024/1024)+"mb");
        long startUsed = (startTotal - startFree) / 1024 / 1024;
        long used = (total - free) / 1024 / 1024;
        log.info("used Memory        : " + ((total - free) / 1024 / 1024) + "mb ~ " + ((double) (total - free) / (double) total * 100.0) + "%");
        log.info("Start used Memory  : " + startUsed + "mb ~ " + ((double) (startUsed) / (double) (startTotal / 1024 / 1024) * 100.0) + "%");
        log.info("Diff used Mem      : " + (used - startUsed) + "mb");
//        log.info("start Total Memory : "+(startTotal/1024/1024)+"mb");
//        log.info("start Max Memory   : "+(startMax/1024/1024)+"mb");
        log.info("================================================");
        return used - startUsed;
    }


}
