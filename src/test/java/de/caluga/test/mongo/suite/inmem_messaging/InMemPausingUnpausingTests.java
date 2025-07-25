package de.caluga.test.mongo.suite.inmem_messaging;

import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.inmem.MorphiumInMemTestBase;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class InMemPausingUnpausingTests extends MorphiumInMemTestBase {
    private final List<Msg> list = new ArrayList<>();
    public volatile boolean gotMessage1 = false;
    public volatile boolean gotMessage2 = false;
    private Logger log = LoggerFactory.getLogger(InMemPausingUnpausingTests.class);



    @Test
    public void pauseUnpauseProcessingTest() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(1000);
        Messaging sender = new Messaging(morphium, 100,  false, 1);
        sender.start();
        Thread.sleep(2500);
        gotMessage1 = false;
        gotMessage2 = false;
        Messaging m1 = new Messaging(morphium, 100,  false, 1);
        m1.addMessageListener((msg, m) -> {
            gotMessage1 = true;
            return new Msg(m.getName(), "got message", "value", 5000);
        });

        try {
            m1.start();
            m1.pauseProcessingOfMessagesNamed("tst1");
            sender.sendMessage(new Msg("test", "a message", "the value"));
            Thread.sleep(2200);
            assert(gotMessage1);
            gotMessage1 = false;
            sender.sendMessage(new Msg("tst1", "a message", "the value"));
            Thread.sleep(1200);
            assert(!gotMessage1);
            Long l = m1.unpauseProcessingOfMessagesNamed("tst1");
            log.info("Processing was paused for ms " + l);
            //m1.findAndProcessPendingMessages("tst1");
            Thread.sleep(1200);
            assert(gotMessage1);
            gotMessage1 = false;
            Thread.sleep(200);
            assert(!gotMessage1);
            gotMessage1 = false;
            sender.sendMessage(new Msg("tst1", "a message", "the value"));
            Thread.sleep(1200);
            assert(gotMessage1);
        } finally {
            m1.terminate();
            sender.terminate();
        }
    }

    @Test
    public void priorityPausedMessagingTest() throws Exception {
        Messaging sender = new Messaging(morphium, 100, false);
        sender.start();
        final AtomicInteger count = new AtomicInteger();
        final AtomicLong lastTS = new AtomicLong(0);
        list.clear();
        Messaging receiver = new Messaging(morphium, 10,  true, 1);
        receiver.start();
        Thread.sleep(100);
        receiver.addListenerForMessageNamed("pause", (msg, m) -> {
            msg.pauseProcessingOfMessagesNamed(m.getName());
            String lst = "";

            if (lastTS.get() != 0) {
                lst = ("Last msg " + (System.currentTimeMillis() - lastTS.get()) + "ms ago");
            }
            lastTS.set(System.currentTimeMillis());
            log.info("Incoming paused message: prio " + m.getPriority() + "  timestamp: " + m.getTimestamp() + " " + lst);

            try {
                Thread.sleep(250);
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
            m.setPriority((int)(Math.random() * 100.0));
            m.setExclusive(true);
            sender.sendMessage(m);

            //Throtteling for first message to wait for pausing
            if (i == 0) Thread.sleep(25);

            if (i % 2 == 0) {
                sender.sendMessage(new Msg("now", "now", "now"));
            }
        }

        long start = System.currentTimeMillis();

        while (count.get() < 10) {
            Thread.sleep(100);
        }

        log.info("Got half at " + (System.currentTimeMillis() - start));

        while (list.size() < 20) {
            Thread.sleep(100);
        }

        assert(list.size() == 20) : "Size wrong " + list.size();
        list.remove(0); //prio of first  is random
        int lastPrio = -1;

        for (Msg m : list) {
            log.info("Msg: " + m.getPriority());
            assert(m.getPriority() >= lastPrio);
            lastPrio = m.getPriority();
        }
    }


    @Test
    public void unpausingTest() throws Exception {
        list.clear();
        final AtomicInteger cnt = new AtomicInteger(0);
        Messaging sender = new Messaging(morphium, 100, true, 1);
        sender.start();
        Messaging receiver = new Messaging(morphium, 10,  true, 10);
        receiver.start();
        Thread.sleep(1000);
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
        Thread.sleep(1500);
        assert(list.size() == 1);
        sender.sendMessage(new Msg("pause", "pause", "pause"));
        sender.sendMessage(new Msg("now", "now", "now"));
        Thread.sleep(1500);
        assert(list.size() == 2);
        sender.sendMessage(new Msg("pause", "pause", "pause"));
        sender.sendMessage(new Msg("pause", "pause", "pause"));
        sender.sendMessage(new Msg("pause", "pause", "pause"));
        assert(cnt.get() == 0) : "Count wrong " + cnt.get();
        Thread.sleep(2000);
        assert(cnt.get() == 1);
        //1st message processed
        Thread.sleep(2000);
        //Message after unpausing:
        assert(cnt.get() == 2) : "Count wrong: " + cnt.get();
        sender.sendMessage(new Msg("now", "now", "now"));
        Thread.sleep(2000);
        assert(list.size() == 3);
        Thread.sleep(2000);
        //Message after unpausing:
        assert(cnt.get() == 4) : "Count wrong: " + cnt.get();
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
        Messaging sender = new Messaging(morphium, 100, true, 1);
        sender.start();
        Thread.sleep(2500);
        log.info("Sender ID: " + sender.getSenderId());
        gotMessage1 = false;
        gotMessage2 = false;
        Messaging m1 = new Messaging(morphium, 10, multithreadded, 1);
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
        log.info("receiver id: " + m1.getSenderId());
        log.info("Testing with non-exclusive messages");
        Msg m = new Msg("test", "test1", "test", 3000000);
        m.setExclusive(false);
        sender.sendMessage(m);
        m = new Msg("test", "test2", "test", 3000000);
        m.setExclusive(false);
        sender.sendMessage(m);
        Thread.sleep(200);
        assert(!gotMessage1);
        assert(!gotMessage2);
        Thread.sleep(5200);
        assert(gotMessage1);
        assert(gotMessage2);
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
        assert(!gotMessage1);
        assert(!gotMessage2);
        Thread.sleep(10000);
        assert(gotMessage1);
        assert(gotMessage2);
        sender.terminate();
        m1.terminate();
    }

    @Test
    public void exclusiveMessageTest() throws Exception {
        Messaging sender = new Messaging(morphium, 100,  true, 10);
        Messaging receiver = new Messaging(morphium, 100,  true, 10);
        sender.start();
        receiver.start();
        Thread.sleep(1000);
        receiver.addListenerForMessageNamed("exclusive_test", (msg, m) -> {
            log.info("Incoming message!");
            return null;
        }
        );
        Msg ex = new Msg("exclusive_test", "a message", "A value");
        ex.setExclusive(true);
        sender.sendMessage(ex);
        log.info("Sent!");
        Thread.sleep(1000);
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
            sender = new Messaging(morphium, 100, true, 1);
            sender.setSenderId("Sender");
            // sender.start();
            log.info("Sender ID: " + sender.getSenderId());
            gotMessage1 = false;
            gotMessage2 = false;
            boolean[] fail = {false};
            m1 = new Messaging(morphium, 100,  multithreadded, 1);
            m1.setSenderId("m1");
            m1.addListenerForMessageNamed("test", (msg, m) -> {
                msg.pauseProcessingOfMessagesNamed("test");

                try {
                    assert(m.isExclusive());
                    //                assert (m.getReceivedBy().contains(msg.getSenderId()));
                    log.info("Incoming message " + m.getMsgId() + "/" + m.getMsg() + " from " + m.getSender() + " my id: " + msg.getSenderId());
                    Thread.sleep(500);

                    if (m.getMsg().equals("test1")) {
                        if (gotMessage1) fail[0] = true;

                        assert(!gotMessage1);
                        gotMessage1 = true;
                    }

                    if (m.getMsg().equals("test2")) {
                        if (gotMessage2) fail[0] = true;

                        assert(!gotMessage2);
                        gotMessage2 = true;
                    }
                } catch (InterruptedException e) {
                }
                msg.unpauseProcessingOfMessagesNamed("test");
                return null;
            });
            m1.start();
            Thread.sleep(1000);
            log.info("receiver id: " + m1.getSenderId());
            log.info("Testing with exclusive messages...");
            gotMessage1 = gotMessage2 = false;
            assert(!fail[0]);
            Msg m = new Msg("test", "test1", "test", 3000000);
            m.setExclusive(true);
            sender.sendMessage(m);
            assert(!fail[0]);
            m = new Msg("test", "test2", "test", 3000000);
            m.setExclusive(true);
            sender.sendMessage(m);
            Thread.sleep(500);
            assert(!gotMessage1);
            assert(!gotMessage2);
            assert(!fail[0]);
            Thread.sleep(6500);
            assert(gotMessage1);
            assert(gotMessage2);
            assert(!fail[0]);
        } finally {
            sender.terminate();
            m1.terminate();
        }
    }


    //    @Test
    //    public void massiveAnswerBigQueueTests() throws Exception {
    //        morphium.dropCollection(Msg.class);
    //        Messaging sender = new Messaging(morphium, 100, false, true, 5);
    //        Messaging receiver1 = new Messaging(morphium, 100, false, true, 5);
    //        Messaging receiver2 = new Messaging(morphium, 100, false, true, 5);
    //        Messaging receiver3 = new Messaging(morphium, 100, false, true, 5);
    //        Messaging receiver4 = new Messaging(morphium, 100, false, true, 5);
    //        sender.start();
    //        receiver1.start();
    //        receiver2.start();
    //        receiver3.start();
    //        receiver4.start();
    //        Thread.sleep(1000);
    //        final AtomicInteger sent = new AtomicInteger(0);
    //        final AtomicInteger answered = new AtomicInteger(0);
    //        MessageListener messageListener = (msg, m) -> {
    //            msg.pauseProcessingOfMessagesNamed(m.getName());
    ////            log.info("Incoming request! " + m.getMsgId());
    //            Thread.sleep(200 - (int) (100.0 * Math.random()));
    //            Msg answer = new Msg("answer", "answer", "answer", 240 * 1000);
    //            answer.setMapValue(m.getMapValue());
    //            msg.unpauseProcessingOfMessagesNamed(m.getName());
    //            return answer;
    //        };
    //        receiver1.addListenerForMessageNamed("answer_me", messageListener);
    //        receiver2.addListenerForMessageNamed("answer_me", messageListener);
    //        receiver3.addListenerForMessageNamed("answer_me", messageListener);
    //        receiver4.addListenerForMessageNamed("answer_me", messageListener);
    //
    //        sender.addListenerForMessageNamed("answer", (msg, m) -> {
    ////            log.info("Anwer came in: " + m.getValue());
    //            answered.incrementAndGet();
    //            return null;
    //        });
    //        Runtime runtime = Runtime.getRuntime();
    //        long startFree = runtime.freeMemory();
    //        long startTotal = runtime.totalMemory();
    //        long startMax = runtime.maxMemory();
    //        int noMsg = 300;
    //        StringBuilder bld = new StringBuilder();
    //        for (int i = 0; i < noMsg; i++) {
    //            bld.setLength(0);
    //            sent.incrementAndGet();
    //            Msg m = new Msg("answer_me", "answer_me_" + i, "answer_me_" + i, 180 * 1000);
    //            for (int b = 0; b < 20240; b++) {
    //                bld.append("- ultra long text -");
    //            }
    //
    //            m.setMapValue(UtilsMap.of("bigValue", bld.toString()));
    //            m.setExclusive(true);
    //            sender.sendMessage(m);
    //        }
    //
    //        long start = System.currentTimeMillis();
    //        while (sent.get() > answered.get()) {
    //            log.info("Got: " + answered.get() + " of " + sent.get());
    //            log.info("=====> Time passed: " + ((System.currentTimeMillis() - start) / 1000 / 60) + " mins");
    //            logmem(startFree, startTotal, startMax);
    //            Thread.sleep(5000);
    //        }
    //        log.info("Got all answers... after " + (System.currentTimeMillis() - start) + "ms");
    //
    //        while (System.currentTimeMillis() - start < 4 * 60 * 1000) {
    //            log.info("=====> Time passed: " + ((System.currentTimeMillis() - start) / 1000 / 60) + " mins");
    //            logmem(startFree, startTotal, startMax);
    //
    //            Thread.sleep(5000);
    //
    //        }
    //        long diff = logmem(startFree, startTotal, startMax);
    //        assert (diff < 10);
    //    }

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
        log.info("used Memory        : " + ((total - free) / 1024 / 1024) + "mb ~ " + ((double)(total - free) / (double) total * 100.0) + "%");
        log.info("Start used Memory  : " + startUsed + "mb ~ " + ((double)(startUsed) / (double)(startTotal / 1024 / 1024) * 100.0) + "%");
        log.info("Diff used Mem      : " + (used - startUsed) + "mb");
        //        log.info("start Total Memory : "+(startTotal/1024/1024)+"mb");
        //        log.info("start Max Memory   : "+(startMax/1024/1024)+"mb");
        log.info("================================================");
        return used - startUsed;
    }


}
