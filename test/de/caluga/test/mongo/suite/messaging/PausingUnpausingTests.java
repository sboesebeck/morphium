package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.MorphiumTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PausingUnpausingTests extends MorphiumTestBase {
    public boolean gotMessage = false;

    public boolean gotMessage1 = false;
    public boolean gotMessage2 = false;
    public boolean gotMessage3 = false;
    public boolean gotMessage4 = false;

    public boolean error = false;

    public MorphiumId lastMsgId;

    public AtomicInteger procCounter = new AtomicInteger(0);

    private List<Msg> list = new ArrayList<>();

    private AtomicInteger queueCount = new AtomicInteger(1000);


    @Test
    public void pauseUnpauseProcessingTest() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(1000);
        Messaging sender = new Messaging(morphium, 100, false);
        sender.start();
        Thread.sleep(2500);
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        Messaging m1 = new Messaging(morphium, 100, false);
        m1.addMessageListener((msg, m) -> {
            gotMessage1 = true;
            return new Msg(m.getName(), "got message", "value", 5000);
        });

        m1.start();

        m1.pauseProcessingOfMessagesNamed("tst1");

        sender.storeMessage(new Msg("test", "a message", "the value"));
        Thread.sleep(1200);
        assert (gotMessage1);

        gotMessage1 = false;

        sender.storeMessage(new Msg("tst1", "a message", "the value"));
        Thread.sleep(1200);
        assert (!gotMessage1);

        long l = m1.unpauseProcessingOfMessagesNamed("tst1");
        log.info("Processing was paused for ms " + l);
        //m1.findAndProcessPendingMessages("tst1");
        Thread.sleep(200);

        assert (gotMessage1);
        gotMessage1 = false;
        Thread.sleep(200);
        assert (!gotMessage1);

        gotMessage1 = false;
        sender.storeMessage(new Msg("tst1", "a message", "the value"));
        Thread.sleep(1200);
        assert (gotMessage1);


        m1.terminate();
        sender.terminate();

    }

    @Test
    public void priorityPausedMessagingTest() throws Exception {
        Messaging sender = new Messaging(morphium, 100, false);
        sender.start();
        final AtomicInteger count = new AtomicInteger();

        list.clear();
        Messaging receiver = new Messaging(morphium, 10, false, true, 100);
        receiver.start();
        Thread.sleep(100);

        receiver.addListenerForMessageNamed("pause", (msg, m) -> {
            msg.pauseProcessingOfMessagesNamed(m.getName());
            log.info("Incoming paused message: prio " + m.getPriority() + "  timestamp: " + m.getTimestamp());
            Thread.sleep(250);
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
            sender.storeMessage(m);
            //Throtteling for first message to wait for pausing
            if (i == 0) Thread.sleep(25);
            if (i % 2 == 0) {
                sender.storeMessage(new Msg("now", "now", "now"));
            }
        }
        Thread.sleep(200);
        assert (count.get() == 10) : "Count wrong " + count.get();
        assert (list.size() < 5);
        Thread.sleep(5200);
        assert (list.size() == 20);

        list.remove(0); //prio of first two is random

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

        Thread.sleep(1000);
        receiver.addListenerForMessageNamed("pause", (msg, m) -> {
            msg.pauseProcessingOfMessagesNamed("pause");
            log.info("Processing pause  message");
            Thread.sleep(2000);
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

        sender.storeMessage(new Msg("now", "now", "now"));
        Thread.sleep(500);
        assert (list.size() == 1);

        sender.storeMessage(new Msg("pause", "pause", "pause"));
        sender.storeMessage(new Msg("now", "now", "now"));
        Thread.sleep(500);
        assert (list.size() == 2);

        sender.storeMessage(new Msg("pause", "pause", "pause"));
        sender.storeMessage(new Msg("pause", "pause", "pause"));
        sender.storeMessage(new Msg("pause", "pause", "pause"));
        assert (cnt.get() == 0) : "Count wrong " + cnt.get();
        Thread.sleep(2000);
        assert (cnt.get() == 1);
        //1st message processed
        Thread.sleep(2000);
        //Message after unpausing:
        assert (cnt.get() == 2) : "Count wrong: " + cnt.get();
        sender.storeMessage(new Msg("now", "now", "now"));
        Thread.sleep(100);
        assert (list.size() == 3);
        Thread.sleep(2000);
        //Message after unpausing:
        assert (cnt.get() == 3) : "Count wrong: " + cnt.get();
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
        log.info("receiver id: " + m1.getSenderId());

        log.info("Testing with non-exclusive messages");
        Msg m = new Msg("test", "test1", "test", 3000000);
        m.setExclusive(false);
        sender.storeMessage(m);

        m = new Msg("test", "test2", "test", 3000000);
        m.setExclusive(false);
        sender.storeMessage(m);

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
        sender.storeMessage(m);

        m = new Msg("test", "test2", "test", 3000000);
        m.setExclusive(true);
        sender.storeMessage(m);
        Thread.sleep(200);
        assert (!gotMessage1);
        assert (!gotMessage2);

        Thread.sleep(5000);
        assert (gotMessage1);
        assert (gotMessage2);

        sender.terminate();
        m1.terminate();

    }

    @Test
    public void exclusiveMessageTest() throws Exception {
        Messaging sender = new Messaging(morphium, 100, true, true, 10);
        Messaging receiver = new Messaging(morphium, 100, true, true, 10);
        sender.start();
        receiver.start();
        Thread.sleep(1000);
        receiver.addListenerForMessageNamed("exclusive_test", (msg, m) -> {
                    log.info("Incoming message!");
                    return null;
                }
        );
        Msg ex=new Msg("exclusive_test","a message","A value");
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
            Thread.sleep(1000);
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

            Thread.sleep(6500);
            assert (gotMessage1);
            assert (gotMessage2);
            assert (!fail[0]);
        } finally {
            sender.terminate();
            m1.terminate();

        }


    }


}
