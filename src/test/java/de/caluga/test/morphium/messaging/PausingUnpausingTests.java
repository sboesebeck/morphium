package de.caluga.test.morphium.messaging;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import de.caluga.morphium.messaging.MorphiumMessaging;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.OutputHelper;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;
import de.caluga.test.mongo.suite.base.TestUtils.Condition;

public class PausingUnpausingTests extends MorphiumTestBase {
    public AtomicBoolean gotMessage = new AtomicBoolean(false);

    public AtomicBoolean gotMessage1 = new AtomicBoolean(false);
    public AtomicBoolean gotMessage2 = new AtomicBoolean(false);
    public AtomicBoolean gotMessage3 = new AtomicBoolean(false);
    public AtomicBoolean gotMessage4 = new AtomicBoolean(false);

    public AtomicBoolean error = new AtomicBoolean(false);

    public MorphiumId lastMsgId;

    public AtomicInteger procCounter = new AtomicInteger(0);

    private final List<Msg> list = new ArrayList<>();



    @Test
    public void pauseUnpauseProcessingTest() throws Exception {

        for (String msgImpl : MorphiumTestBase.messagingsToTest) {

            OutputHelper.figletOutput(log, msgImpl);
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImpl);
            cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

            Morphium morph = new Morphium(cfg);
            try(morph) {
                MorphiumMessaging sender = morph.createMessaging();
                sender.start();
                Thread.sleep(00);
                gotMessage1.set(false);
                gotMessage2.set(false);
                gotMessage3.set(false);
                gotMessage4.set(false);

                MorphiumMessaging m1 = morph.createMessaging();
                log.info("Messaging is {}", m1.getClass().getName());

                log.info("Collecion name for test: {}", m1.getCollectionName("test"));
                morph.dropCollection(Msg.class, m1.getCollectionName("test"), null);
                morph.dropCollection(Msg.class, m1.getCollectionName("tst1"), null);
                Thread.sleep(500);
                m1.addListenerForTopic("test", (msg, m) -> {
                    gotMessage1.set(true);
                    log.info("Test: Incoming {} message: {} - {}", m.getTopic(), m.getMsgId().toString());
                    return new Msg(m.getTopic(), "got message", "value", 5000);
                });
                m1.addListenerForTopic("tst1", (msg, m) -> {
                    gotMessage2.set(true);
                    log.info("Tst1: Incoming {} message:  {}", m.getTopic(), m.getMsgId().toString());
                    return new Msg(m.getTopic(), "got message", "value", 5000);
                });

                try {
                    m1.start();
                    Thread.sleep(500);
                    log.info("Starting test");
                    m1.pauseTopicProcessing("tst1");

                    sender.sendMessage(new Msg("test", "a message", "the value"));
                    log.info("Message sent");
                    var dur = TestUtils.waitForBooleanToBecomeTrue(10000, "Did not get message", gotMessage1, ()-> {log.info("Still waiting..");});
                    log.info("Got it after {}ms", dur);
                    assertTrue(gotMessage1.get());
                    assertFalse(gotMessage2.get());

                    gotMessage1.set(false);
                    gotMessage2.set(false);

                    sender.sendMessage(new Msg("tst1", "a message", "the value"));
                    Thread.sleep(1500);
                    assertFalse(gotMessage2.get());
                    assertFalse(gotMessage1.get());

                    Long l = m1.unpauseTopicProcessing("tst1");
                    log.info("Processing was paused for ms " + l);
                    TestUtils.waitForBooleanToBecomeTrue(10000, "Did not get message", gotMessage2, ()-> {log.info("Still waiting..");});

                    assertTrue (gotMessage2.get());
                    assertFalse(gotMessage1.get());
                    gotMessage2.set(false);
                    Thread.sleep(500);
                    assertFalse (gotMessage1.get());
                    assertFalse (gotMessage2.get());

                    gotMessage1.set(false);
                    sender.sendMessage(new Msg("tst1", "a message", "the value"));
                    TestUtils.waitForBooleanToBecomeTrue(10000, "Did not get message", gotMessage2, ()-> {log.info("Still waiting..");});
                    assertTrue (gotMessage2.get());
                } finally {
                    m1.terminate();
                    sender.terminate();
                }

            }
        }
    }

    @Test
    @Disabled
    public void priorityPausedMessagingTest() throws Exception {
        for (String msgImpl : MorphiumTestBase.messagingsToTest) {

            OutputHelper.figletOutput(log, msgImpl);
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImpl);
            cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

            Morphium morph = new Morphium(cfg);
            try(morph) {
                MorphiumMessaging sender = morph.createMessaging();
                sender.start();
                Thread.sleep(2500);
                final AtomicInteger count = new AtomicInteger();
                final AtomicLong lastTS = new AtomicLong(0);

                list.clear();
                MorphiumMessaging receiver = morph.createMessaging();

                receiver.addListenerForTopic("pause", (msg, m) -> {
                    msg.pauseTopicProcessing(m.getTopic());
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
                    msg.unpauseTopicProcessing(m.getTopic());
                    return null;
                });

                receiver.addListenerForTopic("now", (msg, m) -> {
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

                    assert (System.currentTimeMillis() - s < 5 * morphium.getConfig().getMaxWaitTime());
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
        }
    }


    @Test
    public void simpleSerializingTest() throws Exception {

        for (String msgImpl : MorphiumTestBase.messagingsToTest) {

            OutputHelper.figletOutput(log, msgImpl);
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImpl);
            cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

            Morphium morph = new Morphium(cfg);
            try(morph) {
                final AtomicInteger cnt = new AtomicInteger(0);
                MorphiumMessaging sender = morph.createMessaging();
                morph.dropCollection(Msg.class, sender.getCollectionName("pause"), null);
                sender.start();


                MorphiumMessaging receiver = morph.createMessaging();
                receiver.start();
                receiver.addListenerForTopic("pause", (msg, m) -> {
                    msg.pauseTopicProcessing("pause");
                    log.info("Processing pause message {}", m.getMsgId());
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                    }
                    cnt.incrementAndGet();
                    msg.unpauseTopicProcessing("pause");

                    return null;
                });
                receiver.pauseTopicProcessing("pause");
                for (int i = 0; i < 10; i++) {
                    sender.sendMessage(new Msg("pause", "pausing", "value"));
                }

                receiver.unpauseTopicProcessing("pause");
                TestUtils.waitForIntegerValue(25000, "Did not get all messages?", cnt, 10, ()-> {log.info("Still waiting: {} processed", cnt.get());});

            }
        }

    }

    @Test
    public void unpausingTest() throws Exception {
        for (String msgImpl : MorphiumTestBase.messagingsToTest) {

            OutputHelper.figletOutput(log, msgImpl);
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImpl);
            cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

            Morphium morph = new Morphium(cfg);
            try(morph) {
                list.clear();
                final AtomicInteger cnt = new AtomicInteger(0);
                MorphiumMessaging sender = morph.createMessaging();
                morph.dropCollection(Msg.class, sender.getCollectionName("pause"), null);
                morph.dropCollection(Msg.class, sender.getCollectionName("now"), null);
                sender.start();

                MorphiumMessaging receiver = morph.createMessaging();
                receiver.start();

                Thread.sleep(2000);
                receiver.addListenerForTopic("pause", (msg, m) -> {
                    msg.pauseTopicProcessing("pause");
                    log.info("Processing pause message {}", m.getMsgId());
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                    }
                    cnt.incrementAndGet();
                    msg.unpauseTopicProcessing("pause");

                    return null;
                });

                receiver.addListenerForTopic("now", (msg, m) -> {
                    // log.info("PAusing msg now");
                    msg.pauseTopicProcessing("now");
                    list.add(m);
                    // log.info("Incoming msg..." + m.getMsgId());
                    msg.unpauseTopicProcessing("now");
                    // log.info("Unpaused.");
                    return null;
                });
                sender.sendMessage(new Msg("now", "now", "now"));
                TestUtils.waitForConditionToBecomeTrue(5000, "did not get valaue", ()-> {return list.size() == 1;}, ()-> {log.info("Waiting...");});
                assert (list.size() == 1);

                sender.sendMessage(new Msg("pause", "pause", "pause"));
                sender.sendMessage(new Msg("now", "now", "now"));
                TestUtils.waitForConditionToBecomeTrue(5000, "did not get valaue", ()-> {return list.size() == 2;}, ()-> {log.info("Waiting...");});
                assert (list.size() == 2);

                log.info("Sending several messages");
                sender.sendMessage(new Msg("pause", "pause", "pause"));
                Thread.sleep(100);
                sender.sendMessage(new Msg("pause", "pause", "pause"));
                Thread.sleep(100);
                sender.sendMessage(new Msg("pause", "pause", "pause"));
                assert (cnt.get() == 0) : "Count wrong " + cnt.get();
                TestUtils.waitForIntegerValue(5000, "did not get value", cnt, 1, ()-> {log.info("Waiting for the first.");});
                TestUtils.waitForIntegerValue(5000, "did not get value", cnt, 2, ()-> {log.info("Waiting...{}", cnt.get());});
                sender.sendMessage(new Msg("now", "now", "now"));
                TestUtils.waitForConditionToBecomeTrue(6000, "did not get value", ()-> {return list.size() == 3;}, ()-> {log.info("Waiting...{}", list.size());});
                TestUtils.waitForIntegerValue(8000, "did not get value", cnt, 4, ()-> {log.info("Waiting...{}", cnt.get());});
                sender.terminate();
                receiver.terminate();
                //Message after unpausing:
                OutputHelper.figletOutput(log, "Done");

            }
        }
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
        for (String msgImpl : MorphiumTestBase.messagingsToTest) {

            OutputHelper.figletOutput(log, msgImpl);
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImpl);
            cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

            Morphium morph = new Morphium(cfg);
            try(morph) {
                MorphiumMessaging sender = morph.createMessaging();
                sender.start();
                Thread.sleep(2500);
                log.info("Sender ID: " + sender.getSenderId());

                gotMessage1.set(false);
                gotMessage2.set(false);

                MorphiumMessaging m1 = morph.createMessaging();
                m1.addListenerForTopic("test", (msg, m) -> {
                    msg.pauseTopicProcessing("test");
                    try {
                        log.info("Incoming message " + m.getMsgId() + "/" + m.getMsg() + " from " + m.getSender() + " my id: " + msg.getSenderId());
                        Thread.sleep(1000);
                        if (m.getMsg().equals("test1")) {
                            gotMessage1.set(true);
                        }
                        if (m.getMsg().equals("test2")) {
                            gotMessage2.set(true);
                        }
                    } catch (InterruptedException e) {
                    }
                    msg.unpauseTopicProcessing("test");
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
                    assertFalse (gotMessage1.get());
                    assertFalse (gotMessage2.get());

                    TestUtils.waitForConditionToBecomeTrue(5000, "did not get messages", ()-> {return gotMessage1.get() && gotMessage2.get();}, ()-> {});
                    assertTrue (gotMessage1.get());
                    assertTrue (gotMessage2.get());

                    log.info("... done!");
                    log.info("Testing with exclusive messages...");


                    gotMessage1.set(false);
                    gotMessage2.set(false);

                    m = new Msg("test", "test1", "test", 3000000);
                    m.setExclusive(true);
                    sender.sendMessage(m);

                    m = new Msg("test", "test2", "test", 3000000);
                    m.setExclusive(true);
                    sender.sendMessage(m);
                    Thread.sleep(200);
                    assertFalse(gotMessage1.get());
                    assertFalse(gotMessage2.get());

                    TestUtils.waitForConditionToBecomeTrue(5000, "did not get messages", ()-> {return gotMessage1.get() && gotMessage2.get();}, ()-> {});
                    assertTrue (gotMessage1.get());
                    assertTrue (gotMessage2.get());
                } finally {
                    sender.terminate();
                    m1.terminate();
                }

            }
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
        for (String msgImpl : MorphiumTestBase.messagingsToTest) {

            OutputHelper.figletOutput(log, msgImpl);
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImpl);
            cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

            cfg.messagingSettings().setMessagingMultithreadded(multithreadded);
            Morphium morph = new Morphium(cfg);
            try(morph) {
                MorphiumMessaging sender = null;
                MorphiumMessaging m1 = null;
                try {
                    morphium.dropCollection(Msg.class);
                    Thread.sleep(1000);
                    sender = morph.createMessaging();
                    sender.setSenderId("Sender");
                    // sender.start();
                    log.info("Sender ID: " + sender.getSenderId());

                    gotMessage1.set(false);
                    gotMessage2.set(false);
                    AtomicBoolean fail = new AtomicBoolean(false);
                    m1 = morph.createMessaging();
                    m1.setSenderId("m1");
                    m1.addListenerForTopic("test", (msg, m) -> {
                        msg.pauseTopicProcessing("test");

                        try {
                            assert (m.isExclusive());
                            //                assert (m.getReceivedBy().contains(msg.getSenderId()));
                            log.info("Incoming message " + m.getMsgId() + "/" + m.getMsg() + " from " + m.getSender() + " my id: " + msg.getSenderId());
                            Thread.sleep(500);
                            if (m.getMsg().equals("test1")) {
                                if (gotMessage1.get()) fail.set(true);
                                assertFalse(gotMessage1.get());
                                gotMessage1.set(true);
                            }
                            if (m.getMsg().equals("test2")) {
                                if (gotMessage2.get()) fail.set(true);
                                assertFalse(gotMessage2.get());

                                gotMessage2.set(true);
                            }
                        } catch (InterruptedException e) {
                        }
                        msg.unpauseTopicProcessing("test");
                        return null;
                    });
                    m1.start();
                    Thread.sleep(2000);
                    log.info("receiver id: " + m1.getSenderId());


                    log.info("Testing with exclusive messages...");


                    gotMessage1.set(false);
                    gotMessage2.set(false);
                    assertFalse (fail.get());
                    Msg m = new Msg("test", "test1", "test", 3000000);
                    m.setExclusive(true);
                    sender.sendMessage(m);
                    assertFalse (fail.get());

                    m = new Msg("test", "test2", "test", 3000000);
                    m.setExclusive(true);
                    sender.sendMessage(m);
                    Thread.sleep(500);
                    assertFalse (gotMessage1.get());
                    assertFalse (gotMessage2.get());
                    assertFalse (fail.get());

                    Thread.sleep(8500);
                    assertTrue (gotMessage1.get());
                    assertTrue (gotMessage2.get());
                    assertFalse (fail.get());
                } finally {
                    sender.terminate();
                    m1.terminate();

                }
            }
        }

    }

}
