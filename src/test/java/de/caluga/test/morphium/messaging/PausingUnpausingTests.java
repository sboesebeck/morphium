package de.caluga.test.morphium.messaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import de.caluga.morphium.messaging.MorphiumMessaging;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.OutputHelper;
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
                Thread.sleep(2500);
                gotMessage1 = false;
                gotMessage2 = false;
                gotMessage3 = false;
                gotMessage4 = false;

                MorphiumMessaging m1 = morph.createMessaging();
                m1.addListenerForMessageNamed("test", (msg, m) -> {
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
                sender.start();

                MorphiumMessaging receiver = morph.createMessaging();
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

                gotMessage1 = false;
                gotMessage2 = false;

                MorphiumMessaging m1 = morph.createMessaging();
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

                    gotMessage1 = false;
                    gotMessage2 = false;
                    boolean[] fail = {false};
                    m1 = morph.createMessaging();
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
        }

    }

}
