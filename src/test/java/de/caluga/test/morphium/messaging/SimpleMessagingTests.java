package de.caluga.test.morphium.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggerFactoryBinder;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.messaging.AdvancedSplitCollectionMessaging;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.OutputHelper;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import net.sf.ehcache.util.concurrent.ConcurrentHashMap;

public class SimpleMessagingTests extends MorphiumTestBase {

    private Logger log = LoggerFactory.getLogger(SimpleMessagingTests.class);;

    @Test
    public void simpleBroadcastTest() throws Exception {

        for (String msgImpl : MorphiumTestBase.messagingsToTest) {

            OutputHelper.figletOutput(log, msgImpl);
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImpl);
            cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

            Morphium morph = new Morphium(cfg);
            try(morph) {
                MorphiumMessaging messaging = morph.createMessaging();
                messaging.start();

                AtomicBoolean rec1Received = new AtomicBoolean(false);
                MorphiumMessaging rec1 = morph.createMessaging();
                rec1.start();
                rec1.addListenerForMessageNamed("test", (msg, m)-> {
                    log.info("Rec1 received");
                    rec1Received.set(true);
                    return null;
                });

                AtomicBoolean rec2Received = new AtomicBoolean(false);
                MorphiumMessaging rec2 = morph.createMessaging();

                rec2.init(morphium);
                rec2.start();
                rec2.addListenerForMessageNamed("test", (msg, m)-> {
                    log.info("Rec2 received");
                    rec2Received.set(true);
                    return null;
                });


                Thread.sleep(1000);
                messaging.sendMessage(new Msg("test", "test-msg", "test-value", 30000, false));

                while (!rec2Received.get() || !rec1Received.get()) {
                    Thread.sleep(100);
                }
                log.info("got all messages");
                rec1.terminate();
                rec2.terminate();
                messaging.terminate();
            }

        }
    }

    @Test
    public void simpleExclusiveTest() throws Exception {
        for (String msgImpl : MorphiumTestBase.messagingsToTest) {

            OutputHelper.figletOutput(log, msgImpl);
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImpl);
            cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

            Morphium morph = new Morphium(cfg);
            try(morph) {
                MorphiumMessaging messaging = morph.createMessaging();
                messaging.init(morphium);
                messaging.start();

                AtomicBoolean rec1Received = new AtomicBoolean(false);
                MorphiumMessaging rec1 = morph.createMessaging();

                rec1.init(morphium);
                rec1.start();
                rec1.addListenerForMessageNamed("test", (msg, m)-> {
                    log.info("Rec1 received");
                    rec1Received.set(true);
                    return null;
                });

                AtomicBoolean rec2Received = new AtomicBoolean(false);
                MorphiumMessaging rec2 = morph.createMessaging();

                rec2.init(morphium);
                rec2.start();
                rec2.addListenerForMessageNamed("test", (msg, m)-> {
                    log.info("Rec2 received");
                    rec2Received.set(true);
                    return null;
                });


                Thread.sleep(1000);
                messaging.sendMessage(new Msg("test", "test-msg", "test-value", 30000, true));

                while (!rec2Received.get() && !rec1Received.get()) {
                    Thread.sleep(100);
                }
                assertFalse(rec2Received.get() && rec1Received.get());
                log.info("got all messages");
                rec1.terminate();
                rec2.terminate();
                messaging.terminate();
            }
        }
    }


    @Test
    public void massiveMessagingsExclusiveTest() throws Exception {

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
                sender.init(morphium);
                sender.setSenderId("sender");
                sender.start();

                int messagings = 5;
                int queues = 3;
                int msgToSend = 1000;


                List<MorphiumMessaging> msgs = new Vector<>();
                final Map<MorphiumId, AtomicInteger> recs = new ConcurrentHashMap<>();
                for (int i = 0; i < messagings; i++) {
                    MorphiumMessaging msg = morph.createMessaging();
                    msg.init(morphium);
                    msg.setSenderId("rec" + i);
                    log.info("Starting rec{}", i);
                    msg.start();
                    for (int j = 0; j < queues; j++  ) {
                        log.info("Adding listener #{} to rec{}", j, i);
                        msg.addListenerForMessageNamed("test_" + j, (ms, m)-> {
                            assertNotEquals(msg.getSenderId(), m.getSender());
                            assertNull(recs.get(m.getMsgId()));

                            recs.put(m.getMsgId(), new AtomicInteger(1));
                            return null;
                        });
                    }
                }


                for (int i = 0; i < msgToSend; i++) {
                    log.info("Sending msg #{}", i);
                    int rnd = (int)(Math.random() * queues);
                    sender.sendMessage(new Msg("test_" + rnd, "Test-msg", "test-value", 30000, true));
                }
                Thread.sleep(3000);
                assertEquals(recs.size(), msgToSend);


                for (MorphiumMessaging m : msgs) {
                    m.terminate();
                }
            }
        }
    }
}


