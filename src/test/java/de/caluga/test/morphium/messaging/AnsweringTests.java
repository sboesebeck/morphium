package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MultiCollectionMessaging;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.SingleCollectionMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.OutputHelper;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;

@Tag("messaging")
public class AnsweringTests extends MultiDriverTestBase {
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

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    // @MethodSource("getMorphiumInstancesPooledOnly")
    public void getAnswersTest(Morphium morphium) throws Exception {
        try (morphium) {
            String tstName = new Object() {} .getClass().getEnclosingMethod().getName();

            log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
            for (String msgImpl : MultiDriverTestBase.messagingsToTest) {

                OutputHelper.figletOutput(log, msgImpl);
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

                Morphium morph = new Morphium(cfg);
                MorphiumMessaging messaging1 = morph.createMessaging();
                MorphiumMessaging messaging2 = morph.createMessaging();
                MorphiumMessaging messagingElse = morph.createMessaging();
                gotMessage1 = false;
                gotMessage2 = false;
                messaging1.start();
                messaging2.start();
                messagingElse.start();
                // Wait for all messaging instances to be fully ready
                assertTrue(messaging1.waitForReady(15, TimeUnit.SECONDS), "messaging1 not ready");
                assertTrue(messaging2.waitForReady(15, TimeUnit.SECONDS), "messaging2 not ready");
                assertTrue(messagingElse.waitForReady(15, TimeUnit.SECONDS), "messagingElse not ready");
                messagingElse.addListenerForTopic("something else", (msg, m) -> {
                    log.info("incoming message??");
                    gotMessage1 = true;
                    return null;
                });
                messaging2.addListenerForTopic("q_getAnswer", (msg, m) -> {
                    log.info("Messaging2 got query");
                    Msg answer = m.createAnswerMsg();
                    msg.sendMessage(answer);

                    gotMessage2 = true;
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                    answer = m.createAnswerMsg();
                    return answer;
                });
                // Small delay for topic listeners to be fully registered
                Thread.sleep(500);
                Msg msg = new Msg("not asdf", "will it stick", "uahh", 10000);
                msg.setPriority(1);
                messaging1.sendMessage(msg);
                // Message should NOT be received - wrong topic
                Thread.sleep(5000);
                assertFalse(gotMessage1);
                assertFalse(gotMessage2);
                Msg question = new Msg("q_getAnswer", "question", "a value");
                question.setPriority(5);
                List<Msg> answers = messaging1.sendAndAwaitAnswers(question, 2, 5000);
                assertTrue(answers != null && !answers.isEmpty());
                assertEquals(answers.size(), 2); //: "Got wrong number of answers: " + answers.size();

                for (Msg m : answers) {
                    assertNotNull(m.getInAnswerTo());;
                    assertEquals(m.getInAnswerTo(), question.getMsgId());
                }

                messaging1.terminate();
                messaging2.terminate();
                messagingElse.terminate();
            }
            OutputHelper.figletOutput(log, "Finished");

        }
    }




    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void sendAndWaitforAnswerTestFailing(Morphium morphium) {
        try (morphium) {
            String tstName = new Object() {} .getClass().getEnclosingMethod().getName();

            log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
            for (String msgImpl : MultiDriverTestBase.messagingsToTest) {
                OutputHelper.figletOutput(log, msgImpl);

                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

                Morphium morph = new Morphium(cfg);
                assertThrows(RuntimeException.class, () -> {
                    MorphiumMessaging m1 = morph.createMessaging();
                    log.info("Upcoming Errormessage is expected, because messaging is not processing own messages!");

                    try {
                        m1.addListenerForTopic("test", (msg, m) -> {
                            gotMessage1 = true;
                            return new Msg(m.getTopic(), "got message", "value", 5000);
                        });
                        m1.start();
                        Msg answer = m1.sendAndAwaitFirstAnswer(new Msg("test", "Sender", "sent", 5000), 500);
                        log.info("Got answer {}", answer);
                        assertTrue(gotMessage1);
                        assertNotNull(answer);
                    } finally {
                        //cleaning up
                        m1.terminate();
                    }
                }
                            );
                morph.close();
            }
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    // @MethodSource("getMorphiumInstancesPooledOnly")
    // @MethodSource("getInMemInstanceOnly")
    public void sendAndWaitforAnswerLoadTest(Morphium morphium) throws Exception {
        try (morphium) {
            String tstName = new Object() {} .getClass().getEnclosingMethod().getName();

            log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
            for (String msgImpl : MultiDriverTestBase.messagingsToTest) {
                OutputHelper.figletOutput(log, msgImpl);

                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

                Morphium morph = new Morphium(cfg);
                MorphiumMessaging sender = morph.createMessaging();
                sender.setSenderId("Sender");
                sender.start();
                ArrayList<MorphiumMessaging> msgs = new ArrayList<>();

                for (int i = 0; i < 5; i++) {
                    MorphiumMessaging rec = morph.createMessaging();
                    rec.setSenderId("rec" + i);
                    rec.start();
                    rec.addListenerForTopic("test", new MessageListener<Msg>() {
                        @Override
                        public Msg onMessage(MorphiumMessaging msg, Msg m) {
                            log.info("Got message after ms: " + (System.currentTimeMillis() - m.getTimestamp()));

                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                // TODO Auto-generated catch block
                            }

                            return m.createAnswerMsg();
                        }
                    });

                    msgs.add(rec);
                }
                // Let listeners settle / topic registrations propagate (especially on external replica sets)
                Thread.sleep(5000);

                // Be more tolerant on external setups; first answer can take longer under load / replication lag
                Msg a = sender.sendAndAwaitFirstAnswer(new Msg("test", "Test", "value", 60000, true), 60000, false);
                log.info("Got answer {}", a);
                assertNotNull(a, "Did not get answer");
                Thread.sleep(2000);
                log.info("All recievers instanciated...");

                AtomicInteger nullValues = new AtomicInteger();
                for (int i = 0; i < 100; i++) {
                    Thread.startVirtualThread(() -> {
                        long start = System.currentTimeMillis();
                        Msg answer = sender.sendAndAwaitFirstAnswer(new Msg("test", "Test", "value", 2000, true), 2000, false);
                        long d = System.currentTimeMillis() - start;
                        if (answer == null) {
                            nullValues.incrementAndGet();
                            log.error("Rec NULL after " + d + "ms");
                            return;
                        }
                        log.info("Rec after " + d + "ms");
                    }); // startVirtualThread already starts the thread
                    Thread.sleep(20);
                }

                log.info("Done...");
                assertEquals(0, nullValues.get(), "Some answers were null");
                Thread.sleep(1000);
                sender.terminate();

                for (MorphiumMessaging m : msgs) {
                    log.info("Closing messaging {}", m.getSenderId());
                    m.terminate();
                }
                morph.close();
                log.info("all closed");

            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    // @MethodSource("getInMemInstanceOnly")
    public void sendAndWaitforAnswerTest(Morphium morphium) throws Exception {
        try (morphium) {
            String tstName = new Object() {} .getClass().getEnclosingMethod().getName();

            log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
            //        morphium.dropCollection(Msg.class);
            for (String msgImpl : MultiDriverTestBase.messagingsToTest) {
                OutputHelper.figletOutput(log, msgImpl);

                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

                Morphium morph = new Morphium(cfg);

                MorphiumMessaging sender = morph.createMessaging();
                sender.setSenderId("sender");
                sender.setUseChangeStream(false);;
                sender.start();
                // Thread.sleep(2000);
                gotMessage1 = false;
                gotMessage2 = false;
                gotMessage3 = false;
                gotMessage4 = false;
                MorphiumMessaging m1 = morph.createMessaging();
                m1.setSenderId("m1");
                m1.addListenerForTopic("test", (msg, m) -> {
                    gotMessage1 = true;
                    return new Msg(m.getTopic(), "got message", "value", 5555000);
                });
                m1.setUseChangeStream(false);
                m1.start();
                Thread.sleep(2500);
                Msg answer = sender.sendAndAwaitFirstAnswer(new Msg("test", "Sender", "sent", 115000), 125000);
                assertNotNull(answer);;
                assertEquals(answer.getTopic(), "test");
                assertNotNull(answer.getInAnswerTo());;
                assertNotNull(answer.getRecipients());;
                assertEquals(answer.getMsg(), "got message");
                m1.terminate();
                sender.terminate();
                morph.close();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    // @MethodSource("getInMemInstanceOnly")
    public void sendAndWaitforAnswerTimoutTest(Morphium morphium) throws Exception {
        try (morphium) {
            String tstName = new Object() {} .getClass().getEnclosingMethod().getName();

            log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
            //        morphium.dropCollection(Msg.class);
            for (String msgImpl : MultiDriverTestBase.messagingsToTest) {
                OutputHelper.figletOutput(log, msgImpl);

                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

                Morphium morph = new Morphium(cfg);

                MorphiumMessaging sender = morph.createMessaging();
                sender.setSenderId("sender");
                sender.setUseChangeStream(false);;
                sender.start();
                // Thread.sleep(2000);
                gotMessage1 = false;
                gotMessage2 = false;
                gotMessage3 = false;
                gotMessage4 = false;
                MorphiumMessaging m1 = morph.createMessaging();
                m1.setSenderId("m1");
                m1.addListenerForTopic("test", (msg, m) -> {
                    gotMessage1 = true;
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5));
                    return new Msg(m.getTopic(), "got message", "value", 5000);
                });
                m1.setUseChangeStream(false);
                m1.start();
                Thread.sleep(2500);
                Msg answer = sender.sendAndAwaitFirstAnswer(new Msg("test", "Sender", "sent", 5000), 2000, false);
                assertNull(answer, "should have gotten no message due to timeout");

                assertThrows(Exception.class, ()-> {

                    Msg a = sender.sendAndAwaitFirstAnswer(new Msg("test", "Sender", "sent", 5000), 2000, true);
                    log.info("Should not get {}", a);
                }, "Should have thrown an exceptinio");


                m1.terminate();
                sender.terminate();
                morph.close();
            }
        }
    }


}
