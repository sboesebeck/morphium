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
    public void answeringTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();

        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        error = false;
        try (morphium) {
            for (String msgImpl : MultiDriverTestBase.messagingsToTest) {

                OutputHelper.figletOutput(log, msgImpl);
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

                Morphium morph = new Morphium(cfg);
                final MorphiumMessaging m1;
                final MorphiumMessaging m2;
                final MorphiumMessaging onlyAnswers;
                final AtomicInteger answersReceived = new AtomicInteger(0);

                m1 = morph.createMessaging();
                m2 = morph.createMessaging();
                onlyAnswers = morph.createMessaging();
                m1.setSenderId("m1");
                m2.setSenderId("m2");
                onlyAnswers.setSenderId("onlyAnswers");

                try {
                    m1.start();
                    m2.start();
                    onlyAnswers.start();
                    Thread.sleep(2000);
                    log.info("m1 ID: " + m1.getSenderId());
                    log.info("m2 ID: " + m2.getSenderId());
                    log.info("onlyAnswers ID: " + onlyAnswers.getSenderId());
                    m1.addListenerForTopic("test", (msg, m) -> {
                        gotMessage1 = true;

                        if (m.getTo() != null && !m.getTo().contains(m1.getSenderId())) {
                            log.error("wrongly received message?");
                            error = true;
                        }
                        if (m.getInAnswerTo() != null) {
                            log.error("M1 got an answer, but did not ask?");
                            error = true;
                        }
                        log.info("M1 got message " + m);
                        Msg answer = m.createAnswerMsg();
                        answer.setValue("This is the answer from m1");
                        answer.addValue("something", new Date());
                        answer.addAdditional("String message from m1");
                        return answer;
                    });
                    m2.addListenerForTopic("test", (msg, m) -> {
                        gotMessage2 = true;

                        if (m.getTo() != null && !m.getTo().contains(m2.getSenderId())) {
                            log.error("wrongly received message?");
                            error = true;
                        }
                        log.info("M2 got message " + m);
                        assertNull(m.getInAnswerTo());
                        Msg answer = m.createAnswerMsg();
                        answer.setValue("This is the answer from m2");
                        answer.addValue("when", System.currentTimeMillis());
                        answer.addAdditional("Additional Value von m2");
                        return answer;
                    });
                    onlyAnswers.addListenerForTopic("test", (msg, m) -> {
                        gotMessage3 = true;
                        answersReceived.incrementAndGet();

                        if (m.getTo() != null && !m.getTo().contains(onlyAnswers.getSenderId())) {
                            log.error("wrongly received message?");
                            error = true;
                        }

                        assertNotNull(m.getInAnswerTo(), "was not an answer? " + m);
                        assertEquals(m.getMapValue().size(), 1);
                        assertTrue(m.getMapValue().containsKey("something") || m.getMapValue().containsKey("when"));
                        log.info(msg.getSenderId() + " got answer " + m);
                        assertNotNull(lastMsgId, "Last message == null?");
                        assertEquals(m.getInAnswerTo(), lastMsgId); //: "Wrong answer????" + lastMsgId.toString() + " != " + m.getInAnswerTo().toString();
                        //                assert (m.getSender().equals(m1.getSenderId())) : "Sender is not M1?!?!? m1_id: " + m1.getSenderId() + " - message sender: " + m.getSender();
                        return null;
                    });
                    Msg question = new Msg("test", "This is the message text", "A question param");
                    question.setMsgId(new MorphiumId());
                    lastMsgId = question.getMsgId();
                    onlyAnswers.sendMessage(question);
                    log.info("Send Message with id: " + question.getMsgId());
                    TestUtils.waitForConditionToBecomeTrue(15000, "Answers not received", () -> answersReceived.get() == 2);
                    TestUtils.waitForConditionToBecomeTrue(5000, "Question not received by m1", () -> gotMessage1);
                    TestUtils.waitForConditionToBecomeTrue(5000, "Question not received by m2", () -> gotMessage2);
                    assertFalse(error);
                    gotMessage1 = false;
                    gotMessage2 = false;
                    gotMessage3 = false;
                    Thread.sleep(2000);
                    assertFalse(error);
                    assertTrue(!gotMessage3 && !gotMessage1 && !gotMessage2, "Message processing repeat?");
                    question = new Msg("test", "This is the message text #2", "A question param", 30000, true);
                    question.setMsgId(new MorphiumId());
                    lastMsgId = question.getMsgId();
                    onlyAnswers.sendMessage(question);
                    log.info("Send Message with id: " + question.getMsgId());
                    Thread.sleep(1000);

                    if (gotMessage1) {
                        log.info("Received by m1");
                    }

                    if (gotMessage2) {
                        log.info("Received by m2");
                    }

                    if (gotMessage3) {
                        log.info("Received by onlyAnswers");
                    }
                    assertTrue(gotMessage1 || gotMessage2);
                    assertTrue(gotMessage3);

                } finally {
                    m1.terminate();
                    m2.terminate();
                    onlyAnswers.terminate();
                    morph.close();
                    Thread.sleep(100);
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void answerExclusiveMessagesTest(Morphium morphium) throws Exception {
        try (morphium) {
            String tstName = new Object() {} .getClass().getEnclosingMethod().getName();

            log.info("---------> Running test " + tstName + " with " + morphium.getDriver().getName());

            for (String msgImpl : MultiDriverTestBase.messagingsToTest) {

                OutputHelper.figletOutput(log, msgImpl);
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

                Morphium morph = new Morphium(cfg);
                MorphiumMessaging m1 = morph.createMessaging();//new SingleCollectionMessaging(morphium, 10, false, true, 10);
                m1.setSenderId("m1");
                MorphiumMessaging m2 = morph.createMessaging();
                m2.setSenderId("m2");
                MorphiumMessaging m3 =  morph.createMessaging();//new SingleCollectionMessaging(morphium, 10, false, true, 10);
                m3.setSenderId("m3");
                m1.start();
                m2.start();
                m3.start();
                Thread.sleep(2000);
                m3.addListenerForTopic("test_answer_exclusive", (msg, m) -> {
                    log.info("Incoming message");
                    return m.createAnswerMsg();
                });
                Msg m = new Msg("test_answer_exclusive", "important", "value");
                m.setExclusive(true);
                Msg answer = m1.sendAndAwaitFirstAnswer(m, 10000);
                assertNotNull(answer);;
                assertEquals(answer.getProcessedBy().size(), 1);// : "Size wrong: " + answer.getProcessedBy();

                try {
                    m1.terminate();
                    m2.terminate();
                    m3.terminate();
                } catch (Exception e) {
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    //    @MethodSource("getInMemInstanceOnly")
    public void answers3NodesTest(Morphium morphium) throws Exception {
        try (morphium) {
            String tstName = new Object() {} .getClass().getEnclosingMethod().getName();

            log.info("-------> Running test " + tstName + " with " + morphium.getDriver().getName());
            for (String msgImpl : MultiDriverTestBase.messagingsToTest) {

                OutputHelper.figletOutput(log, msgImpl);
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

                Morphium morph = new Morphium(cfg);
                MorphiumMessaging m1 = morph.createMessaging();// new SingleCollectionMessaging(morphium, 10, false, true, 10);
                m1.setSenderId("m1");
                m1.setUseChangeStream(false);
                MorphiumMessaging m2 = morph.createMessaging();//new SingleCollectionMessaging(morphium, 10, false, true, 10);
                m2.setSenderId("m2");
                m2.setUseChangeStream(false);
                MorphiumMessaging mSrv = morph.createMessaging();//new SingleCollectionMessaging(morphium, 10, false, true, 10);
                mSrv.setSenderId("Srv");
                mSrv.setUseChangeStream(false);
                m1.start();
                m2.start();
                mSrv.start();
                Thread.sleep(1000);
                mSrv.addListenerForTopic("query", (msg, m) -> {
                    log.info("Incoming message - sending result");
                    Msg answer = m.createAnswerMsg();
                    m.setTtl(30000);
                    answer.setValue("Result");
                    return answer;
                });
                Thread.sleep(1000);

                for (int i = 0; i < 10; i++) {
                    Msg m = new Msg("query", "a message", "a query", 30000);
                    m.setExclusive(true);
                    log.info("Sending m1...");
                    Msg answer1 = m1.sendAndAwaitFirstAnswer(m, 30000);
                    assertNotNull(answer1);;
                    m = new Msg("query", "a message", "a query");
                    log.info("... got it. Sending m2");
                    Msg answer2 = m2.sendAndAwaitFirstAnswer(m, 30000);
                    assertNotNull(answer2);;
                    log.info("... got it.");
                }

                try {
                    m1.terminate();
                    m2.terminate();
                    mSrv.terminate();
                } catch (Exception e) {
                }
            }
        }
    }

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
                Thread.sleep(1000);
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
    public void answerWithoutListener(Morphium morphium) throws Exception {
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
                MorphiumMessaging m1 = morph.createMessaging();
                MorphiumMessaging m2 = morph.createMessaging();
                m1.start();
                m2.start();
                // Thread.sleep(2000);
                m2.addListenerForTopic("q_no_listener", (msg, m) -> m.createAnswerMsg());
                m1.sendMessage(new Msg("not asdf", "will it stuck", "uahh", 10000));
                Thread.sleep(1000);
                Msg answer = m1.sendAndAwaitFirstAnswer(new Msg("q_no_listener", "question", "a value"), 5000);
                assertNotNull(answer);;

                try {
                    m1.terminate();
                } catch (Exception e) {
                    //swallow
                }

                try {
                    m2.terminate();
                } catch (Exception e) {
                    //swallow
                }
                try {
                    morph.close();
                } catch (Exception e) {
                    //swallow
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void answerTestDifferentType(Morphium morphium) throws Exception {
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
                MorphiumMessaging recipient = morph.createMessaging();
                sender.start();
                recipient.start();
                Thread.sleep(2000);
                gotMessage1 = false;
                recipient.addListenerForTopic("query", (msg, m) -> {
                    gotMessage1 = true;
                    Msg answer = m.createAnswerMsg();
                    answer.setTopic("queryAnswer");
                    answer.setMsg("the answer");
                    //msg.storeMessage(answer);
                    return answer;
                });
                gotMessage2 = false;
                sender.addListenerForTopic("queryAnswer", (msg, m) -> {
                    gotMessage2 = true;
                    assertNotNull(m.getInAnswerTo());;
                    return null;
                });
                sender.sendMessage(new Msg("query", "a query", "avalue"));
                TestUtils.waitForConditionToBecomeTrue(10000, "Query not received", () -> gotMessage1);
                TestUtils.waitForConditionToBecomeTrue(10000, "Answer not received", () -> gotMessage2);
                Msg answer = sender.sendAndAwaitFirstAnswer(new Msg("query", "query", "avalue"), 1000);
                assertNotNull(answer);;
                sender.terminate();
                recipient.terminate();
                morph.close();
            }
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
                Thread.sleep(1000);

                Msg a = sender.sendAndAwaitFirstAnswer(new Msg("test", "Test", "value", 12000, true), 12000, false);
                log.info("Got answer {}", a);
                assertNotNull(a, "Did not get answer");
                Thread.sleep(2000);
                log.info("All recievers instanciated...");

                for (int i = 0; i < 100; i++) {
                    new Thread(() -> {
                        long start = System.currentTimeMillis();
                        Msg answer = sender.sendAndAwaitFirstAnswer(new Msg("test", "Test", "value", 2000, true), 2000, false);
                        assertNotNull(answer);
                        long d = System.currentTimeMillis() - start;
                        log.info("Rec after " + d + "ms");
                    }).start();
                    Thread.sleep(20);
                }

                log.info("Done...");
                Thread.sleep(1000);
                sender.terminate();

                for (MorphiumMessaging m : msgs) {
                    m.terminate();
                }
                morph.close();
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
