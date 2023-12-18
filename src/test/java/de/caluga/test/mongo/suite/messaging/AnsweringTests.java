package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

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
        try (morphium) {
            String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
            log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            error = false;
            morphium.clearCollection(Msg.class);
            final Messaging m1;
            final Messaging m2;
            final Messaging onlyAnswers;
            m1 = new Messaging(morphium, 100, true);
            m2 = new Messaging(morphium, 100, true);
            onlyAnswers = new Messaging(morphium, 100, true);
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
                m1.addMessageListener((msg, m) -> {
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
                m2.addMessageListener((msg, m) -> {
                    gotMessage2 = true;

                    if (m.getTo() != null && !m.getTo().contains(m2.getSenderId())) {
                        log.error("wrongly received message?");
                        error = true;
                    }
                    log.info("M2 got message " + m);
                    assert(m.getInAnswerTo() == null) : "M2 got an answer, but did not ask?";
                    Msg answer = m.createAnswerMsg();
                    answer.setValue("This is the answer from m2");
                    answer.addValue("when", System.currentTimeMillis());
                    answer.addAdditional("Additional Value von m2");
                    return answer;
                });
                onlyAnswers.addMessageListener((msg, m) -> {
                    gotMessage3 = true;

                    if (m.getTo() != null && !m.getTo().contains(onlyAnswers.getSenderId())) {
                        log.error("wrongly received message?");
                        error = true;
                    }

                    assertNotNull(m.getInAnswerTo(), "was not an answer? " + m);
                    assert(m.getMapValue().size() == 1);
                    assert(m.getMapValue().containsKey("something") || m.getMapValue().containsKey("when"));
                    log.info(msg.getSenderId() + " got answer " + m);
                    assertNotNull(lastMsgId, "Last message == null?");
                    assert(m.getInAnswerTo().equals(lastMsgId)) : "Wrong answer????" + lastMsgId.toString() + " != " + m.getInAnswerTo().toString();
                    //                assert (m.getSender().equals(m1.getSenderId())) : "Sender is not M1?!?!? m1_id: " + m1.getSenderId() + " - message sender: " + m.getSender();
                    return null;
                });
                Msg question = new Msg("QMsg", "This is the message text", "A question param");
                question.setMsgId(new MorphiumId());
                lastMsgId = question.getMsgId();
                onlyAnswers.sendMessage(question);
                log.info("Send Message with id: " + question.getMsgId());
                Thread.sleep(7000);
                long cnt = morphium.createQueryFor(Msg.class, onlyAnswers.getCollectionName()).f(Msg.Fields.inAnswerTo).eq(question.getMsgId()).countAll();
                log.info("Answers in mongo: " + cnt);
                assertEquals(2, cnt);
                assertTrue(gotMessage3);//: "no answer got back?";
                assertTrue(gotMessage1, "Question not received by m1");
                assertTrue(gotMessage2, "Question not received by m2");
                assertFalse(error);
                gotMessage1 = false;
                gotMessage2 = false;
                gotMessage3 = false;
                Thread.sleep(2000);
                assertFalse(error);
                assertTrue(!gotMessage3 && !gotMessage1 && !gotMessage2, "Message processing repeat?");
                question = new Msg("QMsg", "This is the message text #2", "A question param", 30000, true);
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

                cnt = morphium.createQueryFor(Msg.class, onlyAnswers.getCollectionName()).f(Msg.Fields.inAnswerTo).eq(question.getMsgId()).countAll();
                assertEquals(1, cnt);
            } finally {
                m1.terminate();
                m2.terminate();
                onlyAnswers.terminate();
                Thread.sleep(100);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void answerExclusiveMessagesTest(Morphium morphium) throws Exception {
        try (morphium) {
            String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
            log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
            Messaging m1 = new Messaging(morphium, 10, false, true, 10);
            m1.setSenderId("m1");
            Messaging m2 = new Messaging(morphium, 10, false, true, 10);
            m2.setSenderId("m2");
            Messaging m3 = new Messaging(morphium, 10, false, true, 10);
            m3.setSenderId("m3");
            m1.start();
            m2.start();
            m3.start();
            Thread.sleep(2000);
            m3.addListenerForMessageNamed("test_answer_exclusive", (msg, m) -> {
                log.info("Incoming message");
                return m.createAnswerMsg();
            });
            Msg m = new Msg("test_answer_exclusive", "important", "value");
            m.setExclusive(true);
            Msg answer = m1.sendAndAwaitFirstAnswer(m, 10000);
            assertNotNull(answer);;
            assert(answer.getProcessedBy().size() == 1) : "Size wrong: " + answer.getProcessedBy();

            try {
                m1.terminate();
                m2.terminate();
                m3.terminate();
            } catch (Exception e) {
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    //    @MethodSource("getInMemInstanceOnly")
    public void answers3NodesTest(Morphium morphium) throws Exception {
        try (morphium) {
            String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
            log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
            Messaging m1 = new Messaging(morphium, 10, false, true, 10);
            m1.setSenderId("m1");
            m1.setUseChangeStream(false);
            Messaging m2 = new Messaging(morphium, 10, false, true, 10);
            m2.setSenderId("m2");
            m2.setUseChangeStream(false);
            Messaging mSrv = new Messaging(morphium, 10, false, true, 10);
            mSrv.setSenderId("Srv");
            mSrv.setUseChangeStream(false);
            m1.start();
            m2.start();
            mSrv.start();
            Thread.sleep(1000);
            mSrv.addListenerForMessageNamed("query", (msg, m) -> {
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

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void getAnswersTest(Morphium morphium) throws Exception {
        try (morphium) {
            String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
            log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
            Messaging m1 = new Messaging(morphium, 10, false, true, 10);
            Messaging m2 = new Messaging(morphium, 10, false, true, 10);
            Messaging mTst = new Messaging(morphium, 10, false, true, 10);
            m1.start();
            m2.start();
            mTst.start();
            Thread.sleep(1000);
            mTst.addListenerForMessageNamed("something else", (msg, m) -> {
                log.info("incoming message??");
                return null;
            });
            m2.addListenerForMessageNamed("q_getAnswer", (msg, m) -> {
                Msg answer = m.createAnswerMsg();
                msg.sendMessage(answer);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                answer = m.createAnswerMsg();
                return answer;
            });
            Msg m3 = new Msg("not asdf", "will it stuck", "uahh", 10000);
            m3.setPriority(1);
            m1.sendMessage(m3);
            Thread.sleep(5000);
            Msg question = new Msg("q_getAnswer", "question", "a value");
            question.setPriority(5);
            List<Msg> answers = m1.sendAndAwaitAnswers(question, 2, 5000);
            assert(answers != null && !answers.isEmpty());
            assert(answers.size() == 2) : "Got wrong number of answers: " + answers.size();

            for (Msg m : answers) {
                assertNotNull(m.getInAnswerTo());;
                assert(m.getInAnswerTo().equals(question.getMsgId()));
            }

            m1.terminate();
            m2.terminate();
            mTst.terminate();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesPooledOnly")
    public void waitForAnswerTest(Morphium morphium) throws Exception {
        try (morphium) {
            String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
            log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
            MorphiumConfig cfg = MorphiumConfig.createFromJson(morphium.getConfig().toString());
            cfg.setCredentialsDecryptionKey(morphium.getConfig().getCredentialsDecryptionKey());
            cfg.setCredentialsEncryptionKey(morphium.getConfig().getCredentialsEncryptionKey());
            cfg.setCredentialsEncrypted(morphium.getConfig().getCredentialsEncrypted());
            Morphium mor = new Morphium(cfg);
            Messaging m1 = new Messaging(morphium, 10, false, true, 10);
            Messaging m2 = new Messaging(mor, 10, false, true, 10);
            m1.setSenderId("m1");
            m2.setSenderId("m2");
            m1.start();
            m2.start();
            m2.addListenerForMessageNamed("q_wait_for", (msg, m) -> {
                Msg answer = m.createAnswerMsg();
                return answer;
            });
            Thread.sleep(1000);

            for (int i = 0; i < 100; i++) {
                log.info("Sending msg " + i);
                Msg question = new Msg("q_wait_for", "question" + i, "a value " + i);
                question.setPriority(5);
                long start = System.currentTimeMillis();
                Msg answer = m1.sendAndAwaitFirstAnswer(question, 4500);
                long dur = System.currentTimeMillis() - start;
                assertTrue(answer != null && answer.getInAnswerTo() != null);;
                assert(answer.getInAnswerTo().equals(question.getMsgId()));
                log.info("... ok - took " + dur + " ms");
            }

            try {
                m1.terminate();
                m2.terminate();
                mor.close();
            } catch (Exception e) {
            }

            mor.close();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void answerWithoutListener(Morphium morphium) throws Exception {
        try (morphium) {
            String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
            log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
            Messaging m1 = new Messaging(morphium, 10, false, true, 10);
            Messaging m2 = new Messaging(morphium, 10, false, true, 10);
            m1.start();
            m2.start();
            // Thread.sleep(2000);
            m2.addListenerForMessageNamed("q_no_listener", (msg, m) -> m.createAnswerMsg());
            m1.sendMessage(new Msg("not asdf", "will it stuck", "uahh", 10000));
            Thread.sleep(5000);
            Msg answer = m1.sendAndAwaitFirstAnswer(new Msg("q_no_listener", "question", "a value"), 5000);
            assertNotNull(answer);;
            m1.terminate();
            m2.terminate();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void answerTestDifferentType(Morphium morphium) throws Exception {
        try (morphium) {
            String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
            log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
            Messaging sender = new Messaging(morphium, 100, true);
            Messaging recipient = new Messaging(morphium, 100, true);
            sender.start();
            recipient.start();
            Thread.sleep(2000);
            gotMessage1 = false;
            recipient.addListenerForMessageNamed("query", (msg, m) -> {
                gotMessage1 = true;
                Msg answer = m.createAnswerMsg();
                answer.setName("queryAnswer");
                answer.setMsg("the answer");
                //msg.storeMessage(answer);
                return answer;
            });
            gotMessage2 = false;
            sender.addListenerForMessageNamed("queryAnswer", (msg, m) -> {
                gotMessage2 = true;
                assertNotNull(m.getInAnswerTo());;
                return null;
            });
            sender.sendMessage(new Msg("query", "a query", "avalue"));
            Thread.sleep(5000);
            assert(gotMessage1);
            assert(gotMessage2);
            Msg answer = sender.sendAndAwaitFirstAnswer(new Msg("query", "query", "avalue"), 1000);
            assertNotNull(answer);;
            sender.terminate();
            recipient.terminate();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void sendAndWaitforAnswerTestFailing(Morphium morphium) {
        try (morphium) {
            String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
            log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
            assertThrows(RuntimeException.class, () -> {
                Messaging m1 = new Messaging(morphium, 100, false);
                log.info("Upcoming Errormessage is expected!");

                try {
                    m1.addMessageListener((msg, m) -> {
                        gotMessage1 = true;
                        return new Msg(m.getName(), "got message", "value", 5000);
                    });
                    m1.start();
                    Msg answer = m1.sendAndAwaitFirstAnswer(new Msg("test", "Sender", "sent", 5000), 500);
                } finally {
                    //cleaning up
                    m1.terminate();
                    morphium.dropCollection(Msg.class);
                }
            });
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    // @MethodSource("getInMemInstanceOnly")
    public void sendAndWaitforAnswerTestChangeStream(Morphium morphium) throws Exception {
        try (morphium) {
            String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
            log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
            //        morphium.dropCollection(Msg.class);
            Messaging sender = new Messaging(morphium, 100, false);
            sender.setSenderId("sender");
            sender.setUseChangeStream(true);;
            sender.start();
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;
            Messaging m1 = new Messaging(morphium, 100, false);
            m1.setSenderId("m1");
            m1.addMessageListener((msg, m) -> {
                gotMessage1 = true;
                return new Msg(m.getName(), "got message", "value", 5555000);
            });
            m1.setUseChangeStream(false);
            m1.start();
            // Thread.sleep(2500);
            Msg answer = sender.sendAndAwaitFirstAnswer(new Msg("test", "Sender", "sent", 115000), 125000);
            assertNotNull(answer);;
            assert(answer.getName().equals("test"));
            assertNotNull(answer.getInAnswerTo());;
            assertNotNull(answer.getRecipients());;
            assert(answer.getMsg().equals("got message"));
            m1.terminate();
            sender.terminate();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    // @MethodSource("getInMemInstanceOnly")
    public void sendAndWaitforAnswerLoadTest(Morphium morphium) throws Exception {
        try (morphium) {
            String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
            log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
            Messaging sender = new Messaging(morphium, 100, true, true, 100);
            sender.setSenderId("Sender");
            sender.start();
            ArrayList<Messaging> msgs = new ArrayList<>();

            for (int i = 0; i < 5; i++) {
                Messaging rec = new Messaging(morphium, 100, true, true, 100);
                rec.setSenderId("rec" + i);
                rec.start();
                rec.addMessageListener(new MessageListener<Msg>() {
                    @Override
                    public Msg onMessage(Messaging msg, Msg m) {
                        log.info("Got message after ms: "+(System.currentTimeMillis()-m.getTimestamp()));
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

                    Msg a = sender.sendAndAwaitFirstAnswer(new Msg("test", "Test", "value", 2000, true), 2000, false);
            Thread.sleep(2000);
            log.info("All recievers instanciated...");

            for (int i = 0; i < 100; i++) {
                new Thread(() -> {
                    long start = System.currentTimeMillis();
                    Msg answer = sender.sendAndAwaitFirstAnswer(new Msg("test", "Test", "value", 2000, true), 2000, false);
                    long d = System.currentTimeMillis() - start;
                    log.info("Rec after " + d + "ms");
                }).start();
                Thread.sleep(20);
            }
            log.info("Done...");
            Thread.sleep(10000);
            sender.terminate();

            for (Messaging m : msgs) {
                m.terminate();
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
            Messaging sender = new Messaging(morphium, 100, false);
            sender.setSenderId("sender");
            sender.setUseChangeStream(false);;
            sender.start();
            // Thread.sleep(2000);
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;
            Messaging m1 = new Messaging(morphium, 100, false);
            m1.setSenderId("m1");
            m1.addMessageListener((msg, m) -> {
                gotMessage1 = true;
                return new Msg(m.getName(), "got message", "value", 5555000);
            });
            m1.setUseChangeStream(false);
            m1.start();
            Thread.sleep(2500);
            Msg answer = sender.sendAndAwaitFirstAnswer(new Msg("test", "Sender", "sent", 115000), 125000);
            assertNotNull(answer);;
            assert(answer.getName().equals("test"));
            assertNotNull(answer.getInAnswerTo());;
            assertNotNull(answer.getRecipients());;
            assert(answer.getMsg().equals("got message"));
            m1.terminate();
            sender.terminate();
        }
    }

}
