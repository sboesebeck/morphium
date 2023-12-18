package de.caluga.test.mongo.suite.ncmessaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class AnsweringNCTests extends MorphiumTestBase {
    private final List<Msg> list = new ArrayList<>();
    private final AtomicInteger queueCount = new AtomicInteger(1000);
    public boolean gotMessage = false;
    public boolean gotMessage1 = false;
    public boolean gotMessage2 = false;
    public boolean gotMessage3 = false;
    public boolean gotMessage4 = false;
    public boolean error = false;
    public MorphiumId lastMsgId;
    public AtomicInteger procCounter = new AtomicInteger(0);

    @Test
    public void answeringTest() throws Exception {
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
        try {

            m1.setUseChangeStream(false).start();
            m2.setUseChangeStream(false).start();
            onlyAnswers.setUseChangeStream(false).start();
            Thread.sleep(100);

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
                log.info("M1 got message " + m.toString());
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
                log.info("M2 got message " + m.toString());
                assert (m.getInAnswerTo() == null) : "M2 got an answer, but did not ask?";
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

                assertNotNull(m.getInAnswerTo(), "was not an answer? " + m.toString());

                log.info("M3 got answer " + m.toString());
                assertNotNull(lastMsgId, "Last message == null?");
                assert (m.getInAnswerTo().equals(lastMsgId)) : "Wrong answer????" + lastMsgId.toString() + " != " + m.getInAnswerTo().toString();
                //                assert (m.getSender().equals(m1.getSenderId())) : "Sender is not M1?!?!? m1_id: " + m1.getSenderId() + " - message sender: " + m.getSender();
                return null;
            });

            Msg question = new Msg("QMsg", "This is the message text", "A question param");
            question.setMsgId(new MorphiumId());
            lastMsgId = question.getMsgId();
            onlyAnswers.sendMessage(question);
            log.info("Send Message with id: " + question.getMsgId());
            Thread.sleep(3000);
            long cnt = morphium.createQueryFor(Msg.class, onlyAnswers.getCollectionName()).f(Msg.Fields.inAnswerTo).eq(question.getMsgId()).countAll();
            log.info("Answers in mongo: " + cnt);
            assert (cnt == 2);
            assert (gotMessage3) : "no answer got back?";
            assert (gotMessage1) : "Question not received by m1";
            assert (gotMessage2) : "Question not received by m2";
            assert (!error);
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            Thread.sleep(2000);
            assert (!error);

            assert (!gotMessage3 && !gotMessage1 && !gotMessage2) : "Message processing repeat?";

            question = new Msg("QMsg", "This is the message text", "A question param", 30000, true);
            question.setMsgId(new MorphiumId());
            lastMsgId = question.getMsgId();
            onlyAnswers.sendMessage(question);
            log.info("Send Message with id: " + question.getMsgId());
            Thread.sleep(1000);
            cnt = morphium.createQueryFor(Msg.class, onlyAnswers.getCollectionName()).f(Msg.Fields.inAnswerTo).eq(question.getMsgId()).countAll();
            assert (cnt == 1);

        } finally {
            m1.terminate();
            m2.terminate();
            onlyAnswers.terminate();
            Thread.sleep(100);

        }

    }




    @Test
    public void answerExclusiveMessagesTest() throws Exception {
        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        m1.setSenderId("m1");
        Messaging m2 = new Messaging(morphium, 10, false, true, 10);
        m2.setSenderId("m2");
        Messaging m3 = new Messaging(morphium, 10, false, true, 10);
        m3.setSenderId("m3");
        m1.setUseChangeStream(false).start();
        m2.setUseChangeStream(false).start();
        m3.setUseChangeStream(false).start();

        m3.addListenerForMessageNamed("test", (msg, m) -> {
            log.info("Incoming message");
            return m.createAnswerMsg();
        });

        Msg m = new Msg("test", "important", "value");
        m.setExclusive(true);
        Msg answer = m1.sendAndAwaitFirstAnswer(m, 60000);
        Thread.sleep(500);
        assertNotNull(answer);
        ;
        assert (answer.getProcessedBy().size() == 1) : "Size wrong: " + answer.getProcessedBy();
    }


    @Test
    public void answers3NodesTest() throws Exception {
        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        m1.setSenderId("m1");
        Messaging m2 = new Messaging(morphium, 10, false, true, 10);
        m2.setSenderId("m2");
        Messaging mSrv = new Messaging(morphium, 10, false, true, 10);
        mSrv.setSenderId("Srv");

        m1.setUseChangeStream(false).start();
        m2.setUseChangeStream(false).start();
        mSrv.setUseChangeStream(false).start();

        mSrv.addListenerForMessageNamed("query", (msg, m) -> {
            log.info("Incoming message - sending result");
            Msg answer = m.createAnswerMsg();
            answer.setValue("Result");
            return answer;
        });
        Thread.sleep(1000);


        for (int i = 0; i < 10; i++) {
            Msg m = new Msg("query", "a message", "a query");
            m.setExclusive(true);
            log.info("Sending m1...");
            Msg answer1 = m1.sendAndAwaitFirstAnswer(m, 1000);
            assertNotNull(answer1);
            ;
            m = new Msg("query", "a message", "a query");
            log.info("... got it. Sending m2");
            Msg answer2 = m2.sendAndAwaitFirstAnswer(m, 1000);
            assertNotNull(answer2);
            ;
            log.info("... got it.");
        }

        m1.terminate();
        m2.terminate();
        mSrv.terminate();

    }

    @Test
    @Disabled
    public void getAnswersTest() throws Exception {
        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        Messaging m2 = new Messaging(morphium, 10, false, true, 10);
        Messaging mTst = new Messaging(morphium, 10, false, true, 10);

        m1.setUseChangeStream(false).start();
        m2.setUseChangeStream(false).start();
        mTst.setUseChangeStream(false).start();


        mTst.addListenerForMessageNamed("somethign else", (msg, m) -> {
            log.info("incoming message??");
            return null;
        });

        m2.addListenerForMessageNamed("question", (msg, m) -> {
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

        Msg question = new Msg("question", "question", "a value");
        question.setPriority(5);
        List<Msg> answers = m1.sendAndAwaitAnswers(question, 2, 10000);
        assert (answers != null && !answers.isEmpty());
        assert (answers.size() == 2) : "Got wrong number of answers: " + answers.size();
        for (Msg m : answers) {
            assertNotNull(m.getInAnswerTo());
            ;
            assert (m.getInAnswerTo().equals(question.getMsgId()));
        }
        m1.terminate();
        m2.terminate();
        mTst.terminate();
    }

    @Test
    public void waitForAnswerTest() throws Exception {
        MorphiumConfig cfg = MorphiumConfig.createFromJson(morphium.getConfig().toString());
        Morphium mor = new Morphium(cfg);

        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        Messaging m2 = new Messaging(mor, 10, false, true, 10);
        m1.setSenderId("m1");
        m2.setSenderId("m2");
        m1.setUseChangeStream(false).start();
        m2.setUseChangeStream(false).start();

        m2.addListenerForMessageNamed("question", (msg, m) -> {
            Msg answer = m.createAnswerMsg();
            return answer;
        });

        for (int i = 0; i < 100; i++) {
            log.info("Sending msg " + i);
            Msg question = new Msg("question", "question" + i, "a value " + i);
            question.setPriority(5);
            long start = System.currentTimeMillis();
            Msg answer = m1.sendAndAwaitFirstAnswer(question, 15000);
            long dur = System.currentTimeMillis() - start;
            assertTrue(answer != null && answer.getInAnswerTo() != null);
            assert (answer.getInAnswerTo().equals(question.getMsgId()));
            log.info("... ok - took " + dur + " ms");
        }
        m1.terminate();
        m2.terminate();
        mor.close();
    }

    @Test
    @Disabled
    public void answerWithoutListener() throws Exception {
        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        Messaging m2 = new Messaging(morphium, 10, false, true, 10);

        m1.setUseChangeStream(false).start();
        m2.setUseChangeStream(false).start();

        m2.addListenerForMessageNamed("question", (msg, m) -> m.createAnswerMsg());

        m1.sendMessage(new Msg("not asdf", "will it stuck", "uahh", 10000));
        Thread.sleep(10000);

        Msg answer = m1.sendAndAwaitFirstAnswer(new Msg("question", "question", "a value"), 10000);
        assertNotNull(answer);
        ;
        m1.terminate();
        m2.terminate();

    }


    @Test
    public void answerTestDifferentType() throws Exception {
        Messaging sender = new Messaging(morphium, 100, true);
        Messaging recipient = new Messaging(morphium, 100, true);
        sender.setUseChangeStream(false).start();
        recipient.setUseChangeStream(false).start();
        gotMessage1 = false;
        recipient.addListenerForMessageNamed("query", new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m)  {
                gotMessage1 = true;
                Msg answer = m.createAnswerMsg();
                answer.setName("queryAnswer");
                answer.setMsg("the answer");
                //msg.storeMessage(answer);
                return answer;
            }
        });
        gotMessage2 = false;
        sender.addListenerForMessageNamed("queryAnswer", new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m)  {
                gotMessage2 = true;
                assertNotNull(m.getInAnswerTo());
                ;
                return null;
            }
        });

        sender.sendMessage(new Msg("query", "a query", "avalue"));
        Thread.sleep(1000);
        assert (gotMessage1);
        assert (gotMessage2);

        Msg answer = sender.sendAndAwaitFirstAnswer(new Msg("query", "query", "avalue"), 1000);
        assertNotNull(answer);
        ;
        sender.terminate();
        recipient.terminate();
    }


    @Test
    public void sendAndWaitforAnswerTestFailing() {
        assertThrows(RuntimeException.class,()-> {
            Messaging m1 = new Messaging(morphium, 100, false);
            log.info("Upcoming Errormessage is expected!");
            try {
                m1.addMessageListener((msg, m) -> {
                    gotMessage1 = true;
                    return new Msg(m.getName(), "got message", "value", 5000);
                });

                m1.setUseChangeStream(false).start();

                Msg answer = m1.sendAndAwaitFirstAnswer(new Msg("test", "Sender", "sent", 5000), 500);
            } finally {
                //cleaning up
                m1.terminate();
                morphium.dropCollection(Msg.class);
            }
        });

    }

    @Test
    public void sendAndWaitforAnswerTest() throws Exception {
//        morphium.dropCollection(Msg.class);
        Messaging sender = new Messaging(morphium, 100, false);
        sender.setUseChangeStream(false).start();

        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        Messaging m1 = new Messaging(morphium, 100, false);
        m1.addMessageListener((msg, m) -> {
            gotMessage1 = true;
            return new Msg(m.getName(), "got message", "value", 5000);
        });

        m1.setUseChangeStream(false).start();
        Thread.sleep(2500);

        Msg answer = sender.sendAndAwaitFirstAnswer(new Msg("test", "Sender", "sent", 15000), 15000);
        assertNotNull(answer);
        ;
        assert (answer.getName().equals("test"));
        assertNotNull(answer.getInAnswerTo());
        ;
        assertNotNull(answer.getRecipients());
        ;
        assert (answer.getMsg().equals("got message"));
        m1.terminate();
        sender.terminate();
    }


}
