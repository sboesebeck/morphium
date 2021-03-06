package de.caluga.test.mongo.suite.inmem_messaging;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.inmem.MorphiumInMemTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemAnsweringTests extends MorphiumInMemTestBase {
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

            m1.start();
            m2.start();
            onlyAnswers.start();
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

                assert (m.getInAnswerTo() != null) : "was not an answer? " + m.toString();
                assert (m.getMapValue().size() == 1);
                assert (m.getMapValue().containsKey("something") || m.getMapValue().containsKey("when"));
                log.info("M3 got answer " + m.toString());
                assert (lastMsgId != null) : "Last message == null?";
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
    public void answerListenerTest() throws Exception {
        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        Messaging m2 = new Messaging(morphium, 10, false, true, 10);
        Messaging m3 = new Messaging(morphium, 10, false, true, 10);
        try {
            m1.setSenderId("m1");
            m2.setSenderId("m2");
            m3.setSenderId("m3");
            m1.setUseChangeStream(true).start();
            m2.setUseChangeStream(true).start();
            m3.setUseChangeStream(true).start();


            //different behaviour:
            //NONE: no listener will be called for answers. Waiting For messages will still pe processed
            //ONLY_MINE: just answers directly sent to me will be processed (usually, because I sent the query)
            //ALL: well, all answers will be passed on to listeners
            final ConcurrentHashMap<String, AtomicInteger> receivedById = new ConcurrentHashMap<>();

            m1.addListenerForMessageNamed("test", ((msg, m) -> {
                log.info("m1 got msg test");
                receivedById.putIfAbsent(msg.getSenderId(), new AtomicInteger());
                receivedById.get(msg.getSenderId()).incrementAndGet();
                return null;
            }));
            m2.addMessageListener(((msg, m) -> {
                log.info("m2 got msg");
                receivedById.putIfAbsent(msg.getSenderId(), new AtomicInteger());
                receivedById.get(msg.getSenderId()).incrementAndGet();
                return null;
            }));

            m3.addListenerForMessageNamed("test", ((msg, m) -> {
                log.info("m3 got msg test");
                receivedById.putIfAbsent(msg.getSenderId(), new AtomicInteger());
                receivedById.get(msg.getSenderId()).incrementAndGet();
                return null;
            }));
            m3.addListenerForMessageNamed("test2", ((msg, m) -> {
                log.info("m3 got msg test2");
                receivedById.putIfAbsent(msg.getSenderId(), new AtomicInteger());
                receivedById.get(msg.getSenderId()).incrementAndGet();
                return null;
            }));


            //sending an answer to all (broadcast)
            m1.setReceiveAnswers(Messaging.ReceiveAnswers.ALL);
            m2.setReceiveAnswers(Messaging.ReceiveAnswers.ALL);
            Msg answer = new Msg("test", "An answer", "41");
            answer.setInAnswerTo(new MorphiumId());
            m3.sendMessage(answer);
            Thread.sleep(1000);
            assert (receivedById.size() == 2) : "Wrong number of answers: " + receivedById.size();
            assert (receivedById.get("m1").get() == 1);
            assert (receivedById.get("m2").get() == 1);

            //sending a direct message
            log.info("Sending direct message");
            receivedById.clear();
            answer = new Msg("test", "An answer", "42");
            answer.setInAnswerTo(new MorphiumId());
            answer.setRecipient("m2");
            m3.sendMessage(answer);
            Thread.sleep(1000);
            assert (receivedById.size() == 1) : "Wrong number of receivers: " + receivedById.size();
            assert (receivedById.get("m1") == null);
            assert (receivedById.get("m2").get() == 1);

            //exclusive answer!
            log.info("Exclusive answer test");
            receivedById.clear();
            answer = new Msg("test", "An exclusive answer", "43");
            answer.setInAnswerTo(new MorphiumId());
            answer.setExclusive(true);
//            answer.setTtl(100000);
            m3.sendMessage(answer);
            Thread.sleep(500);
            assert (receivedById.size() == 1) : "Receive count is " + receivedById.size();
            assert ((receivedById.get("m1") == null && receivedById.get("m2").get() == 1)
                    || (receivedById.get("m2") == null && receivedById.get("m1").get() == 1));

            //
            receivedById.clear();
            m1.setReceiveAnswers(Messaging.ReceiveAnswers.ONLY_MINE);
            m2.setReceiveAnswers(Messaging.ReceiveAnswers.ONLY_MINE);
            receivedById.clear();
            answer = new Msg("test", "An answer", "44");
            answer.setInAnswerTo(new MorphiumId());
            answer.setRecipient("m2");
            m3.sendMessage(answer);
            Thread.sleep(1500);
            assert (receivedById.size() == 1);
            assert (receivedById.get("m1") == null);
            assert (receivedById.get("m2").get() == 1);

            //
            receivedById.clear();
            m1.setReceiveAnswers(Messaging.ReceiveAnswers.ALL);
            m2.setReceiveAnswers(Messaging.ReceiveAnswers.NONE);
            receivedById.clear();
            answer = new Msg("test", "An answer", "45");
            answer.setInAnswerTo(new MorphiumId());
            answer.setExclusive(true);
            m3.sendMessage(answer);
            Thread.sleep(1500);
            assert (receivedById.size() == 1) : "Wrong result, expected one, got: " + receivedById.size();
            assert (receivedById.get("m1").get() == 1);
            assert (!receivedById.containsKey("m2"));

            //checking wait for
            MessageListener<Msg> listener = new MessageListener<Msg>() {
                @Override
                public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                    return m.createAnswerMsg();
                }
            };
            m1.addListenerForMessageNamed("test2", listener);
            receivedById.clear();
            m1.setReceiveAnswers(Messaging.ReceiveAnswers.NONE);
            m2.setReceiveAnswers(Messaging.ReceiveAnswers.NONE);
            m3.setReceiveAnswers(Messaging.ReceiveAnswers.ONLY_MINE);
            receivedById.clear();
            answer = m3.sendAndAwaitFirstAnswer(new Msg("test2", "An answer", "46").setRecipient("m1"), 1400);
            Thread.sleep(1000);
            assert (receivedById.size() == 1);

            receivedById.clear();
            m1.setReceiveAnswers(Messaging.ReceiveAnswers.ALL);
            m2.setReceiveAnswers(Messaging.ReceiveAnswers.ALL);
            m3.setReceiveAnswers(Messaging.ReceiveAnswers.ALL);
            receivedById.clear();
            answer = m3.sendAndAwaitFirstAnswer(new Msg("test2", "An answer", "47").setRecipient("m1"), 1400);
            Thread.sleep(1000); //wait for onMessage to be called
            assert (receivedById.size() == 1);

            receivedById.clear();
            m1.setReceiveAnswers(Messaging.ReceiveAnswers.ALL);
            m2.setReceiveAnswers(Messaging.ReceiveAnswers.ALL);
            m3.setReceiveAnswers(Messaging.ReceiveAnswers.NONE);
            receivedById.clear();
            answer = m3.sendAndAwaitFirstAnswer(new Msg("test2", "An answer", "48").setRecipient("m1"), 1400);
            Thread.sleep(1000); //wait for onMessage to be called
            assert (receivedById.size() == 0);

        } finally {
            m3.terminate();
            m2.terminate();
            m1.terminate();
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
        m1.start();
        m2.start();
        m3.start();

        m3.addListenerForMessageNamed("test_answer_exclusive", (msg, m) -> {
            log.info("Incoming message");
            return m.createAnswerMsg();
        });

        Msg m = new Msg("test_answer_exclusive", "important", "value");
        m.setExclusive(true);
        Msg answer = m1.sendAndAwaitFirstAnswer(m, 1000);
        assert (answer != null);
        assert (answer.getProcessedBy().size() == 1) : "Size wrong: " + answer.getProcessedBy();

        try {
            m1.terminate();
            m2.terminate();
            m3.terminate();
        } catch (Exception e) {
        }

    }


    @Test
    public void answers3NodesTest() throws Exception {
        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        m1.setSenderId("m1");
        Messaging m2 = new Messaging(morphium, 10, false, true, 10);
        m2.setSenderId("m2");
        Messaging mSrv = new Messaging(morphium, 10, false, true, 10);
        mSrv.setSenderId("Srv");

        m1.start();
        m2.start();
        mSrv.start();

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
            assert (answer1 != null);
            m = new Msg("query", "a message", "a query");
            log.info("... got it. Sending m2");
            Msg answer2 = m2.sendAndAwaitFirstAnswer(m, 1000);
            assert (answer2 != null);
            log.info("... got it.");
        }

        try {
            m1.terminate();
            m2.terminate();
            mSrv.terminate();
        } catch (Exception e) {
        }

    }

    @Test
    public void getAnswersTest() throws Exception {
        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        Messaging m2 = new Messaging(morphium, 10, false, true, 10);
        Messaging mTst = new Messaging(morphium, 10, false, true, 10);

        m1.start();
        m2.start();
        mTst.start();


        mTst.addListenerForMessageNamed("something else", (msg, m) -> {
            log.info("incoming message??");
            return null;
        });

        m2.addListenerForMessageNamed("q_getAnswer", (msg, m) -> {
            Msg answer = m.createAnswerMsg();
            msg.sendMessage(answer);
            Thread.sleep(1000);
            answer = m.createAnswerMsg();
            return answer;
        });

        Msg m3 = new Msg("not asdf", "will it stuck", "uahh", 10000);
        m3.setPriority(1);
        m1.sendMessage(m3);
        Thread.sleep(1000);

        Msg question = new Msg("q_getAnswer", "question", "a value");
        question.setPriority(5);
        List<Msg> answers = m1.sendAndAwaitAnswers(question, 2, 5000);
        assert (answers != null && !answers.isEmpty());
        assert (answers.size() == 2) : "Got wrong number of answers: " + answers.size();
        for (Msg m : answers) {
            assert (m.getInAnswerTo() != null);
            assert (m.getInAnswerTo().equals(question.getMsgId()));
        }
        m1.terminate();
        m2.terminate();
        mTst.terminate();
    }

    @Test
    public void waitForAnswerTest() throws Exception {

        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        Messaging m2 = new Messaging(morphium, 10, false, true, 10);
        m1.setSenderId("m1");
        m2.setSenderId("m2");
        m1.start();
        m2.start();

        m2.addListenerForMessageNamed("q_wait_for", (msg, m) -> {
            Msg answer = m.createAnswerMsg();
            return answer;
        });

        for (int i = 0; i < 100; i++) {
            log.info("Sending msg " + i);
            Msg question = new Msg("q_wait_for", "question" + i, "a value " + i);
            question.setPriority(5);
            long start = System.currentTimeMillis();
            Msg answer = m1.sendAndAwaitFirstAnswer(question, 2500);
            long dur = System.currentTimeMillis() - start;
            assert (answer != null && answer.getInAnswerTo() != null);
            assert (answer.getInAnswerTo().equals(question.getMsgId()));
            log.info("... ok - took " + dur + " ms");
        }
        try {
            m1.terminate();
            m2.terminate();
        } catch (Exception e) {
        }
    }

    @Test
    public void answerWithoutListener() throws Exception {
        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        Messaging m2 = new Messaging(morphium, 10, false, true, 10);

        m1.start();
        m2.start();

        m2.addListenerForMessageNamed("q_no_listener", (msg, m) -> m.createAnswerMsg());

        m1.sendMessage(new Msg("not asdf", "will it stuck", "uahh", 10000));
        Thread.sleep(1000);

        Msg answer = m1.sendAndAwaitFirstAnswer(new Msg("q_no_listener", "question", "a value"), 5000);
        assert (answer != null);
        m1.terminate();
        m2.terminate();

    }


    @Test
    public void answerTestDifferentType() throws Exception {
        Messaging sender = new Messaging(morphium, 100, true);
        Messaging recipient = new Messaging(morphium, 100, true);
        sender.start();
        recipient.start();
        gotMessage1 = false;
        recipient.addListenerForMessageNamed("query", new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
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
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                gotMessage2 = true;
                assert (m.getInAnswerTo() != null);
                return null;
            }
        });

        sender.sendMessage(new Msg("query", "a query", "avalue"));
        Thread.sleep(1000);
        assert (gotMessage1);
        assert (gotMessage2);

        Msg answer = sender.sendAndAwaitFirstAnswer(new Msg("query", "query", "avalue"), 1000);
        assert (answer != null);
        sender.terminate();
        recipient.terminate();
    }


    @Test(expected = RuntimeException.class)
    public void sendAndWaitforAnswerTestFailing() {
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

    }

    @Test
    public void sendAndWaitforAnswerTest() throws Exception {
//        morphium.dropCollection(Msg.class);
        Messaging sender = new Messaging(morphium, 100, false);
        sender.start();

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
        Thread.sleep(2500);

        Msg answer = sender.sendAndAwaitFirstAnswer(new Msg("test", "Sender", "sent", 15000), 15000);
        assert (answer != null);
        assert (answer.getName().equals("test"));
        assert (answer.getInAnswerTo() != null);
        assert (answer.getRecipients() != null);
        assert (answer.getMsg().equals("got message"));
        m1.terminate();
        sender.terminate();
    }


}
