package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MessageRejectedException;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.query.Query;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 17:34
 * <p/>
 */
public class InMemMessagingTest extends MorphiumInMemTestBase {
    public boolean gotMessage = false;

    public boolean gotMessage1 = false;
    public boolean gotMessage2 = false;
    public boolean gotMessage3 = false;
    public boolean gotMessage4 = false;

    public boolean error = false;

    public MorphiumId lastMsgId;

    public int procCounter = 0;

    private Logger log = LoggerFactory.getLogger(InMemMessagingTest.class);
    private List<Msg> list = new ArrayList<>();

    public InMemMessagingTest() {

    }


    @Test
    public void getAnswersTest() throws Exception {
        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        Messaging m2 = new Messaging(morphium, 10, false, true, 10);

        m1.start();
        m2.start();

        m2.addListenerForMessageNamed("question", (msg, m) -> {
            Msg answer = m.createAnswerMsg();
            msg.storeMessage(answer);
            Thread.sleep(1000);
            answer = m.createAnswerMsg();
            return answer;
        });

        Msg m3 = new Msg("not asdf", "will it stuck", "uahh", 10000);
        m3.setPriority(1);
        m1.storeMessage(m3);
        Thread.sleep(1000);

        Msg question = new Msg("question", "question", "a value");
        question.setPriority(5);
        List<Msg> answers = m1.sendAndAwaitAnswers(question, 2, 5000);
        assert (answers != null && !answers.isEmpty());
        assert (answers.size() == 2);
        for (Msg m : answers) {
            assert (m.getInAnswerTo() != null);
            assert (m.getInAnswerTo().equals(question.getMsgId()));
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

        m3.addListenerForMessageNamed("test", (msg, m) -> {
            log.info("INcoming message");
            return m.createAnswerMsg();
        });

        Msg m = new Msg("test", "important", "value");
        m.setExclusive(true);
        Msg answer = m1.sendAndAwaitFirstAnswer(m, 6000);
        Thread.sleep(500);
        assert (answer != null);
        assert (answer.getProcessedBy().size() == 1);
        assert (answer.getProcessedBy().contains("m3"));
    }


    @Test
    public void ignoringMessagesTest() throws Exception {
        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        m1.setSenderId("m1");
        Messaging m2 = new Messaging(morphium, 10, false, true, 10);
        m2.setSenderId("m2");
        m1.start();
        m2.start();

        Msg m = new Msg("test", "ignore me please", "value");
        m1.storeMessage(m);
        Thread.sleep(1000);
        m = morphium.reread(m);
        assert (m.getProcessedBy().size() == 1) : "wrong number of proccessed by entries: " + m.getProcessedBy().size();
    }

    @Test
    public void severalMessagingsTest() throws Exception {
        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        m1.setSenderId("m1");
        Messaging m2 = new Messaging(morphium, 10, false, true, 10);
        m2.setSenderId("m2");
        Messaging m3 = new Messaging(morphium, 10, false, true, 10);
        m3.setSenderId("m3");
        m1.start();
        m2.start();
        m3.start();

        m3.addListenerForMessageNamed("test", (msg, m) -> {
            //log.info("Got message: "+m.getName());
            log.info("Sending answer for " + m.getMsgId());
            return new Msg("test", "answer", "value", 600000);
        });

        procCounter = 0;
        for (int i = 0; i < 180; i++) {
            new Thread() {
                public void run() {
                    Msg m = new Msg("test", "nothing", "value");
                    m.setTtl(60000000);
                    Msg a = m1.sendAndAwaitFirstAnswer(m, 6000);
                    assert (a != null);
                    procCounter++;
                }
            }.start();

        }
        while (procCounter < 150) {
            Thread.yield();
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

    }

    @Test
    public void waitForAnswerTest() throws Exception {
        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        Messaging m2 = new Messaging(morphium, 10, false, true, 10);
        m1.setSenderId("m1");
        m2.setSenderId("m2");
        m1.start();
        m2.start();

        m2.addListenerForMessageNamed("question", (msg, m) -> {
            Msg answer = m.createAnswerMsg();
            return answer;
        });
        Thread.sleep(1000);
        for (int i = 0; i < 100; i++) {
            log.info("Sending msg " + i);
            Msg question = new Msg("question", "question" + i, "a value " + i);
            question.setPriority(5);
            long start = System.currentTimeMillis();
            Msg answer = m1.sendAndAwaitFirstAnswer(question, 1500);
            long dur = System.currentTimeMillis() - start;
            assert (answer != null && answer.getInAnswerTo() != null);
            assert (answer.getInAnswerTo().equals(question.getMsgId()));
            log.info("... ok - took " + dur + " ms");
        }
    }




    @Test
    public void answerWithoutListener() throws Exception {
        Messaging m1 = new Messaging(morphium, 100, false, true, 10);
        Messaging m2 = new Messaging(morphium, 100, false, true, 10);
        try {
            m1.start();
            m2.start();

            m2.addListenerForMessageNamed("question", (msg, m) -> {
                return m.createAnswerMsg();
            });

            Msg stuck = new Msg("not asdf", "will it stuck", "uahh", 10000);
            stuck.setPriority(0);
            m1.storeMessage(stuck);

            Msg answer = m1.sendAndAwaitFirstAnswer(new Msg("question", "question", "a value"), 5000);
            assert (answer != null);
        } finally {
            m1.terminate();
            m2.terminate();
        }
    }


    @Test
    public void testMsgQueName() throws Exception {
        morphium.dropCollection(Msg.class);
        morphium.dropCollection(Msg.class, "mmsg_msg2", null);

        Messaging m = new Messaging(morphium, 500, true);
        m.addMessageListener((msg, m1) -> {
            gotMessage1 = true;
            return null;
        });
        m.start();

        Messaging m2 = new Messaging(morphium, "msg2", 500, true);
        m2.addMessageListener((msg, m1) -> {
            gotMessage2 = true;
            return null;
        });
        m2.start();

        Msg msg = new Msg("tst", "msg", "value", 30000);
        msg.setExclusive(false);
        m.storeMessage(msg);
        Thread.sleep(1);
        Query<Msg> q = morphium.createQueryFor(Msg.class);
        assert (q.countAll() == 1);
        q.setCollectionName("mmsg_msg2");
        assert (q.countAll() == 0);

        msg = new Msg("tst2", "msg", "value", 30000);
        msg.setExclusive(false);
        m2.storeMessage(msg);
        q = morphium.createQueryFor(Msg.class);
        assert (q.countAll() == 1);
        q.setCollectionName("mmsg_msg2");
        assert (q.countAll() == 1) : "Count is " + q.countAll();

        Thread.sleep(4000);
        assert (!gotMessage1);
        assert (!gotMessage2);
        m.terminate();
        m2.terminate();
        Thread.sleep(1000);
        assert (!m.isAlive());
        assert (!m2.isAlive());

    }

    @Test
    public void testMsgLifecycle() throws Exception {
        Msg m = new Msg();
        m.setSender("Meine wunderbare ID " + System.currentTimeMillis());
        m.setMsgId(new MorphiumId());
        m.setName("A name");
        morphium.store(m);
        Thread.sleep(5000);

        assert (m.getTimestamp() > 0) : "Timestamp not updated?";

    }


    @Test
    public void messageQueueTest() {
        morphium.clearCollection(Msg.class);
        String id = "meine ID";


        Msg m = new Msg("name", "Msgid1", "value", 5000);
        m.setSender(id);
        m.setExclusive(true);
        morphium.store(m);

        Query<Msg> q = morphium.createQueryFor(Msg.class);
        //        morphium.remove(q);
        //locking messages...
        q = q.f(Msg.Fields.sender).ne(id).f(Msg.Fields.lockedBy).eq(null).f(Msg.Fields.processedBy).ne(id);
        morphium.set(q, Msg.Fields.lockedBy, id);

        q = q.q();
        q = q.f(Msg.Fields.lockedBy).eq(id);
        q.sort(Msg.Fields.timestamp);

        List<Msg> messagesList = q.asList();
        assert (messagesList.isEmpty()) : "Got my own message?!?!?!" + messagesList.get(0).toString();

        m = new Msg("name", "msgid2", "value", 5000);
        m.setSender("sndId2");
        m.setExclusive(true);
        morphium.store(m);

        q = morphium.createQueryFor(Msg.class);
        //locking messages...
        q = q.f(Msg.Fields.sender).ne(id).f(Msg.Fields.lockedBy).eq(null).f(Msg.Fields.processedBy).ne(id);
        morphium.set(q, Msg.Fields.lockedBy, id);

        q = q.q();
        q = q.f(Msg.Fields.lockedBy).eq(id);
        q.sort(Msg.Fields.timestamp);

        messagesList = q.asList();
        assert (messagesList.size() == 1) : "should get annother id - did not?!?!?! " + messagesList.size();

        log.info("Got msg: " + messagesList.get(0).toString());

    }

    @Test
    public void multithreaddingTest() throws Exception {
        Messaging producer = new Messaging(morphium, 500, false);
        producer.start();
        for (int i = 0; i < 1000; i++) {
            Msg m = new Msg("test" + i, "tm", "" + i + System.currentTimeMillis(), 10000);
            producer.storeMessage(m);
        }
        final int[] count = {0};
        Messaging consumer = new Messaging(morphium, 500, false, true, 1000);
        consumer.addMessageListener((msg, m) -> {
            //log.info("Got message!");
            count[0]++;
            return null;
        });

        consumer.start();

        Thread.sleep(10000);
        consumer.terminate();
        producer.terminate();
        log.info("Messages processed: " + count[0]);
        log.info("Messages left: " + consumer.getMessageCount());

    }


    @Test
    public void messagingTest() throws Exception {
        error = false;

        morphium.dropCollection(Msg.class);
        Thread.sleep(500);

        final Messaging messaging = new Messaging(morphium, 500, true);
        messaging.start();

        messaging.addMessageListener((msg, m) -> {
            log.info("Got Message: " + m.toString());
            gotMessage = true;
            return null;
        });
        messaging.storeMessage(new Msg("Testmessage", "A message", "the value - for now", 5000));

        Thread.sleep(1000);
        assert (!gotMessage) : "Message recieved from self?!?!?!";
        log.info("Dig not get own message - cool!");

        Msg m = new Msg("meine Message", "The Message", "value is a string", 5000);
        m.setMsgId(new MorphiumId());
        m.setSender("Another sender");

        morphium.store(m);

        Thread.sleep(5000);
        assert (gotMessage) : "Message did not come?!?!?";

        gotMessage = false;
        Thread.sleep(5000);
        assert (!gotMessage) : "Got message again?!?!?!";

        messaging.terminate();
        Thread.sleep(1000);
        assert (!messaging.isAlive()) : "Messaging still running?!?";
    }


    @Test
    public void systemTest() throws Exception {
        morphium.dropCollection(Msg.class);
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;
        error = false;

        morphium.clearCollection(Msg.class);
        final Messaging m1 = new Messaging(morphium, 500, true);
        final Messaging m2 = new Messaging(morphium, 500, true);
        m1.start();
        m2.start();

        m1.addMessageListener((msg, m) -> {
            gotMessage1 = true;
            log.info("M1 got message " + m.toString());
            if (!m.getSender().equals(m2.getSenderId())) {
                log.error("Sender is not M2?!?!? m2_id: " + m2.getSenderId() + " - message sender: " + m.getSender());
                error = true;
            }
            return null;
        });

        m2.addMessageListener((msg, m) -> {
            gotMessage2 = true;
            log.info("M2 got message " + m.toString());
            if (!m.getSender().equals(m1.getSenderId())) {
                log.error("Sender is not M1?!?!? m1_id: " + m1.getSenderId() + " - message sender: " + m.getSender());
                error = true;
            }
            return null;
        });

        m1.storeMessage(new Msg("testmsg1", "The message from M1", "Value"));
        Thread.sleep(1000);
        assert (gotMessage2) : "Message not recieved yet?!?!?";
        gotMessage2 = false;

        m2.storeMessage(new Msg("testmsg2", "The message from M2", "Value"));
        Thread.sleep(1000);
        assert (gotMessage1) : "Message not recieved yet?!?!?";
        gotMessage1 = false;
        assert (!error);
        m1.terminate();
        m2.terminate();
        Thread.sleep(1000);
        assert (!m1.isAlive()) : "m1 still running?";
        assert (!m2.isAlive()) : "m2 still running?";

    }

    @Test
    public void severalSystemsTest() throws Exception {
        morphium.clearCollection(Msg.class);
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;
        error = false;


        final Messaging m1 = new Messaging(morphium, 100, true);
        final Messaging m2 = new Messaging(morphium, 100, true);
        final Messaging m3 = new Messaging(morphium, 100, true);
        final Messaging m4 = new Messaging(morphium, 100, true);

        m1.start();
        m2.start();
        m3.start();
        m4.start();

        m1.addMessageListener((msg, m) -> {
            gotMessage1 = true;
            log.info("M1 got message " + m.toString());
            return null;
        });

        m2.addMessageListener((msg, m) -> {
            gotMessage2 = true;
            log.info("M2 got message " + m.toString());
            return null;
        });

        m3.addMessageListener((msg, m) -> {
            gotMessage3 = true;
            log.info("M3 got message " + m.toString());
            return null;
        });

        m4.addMessageListener((msg, m) -> {
            gotMessage4 = true;
            log.info("M4 got message " + m.toString());
            return null;
        });

        m1.storeMessage(new Msg("testmsg1", "The message from M1", "Value"));
        Thread.sleep(5000);
        assert (gotMessage2) : "Message not recieved yet by m2?!?!?";
        assert (gotMessage3) : "Message not recieved yet by m3?!?!?";
        assert (gotMessage4) : "Message not recieved yet by m4?!?!?";
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        m2.storeMessage(new Msg("testmsg2", "The message from M2", "Value"));
        Thread.sleep(5000);
        assert (gotMessage1) : "Message not recieved yet by m1?!?!?";
        assert (gotMessage3) : "Message not recieved yet by m3?!?!?";
        assert (gotMessage4) : "Message not recieved yet by m4?!?!?";


        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        m1.storeMessage(new Msg("testmsg_excl", "This is the message", "value", 30000, true));
        Thread.sleep(5000);
        int cnt = 0;
        if (gotMessage1) cnt++;
        if (gotMessage2) cnt++;
        if (gotMessage3) cnt++;
        if (gotMessage4) cnt++;

        assert (cnt != 0) : "Message was  not received";
        assert (cnt == 1) : "Message was received too often: " + cnt;


        m1.terminate();
        m2.terminate();
        m3.terminate();
        m4.terminate();
        Thread.sleep(2000);
        assert (!m1.isAlive()) : "M1 still running";
        assert (!m2.isAlive()) : "M2 still running";
        assert (!m3.isAlive()) : "M3 still running";
        assert (!m4.isAlive()) : "M4 still running";


    }


    @Test
    public void testRejectMessage() throws Exception {
        morphium.dropCollection(Msg.class);
        final Messaging sender = new Messaging(morphium, 100, false);
        final Messaging rec1 = new Messaging(morphium, 100, false);
        final Messaging rec2 = new Messaging(morphium, 500, false);

        sender.start();
        rec1.start();
        rec2.start();
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;

        rec1.addMessageListener((msg, m) -> {
            gotMessage1 = true;
            throw new MessageRejectedException("rejected", true, false);
        });
        rec2.addMessageListener((msg, m) -> {
            gotMessage2 = true;
            log.info("Processing message " + m.getValue());
            return null;
        });
        sender.addMessageListener((msg, m) -> {
            gotMessage3 = true;
            log.info("Receiver got message");
            if (m.getInAnswerTo() == null) {
                log.error("Message is not an answer! ERROR!");
                throw new RuntimeException("Error");
            }
            return null;
        });

        sender.storeMessage(new Msg("test", "message", "value"));

        Thread.sleep(1000);
        assert (gotMessage1);
        assert (gotMessage2);
        assert (!gotMessage3);


        sender.terminate();
        rec1.terminate();
        rec2.terminate();
        Thread.sleep(2000);


    }

    @Test
    public void directedMessageTest() throws Exception {
        morphium.clearCollection(Msg.class);
        final Messaging m1 = new Messaging(morphium, 100, true);
        final Messaging m2 = new Messaging(morphium, 100, true);
        final Messaging m3 = new Messaging(morphium, 100, true);

        m1.start();
        m2.start();
        m3.start();
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        log.info("m1 ID: " + m1.getSenderId());
        log.info("m2 ID: " + m2.getSenderId());
        log.info("m3 ID: " + m3.getSenderId());

        m1.addMessageListener((msg, m) -> {
            gotMessage1 = true;
            if (m.getTo() != null && !m.getTo().contains(m1.getSenderId())) {
                log.error("wrongly received message?");
                error = true;
            }
            log.info("DM-M1 got message " + m.toString());
            //                assert (m.getSender().equals(m2.getSenderId())) : "Sender is not M2?!?!? m2_id: " + m2.getSenderId() + " - message sender: " + m.getSender();
            return null;
        });

        m2.addMessageListener((msg, m) -> {
            gotMessage2 = true;
            assert (m.getTo() == null || m.getTo().contains(m2.getSenderId())) : "wrongly received message?";
            log.info("DM-M2 got message " + m.toString());
            //                assert (m.getSender().equals(m1.getSenderId())) : "Sender is not M1?!?!? m1_id: " + m1.getSenderId() + " - message sender: " + m.getSender();
            return null;
        });

        m3.addMessageListener((msg, m) -> {
            gotMessage3 = true;
            assert (m.getTo() == null || m.getTo().contains(m3.getSenderId())) : "wrongly received message?";
            log.info("DM-M3 got message " + m.toString());
            //                assert (m.getSender().equals(m1.getSenderId())) : "Sender is not M1?!?!? m1_id: " + m1.getSenderId() + " - message sender: " + m.getSender();
            return null;
        });

        //sending message to all
        log.info("Sending broadcast message");
        m1.storeMessage(new Msg("testmsg1", "The message from M1", "Value"));
        Thread.sleep(3000);
        assert (gotMessage2) : "Message not recieved yet by m2?!?!?";
        assert (gotMessage3) : "Message not recieved yet by m3?!?!?";
        assert (!error);
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        error = false;
        waitForWrites();
        Thread.sleep(2500);
        assert (!gotMessage1) : "Message recieved again by m1?!?!?";
        assert (!gotMessage2) : "Message recieved again by m2?!?!?";
        assert (!gotMessage3) : "Message recieved again by m3?!?!?";
        assert (!error);

        log.info("Sending direct message");
        Msg m = new Msg("testmsg1", "The message from M1", "Value");
        m.addRecipient(m2.getSenderId());
        m1.storeMessage(m);
        Thread.sleep(1000);
        assert (gotMessage2) : "Message not received by m2?";
        assert (!gotMessage1) : "Message recieved by m1?!?!?";
        assert (!gotMessage3) : "Message  recieved again by m3?!?!?";
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        error = false;
        Thread.sleep(1000);
        assert (!gotMessage1) : "Message recieved again by m1?!?!?";
        assert (!gotMessage2) : "Message not recieved again by m2?!?!?";
        assert (!gotMessage3) : "Message not recieved again by m3?!?!?";
        assert (!error);

        log.info("Sending message to 2 recipients");
        log.info("Sending direct message");
        m = new Msg("testmsg1", "The message from M1", "Value");
        m.addRecipient(m2.getSenderId());
        m.addRecipient(m3.getSenderId());
        m1.storeMessage(m);
        Thread.sleep(1000);
        assert (gotMessage2) : "Message not received by m2?";
        assert (!gotMessage1) : "Message recieved by m1?!?!?";
        assert (gotMessage3) : "Message not recieved by m3?!?!?";
        assert (!error);
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;

        Thread.sleep(1000);
        assert (!gotMessage1) : "Message recieved again by m1?!?!?";
        assert (!gotMessage2) : "Message not recieved again by m2?!?!?";
        assert (!gotMessage3) : "Message not recieved again by m3?!?!?";
        assert (!error);

        m1.terminate();
        m2.terminate();
        m3.terminate();
        Thread.sleep(1000);
    }

    @Test
    public void answeringTest() throws Exception {
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        error = false;

        morphium.clearCollection(Msg.class);
        final Messaging m1 = new Messaging(morphium, 100, true);
        final Messaging m2 = new Messaging(morphium, 100, true);
        final Messaging onlyAnswers = new Messaging(morphium, 100, true);

        m1.start();
        m2.start();
        onlyAnswers.start();

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

            log.info("M3 got answer " + m.toString());
            assert (lastMsgId != null) : "Last message == null?";
            assert (m.getInAnswerTo().equals(lastMsgId)) : "Wrong answer????" + lastMsgId.toString() + " != " + m.getInAnswerTo().toString();
            //                assert (m.getSender().equals(m1.getSenderId())) : "Sender is not M1?!?!? m1_id: " + m1.getSenderId() + " - message sender: " + m.getSender();
            return null;
        });

        Msg question = new Msg("QMsg", "This is the message text", "A question param");
        question.setMsgId(new MorphiumId());
        lastMsgId = question.getMsgId();
        onlyAnswers.storeMessage(question);

        log.info("Send Message with id: " + question.getMsgId());
        Thread.sleep(3000);
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

        m1.terminate();
        m2.terminate();
        onlyAnswers.terminate();
        Thread.sleep(1000);
    }


    @Test
    public void massiveMessagingTest() throws Exception {
        int numberOfWorkers = 10;
        int numberOfMessages = 100;
        long ttl = 15000; //15 sec


        morphium.clearCollection(Msg.class);
        List<Messaging> systems = new ArrayList<>();

        final Map<MorphiumId, Integer> processedMessages = new Hashtable<>();

        for (int i = 0; i < numberOfWorkers; i++) {
            //creating messaging instances
            Messaging m = new Messaging(morphium, 100, true);
            m.start();
            systems.add(m);
            MessageListener l = new MessageListener() {
                Messaging msg;
                List<String> ids = Collections.synchronizedList(new ArrayList<>());

                @Override
                public Msg onMessage(Messaging msg, Msg m) {
                    assert (!ids.contains(msg.getSenderId() + "/" + m.getMsgId())) : "Re-getting message?!?!? " + m.getMsgId() + " MyId: " + msg.getSenderId();
                    ids.add(msg.getSenderId() + "/" + m.getMsgId());
                    assert (m.getTo() == null || m.getTo().contains(msg.getSenderId())) : "got message not for me?";
                    assert (!m.getSender().equals(msg.getSenderId())) : "Got message from myself?";
                    synchronized (processedMessages) {
                        Integer pr = processedMessages.get(m.getMsgId());
                        if (pr == null) {
                            pr = 0;
                        }
                        processedMessages.put(m.getMsgId(), pr + 1);
                        procCounter++;
                    }
                    return null;
                }

            };
            m.addMessageListener(l);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfMessages; i++) {
            int m = (int) (Math.random() * systems.size());
            Msg msg = new Msg("test" + i, "The message for msg " + i, "a value", ttl);
            msg.addAdditional("Additional Value " + i);
            msg.setExclusive(false);
            systems.get(m).storeMessage(msg);
        }

        long dur = System.currentTimeMillis() - start;
        log.info("Queueing " + numberOfMessages + " messages took " + dur + " ms - now waiting for writes..");
        waitForWrites();
        log.info("...all messages persisted!");
        int last = 0;
        Thread.sleep(1000);
        //See if whole number of messages processed is correct
        //keep in mind: a message is never recieved by the sender, hence numberOfWorkers-1
        while (true) {
            if (procCounter == numberOfMessages * (numberOfWorkers - 1)) {
                break;
            }
            if (last == procCounter) {
                log.info("No change in procCounter?! somethings wrong...");
                break;

            }
            last = procCounter;
            log.info("Waiting for messages to be processed - procCounter: " + procCounter);
            Thread.sleep(2000);
        }
        Thread.sleep(1000);
        log.info("done");

        assert (processedMessages.size() == numberOfMessages) : "sent " + numberOfMessages + " messages, but only " + processedMessages.size() + " were recieved?";
        for (MorphiumId id : processedMessages.keySet()) {
            assert (processedMessages.get(id) == numberOfWorkers - 1) : "Message " + id + " was not recieved by all " + (numberOfWorkers - 1) + " other workers? only by " + processedMessages.get(id);
        }
        assert (procCounter == numberOfMessages * (numberOfWorkers - 1)) : "Still processing messages?!?!?";

        //Waiting for all messages to be outdated and deleted

        //Stopping all
        for (Messaging m : systems) {
            m.terminate();
        }
        Thread.sleep(1000);
        for (Messaging m : systems) {
            assert (!m.isAlive()) : "Thread still running?";
        }


    }

    @Test
    public void broadcastTest() throws Exception {
        morphium.clearCollection(Msg.class);
        final Messaging m1 = new Messaging(morphium, 1000, true);
        final Messaging m2 = new Messaging(morphium, 1000, true);
        final Messaging m3 = new Messaging(morphium, 1000, true);
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;
        error = false;

        m1.start();
        m2.start();
        m3.start();

        log.info("m1 ID: " + m1.getSenderId());
        log.info("m2 ID: " + m2.getSenderId());
        log.info("m3 ID: " + m3.getSenderId());

        m1.addMessageListener((msg, m) -> {
            gotMessage1 = true;
            if (m.getTo() != null && m.getTo().contains(m1.getSenderId())) {
                log.error("wrongly received message m1?");
                error = true;
            }
            log.info("M1 got message " + m.toString());
            return null;
        });

        m2.addMessageListener((msg, m) -> {
            gotMessage2 = true;
            if (m.getTo() != null && !m.getTo().contains(m2.getSenderId())) {
                log.error("wrongly received message m2?");
                error = true;
            }
            log.info("M2 got message " + m.toString());
            return null;
        });

        m3.addMessageListener((msg, m) -> {
            gotMessage3 = true;
            if (m.getTo() != null && !m.getTo().contains(m3.getSenderId())) {
                log.error("wrongly received message m3?");
                error = true;
            }
            log.info("M3 got message " + m.toString());
            return null;
        });

        Msg m = new Msg("test", "A message", "a value");
        m.setExclusive(false);
        m1.storeMessage(m);

        Thread.sleep(1200);
        assert (!gotMessage1) : "Got message again?";
        assert (gotMessage2) : "m2 did not get msg?";
        assert (gotMessage3) : "m3 did not get msg";
        assert (!error);
        gotMessage2 = false;
        gotMessage3 = false;
        Thread.sleep(1200);
        assert (!gotMessage1) : "Got message again?";
        assert (!gotMessage2) : "m2 did get msg again?";
        assert (!gotMessage3) : "m3 did get msg again?";
        assert (!error);

        m1.terminate();
        m2.terminate();
        m3.terminate();
        Thread.sleep(1000);
    }

    @Test
    public void messagingSendReceiveThreaddedTest() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(100);
        final Messaging producer = new Messaging(morphium, 100, true, false, 10);
        final Messaging consumer = new Messaging(morphium, 10, true, true, 10);
        producer.start();
        consumer.start();
        final int[] processed = {0};
        consumer.addMessageListener((msg, m) -> {
            processed[0]++;
//                log.info("Got message...");
            //simulate processing
            try {
                Thread.sleep((long) (100 * Math.random()));
            } catch (InterruptedException e) {

            }
            return null;
        });

        int amount = 100;

        for (int i = 0; i < amount; i++) {
            producer.storeMessage(new Msg("Test " + i, "msg " + i, "value " + i));
        }

        for (int i = 0; i < 30 && processed[0] < amount; i++) {
            Thread.sleep(1000);
            log.info("Still processing: " + processed[0]);
        }
        assert (processed[0] == amount) : "Did process " + processed[0];

        producer.terminate();
        consumer.terminate();
        Thread.sleep(1000);

    }


    @Test
    public void messagingSendReceiveTest() throws Exception {
        morphium.clearCollection(Msg.class);
        final Messaging producer = new Messaging(morphium, 100, true);
        final Messaging consumer = new Messaging(morphium, 10, true);
        producer.start();
        consumer.start();
        final int[] processed = {0};
        consumer.addMessageListener((msg, m) -> {
//                log.info("Got Message "+m.getName()+" / "+m.getMsg()+" / "+m.getValue());
            processed[0]++;
            //simulate processing
            try {
                Thread.sleep((long) (10 * Math.random()));
            } catch (InterruptedException e) {

            }
            return null;
        });

        int amount = 1000;

        for (int i = 0; i < amount; i++) {
            producer.storeMessage(new Msg("Test " + i, "msg " + i, "value " + i));
        }

        for (int i = 0; i < 30 && processed[0] < amount; i++) {
            Thread.sleep(1000);
            log.info("Still processing: " + processed[0]);
        }
        assert (processed[0] == amount) : "Did process " + processed[0];

        producer.terminate();
        consumer.terminate();
        Thread.sleep(1000);

    }


    @Test
    public void mutlithreaddedMessagingPerformanceTest() throws Exception {
        morphium.clearCollection(Msg.class);
        final Messaging producer = new Messaging(morphium, 100, true);
        final Messaging consumer = new Messaging(morphium, 10, true, true, 2000);
        consumer.start();
        producer.start();
        final int[] processed = {0};
        final Map<String, Long> msgCountById = new Hashtable<>();
        consumer.addMessageListener((msg, m) -> {
            synchronized (processed) {
                processed[0]++;
            }
            if (processed[0] % 100 == 0) {
                log.info("Processed: " + processed[0]);
            }
            assert (!m.getProcessedBy().contains(msg.getSenderId()));
            //                assert(!msgCountById.containsKey(m.getMsgId().toString()));
            msgCountById.put(m.getMsgId().toString(), 1L);
            //simulate processing
            try {
                Thread.sleep((long) (10 * Math.random()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        });

        int numberOfMessages = 1000;
        for (int i = 0; i < numberOfMessages; i++) {
            Msg m = new Msg("msg", "m", "v");
            m.setTtl(5 * 60 * 1000);
            if (i % 1000 == 0) {
                log.info("created msg " + i + " / " + numberOfMessages);
            }
            producer.storeMessage(m);
        }

        long start = System.currentTimeMillis();

        while (processed[0] < numberOfMessages) {
            //            ThreadMXBean thbean = ManagementFactory.getThreadMXBean();
            //            log.info("Running threads: " + thbean.getThreadCount());
            log.info("Processed " + processed[0]);
            Thread.sleep(1500);
        }
        long dur = System.currentTimeMillis() - start;
        log.info("Processing took " + dur + " ms");
        producer.terminate();
        consumer.terminate();
        log.info("Waitingh for threads to finish");
        Thread.sleep(1000);


    }


    @Test
    public void exclusiveMessageCustomQueueTest() throws Exception {
        morphium.dropCollection(Msg.class);
        morphium.dropCollection(Msg.class, "test", null);
        morphium.dropCollection(Msg.class, "test2", null);
        Messaging sender = new Messaging(morphium, "test", 100, false);
        sender.start();
        Messaging sender2 = new Messaging(morphium, "test2", 100, false);
        sender2.start();

        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        Messaging m1 = new Messaging(morphium, "test", 100, false);
        m1.addMessageListener((msg, m) -> {
            gotMessage1 = true;
            return null;
        });
        Messaging m2 = new Messaging(morphium, "test", 100, false);
        m2.addMessageListener((msg, m) -> {
            gotMessage2 = true;
            return null;
        });
        Messaging m3 = new Messaging(morphium, "test2", 100, false);
        m3.addMessageListener((msg, m) -> {
            gotMessage3 = true;
            return null;
        });
        Messaging m4 = new Messaging(morphium, "test2", 100, false);
        m4.addMessageListener((msg, m) -> {
            gotMessage4 = true;
            return null;
        });

        m1.start();
        m2.start();
        m3.start();
        m4.start();

        Msg m = new Msg();
        m.setExclusive(true);
        m.setName("A message");

        sender.queueMessage(m);
        Thread.sleep(5000);

        assert (!gotMessage3);
        assert (!gotMessage4);

        int rec = 0;
        if (gotMessage1) {
            rec++;
        }
        if (gotMessage2) {
            rec++;
        }
        assert (rec == 1) : "rec is " + rec;

        gotMessage1 = false;
        gotMessage2 = false;

        m = new Msg();
        m.setExclusive(true);
        m.setName("A message");
        sender2.storeMessage(m);
        Thread.sleep(500);
        assert (!gotMessage1);
        assert (!gotMessage2);

        rec = 0;
        if (gotMessage3) {
            rec++;
        }
        if (gotMessage4) {
            rec++;
        }
        assert (rec == 1) : "rec is " + rec;
        assert (m1.getNumberOfMessages() == 0);
        m1.terminate();
        m2.terminate();
        m3.terminate();
        sender.terminate();
        sender2.terminate();
    }

    @Test
    public void exclusiveMessageTest() throws Exception {
        morphium.dropCollection(Msg.class);
        Messaging sender = new Messaging(morphium, 100, false);
        sender.start();

        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        Messaging m1 = new Messaging(morphium, 100, false);
        m1.addMessageListener((msg, m) -> {
            gotMessage1 = true;
            return null;
        });
        Messaging m2 = new Messaging(morphium, 100, false);
        m2.addMessageListener((msg, m) -> {
            gotMessage2 = true;
            return null;
        });
        Messaging m3 = new Messaging(morphium, 100, false);
        m3.addMessageListener((msg, m) -> {
            gotMessage3 = true;
            return null;
        });

        m1.start();
        m2.start();
        m3.start();

        Msg m = new Msg();
        m.setExclusive(true);
        m.setName("A message");

        sender.queueMessage(m);
        Thread.sleep(5000);

        int rec = 0;
        if (gotMessage1) {
            rec++;
        }
        if (gotMessage2) {
            rec++;
        }
        if (gotMessage3) {
            rec++;
        }
        assert (rec == 1) : "rec is " + rec;

        assert (m1.getNumberOfMessages() == 0);
        m1.terminate();
        m2.terminate();
        m3.terminate();
        sender.terminate();


    }

    @Test
    public void selfMessages() throws Exception {
        morphium.dropCollection(Msg.class);
        Messaging sender = new Messaging(morphium, 100, false);
        sender.start();
        sender.addMessageListener(((msg, m) -> {
            gotMessage = true;
            log.info("Got message: " + m.getMsg() + "/" + m.getName());
            return null;
        }));
        gotMessage = false;
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

        sender.sendMessageToSelf(new Msg("testmsg", "Selfmessage", "value"));
        Thread.sleep(500);
        assert (gotMessage);
        //noinspection PointlessBooleanExpression
        assert (gotMessage1 == false);

        m1.terminate();
        sender.terminate();
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
    public void sendAndWaitforAnswerTest() {
        morphium.dropCollection(Msg.class);
        Messaging sender = new Messaging(morphium, 100, false);
        sender.setSenderId("sender");
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
        m1.setSenderId("m1");
        m1.start();

        Msg answer = sender.sendAndAwaitFirstAnswer(new Msg("test", "Sender", "sent", 1555000), 1555000);
        assert (answer != null);
        assert (answer.getName().equals("test"));
        assert (answer.getInAnswerTo() != null);
        assert (answer.getRecipient() != null);
        assert (answer.getMsg().equals("got message"));
        m1.terminate();
        sender.terminate();
    }


    @Test
    public void pauseUnpauseProcessingTest() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(1000);
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
    public void getPendingMessagesOnStartup() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(1000);
        Messaging sender = new Messaging(morphium, 100, false);
        sender.start();

        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        Messaging m3 = new Messaging(morphium, 100, false);
        m3.addMessageListener((msg, m) -> {
            gotMessage3 = true;
            return null;
        });

        m3.start();

        Thread.sleep(1500);


        sender.storeMessage(new Msg("test", "testmsg", "testvalue", 120000, false));

        Thread.sleep(1000);
        assert (gotMessage3);
        Thread.sleep(2000);


        Messaging m1 = new Messaging(morphium, 100, false);
        m1.addMessageListener((msg, m) -> {
            gotMessage1 = true;
            return null;
        });

        m1.start();

        Thread.sleep(1500);
        assert (gotMessage1);


        Messaging m2 = new Messaging(morphium, 100, false);
        m2.addMessageListener((msg, m) -> {
            gotMessage2 = true;
            return null;
        });

        m2.start();

        Thread.sleep(1500);
        assert (gotMessage2);


        m1.terminate();
        m2.terminate();
        m3.terminate();
        sender.terminate();


    }


    @Test
    public void testPausingUnpausingInListener() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(1000);
        Messaging sender = new Messaging(morphium, 100, false);
        sender.start();

        log.info("Sender ID: " + sender.getSenderId());

        gotMessage1 = false;
        gotMessage2 = false;

        Messaging m1 = new Messaging(morphium, 100, true, true, 10);
        m1.addListenerForMessageNamed("test", (msg, m) -> {
            msg.pauseProcessingOfMessagesNamed("test");
            try {
                log.info("Incoming message " + m.getMsg() + " my id: " + msg.getSenderId());
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
    public void waitingForMessagesIfNonMultithreadded() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(1000);
        Messaging sender = new Messaging(morphium, 100, false, false, 10);
        sender.start();

        list.clear();
        Messaging receiver = new Messaging(morphium, 100, false, false, 10);
        receiver.addMessageListener((msg, m) -> {
            list.add(m);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {

            }

            return null;
        });
        receiver.start();
        Thread.sleep(500);
        sender.storeMessage(new Msg("test", "test", "test"));
        sender.storeMessage(new Msg("test", "test", "test"));

        Thread.sleep(500);
        assert (list.size() == 1) : "Size wrong: " + list.size();
        Thread.sleep(2200);
        assert (list.size() == 2);

        sender.terminate();
        receiver.terminate();

    }

    @Test
    public void waitingForMessagesIfMultithreadded() throws Exception {
        morphium.dropCollection(Msg.class);
        morphium.getConfig().setThreadPoolMessagingCoreSize(15);
        log.info("Max threadpool:" + morphium.getConfig().getThreadPoolMessagingCoreSize());
        Thread.sleep(1000);
        Messaging sender = new Messaging(morphium, 100, false, true, 10);
        //sender.start();

        list.clear();
        Messaging receiver = new Messaging(morphium, 100, true, true, 10);
        receiver.addMessageListener((msg, m) -> {

            log.info("Incoming message..." + m.getMsgId().toString() + " processed by: " + m.getProcessedBy().size() + "/" + m.getProcessedBy().get(0));
            list.add(m);
            try {
                Thread.sleep(12000);
            } catch (InterruptedException e) {

            }
            log.info("Processing finshed");
            return null;
        });
        receiver.start();
        Thread.sleep(100);
        log.info("Store 1");
        sender.storeMessage(new Msg("test", "test", "test"));
        log.info("Store 2");
        sender.storeMessage(new Msg("test", "test", "test"));

        long start = System.currentTimeMillis();
        while (list.size() < 2) {
            Thread.sleep(1000);
            assert System.currentTimeMillis() - start <= 5555000 || (list.size() == 2);
        }

        sender.terminate();
        receiver.terminate();

    }


    @Test
    public void priorityTest() throws Exception {
        Messaging sender;
        Messaging receiver;

        morphium.dropCollection(Msg.class);
        Thread.sleep(1000);
        sender = new Messaging(morphium, 1000, false);
        sender.start();

        list.clear();

        receiver = new Messaging(morphium, 100, false);
        receiver.addMessageListener((msg, m) -> {
            assert (!list.contains(m));
            list.add(m);
            return null;
        });
        try {
            for (int i = 0; i < 10; i++) {
                Msg m = new Msg("test", "test", "test");
                m.setPriority((int) (1000.0 * Math.random()));
                log.info("Stored prio: " + m.getPriority());
                sender.storeMessage(m);
            }


            receiver.start();

            while (list.size() < 10) {
                Thread.yield();
            }

            int lastValue = -888888;

            for (Msg m : list) {
                log.info("prio: " + m.getPriority());
                assert (m.getPriority() >= lastValue);
                lastValue = m.getPriority();
            }


            list.clear();
            receiver.pauseProcessingOfMessagesNamed("test");
            Thread.sleep(100);
            for (int i = 0; i < 10; i++) {
                Msg m = new Msg("test", "test", "test");
                m.setPriority((int) (10000.0 * Math.random()));
                log.info("Stored prio: " + m.getPriority());
                sender.storeMessage(m);
            }

            Thread.sleep(100);
            receiver.unpauseProcessingOfMessagesNamed("test");
            while (list.size() < 10) {
                Thread.yield();
            }

            lastValue = -888888;

            for (Msg m : list) {
                log.info("prio: " + m.getPriority());
                assert (m.getPriority() >= lastValue);
                lastValue = m.getPriority();
            }
        } finally {

            sender.terminate();
            receiver.terminate();

        }


    }

}
