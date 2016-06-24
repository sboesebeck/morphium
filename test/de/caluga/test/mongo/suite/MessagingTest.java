package de.caluga.test.mongo.suite;

import de.caluga.morphium.driver.bson.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MsgType;
import de.caluga.morphium.query.Query;
import org.junit.Test;

import java.util.*;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 17:34
 * <p/>
 */
public class MessagingTest extends MongoTest {
    public boolean gotMessage = false;

    public boolean gotMessage1 = false;
    public boolean gotMessage2 = false;
    public boolean gotMessage3 = false;
    public boolean gotMessage4 = false;

    public boolean error = false;

    public MorphiumId lastMsgId;

    public int procCounter = 0;

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

        Msg msg = new Msg("tst", MsgType.MULTI, "msg", "value", 30000);
        msg.setExclusive(false);
        m.storeMessage(msg);
        Thread.sleep(1);
        Query<Msg> q = morphium.createQueryFor(Msg.class);
        assert (q.countAll() == 1);
        q.setCollectionName("mmsg_msg2");
        assert (q.countAll() == 0);

        msg = new Msg("tst2", MsgType.MULTI, "msg", "value", 30000);
        msg.setExclusive(false);
        m2.storeMessage(msg);
        q = morphium.createQueryFor(Msg.class);
        assert (q.countAll() == 1);
        q.setCollectionName("mmsg_msg2");
        assert (q.countAll() == 1) : "Count is " + q.countAll();

        Thread.sleep(4000);
        assert (!gotMessage1);
        assert (!gotMessage2);
        m.setRunning(false);
        m2.setRunning(false);
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
        assert (m.getType().equals(MsgType.SINGLE)) : "Default should be single?";

    }


    @Test
    public void messageQueueTest() throws Exception {
        morphium.clearCollection(Msg.class);
        String id = "meine ID";


        Msg m = new Msg("name", MsgType.SINGLE, "Msgid1", "value", 5000);
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

        m = new Msg("name", MsgType.SINGLE, "msgid2", "value", 5000);
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
        assert (messagesList.size() == 1) : "should get annother id - did not?!?!?!" + messagesList.size();

        log.info("Got msg: " + messagesList.get(0).toString());

    }

    @Test
    public void multithreaddingTest() throws Exception {
        Messaging producer = new Messaging(morphium, 500, false);
        producer.start();
        for (int i = 0; i < 1000; i++) {
            Msg m = new Msg("test" + i, MsgType.SINGLE, "tm", "" + i + System.currentTimeMillis(), 10000);
            producer.storeMessage(m);
        }
        final int[] count = {0};
        Messaging consumer = new Messaging(morphium, 500, false, true, 1000);
        consumer.addMessageListener((msg, m) -> {
            log.info("Got message!");
            count[0]++;
            return null;
        });

        consumer.start();

        Thread.sleep(10000);
        consumer.setRunning(false);
        producer.setRunning(false);
        log.info("Messages processed: " + count[0]);
        log.info("Messages left: " + consumer.getMessageCount());

    }


    @Test
    public void messagingTest() throws Exception {
        error = false;

        morphium.clearCollection(Msg.class);

        final Messaging messaging = new Messaging(morphium, 500, true);
        messaging.start();

        messaging.addMessageListener((msg, m) -> {
            log.info("Got Message: " + m.toString());
            gotMessage = true;
            return null;
        });
        messaging.storeMessage(new Msg("Testmessage", MsgType.MULTI, "A message", "the value - for now", 5000));

        Thread.sleep(1000);
        assert (!gotMessage) : "Message recieved from self?!?!?!";
        log.info("Dig not get own message - cool!");

        Msg m = new Msg("meine Message", MsgType.SINGLE, "The Message", "value is a string", 5000);
        m.setMsgId(new MorphiumId());
        m.setSender("Another sender");

        morphium.store(m);

        Thread.sleep(5000);
        assert (gotMessage) : "Message did not come?!?!?";

        gotMessage = false;
        Thread.sleep(5000);
        assert (!gotMessage) : "Got message again?!?!?!";

        messaging.setRunning(false);
        Thread.sleep(1000);
        assert (!messaging.isAlive()) : "Messaging still running?!?";
    }


    @Test
    public void systemTest() throws Exception {
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
        m1.setRunning(false);
        m2.setRunning(false);
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
        m1.setRunning(false);
        m2.setRunning(false);
        m3.setRunning(false);
        m4.setRunning(false);
        Thread.sleep(1000);
        assert (!m1.isAlive()) : "M1 still running";
        assert (!m2.isAlive()) : "M2 still running";
        assert (!m3.isAlive()) : "M3 still running";
        assert (!m4.isAlive()) : "M4 still running";


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

        m1.setRunning(false);
        m2.setRunning(false);
        m3.setRunning(false);
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

        m1.setRunning(false);
        m2.setRunning(false);
        onlyAnswers.setRunning(false);
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
            Msg msg = new Msg("test" + i, MsgType.MULTI, "The message for msg " + i, "a value", ttl);
            msg.addAdditional("Additional Value " + i);
            msg.setExclusive(false);
            systems.get(m).storeMessage(msg);
        }

        long dur = System.currentTimeMillis() - start;
        log.info("Queueing " + numberOfMessages + " messages took " + dur + " ms - now waiting for writes..");
        waitForWrites();
        log.info("...all messages persisted!");
        int last = 0;
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
            m.setRunning(false);
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

        m1.setRunning(false);
        m2.setRunning(false);
        m3.setRunning(false);
        Thread.sleep(1000);
    }


    @Test
    public void messagingPerformanceTest() throws Exception {
        morphium.clearCollection(Msg.class);
        final Messaging producer = new Messaging(morphium, 100, true);
        final Messaging consumer = new Messaging(morphium, 10, true);
        final int[] processed = {0};
        consumer.addMessageListener((msg, m) -> {
            processed[0]++;
            if (processed[0] % 1000 == 0) {
                log.info("Processed: " + processed[0]);
            }
            //simulate processing
            try {
                Thread.sleep((long) (10 * Math.random()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        });

        int numberOfMessages = 10000;
        for (int i = 0; i < numberOfMessages; i++) {
            Msg m = new Msg("msg", "m", "v");
            m.setTtl(5 * 60 * 1000);
            if (i % 1000 == 0) {
                log.info("created msg " + i + " / " + numberOfMessages);
            }
            producer.storeMessage(m);
        }
        log.info("Start message processing....");
        long start = System.currentTimeMillis();
        consumer.start();
        while (processed[0] < numberOfMessages) {
            //            ThreadMXBean thbean = ManagementFactory.getThreadMXBean();
            //            log.info("Running threads: " + thbean.getThreadCount());
            Thread.sleep(15);
        }
        long dur = System.currentTimeMillis() - start;
        log.info("Processing took " + dur + " ms");
        producer.setRunning(false);
        consumer.setRunning(false);
        Thread.sleep(1000);
    }


    @Test
    public void mutlithreaddedMessagingPerformanceTest() throws Exception {
        morphium.clearCollection(Msg.class);
        final Messaging producer = new Messaging(morphium, 100, true);
        final Messaging consumer = new Messaging(morphium, 10, true, true, 2000);
        final int[] processed = {0};
        final Map<String, Long> msgCountById = new Hashtable<>();
        consumer.addMessageListener((msg, m) -> {
            synchronized (processed) {
                processed[0]++;
            }
            if (processed[0] % 1000 == 0) {
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

        int numberOfMessages = 10000;
        for (int i = 0; i < numberOfMessages; i++) {
            Msg m = new Msg("msg", "m", "v");
            m.setTtl(5 * 60 * 1000);
            if (i % 1000 == 0) {
                log.info("created msg " + i + " / " + numberOfMessages);
            }
            producer.storeMessage(m);
        }
        log.info("Start message processing....");
        long start = System.currentTimeMillis();
        consumer.start();
        while (processed[0] < numberOfMessages) {
            //            ThreadMXBean thbean = ManagementFactory.getThreadMXBean();
            //            log.info("Running threads: " + thbean.getThreadCount());
            log.info("Processed " + processed[0]);
            Thread.sleep(1500);
        }
        long dur = System.currentTimeMillis() - start;
        log.info("Processing took " + dur + " ms");
        producer.setRunning(false);
        consumer.setRunning(false);
        log.info("Waitingh for threads to finish");
        Thread.sleep(1000);


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
        Thread.sleep(1000);

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
        assert (rec == 1);

        assert (m1.getNumberOfMessages() == 0);
        m1.setRunning(false);
        m2.setRunning(false);
        m3.setRunning(false);


    }
}
