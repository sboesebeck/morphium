package de.caluga.test.mongo.suite;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MessageRejectedException;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.query.Query;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 17:34
 * <p/>
 */
@SuppressWarnings("Duplicates")
public class MessagingTest extends MongoTest {
    public boolean gotMessage = false;

    public boolean gotMessage1 = false;
    public boolean gotMessage2 = false;
    public boolean gotMessage3 = false;
    public boolean gotMessage4 = false;

    public boolean error = false;

    public MorphiumId lastMsgId;

    public AtomicInteger procCounter = new AtomicInteger(0);

    private List<Msg> list = new ArrayList<>();

    private AtomicInteger queueCount=new AtomicInteger(1000);
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
        assert (messagesList.size() == 1) : "should get annother id - did not?!?!?!" + messagesList.size();

        log.info("Got msg: " + messagesList.get(0).toString());

    }

    @SuppressWarnings("Duplicates")
    @Test
    public void multithreaddingTest() throws Exception {
        Messaging producer = new Messaging(morphium, 500, false);
        // producer.start();
        Thread.sleep(2500);
        for (int i = 0; i < 1000; i++) {
            Msg m = new Msg("test" + i, "tm", "" + i + System.currentTimeMillis(), 10000);
            producer.storeMessage(m);
        }
        Messaging consumer = new Messaging(morphium, 500, false, true, 1000);
        procCounter.set(0);
        consumer.addMessageListener((msg, m) -> {
            //log.info("Got message!");
            procCounter.incrementAndGet();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
            return null;
        });

        consumer.start();

        Thread.sleep(10000);
        consumer.terminate();
        producer.terminate();
        log.info("Messages processed: " + procCounter.get());
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
        Thread.sleep(100);
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


        final Messaging m1 = new Messaging(morphium, 10, true);
        final Messaging m2 = new Messaging(morphium, 10, true);
        final Messaging m3 = new Messaging(morphium, 10, true);
        final Messaging m4 = new Messaging(morphium, 10, true);

        m4.start();
        m1.start();
        m2.start();
        m3.start();
        Thread.sleep(200);

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
        Thread.sleep(500);
        assert (gotMessage2) : "Message not recieved yet by m2?!?!?";
        assert (gotMessage3) : "Message not recieved yet by m3?!?!?";
        assert (gotMessage4) : "Message not recieved yet by m4?!?!?";
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        m2.storeMessage(new Msg("testmsg2", "The message from M2", "Value"));
        Thread.sleep(500);
        assert (gotMessage1) : "Message not recieved yet by m1?!?!?";
        assert (gotMessage3) : "Message not recieved yet by m3?!?!?";
        assert (gotMessage4) : "Message not recieved yet by m4?!?!?";


        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        m1.storeMessage(new Msg("testmsg_excl","This is the message","value",30000,true));
        Thread.sleep(500);
        int cnt=0;
        if (gotMessage1)cnt++;
        if (gotMessage2)cnt++;
        if (gotMessage3)cnt++;
        if (gotMessage4)cnt++;

        assert(cnt!=0):"Message was  not received";
        assert(cnt==1):"Message was received too often: "+cnt;


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
        morphium.clearCollection(Msg.class);
        Messaging sender = null;
        Messaging rec1 = null;
        Messaging rec2 = null;
        try {
            sender = new Messaging(morphium, 100, false);
            rec1 = new Messaging(morphium, 100, false);
            rec2 = new Messaging(morphium, 500, false);

            sender.start();
            rec1.start();
            rec2.start();
            Thread.sleep(2000);
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
                    throw new RuntimeException("Message is not an answer");
                }
                return null;
            });

            sender.storeMessage(new Msg("test", "message", "value"));

            Thread.sleep(1000);
            assert (gotMessage1);
            assert (gotMessage2);
            assert (!gotMessage3);
        } finally {
            sender.terminate();
            rec1.terminate();
            rec2.terminate();
            sender.storeMessage(new Msg("quitting", "quit", "quit", 10, false));
            Thread.sleep(1000);
        }





    }

    @Test
    public void directedMessageTest() throws Exception {
        morphium.clearCollection(Msg.class);
        final Messaging m1;
        final Messaging m2;
        final Messaging m3;
        m1 = new Messaging(morphium, 100, true);
        m2 = new Messaging(morphium, 100, true);
        m3 = new Messaging(morphium, 100, true);
        try {

            m1.start();
            m2.start();
            m3.start();
            Thread.sleep(2500);
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
        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
            Thread.sleep(1000);

        }

    }

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
        } finally {
            m1.terminate();
            m2.terminate();
            onlyAnswers.terminate();
            Thread.sleep(1000);

        }

    }

    @Test
    public void getAnswersTest() throws Exception {
        Messaging m1=new Messaging(morphium, 10, false,true,10);
        Messaging m2=new Messaging(morphium, 10, false,true,10);

        m1.start();
        m2.start();

        m2.addListenerForMessageNamed("question",(msg,m)->{
            Msg answer=m.createAnswerMsg();
            msg.storeMessage(answer);
            Thread.sleep(1000);
            answer=m.createAnswerMsg();
            return answer;
        });

        Msg m3 = new Msg("not asdf", "will it stuck", "uahh", 10000);
        m3.setPriority(1);
        m1.storeMessage(m3);
        Thread.sleep(1000);

        Msg question = new Msg("question", "question", "a value");
        question.setPriority(5);
        List<Msg> answers=m1.sendAndAwaitAnswers(question,2,5000);
        assert(answers!=null&&!answers.isEmpty());
        assert(answers.size()==2);
        for (Msg m:answers){
            assert(m.getInAnswerTo()!=null);
            assert(m.getInAnswerTo().equals(question.getMsgId()));
        }
    }

    @Test
    public void answerWithoutListener() throws Exception {
        Messaging m1=new Messaging(morphium, 10, false,true,10);
        Messaging m2=new Messaging(morphium, 10, false,true,10);

        m1.start();
        m2.start();

        m2.addListenerForMessageNamed("question", (msg, m) -> m.createAnswerMsg());

        m1.storeMessage(new Msg("not asdf","will it stuck","uahh",10000));
        Thread.sleep(1000);

        Msg answer=m1.sendAndAwaitFirstAnswer(new Msg("question","question","a value"),5000);
        assert(answer!=null);
    }


    @Test
    public void massiveMessagingTest() throws Exception {
        List<Messaging> systems;
        systems = new ArrayList<>();
        try {
            int numberOfWorkers = 10;
            int numberOfMessages = 100;
            long ttl = 15000; //15 sec

            final boolean[] failed = {false};
            morphium.clearCollection(Msg.class);

            final Map<MorphiumId, Integer> processedMessages = new Hashtable<>();
            procCounter.set(0);
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
                        if (ids.contains(msg.getSenderId() + "/" + m.getMsgId())) failed[0] = true;
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
                            procCounter.incrementAndGet();
                        }
                        return null;
                    }

                };
                m.addMessageListener(l);
            }
            Thread.sleep(100);

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
            assert (!failed[0]);
            Thread.sleep(1000);
            //See if whole number of messages processed is correct
            //keep in mind: a message is never recieved by the sender, hence numberOfWorkers-1
            while (true) {
                if (procCounter.get() == numberOfMessages * (numberOfWorkers - 1)) {
                    break;
                }
                if (last == procCounter.get()) {
                    log.info("No change in procCounter?! somethings wrong...");
                    break;

                }
                last = procCounter.get();
                log.info("Waiting for messages to be processed - procCounter: " + procCounter.get());
                Thread.sleep(2000);
            }
            assert (!failed[0]);
            Thread.sleep(1000);
            log.info("done");
            assert (!failed[0]);

            assert (processedMessages.size() == numberOfMessages) : "sent " + numberOfMessages + " messages, but only " + processedMessages.size() + " were recieved?";
            for (MorphiumId id : processedMessages.keySet()) {
                assert (processedMessages.get(id) == numberOfWorkers - 1) : "Message " + id + " was not recieved by all " + (numberOfWorkers - 1) + " other workers? only by " + processedMessages.get(id);
            }
            assert (procCounter.get() == numberOfMessages * (numberOfWorkers - 1)) : "Still processing messages?!?!?";

            //Waiting for all messages to be outdated and deleted
        } finally {
            //Stopping all
            for (Messaging m : systems) {
                m.terminate();
            }
            Thread.sleep(1000);
            for (Messaging m : systems) {
                assert (!m.isAlive()) : "Thread still running?";
            }

        }



    }

    @Test
    public void broadcastTest() throws Exception {
        morphium.clearCollection(Msg.class);
        final Messaging m1 = new Messaging(morphium, 1000, true);
        final Messaging m2 = new Messaging(morphium, 10, true);
        final Messaging m3 = new Messaging(morphium, 10, true);
        final Messaging m4 = new Messaging(morphium, 10, true);
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;
        error = false;

        m4.start();
        m1.start();
        m3.start();
        m2.start();
        Thread.sleep(300);

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
        m4.addMessageListener((msg, m) -> {
            gotMessage4 = true;
            if (m.getTo() != null && !m.getTo().contains(m3.getSenderId())) {
                log.error("wrongly received message m4?");
                error = true;
            }
            log.info("M4 got message " + m.toString());
            return null;
        });

        Msg m = new Msg("test", "A message", "a value");
        m.setExclusive(false);
        m1.storeMessage(m);

        Thread.sleep(500);
        assert (!gotMessage1) : "Got message again?";
        assert (gotMessage4) : "m4 did not get msg?";
        assert (gotMessage2) : "m2 did not get msg?";
        assert (gotMessage3) : "m3 did not get msg";
        assert (!error);
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;
        Thread.sleep(500);
        assert (!gotMessage1) : "Got message again?";
        assert (!gotMessage2) : "m2 did get msg again?";
        assert (!gotMessage3) : "m3 did get msg again?";
        assert (!gotMessage4) : "m4 did get msg again?";
        assert (!error);

        m1.terminate();
        m2.terminate();
        m3.terminate();
        m = new Msg("test", "end", "a value");
        m.setExclusive(false);
        m1.storeMessage(m);
        Thread.sleep(1000);

    }

    @Test
    public void messagingSendReceiveThreaddedTest() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(2500);
        final Messaging producer = new Messaging(morphium, 100, true,false,10);
        final Messaging consumer = new Messaging(morphium, 100, true, true, 2000);
        producer.start();
        consumer.start();
        Vector<String> processedIds = new Vector<>();
        procCounter.set(0);
        consumer.addMessageListener((msg, m) -> {
            procCounter.incrementAndGet();
            if (processedIds.contains(m.getMsgId().toString())) {
                log.error("Received msg twice: " + procCounter.get() + "/" + m.getMsgId());
                return null;
            }
            processedIds.add(m.getMsgId().toString());
            //simulate processing
            try {
                Thread.sleep((long) (100 * Math.random()));
            } catch (InterruptedException e) {

            }
            return null;
        });
        Thread.sleep(2500);
        int amount=1000;
        log.info("------------- sending messages");
        for (int i =0;i<amount;i++){
            producer.storeMessage(new Msg("Test "+i,"msg "+i,"value "+i));
        }

        for (int i = 0; i < 30 && procCounter.get() < amount; i++) {
            Thread.sleep(1000);
            log.info("Still processing: " + procCounter.get());
        }
        producer.terminate();
        consumer.terminate();
        assert (procCounter.get() == amount) : "Did process " + procCounter.get();

        Thread.sleep(1000);

    }


    @Test
    public void messagingSendReceiveTest() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(100);
        final Messaging producer = new Messaging(morphium, 100, true);
        final Messaging consumer = new Messaging(morphium, 10, true);
        producer.start();
        consumer.start();
        Thread.sleep(2500);
        final int[] processed={0};
        final Vector<String> messageIds = new Vector<>();
        consumer.addMessageListener((msg, m) -> {
            processed[0]++;
            if (processed[0] % 50 == 1) {
                log.info(processed[0] + "... Got Message " + m.getName() + " / " + m.getMsg() + " / " + m.getValue());
            }
            assert (!messageIds.contains(m.getMsgId().toString())) : "Duplicate message: " + processed[0];
            messageIds.add(m.getMsgId().toString());
            //simulate processing
            try {
                Thread.sleep((long) (10 * Math.random()));
            } catch (InterruptedException e) {

            }
            return null;
        });

        int amount=1000;

        for (int i =0;i<amount;i++){
            producer.storeMessage(new Msg("Test "+i,"msg "+i,"value "+i));
        }

        for (int i =0; i<30 && processed[0]<amount;i++) {
            log.info("Still processing: " + processed[0]);
            Thread.sleep(1000);
        }
        assert(processed[0]==amount):"Did process "+processed[0];

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
        Thread.sleep(2500);
        final int[] processed = {0};
        final Map<String, Long> msgCountById = new Hashtable<>();
        consumer.addMessageListener((msg, m) -> {
            synchronized (processed) {
                processed[0]++;
            }
            if (processed[0] % 1000 == 0) {
                log.info("Consumed " + processed[0]);
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
        Messaging sender = null;
        Messaging sender2 = null;
        Messaging m1 = null;
        Messaging m2 = null;
        Messaging m3 = null;
        try {
            morphium.dropCollection(Msg.class);
            morphium.dropCollection(Msg.class, "test", null);
            morphium.dropCollection(Msg.class, "test2", null);
            sender = new Messaging(morphium, "test", 100, false);
            sender.start();
            sender2 = new Messaging(morphium, "test2", 100, false);
            sender2.start();

            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;

            m1 = new Messaging(morphium, "test", 100, false);
            m1.addMessageListener((msg, m) -> {
                gotMessage1 = true;
                log.info("Got message m1");
                return null;
            });
            m2 = new Messaging(morphium, "test", 100, false);
            m2.addMessageListener((msg, m) -> {
                gotMessage2 = true;
                log.info("Got message m2");
                return null;
            });
            m3 = new Messaging(morphium, "test2", 100, false);
            m3.addMessageListener((msg, m) -> {
                gotMessage3 = true;
                log.info("Got message m3");
                return null;
            });
            Messaging m4 = new Messaging(morphium, "test2", 100, false);
            m4.addMessageListener((msg, m) -> {
                gotMessage4 = true;
                log.info("Got message m4");
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

            assert (!gotMessage3);
            assert (!gotMessage4);
            Thread.sleep(2000);

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
        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
            sender.terminate();
            sender2.terminate();

        }
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
        Thread.sleep(100);


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
        assert (rec == 1):"rec is "+rec;

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
        Thread.sleep(2500);
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
        Thread.sleep(1500);
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
        Thread.sleep(2500);
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
    public void testPausingUnpausingInListenerMultithreadded() throws Exception {
        testPausingUnpausingInListener(true);
    }

    @Test
    public void testPausingUnpausingInListenerSinglethreadded() throws Exception {
        testPausingUnpausingInListener(false);
    }

    private void testPausingUnpausingInListener(boolean multithreadded) throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(1000);
        Messaging sender = new Messaging(morphium, 100, false);
        sender.start();
        Thread.sleep(2500);
        log.info("Sender ID: "+sender.getSenderId());

        gotMessage1=false;
        gotMessage2=false;

        Messaging m1 = new Messaging(morphium, 10, false, multithreadded, 10);
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
        log.info("receiver id: "+m1.getSenderId());

        log.info("Testing with non-exclusive messages");
        Msg m = new Msg("test", "test1", "test", 3000000);
        m.setExclusive(false);
        sender.storeMessage(m);

        m = new Msg("test", "test2", "test", 3000000);
        m.setExclusive(false);
        sender.storeMessage(m);

        Thread.sleep(200);
        assert(!gotMessage1);
        assert(!gotMessage2);

        Thread.sleep(5200);
        assert(gotMessage1);
        assert(gotMessage2);

        log.info("... done!");
        log.info("Testing with exclusive messages...");


        gotMessage1=gotMessage2=false;

        m = new Msg("test", "test1", "test", 3000000);
        m.setExclusive(true);
        sender.storeMessage(m);

        m = new Msg("test", "test2", "test", 3000000);
        m.setExclusive(true);
        sender.storeMessage(m);
        Thread.sleep(200);
        assert(!gotMessage1);
        assert(!gotMessage2);

        Thread.sleep(5000);
        assert(gotMessage1);
        assert(gotMessage2);

        sender.terminate();
        m1.terminate();

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
        Messaging sender = null;
        Messaging m1 = null;
        try {
            morphium.dropCollection(Msg.class);
            Thread.sleep(1000);
            sender = new Messaging(morphium, 100, false);
            sender.start();
            Thread.sleep(2500);
            log.info("Sender ID: " + sender.getSenderId());

            gotMessage1 = false;
            gotMessage2 = false;
            boolean[] fail = {false};
            m1 = new Messaging(morphium, 100, true, multithreadded, 10);
            m1.addListenerForMessageNamed("test", (msg, m) -> {
                msg.pauseProcessingOfMessagesNamed("test");

                try {
                    assert (m.isExclusive());
                    //                assert (m.getReceivedBy().contains(msg.getSenderId()));
                    log.info("Incoming message " + m.getMsgId() + "/" + m.getMsg() + " from " + m.getSender() + " my id: " + msg.getSenderId());
                    Thread.sleep(1000);
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
            log.info("receiver id: " + m1.getSenderId());


            log.info("Testing with exclusive messages...");


            gotMessage1 = gotMessage2 = false;
            assert (!fail[0]);
            Msg m = new Msg("test", "test1", "test", 3000000);
            m.setExclusive(true);
            sender.storeMessage(m);
            assert (!fail[0]);

            m = new Msg("test", "test2", "test", 3000000);
            m.setExclusive(true);
            sender.storeMessage(m);
            Thread.sleep(200);
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
        morphium.getConfig().setThreadPoolMessagingCoreSize(5);
        log.info("Max threadpool:" + morphium.getConfig().getThreadPoolMessagingCoreSize());
        Thread.sleep(1000);
        Messaging sender = new Messaging(morphium, 100, false, true, 10);
        sender.start();

        list.clear();
        Messaging receiver = new Messaging(morphium, 100, false, true, 10);
        receiver.addMessageListener((msg, m) -> {
            log.info("Incoming message...");
            list.add(m);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {

            }

            return null;
        });
        receiver.start();
        Thread.sleep(100);
        sender.storeMessage(new Msg("test", "test", "test"));
        sender.storeMessage(new Msg("test", "test", "test"));
        Thread.sleep(1000);

        assert (list.size() == 2) : "Size wrong: " + list.size();

        sender.terminate();
        receiver.terminate();

    }


    @Test
    public void priorityPausedMessagingTest() throws Exception {
        Messaging sender = new Messaging(morphium, 100, false);
        sender.start();
        final AtomicInteger count = new AtomicInteger();

        list.clear();
        Messaging receiver = new Messaging(morphium, 10, false, true, 100);
        receiver.start();
        Thread.sleep(100);

        receiver.addListenerForMessageNamed("pause", (msg, m) -> {
            msg.pauseProcessingOfMessagesNamed(m.getName());
            log.info("Incoming paused message: prio " + m.getPriority() + "  timestamp: " + m.getTimestamp());
            Thread.sleep(250);
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
            sender.storeMessage(m);
            //Throtteling for first message to wait for pausing
            if (i == 0) Thread.sleep(25);
            if (i % 2 == 0) {
                sender.storeMessage(new Msg("now", "now", "now"));
            }
        }
        Thread.sleep(200);
        assert (count.get() == 10) : "Count wrong " + count.get();
        assert (list.size() < 5);
        Thread.sleep(5200);
        assert (list.size() == 20);

        list.remove(0); //prio of first two is random

        int lastPrio = -1;

        for (Msg m : list) {
            log.info("Msg: " + m.getPriority());
            assert (m.getPriority() >= lastPrio);
            lastPrio = m.getPriority();
        }

    }


    @Test
    public void priorityTest() throws Exception {
        Messaging sender = new Messaging(morphium, 100, false);
        sender.start();

        list.clear();
        //if running multithreadded, the execution order might differ a bit because of the concurrent
        //execution - hence if set to multithreadded, the test will fail!
        Messaging receiver = new Messaging(morphium, 10, false, false, 100);

        receiver.addMessageListener((msg, m) -> {
            log.info("Incoming message: prio " + m.getPriority() + "  timestamp: " + m.getTimestamp());
            list.add(m);
            return null;
        });

        for (int i = 0; i < 10; i++) {
            Msg m = new Msg("test", "test", "test");
            m.setPriority((int) (1000.0 * Math.random()));
            log.info("Stored prio: " + m.getPriority());
            sender.storeMessage(m);
        }

        Thread.sleep(1000);
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


        receiver.pauseProcessingOfMessagesNamed("test");
        list.clear();
        for (int i = 0; i < 10; i++) {
            Msg m = new Msg("test", "test", "test");
            m.setPriority((int) (10000.0 * Math.random()));
            log.info("Stored prio: " + m.getPriority());
            sender.storeMessage(m);
        }

        Thread.sleep(1000);
        receiver.unpauseProcessingOfMessagesNamed("test");
        receiver.findAndProcessPendingMessages("test");
        while (list.size() < 10) {
            Thread.yield();
        }

        lastValue = -888888;

        for (Msg m : list) {
            log.info("prio: " + m.getPriority());
            assert (m.getPriority() >= lastValue);
            lastValue = m.getPriority();
        }


        sender.terminate();
        receiver.terminate();
    }


    @Test
    public void unpausingTest() throws Exception {
        list.clear();
        final AtomicInteger cnt=new AtomicInteger(0);
        Messaging sender = new Messaging(morphium, 100, false);
        sender.start();

        Messaging receiver=new Messaging(morphium,10,false,true,10);
        receiver.start();

        Thread.sleep(1000);
        receiver.addListenerForMessageNamed("pause", (msg, m) -> {
            msg.pauseProcessingOfMessagesNamed("pause");
            log.info("Processing pause  message");
            Thread.sleep(2000);
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

        sender.storeMessage(new Msg("now","now","now"));
        Thread.sleep(500);
        assert(list.size()==1);

        sender.storeMessage(new Msg("pause","pause","pause"));
        sender.storeMessage(new Msg("now","now","now"));
        Thread.sleep(500);
        assert(list.size()==2);

        sender.storeMessage(new Msg("pause","pause","pause"));
        sender.storeMessage(new Msg("pause","pause","pause"));
        sender.storeMessage(new Msg("pause","pause","pause"));
        assert(cnt.get()==0):"Count wrong "+cnt.get();
        Thread.sleep(2000);
        assert(cnt.get()==1);
        //1st message processed
        Thread.sleep(2000);
        //Message after unpausing:
        assert(cnt.get()==2):"Count wrong: "+cnt.get();
        sender.storeMessage(new Msg("now","now","now"));
        Thread.sleep(100);
        assert(list.size()==3);
        Thread.sleep(2000);
        //Message after unpausing:
        assert(cnt.get()==3):"Count wrong: "+cnt.get();
    }


    @Test
    public void deleteExclusiveMessageTest() throws Exception {
        Messaging sender = new Messaging(morphium, 100, false);
        sender.start();

        Messaging receiver = new Messaging(morphium, 10, false, true, 10);
        receiver.start();
        Messaging receiver2 = new Messaging(morphium, 10, false, true, 10);
        receiver2.start();


        Thread.sleep(100);
        receiver.addMessageListener((msg, m) -> {
            log.info("R1: Incoming message");
            return null;
        });

        receiver2.addMessageListener((msg, m) -> {
            log.info("R2: Incoming message");
            return null;
        });


        for (int i = 0; i < 100; i++) {
            Msg m = new Msg("test", "test", "value", 3000000, true);
            sender.storeMessage(m);
            if (i==50){
                receiver2.pauseProcessingOfMessagesNamed("test");
            } else if (i==60){
                receiver.pauseProcessingOfMessagesNamed("test");
            } else if (i==80){
                receiver.unpauseProcessingOfMessagesNamed("test");
                receiver.findAndProcessPendingMessages("test");
                receiver2.unpauseProcessingOfMessagesNamed("test");
                receiver2.findAndProcessPendingMessages("test");
            }

        }

        long start=System.currentTimeMillis();
        Query<Msg> q = morphium.createQueryFor(Msg.class).f(Msg.Fields.name).eq("test");
        while (q.countAll()>0) {
            log.info("Count is still: "+q.countAll());
            Thread.sleep(500);
            if (System.currentTimeMillis()-start > 10000){
                break;
            }
        }
        assert (q.countAll() == 0):"Count is wrong";
//

        receiver.terminate();
        sender.terminate();

    }

}
