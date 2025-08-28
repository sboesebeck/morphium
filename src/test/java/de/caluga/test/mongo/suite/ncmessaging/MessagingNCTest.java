package de.caluga.test.mongo.suite.ncmessaging;

import de.caluga.morphium.*;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.*;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 17:34
 * <p/>
 */
@SuppressWarnings("ALL")
@Disabled
public class MessagingNCTest extends MorphiumTestBase {
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
    public void testMsgQueName() throws Exception {
        morphium.dropCollection(Msg.class);
        morphium.dropCollection(Msg.class, "mmsg_msg2", null);

        StdMessaging m = new StdMessaging(morphium, 100, true);
        m.addListenerForTopic("test", (msg, m1) -> {
            gotMessage1 = true;
            return null;
        });
        m.setUseChangeStream(false).start();

        StdMessaging m2 = new StdMessaging(morphium, "msg2", 100, true);
        m2.addListenerForTopic("test", (msg, m1) -> {
            gotMessage2 = true;
            return null;
        });
        m2.setUseChangeStream(false).start();
        try {
            Msg msg = new Msg("test", "msg", "value", 30000);
            msg.setExclusive(false);
            m.sendMessage(msg);
            Thread.sleep(200);
            Query<Msg> q = morphium.createQueryFor(Msg.class);
            assert (q.countAll() == 1) : "Count wrong: " + q.countAll() + " - should be 1!";
            q.setCollectionName(m2.getCollectionName());
            assert (q.countAll() == 0);

            msg = new Msg("test", "msg", "value", 30000);
            msg.setExclusive(false);
            m2.sendMessage(msg);
            Thread.sleep(600);
            q = morphium.createQueryFor(Msg.class);
            assert (q.countAll() == 1);
            q.setCollectionName("mmsg_msg2");
            assert (q.countAll() == 1) : "Count is " + q.countAll();

            Thread.sleep(4000);
            assert (!gotMessage1);
            assert (!gotMessage2);
        } finally {
            m.terminate();
            m2.terminate();
        }

    }

    @Test
    public void testMsgLifecycle() throws Exception {
        Msg m = new Msg();
        m.setSender("Meine wunderbare ID " + System.currentTimeMillis());
        m.setMsgId(new MorphiumId());
        m.setTopic("A name");
        morphium.store(m);
        Thread.sleep(5000);
        assert (m.getTimestamp() > 0) : "Timestamp not updated?";

    }

    @SuppressWarnings("Duplicates")
    @Test
    public void multithreaddingTestSingle() throws Exception {
        int amount = 65;
        StdMessaging producer = new StdMessaging(morphium, 500, false);
        producer.start();
        for (int i = 0; i < amount; i++) {
            if (i % 10 == 0) {
                log.info("Messages sent: " + i);
            }
            Msg m = new Msg("test", "tm", "" + i + System.currentTimeMillis(), 30000);
            producer.sendMessage(m);
        }
        final AtomicInteger count = new AtomicInteger();
        StdMessaging consumer = new StdMessaging(morphium, 100, false, true, 1000);
        consumer.addListenerForTopic("test", (msg, m) -> {
//            log.info("Got message!");
            count.incrementAndGet();
            return null;
        });
        long start = System.currentTimeMillis();
        consumer.setUseChangeStream(false).start();
        while (count.get() < amount) {
            log.info("Messages processed: " + count.get());
            Thread.sleep(1000);
            if (System.currentTimeMillis() - start > 20000) throw new RuntimeException("Timeout");
        }
        long dur = System.currentTimeMillis() - start;
        log.info("processing " + amount + " multithreaded but single messages took " + dur + "ms == " + (amount / (dur / 1000)) + " msg/sec");

        consumer.terminate();
        producer.terminate();

    }


    @Test
    public void mutlithreaddingTestMultiple() throws Exception {
        int amount = 650;
        StdMessaging producer = new StdMessaging(morphium, 500, false);
        producer.start();
        log.info("now multithreadded and multiprocessing");
        for (int i = 0; i < amount; i++) {
            if (i % 10 == 0) {
                log.info("Messages sent: " + i);
            }
            Msg m = new Msg("test", "tm", "" + i + System.currentTimeMillis(), 30000);
            producer.sendMessage(m);
        }
        final AtomicInteger count = new AtomicInteger();
        count.set(0);
        StdMessaging consumer = new StdMessaging(morphium, 100, true, true, 100);
        consumer.addListenerForTopic("test", (msg, m) -> {
//            log.info("Got message!");
            count.incrementAndGet();
            return null;
        });
        long start = System.currentTimeMillis();
        consumer.setUseChangeStream(false).start();
        while (count.get() < amount) {
            log.info("Messages processed: " + count.get());
            Thread.sleep(1000);
            if (System.currentTimeMillis() - start > 20000) throw new RuntimeException("Timeout!");
        }
        long dur = System.currentTimeMillis() - start;
        log.info("processing 2500 multithreaded and multiprocessing messages took " + dur + "ms == " + (2500 / (dur / 1000)) + " msg/sec");


        consumer.terminate();
        producer.terminate();
        log.info("Messages processed: " + count.get());
        log.info("Messages left: " + consumer.getPendingMessagesCount());
    }

    @Test
    public void messagingTest() throws Exception {
        error = false;

        morphium.dropCollection(Msg.class);

        final StdMessaging messaging = new StdMessaging(morphium, 100, true);
        try {
            messaging.setUseChangeStream(false).start();
            Thread.sleep(500);

            messaging.addListenerForTopic("test", (msg, m) -> {
                log.info("Got Message: " + m.toString());
                gotMessage = true;
                return null;
            });
            messaging.sendMessage(new Msg("test", "A message", "the value - for now", 5000000));

            Thread.sleep(1000);
            assert (!gotMessage) : "Message recieved from self?!?!?!";
            log.info("Dig not get own message - cool!");

            Msg m = new Msg("test", "The Message", "value is a string", 5000000);
            m.setMsgId(new MorphiumId());
            m.setSender("Another sender");

            morphium.store(m, messaging.getCollectionName(), null);

            long start = System.currentTimeMillis();
            while (!gotMessage) {
                Thread.sleep(100);
                assert (System.currentTimeMillis() - start < 5000) : " Message did not come?!?!?";
            }
            assert (gotMessage);
            gotMessage = false;
            Thread.sleep(200);
            assert (!gotMessage) : "Got message again?!?!?!";
        } finally {
            messaging.terminate();
            Thread.sleep(200);
            assert (!messaging.isAlive()) : "Messaging still running?!?";
        }


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
        final StdMessaging m1 = new StdMessaging(morphium, 100, true);
        final StdMessaging m2 = new StdMessaging(morphium, 100, true);
        try {
            m1.setUseChangeStream(false).start();
            m2.setUseChangeStream(false).start();
            Thread.sleep(100);
            m1.addListenerForTopic("test", (msg, m) -> {
                gotMessage1 = true;
                log.info("M1 got message " + m.toString());
                if (!m.getSender().equals(m2.getSenderId())) {
                    log.error("Sender is not M2?!?!? m2_id: " + m2.getSenderId() + " - message sender: " + m.getSender());
                    error = true;
                }
                return null;
            });

            m2.addListenerForTopic("test", (msg, m) -> {
                gotMessage2 = true;
                log.info("M2 got message " + m.toString());
                if (!m.getSender().equals(m1.getSenderId())) {
                    log.error("Sender is not M1?!?!? m1_id: " + m1.getSenderId() + " - message sender: " + m.getSender());
                    error = true;
                }
                return null;
            });

            m1.sendMessage(new Msg("test", "The message from M1", "Value"));
            Thread.sleep(1000);
            assert (gotMessage2) : "Message not recieved yet?!?!?";
            gotMessage2 = false;

            m2.sendMessage(new Msg("test", "The message from M2", "Value"));
            Thread.sleep(1000);
            assert (gotMessage1) : "Message not recieved yet?!?!?";
            gotMessage1 = false;
            assert (!error);
        } finally {
            m1.terminate();
            m2.terminate();
            Thread.sleep(200);
            assert (!m1.isAlive()) : "m1 still running?";
            assert (!m2.isAlive()) : "m2 still running?";
        }


    }

    @Test
    public void severalSystemsTest() throws Exception {
        morphium.clearCollection(Msg.class);
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;
        error = false;


        final StdMessaging m1 = new StdMessaging(morphium, 10, true);
        final StdMessaging m2 = new StdMessaging(morphium, 10, true);
        final StdMessaging m3 = new StdMessaging(morphium, 10, true);
        final StdMessaging m4 = new StdMessaging(morphium, 10, true);

        try {
            m4.setUseChangeStream(false).start();
            m1.setUseChangeStream(false).start();
            m2.setUseChangeStream(false).start();
            m3.setUseChangeStream(false).start();
            Thread.sleep(200);

            m1.addListenerForTopic("test", (msg, m) -> {
                gotMessage1 = true;
                log.info("M1 got message " + m.toString());
                return null;
            });

            m2.addListenerForTopic("test", (msg, m) -> {
                gotMessage2 = true;
                log.info("M2 got message " + m.toString());
                return null;
            });

            m3.addListenerForTopic("test", (msg, m) -> {
                gotMessage3 = true;
                log.info("M3 got message " + m.toString());
                return null;
            });

            m4.addListenerForTopic("test", (msg, m) -> {
                gotMessage4 = true;
                log.info("M4 got message " + m.toString());
                return null;
            });

            m1.sendMessage(new Msg("test", "The message from M1", "Value"));
            Thread.sleep(500);
            assert (gotMessage2) : "Message not recieved yet by m2?!?!?";
            assert (gotMessage3) : "Message not recieved yet by m3?!?!?";
            assert (gotMessage4) : "Message not recieved yet by m4?!?!?";
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;

            m2.sendMessage(new Msg("test", "The message from M2", "Value"));
            Thread.sleep(500);
            assert (gotMessage1) : "Message not recieved yet by m1?!?!?";
            assert (gotMessage3) : "Message not recieved yet by m3?!?!?";
            assert (gotMessage4) : "Message not recieved yet by m4?!?!?";


            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;

            m1.sendMessage(new Msg("test", "This is the message", "value", 30000000, true));
            Thread.sleep(500);
            int cnt = 0;
            if (gotMessage1) cnt++;
            if (gotMessage2) cnt++;
            if (gotMessage3) cnt++;
            if (gotMessage4) cnt++;


            Thread.sleep(1000);

            assert (cnt != 0) : "Message was  not received";
            assert (cnt == 1) : "Message was received too often: " + cnt;
        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
            m4.terminate();
            Thread.sleep(200);
            assert (!m1.isAlive()) : "M1 still running";
            assert (!m2.isAlive()) : "M2 still running";
            assert (!m3.isAlive()) : "M3 still running";
            assert (!m4.isAlive()) : "M4 still running";
        }


    }

    @Test
    public void testRejectExclusiveMessage() throws Exception {
        StdMessaging sender = null;
        StdMessaging rec1 = null;
        StdMessaging rec2 = null;
        try {
            sender = new StdMessaging(morphium, 100, false);
            sender.setSenderId("sender");
            rec1 = new StdMessaging(morphium, 100, false);
            rec1.setSenderId("rec1");
            rec2 = new StdMessaging(morphium, 100, false);
            rec2.setSenderId("rec2");
            morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
            Thread.sleep(10);
            sender.setUseChangeStream(false).start();
            rec1.setUseChangeStream(false).start();
            rec2.setUseChangeStream(false).start();
            Thread.sleep(2000);
            final AtomicInteger recFirst = new AtomicInteger(0);

            gotMessage = false;

            rec1.addListenerForTopic("test", (msg, m) -> {
                if (recFirst.get() == 0) {
                    recFirst.set(1);
                    throw new MessageRejectedException("rejected", true, true);
                }
                gotMessage = true;
                return null;
            });
            rec2.addListenerForTopic("test", (msg, m) -> {
                if (recFirst.get() == 0) {
                    recFirst.set(1);
                    throw new MessageRejectedException("rejected", true, true);
                }
                gotMessage = true;
                return null;
            });
            sender.addListenerForTopic("test", (msg, m) -> {
                if (m.getInAnswerTo() == null) {
                    log.error("Message is not an answer! ERROR!");
                    return null;
                } else {
                    log.info("Got answer");
                }
                gotMessage3 = true;
                log.info("Receiver " + m.getSender() + " rejected message");
                return null;
            });


            sender.sendMessage(new Msg("test", "message", "value", 3000000, true));
            TestUtils.waitForConditionToBecomeTrue(5000, "did not getMessage at all!", ()-> gotMessage && gotMessage3);
        } finally {
            sender.terminate();
            rec1.terminate();
            rec2.terminate();
        }


    }


    @Test
    public void testRejectMessage() throws Exception {
        StdMessaging sender = null;
        StdMessaging rec1 = null;
        StdMessaging rec2 = null;
        try {
            sender = new StdMessaging(morphium, 100, false);
            rec1 = new StdMessaging(morphium, 100, false);
            rec2 = new StdMessaging(morphium, 500, false);
            morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
            Thread.sleep(10);
            sender.setUseChangeStream(false).start();
            rec1.setUseChangeStream(false).start();
            rec2.setUseChangeStream(false).start();
            Thread.sleep(2000);
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;

            rec1.addListenerForTopic("test", (msg, m) -> {
                gotMessage1 = true;
                throw new MessageRejectedException("rejected", true, true);
            });
            rec2.addListenerForTopic("test", (msg, m) -> {
                gotMessage2 = true;
                log.info("Processing message " + m.getValue());
                return null;
            });
            sender.addListenerForTopic("test", (msg, m) -> {
                if (m.getInAnswerTo() == null) {
                    log.error("Message is not an answer! ERROR!");
                    return null;
                }
                gotMessage3 = true;
                log.info("Receiver rejected message");
                return null;
            });

            sender.sendMessage(new Msg("test", "message", "value"));

            Thread.sleep(1000);
            assert (gotMessage1);
            assert (gotMessage2);
            assert (gotMessage3);
        } finally {
            sender.terminate();
            rec1.terminate();
            rec2.terminate();
        }


    }

    @Test
    public void directedMessageTest() throws Exception {
        morphium.clearCollection(Msg.class);
        final StdMessaging m1;
        final StdMessaging m2;
        final StdMessaging m3;
        m1 = new StdMessaging(morphium, 100, true);
        m2 = new StdMessaging(morphium, 100, true);
        m3 = new StdMessaging(morphium, 100, true);
        try {

            m1.setUseChangeStream(false).start();
            m2.setUseChangeStream(false).start();
            m3.setUseChangeStream(false).start();
            Thread.sleep(2500);
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;

            log.info("m1 ID: " + m1.getSenderId());
            log.info("m2 ID: " + m2.getSenderId());
            log.info("m3 ID: " + m3.getSenderId());

            m1.addListenerForTopic("test", (msg, m) -> {
                gotMessage1 = true;
                if (m.getTo() != null && !m.getTo().contains(m1.getSenderId())) {
                    log.error("wrongly received message?");
                    error = true;
                }
                log.info("DM-M1 got message " + m.toString());
                //                assert (m.getSender().equals(m2.getSenderId())) : "Sender is not M2?!?!? m2_id: " + m2.getSenderId() + " - message sender: " + m.getSender();
                return null;
            });

            m2.addListenerForTopic("test", (msg, m) -> {
                gotMessage2 = true;
                assert (m.getTo() == null || m.getTo().contains(m2.getSenderId())) : "wrongly received message?";
                log.info("DM-M2 got message " + m.toString());
                //                assert (m.getSender().equals(m1.getSenderId())) : "Sender is not M1?!?!? m1_id: " + m1.getSenderId() + " - message sender: " + m.getSender();
                return null;
            });

            m3.addListenerForTopic("test", (msg, m) -> {
                gotMessage3 = true;
                assert (m.getTo() == null || m.getTo().contains(m3.getSenderId())) : "wrongly received message?";
                log.info("DM-M3 got message " + m.toString());
                //                assert (m.getSender().equals(m1.getSenderId())) : "Sender is not M1?!?!? m1_id: " + m1.getSenderId() + " - message sender: " + m.getSender();
                return null;
            });

            //sending message to all
            log.info("Sending broadcast message");
            m1.sendMessage(new Msg("test", "The message from M1", "Value"));
            Thread.sleep(3000);
            assert (gotMessage2) : "Message not recieved yet by m2?!?!?";
            assert (gotMessage3) : "Message not recieved yet by m3?!?!?";
            assert (!error);
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            error = false;
            TestUtils.waitForWrites(morphium, log);
            Thread.sleep(2500);
            assert (!gotMessage1) : "Message recieved again by m1?!?!?";
            assert (!gotMessage2) : "Message recieved again by m2?!?!?";
            assert (!gotMessage3) : "Message recieved again by m3?!?!?";
            assert (!error);

            log.info("Sending direct message");
            Msg m = new Msg("test", "The message from M1", "Value");
            m.addRecipient(m2.getSenderId());
            m1.sendMessage(m);
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
            m = new Msg("test", "The message from M1", "Value");
            m.addRecipient(m2.getSenderId());
            m.addRecipient(m3.getSenderId());
            m1.sendMessage(m);
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
    public void ignoringMessagesTest() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(100);
        StdMessaging m1 = new StdMessaging(morphium, 10, false, true, 10);
        m1.setSenderId("m1");
        StdMessaging m2 = new StdMessaging(morphium, 10, false, true, 10);
        m2.setSenderId("m2");
        try {
            m1.setUseChangeStream(false).start();
            m2.setUseChangeStream(false).start();
            Thread.sleep(250);
            Msg m = new Msg("test", "ignore me please", "value");
            m1.sendMessage(m);
            Thread.sleep(1000);
            m = morphium.reread(m);
            assertEquals(0, m.getProcessedBy().size()); //is marked as processed, performance optimization
        } finally {
            m1.terminate();
            m2.terminate();
        }
    }

    @Test
    public void ignoringExclusiveMessagesTest() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(100);
        StdMessaging m1 = new StdMessaging(morphium, 10, false, true, 10);
        m1.setSenderId("m1");
        StdMessaging m2 = new StdMessaging(morphium, 10, false, true, 10);
        m2.setSenderId("m2");
        StdMessaging m3 = new StdMessaging(morphium, 10, false, true, 10);
        m3.setSenderId("m3");
        m3.addListenerForTopic("test", new MessageListener() {
            @Override
            public Msg onMessage(MorphiumMessaging msg, Msg m)  {
                return null;
            }
        });
        try {
            m1.setUseChangeStream(false).start();
            m2.setUseChangeStream(false).start();
            m3.setUseChangeStream(false).start();
            Thread.sleep(250);
            for (int i = 0; i < 10; i++) {
                Msg m = new Msg("test", "ignore me please", "value", 2000, true);
                m1.sendMessage(m);
                Thread.sleep(1000);
                m = morphium.reread(m);
                assertEquals(1, m.getProcessedBy().size());
                assertTrue(m.getProcessedBy().contains("m3"));
            }
        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
        }
    }

    @Test
    public void severalMessagingsTest() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(100);
        StdMessaging m1 = new StdMessaging(morphium, 10, false, true, 10);
        m1.setSenderId("m1");
        StdMessaging m2 = new StdMessaging(morphium, 10, false, true, 10);
        m2.setSenderId("m2");
        StdMessaging m3 = new StdMessaging(morphium, 10, false, true, 10);
        m3.setSenderId("m3");
        m1.setUseChangeStream(false).start();
        m2.setUseChangeStream(false).start();
        m3.setUseChangeStream(false).start();
        try {
            m3.addListenerForTopic("test", (msg, m) -> {
                //log.info("Got message: "+m.getName());
                if (m.getInAnswerTo() != null) {
                    log.error("Got an answer here?");
                }
                log.info("Sending answer for " + m.getMsgId());
                return new Msg("test", "answer", "value", 600000);
            });

            procCounter.set(0);
            for (int i = 0; i < 10; i++) {
                new Thread() {
                    public void run() {
                        Msg m = new Msg("test", "nothing", "value");
                        m.setTtl(60000000);
                        Msg a = m1.sendAndAwaitFirstAnswer(m, 36000);
                        assertNotNull(a);
                        ;
                        procCounter.incrementAndGet();
                    }
                } .start();

            }
            while (procCounter.get() < 10) {
                Thread.sleep(1000);
                log.info("Recieved " + procCounter.get());
            }
        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
        }

    }


    @Test
    public void massiveMessagingTest() throws Exception {
        List<StdMessaging> systems;
        systems = new ArrayList<>();
        try {
            int numberOfWorkers = 20;
            int numberOfMessages = 200;
            long ttl = 150000; //15 sec

            final boolean[] failed = {false};
            morphium.clearCollection(Msg.class);

            final Map<MorphiumId, Integer> processedMessages = new Hashtable<>();
            procCounter.set(0);
            for (int i = 0; i < numberOfWorkers; i++) {
                //creating messaging instances
                StdMessaging m = new StdMessaging(morphium, 100, true);
                m.setUseChangeStream(false).start();
                systems.add(m);
                MessageListener l = new MessageListener() {
                    final List<String> ids = Collections.synchronizedList(new ArrayList<>());
                    StdMessaging msg;

                    @Override
                    public Msg onMessage(MorphiumMessaging msg, Msg m) {
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
                m.addListenerForTopic("test", l);
            }
            Thread.sleep(100);

            long start = System.currentTimeMillis();
            for (int i = 0; i < numberOfMessages; i++) {
                int m = (int) (Math.random() * systems.size());
                Msg msg = new Msg("test", "The message for msg " + i, "a value", ttl);
                msg.addAdditional("Additional Value " + i);
                msg.setExclusive(false);
                systems.get(m).sendMessage(msg);
            }

            long dur = System.currentTimeMillis() - start;
            log.info("Queueing " + numberOfMessages + " messages took " + dur + " ms - now waiting for writes..");
            TestUtils.waitForWrites(morphium, log);
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
                log.info(id + "---- ok!");
                assert (processedMessages.get(id) == numberOfWorkers - 1) : "Message " + id + " was not recieved by all " + (numberOfWorkers - 1) + " other workers? only by " + processedMessages.get(id);
            }
            assert (procCounter.get() == numberOfMessages * (numberOfWorkers - 1)) : "Still processing messages?!?!?";

            //Waiting for all messages to be outdated and deleted
        } finally {
            //Stopping all
            for (StdMessaging m : systems) {
                m.terminate();
            }
            Thread.sleep(1000);
            for (StdMessaging m : systems) {
                assert (!m.isAlive()) : "Thread still running?";
            }

        }


    }


    @Test
    public void broadcastTest() throws Exception {
        morphium.clearCollection(Msg.class);
        final StdMessaging m1 = new StdMessaging(morphium, 1000, true);
        final StdMessaging m2 = new StdMessaging(morphium, 10, true);
        final StdMessaging m3 = new StdMessaging(morphium, 10, true);
        final StdMessaging m4 = new StdMessaging(morphium, 10, true);
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;
        error = false;

        m4.setUseChangeStream(false).start();
        m1.setUseChangeStream(false).start();
        m3.setUseChangeStream(false).start();
        m2.setUseChangeStream(false).start();
        Thread.sleep(300);
        try {
            log.info("m1 ID: " + m1.getSenderId());
            log.info("m2 ID: " + m2.getSenderId());
            log.info("m3 ID: " + m3.getSenderId());

            m1.addListenerForTopic("test", (msg, m) -> {
                gotMessage1 = true;
                if (m.getTo() != null && m.getTo().contains(m1.getSenderId())) {
                    log.error("wrongly received message m1?");
                    error = true;
                }
                log.info("M1 got message " + m.toString());
                return null;
            });

            m2.addListenerForTopic("test", (msg, m) -> {
                gotMessage2 = true;
                if (m.getTo() != null && !m.getTo().contains(m2.getSenderId())) {
                    log.error("wrongly received message m2?");
                    error = true;
                }
                log.info("M2 got message " + m.toString());
                return null;
            });

            m3.addListenerForTopic("test", (msg, m) -> {
                gotMessage3 = true;
                if (m.getTo() != null && !m.getTo().contains(m3.getSenderId())) {
                    log.error("wrongly received message m3?");
                    error = true;
                }
                log.info("M3 got message " + m.toString());
                return null;
            });
            m4.addListenerForTopic("test", (msg, m) -> {
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
            m1.sendMessage(m);

            while (!gotMessage2 || !gotMessage3 || !gotMessage4) {
                Thread.sleep(500);
            }
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
        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
            m4.terminate();
        }
    }

    @Test
    public void messagingSendReceiveThreaddedTest() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(2500);
        final StdMessaging producer = new StdMessaging(morphium, 100, true, false, 10);
        final StdMessaging consumer = new StdMessaging(morphium, 100, true, true, 2000);
        producer.setUseChangeStream(false).start();
        consumer.setUseChangeStream(false).start();
        try {
            Vector<String> processedIds = new Vector<>();
            procCounter.set(0);
            consumer.addListenerForTopic("test", (msg, m) -> {
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
            int amount = 1000;
            log.info("------------- sending messages");
            for (int i = 0; i < amount; i++) {
                producer.sendMessage(new Msg("test", "msg " + i, "value " + i));
            }

            for (int i = 0; i < 30 && procCounter.get() < amount; i++) {
                Thread.sleep(1000);
                log.info("Still processing: " + procCounter.get());
            }
            assert (procCounter.get() == amount) : "Did process " + procCounter.get();
        } finally {
            producer.terminate();
            consumer.terminate();
        }
    }


    @Test
    public void messagingSendReceiveTest() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(100);
        final StdMessaging producer = new StdMessaging(morphium, 100, true);
        final StdMessaging consumer = new StdMessaging(morphium, 10, true);
        producer.setUseChangeStream(false).start();
        consumer.setUseChangeStream(false).start();
        Thread.sleep(2500);
        try {
            final int[] processed = {0};
            final Vector<String> messageIds = new Vector<>();
            consumer.addListenerForTopic("test", (msg, m) -> {
                processed[0]++;
                if (processed[0] % 50 == 1) {
                    log.info(processed[0] + "... Got Message " + m.getTopic() + " / " + m.getMsg() + " / " + m.getValue());
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

            int amount = 1000;

            for (int i = 0; i < amount; i++) {
                producer.sendMessage(new Msg("test", "msg " + i, "value " + i));
            }

            for (int i = 0; i < 30 && processed[0] < amount; i++) {
                log.info("Still processing: " + processed[0]);
                Thread.sleep(1000);
            }
            assert (processed[0] == amount) : "Did process " + processed[0];
        } finally {
            producer.terminate();
            consumer.terminate();
        }
    }


    @Test
    public void mutlithreaddedMessagingPerformanceTest() throws Exception {
        morphium.clearCollection(Msg.class);
        final StdMessaging producer = new StdMessaging(morphium, 100, true);
        final StdMessaging consumer = new StdMessaging(morphium, 10, true, true, 2000);
        consumer.setUseChangeStream(false).start();
        producer.setUseChangeStream(false).start();
        Thread.sleep(2500);
        try {
            final AtomicInteger processed = new AtomicInteger();
            final Map<String, AtomicInteger> msgCountById = new ConcurrentHashMap<>();
            consumer.addListenerForTopic("test", (msg, m) -> {
                processed.incrementAndGet();
                if (processed.get() % 1000 == 0) {
                    log.info("Consumed " + processed.get());
                }
                assert (!msgCountById.containsKey(m.getMsgId().toString()));
                msgCountById.putIfAbsent(m.getMsgId().toString(), new AtomicInteger());
                msgCountById.get(m.getMsgId().toString()).incrementAndGet();
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
                Msg m = new Msg("test", "m", "v");
                m.setTtl(5 * 60 * 1000);
                if (i % 1000 == 0) {
                    log.info("created msg " + i + " / " + numberOfMessages);
                }
                producer.sendMessage(m);
            }

            long start = System.currentTimeMillis();

            while (processed.get() < numberOfMessages) {
                //            ThreadMXBean thbean = ManagementFactory.getThreadMXBean();
                //            log.info("Running threads: " + thbean.getThreadCount());
                log.info("Processed " + processed.get());
                Thread.sleep(1500);
            }
            long dur = System.currentTimeMillis() - start;
            log.info("Processing took " + dur + " ms");

            assert (processed.get() == numberOfMessages);
            for (String id : msgCountById.keySet()) {
                assert (msgCountById.get(id).get() == 1);
            }
        } finally {
            producer.terminate();
            consumer.terminate();
        }


    }


    @Test
    public void exclusiveMessageCustomQueueTest() throws Exception {
        StdMessaging sender = null;
        StdMessaging sender2 = null;
        StdMessaging m1 = null;
        StdMessaging m2 = null;
        StdMessaging m3 = null;
        StdMessaging m4 = null;
        try {
            morphium.dropCollection(Msg.class);

            sender = new StdMessaging(morphium, "test", 100, false);
            sender.setSenderId("sender1");
            morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
            sender.setUseChangeStream(false).start();
            sender2 = new StdMessaging(morphium, "test2", 100, false);
            sender2.setSenderId("sender2");
            morphium.dropCollection(Msg.class, sender2.getCollectionName(), null);
            sender2.setUseChangeStream(false).start();
            Thread.sleep(200);
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;

            m1 = new StdMessaging(morphium, "test", 100, false);
            m1.setSenderId("m1");
            m1.addListenerForTopic("test", (msg, m) -> {
                gotMessage1 = true;
                log.info("Got message m1");
                return null;
            });
            m2 = new StdMessaging(morphium, "test", 100, false);
            m2.setSenderId("m2");
            m2.addListenerForTopic("test", (msg, m) -> {
                gotMessage2 = true;
                log.info("Got message m2");
                return null;
            });
            m3 = new StdMessaging(morphium, "test2", 100, false);
            m3.setSenderId("m3");
            m3.addListenerForTopic("test", (msg, m) -> {
                gotMessage3 = true;
                log.info("Got message m3");
                return null;
            });
            m4 = new StdMessaging(morphium, "test2", 100, false);
            m4.setSenderId("m4");
            m4.addListenerForTopic("test", (msg, m) -> {
                gotMessage4 = true;
                log.info("Got message m4");
                return null;
            });

            m1.setUseChangeStream(false).start();
            m2.setUseChangeStream(false).start();
            m3.setUseChangeStream(false).start();
            m4.setUseChangeStream(false).start();
            Thread.sleep(200);
            Msg m = new Msg();
            m.setExclusive(true);
            m.setTtl(3000000);
            m.setTopic("A message");

            sender.sendMessage(m);

            assert (!gotMessage3);
            assert (!gotMessage4);
            Thread.sleep(1200);

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
            m.setTopic("A message");
            m.setTtl(3000000);
            sender2.sendMessage(m);
            Thread.sleep(1500);
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
            Thread.sleep(2500);

            for (StdMessaging ms : Arrays.asList(m1, m2, m3)) {
                if (ms.getNumberOfMessages() > 0) {
                    Query<Msg> q1 = morphium.createQueryFor(Msg.class, ms.getCollectionName());
                    q1.f(Msg.Fields.sender).ne(ms.getSenderId());
                    q1.f(Msg.Fields.processedBy).ne(ms.getSenderId());
                    List<Msg> ret = q1.asList();
                    for (Msg f : ret) {
                        log.info("Found elements for " + ms.getSenderId() + ": " + f.toString());
                    }
                }
            }
            for (StdMessaging ms : Arrays.asList(m1, m2, m3)) {
                assert (ms.getNumberOfMessages() == 0) : "Number of messages " + ms.getSenderId() + " is " + ms.getNumberOfMessages();
            }
        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
            m4.terminate();
            sender.terminate();
            sender2.terminate();

        }
    }

    @Test
    public void exclusiveMessageTest() throws Exception {
        morphium.dropCollection(Msg.class);
        StdMessaging sender = new StdMessaging(morphium, 100, false);
        sender.setUseChangeStream(false).start();

        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        StdMessaging m1 = new StdMessaging(morphium, 100, false);
        m1.addListenerForTopic("test", (msg, m) -> {
            gotMessage1 = true;
            return null;
        });
        StdMessaging m2 = new StdMessaging(morphium, 100, false);
        m2.addListenerForTopic("test", (msg, m) -> {
            gotMessage2 = true;
            return null;
        });
        StdMessaging m3 = new StdMessaging(morphium, 100, false);
        m3.addListenerForTopic("test", (msg, m) -> {
            gotMessage3 = true;
            return null;
        });

        m1.setUseChangeStream(false).start();
        m2.setUseChangeStream(false).start();
        m3.setUseChangeStream(false).start();
        try {
            Thread.sleep(100);


            Msg m = new Msg();
            m.setExclusive(true);
            m.setTopic("test");

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
        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
            sender.terminate();
        }

    }


    @Test
    public void removeMessageTest() throws Exception {
        StdMessaging m1 = new StdMessaging(morphium, 1000, false);
        try {
            Msg m = new Msg().setMsgId(new MorphiumId()).setMsg("msg").setTopic("name").setValue("a value");
            m1.sendMessage(m);
            Thread.sleep(100);
            m1.removeMessage(m);
            Thread.sleep(100);
            assert (morphium.createQueryFor(Msg.class).countAll() == 0);
        } finally {
            m1.terminate();
        }

    }

    @Test
    public void timeoutMessages() throws Exception {
        final AtomicInteger cnt = new AtomicInteger();
        StdMessaging m1 = new StdMessaging(morphium, 1000, false);
        try {
            m1.addListenerForTopic("test", (msg, m) -> {
                log.error("ERROR!");
                cnt.incrementAndGet();
                return null;
            });
            m1.setUseChangeStream(false).start();
            Thread.sleep(100);
            Msg m = new Msg().setMsgId(new MorphiumId()).setMsg("test").setTopic("name").setValue("a value").setTtl(-1000);
            m1.sendMessage(m);
            Thread.sleep(200);
            assert (cnt.get() == 0);
        } finally {
            m1.terminate();
        }


    }

    @Test
    public void selfMessages() throws Exception {
        morphium.dropCollection(Msg.class);
        StdMessaging sender = new StdMessaging(morphium, 100, false);
        sender.setUseChangeStream(false).start();
        Thread.sleep(2500);
        sender.addListenerForTopic("test", ((msg, m) -> {
            gotMessage = true;
            log.info("Got message: " + m.getMsg() + "/" + m.getTopic());
            return null;
        }));

        gotMessage = false;
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        StdMessaging m1 = new StdMessaging(morphium, 100, false);
        m1.addListenerForTopic("test", (msg, m) -> {
            gotMessage1 = true;
            return new Msg(m.getTopic(), "got message", "value", 5000);
        });
        m1.setUseChangeStream(false).start();
        try {
            sender.sendMessageToSelf(new Msg("test", "Selfmessage", "value"));
            Thread.sleep(1500);
            assert (gotMessage);
            assert (!gotMessage1);
        } finally {
            m1.terminate();
            sender.terminate();
        }
    }


    @Test
    public void getPendingMessagesOnStartup() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(1000);
        StdMessaging sender = new StdMessaging(morphium, 100, false);
        sender.setUseChangeStream(false).start();

        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        StdMessaging m3 = new StdMessaging(morphium, 100, false);
        StdMessaging m2 = new StdMessaging(morphium, 100, false);
        StdMessaging m1 = new StdMessaging(morphium, 100, false);

        try {
            m3.addListenerForTopic("test", (msg, m) -> {
                gotMessage3 = true;
                return null;
            });

            m3.setUseChangeStream(false).start();

            Thread.sleep(1500);


            sender.sendMessage(new Msg("test", "testmsg", "testvalue", 120000, false));

            Thread.sleep(1000);
            assert (gotMessage3);
            Thread.sleep(2000);


            m1.addListenerForTopic("test", (msg, m) -> {
                gotMessage1 = true;
                return null;
            });

            m1.setUseChangeStream(false).start();

            Thread.sleep(1500);
            assert (gotMessage1);


            m2.addListenerForTopic("test", (msg, m) -> {
                gotMessage2 = true;
                return null;
            });

            m2.setUseChangeStream(false).start();

            Thread.sleep(1500);
            assert (gotMessage2);

        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
            sender.terminate();
        }

    }


    @Test
    public void waitingForMessagesIfNonMultithreadded() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(1000);
        StdMessaging sender = new StdMessaging(morphium, 100, false, false, 10);
        sender.setUseChangeStream(false).start();

        list.clear();
        StdMessaging receiver = new StdMessaging(morphium, 100, false, false, 10);
        receiver.addListenerForTopic("test", (msg, m) -> {
            list.add(m);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {

            }

            return null;
        });
        receiver.setUseChangeStream(false).start();
        try {
            Thread.sleep(500);
            sender.sendMessage(new Msg("test", "test", "test"));
            sender.sendMessage(new Msg("test", "test", "test"));

            Thread.sleep(500);
            assert (list.size() == 1) : "Size wrong: " + list.size();
            Thread.sleep(2200);
            assert (list.size() == 2);
        } finally {
            sender.terminate();
            receiver.terminate();
        }
    }

    @Test
    public void waitingForMessagesIfMultithreadded() throws Exception {
        morphium.dropCollection(Msg.class);
        morphium.getConfig().setThreadPoolMessagingCoreSize(5);
        log.info("Max threadpool:" + morphium.getConfig().getThreadPoolMessagingCoreSize());
        Thread.sleep(1000);
        StdMessaging sender = new StdMessaging(morphium, 100, false, true, 10);
        sender.setUseChangeStream(false).start();

        list.clear();
        StdMessaging receiver = new StdMessaging(morphium, 100, false, true, 10);
        receiver.addListenerForTopic("test", (msg, m) -> {
            log.info("Incoming message...");
            list.add(m);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {

            }

            return null;
        });
        receiver.setUseChangeStream(false).start();
        try {
            Thread.sleep(100);
            sender.sendMessage(new Msg("test", "test", "test"));
            sender.sendMessage(new Msg("test", "test", "test"));
            Thread.sleep(1000);

            assert (list.size() == 2) : "Size wrong: " + list.size();
        } finally {
            sender.terminate();
            receiver.terminate();
        }
    }


    @Test
    public void priorityTest() throws Exception {
        StdMessaging sender = new StdMessaging(morphium, 100, false);
        sender.setUseChangeStream(false).start();
        Thread.sleep(250);
        list.clear();
        //if running multithreadded, the execution order might differ a bit because of the concurrent
        //execution - hence if set to multithreadded, the test will fail!
        StdMessaging receiver = new StdMessaging(morphium, 10, false, false, 100);
        try {
            receiver.addListenerForTopic("test", (msg, m) -> {
                log.info("Incoming message: prio " + m.getPriority() + "  timestamp: " + m.getTimestamp());
                list.add(m);
                return null;
            });

            for (int i = 0; i < 10; i++) {
                Msg m = new Msg("test", "test", "test");
                m.setPriority((int) (1000.0 * Math.random()));
                log.info("Stored prio: " + m.getPriority());
                sender.sendMessage(m);
            }

            Thread.sleep(1000);
            receiver.setUseChangeStream(false).start();

            while (list.size() < 10) {
                Thread.yield();
            }

            int lastValue = -888888;

            for (Msg m : list) {
                log.info("prio: " + m.getPriority());
                assert (m.getPriority() >= lastValue);
                lastValue = m.getPriority();
            }


            receiver.pauseTopicProcessing("test");
            list.clear();
            for (int i = 0; i < 10; i++) {
                Msg m = new Msg("test", "test", "test");
                m.setPriority((int) (10000.0 * Math.random()));
                log.info("Stored prio: " + m.getPriority());
                sender.sendMessage(m);
            }

            Thread.sleep(1000);
            receiver.unpauseTopicProcessing("test");
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


    @Test
    public void markExclusiveMessageTest() throws Exception {

        StdMessaging sender = new StdMessaging(morphium, 100, false);
        morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
        sender.setUseChangeStream(false).start();
        StdMessaging receiver = new StdMessaging(morphium, 10, false, true, 10);
        receiver.setUseChangeStream(false).start();
        StdMessaging receiver2 = new StdMessaging(morphium, 10, false, true, 10);
        receiver2.setUseChangeStream(false).start();

        final AtomicInteger pausedReciever = new AtomicInteger(0);

        try {
            Thread.sleep(100);
            receiver.addListenerForTopic("test", (msg, m) -> {
//                log.info("R1: Incoming message");
                assert (pausedReciever.get() != 1);
                return null;
            });

            receiver2.addListenerForTopic("test", (msg, m) -> {
//                log.info("R2: Incoming message");
                assert (pausedReciever.get() != 2);
                return null;
            });


            for (int i = 0; i < 200; i++) {
                Msg m = new Msg("test", "test", "value", 3000000, true);
                sender.sendMessage(m);
                if (i == 100) {
                    receiver2.pauseTopicProcessing("test");
                    Thread.sleep(50);
                    pausedReciever.set(2);
                } else if (i == 120) {
                    receiver.pauseTopicProcessing("test");
                    Thread.sleep(50);
                    pausedReciever.set(1);
                } else if (i == 160) {
                    receiver.unpauseTopicProcessing("test");
                    //receiver.findAndProcessPendingMessages("test");
                    receiver2.unpauseTopicProcessing("test");
                    //receiver2.findAndProcessPendingMessages("test");
                    pausedReciever.set(0);
                }

            }

            long start = System.currentTimeMillis();
            Query<Msg> q = morphium.createQueryFor(Msg.class).f(Msg.Fields.topic).eq("test").f(Msg.Fields.processedBy).eq(null);
            while (q.countAll() > 0) {
                log.info("Count is still: " + q.countAll());
                Thread.sleep(500);
            }
            assert (q.countAll() == 0) : "Count is wrong: " + q.countAll();
//
        } finally {
            receiver.terminate();
            receiver2.terminate();
            sender.terminate();
        }

    }

    @Test
    public void exclusivityPausedUnpausingTest() throws Exception {
        StdMessaging sender = new StdMessaging(morphium, 1000, false);
        sender.setSenderId("sender");
        morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
        Thread.sleep(100);
        sender.setUseChangeStream(false).start();
        Morphium morphium2 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        morphium2.getConfig().setThreadPoolMessagingMaxSize(10);
        morphium2.getConfig().setThreadPoolMessagingCoreSize(5);
        morphium2.getConfig().setThreadPoolAsyncOpMaxSize(10);
        StdMessaging receiver = new StdMessaging(morphium2, (int) (50 + 100 * Math.random()), true, true, 15);
        receiver.setSenderId("r1");
        receiver.setUseChangeStream(false).start();

        Morphium morphium3 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        morphium3.getConfig().setThreadPoolMessagingMaxSize(10);
        morphium3.getConfig().setThreadPoolMessagingCoreSize(5);
        morphium3.getConfig().setThreadPoolAsyncOpMaxSize(10);
        StdMessaging receiver2 = new StdMessaging(morphium3, (int) (50 + 100 * Math.random()), false, false, 15);
        receiver2.setSenderId("r2");
        receiver2.setUseChangeStream(false).start();

        Morphium morphium4 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        morphium4.getConfig().setThreadPoolMessagingMaxSize(10);
        morphium4.getConfig().setThreadPoolMessagingCoreSize(5);
        morphium4.getConfig().setThreadPoolAsyncOpMaxSize(10);
        StdMessaging receiver3 = new StdMessaging(morphium4, (int) (50 + 100 * Math.random()), true, false, 15);
        receiver3.setSenderId("r3");
        receiver3.setUseChangeStream(false).start();

        Morphium morphium5 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        morphium5.getConfig().setThreadPoolMessagingMaxSize(10);
        morphium5.getConfig().setThreadPoolMessagingCoreSize(5);
        morphium5.getConfig().setThreadPoolAsyncOpMaxSize(10);
        StdMessaging receiver4 = new StdMessaging(morphium5, (int) (50 + 100 * Math.random()), false, true, 15);
        receiver4.setSenderId("r4");
        receiver4.setUseChangeStream(false).start();


        final AtomicInteger received = new AtomicInteger();
        final AtomicInteger dups = new AtomicInteger();
        final Map<String, Long> ids = new ConcurrentHashMap<>();
        final Map<String, String> recById = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> recieveCount = new ConcurrentHashMap<>();
        Thread.sleep(100);
        try {
            MessageListener messageListener = (msg, m) -> {
                msg.pauseTopicProcessing("m");
                try {
                    Thread.sleep((long) (300 * Math.random()));
                } catch (InterruptedException e) {
                }
                //log.info("R1: Incoming message "+m.getValue());
                received.incrementAndGet();
                recieveCount.putIfAbsent(msg.getSenderId(), new AtomicInteger());
                recieveCount.get(msg.getSenderId()).incrementAndGet();
                if (ids.containsKey(m.getMsgId().toString())) {
                    if (m.isExclusive()) {
                        log.error("Duplicate recieved message " + msg.getSenderId() + " " + (System.currentTimeMillis() - ids.get(m.getMsgId().toString())) + "ms ago");
                        if (recById.get(m.getMsgId().toString()).equals(msg.getSenderId())) {
                            log.error("--- duplicate was processed before by me!");
                        } else {
                            log.error("--- duplicate processed by someone else");
                        }
                        dups.incrementAndGet();
                    }
                }
                ids.put(m.getMsgId().toString(), System.currentTimeMillis());
                recById.put(m.getMsgId().toString(), msg.getSenderId());
                msg.unpauseTopicProcessing("m");
                return null;
            };
            receiver.addListenerForTopic("m", messageListener);
            receiver2.addListenerForTopic("m", messageListener);
            receiver3.addListenerForTopic("m", messageListener);
            receiver4.addListenerForTopic("m", messageListener);
            int exclusiveAmount = 50;
            int broadcastAmount = 100;
            for (int i = 0; i < exclusiveAmount; i++) {
                int rec = received.get();
                long messageCount = receiver.getPendingMessagesCount();
                if (i % 100 == 0) log.info("Send " + i + " recieved: " + rec + " queue: " + messageCount);
                Msg m = new Msg("m", "m", "v" + i, 3000000, true);
                m.setExclusive(true);
                sender.sendMessage(m);
            }
            for (int i = 0; i < broadcastAmount; i++) {
                int rec = received.get();
                long messageCount = receiver.getPendingMessagesCount();
                if (i % 100 == 0) log.info("Send boadcast" + i + " recieved: " + rec + " queue: " + messageCount);
                Msg m = new Msg("m", "m", "v" + i, 3000000, false);
                sender.sendMessage(m);
            }

            while (received.get() != exclusiveAmount + broadcastAmount * 4) {
                int rec = received.get();
                long messageCount = sender.getPendingMessagesCount();

                log.info("Send excl: " + exclusiveAmount + "  brodadcast: " + broadcastAmount + " recieved: " + rec + " queue: " + messageCount + " currently processing: " + (exclusiveAmount + broadcastAmount * 4 - rec - messageCount));
                for (StdMessaging m : Arrays.asList(receiver, receiver2, receiver3, receiver4)) {
                    assert (m.getRunningTasks() <= 10) : m.getSenderId() + " runs too many tasks! " + m.getRunningTasks();
                }
                assert (dups.get() == 0) : "got duplicate message";

                Thread.sleep(1000);
            }
            int rec = received.get();
            long messageCount = sender.getPendingMessagesCount();
            log.info("Send " + exclusiveAmount + " recieved: " + rec + " queue: " + messageCount);
            assert (received.get() == exclusiveAmount + broadcastAmount * 4) : "should have received " + (exclusiveAmount + broadcastAmount * 4) + " but actually got " + received.get();

            for (String id : recieveCount.keySet()) {
                log.info("Reciever " + id + " message count: " + recieveCount.get(id).get());
            }
            log.info("R1 active: " + receiver.getRunningTasks());
            log.info("R2 active: " + receiver2.getRunningTasks());
            log.info("R3 active: " + receiver3.getRunningTasks());
            log.info("R4 active: " + receiver4.getRunningTasks());


            logStats(morphium);
            logStats(morphium2);
            logStats(morphium3);
            logStats(morphium4);
            logStats(morphium5);
        } finally {

            sender.terminate();
            receiver.terminate();
            receiver2.terminate();
            receiver3.terminate();
            receiver4.terminate();
            morphium2.close();
            morphium3.close();
            morphium4.close();
            morphium5.close();
        }

    }

    @Test
    public void exclusivityTest() throws Exception {
        StdMessaging sender = new StdMessaging(morphium, 100, false);
        sender.setSenderId("sender");
        morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
        Thread.sleep(100);
        sender.setUseChangeStream(false).start();
        Morphium morphium2 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        morphium2.getConfig().setThreadPoolMessagingMaxSize(10);
        morphium2.getConfig().setThreadPoolMessagingCoreSize(5);
        morphium2.getConfig().setThreadPoolAsyncOpMaxSize(10);
        StdMessaging receiver = new StdMessaging(morphium2, 10, true, true, 15);
        receiver.setSenderId("r1");
        receiver.setUseChangeStream(false).start();

        Morphium morphium3 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        morphium3.getConfig().setThreadPoolMessagingMaxSize(10);
        morphium3.getConfig().setThreadPoolMessagingCoreSize(5);
        morphium3.getConfig().setThreadPoolAsyncOpMaxSize(10);
        StdMessaging receiver2 = new StdMessaging(morphium3, 10, false, false, 15);
        receiver2.setSenderId("r2");
        receiver2.setUseChangeStream(false).start();

        Morphium morphium4 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        morphium4.getConfig().setThreadPoolMessagingMaxSize(10);
        morphium4.getConfig().setThreadPoolMessagingCoreSize(5);
        morphium4.getConfig().setThreadPoolAsyncOpMaxSize(10);
        StdMessaging receiver3 = new StdMessaging(morphium4, 10, true, false, 15);
        receiver3.setSenderId("r3");
        receiver3.setUseChangeStream(false).start();

        Morphium morphium5 = new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));
        morphium5.getConfig().setThreadPoolMessagingMaxSize(10);
        morphium5.getConfig().setThreadPoolMessagingCoreSize(5);
        morphium5.getConfig().setThreadPoolAsyncOpMaxSize(10);
        StdMessaging receiver4 = new StdMessaging(morphium5, 10, false, true, 15);
        receiver4.setSenderId("r4");
        receiver4.setUseChangeStream(false).start();
        final AtomicInteger received = new AtomicInteger();
        final AtomicInteger dups = new AtomicInteger();
        final Map<String, Long> ids = new ConcurrentHashMap<>();
        final Map<String, String> recById = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> recieveCount = new ConcurrentHashMap<>();
        Thread.sleep(100);

        try {
            MessageListener messageListener = (msg, m) -> {
                try {
                    Thread.sleep((long) (500 * Math.random()));
                } catch (InterruptedException e) {
                }
                received.incrementAndGet();
                recieveCount.putIfAbsent(msg.getSenderId(), new AtomicInteger());
                recieveCount.get(msg.getSenderId()).incrementAndGet();
                if (ids.containsKey(m.getMsgId().toString()) && m.isExclusive()) {
                    log.error("Duplicate recieved message " + msg.getSenderId() + " " + (System.currentTimeMillis() - ids.get(m.getMsgId().toString())) + "ms ago");
                    if (recById.get(m.getMsgId().toString()).equals(msg.getSenderId())) {
                        log.error("--- duplicate was processed before by me!");
                    } else {
                        log.error("--- duplicate processed by someone else");
                    }
                    dups.incrementAndGet();
                }
                ids.put(m.getMsgId().toString(), System.currentTimeMillis());
                recById.put(m.getMsgId().toString(), msg.getSenderId());
                //msg.unpauseProcessingOfMessagesNamed("m");
                return null;
            };
            receiver.addListenerForTopic("m", messageListener);
            receiver2.addListenerForTopic("m", messageListener);
            receiver3.addListenerForTopic("m", messageListener);
            receiver4.addListenerForTopic("m", messageListener);
            int amount = 200;
            int broadcastAmount = 50;
            for (int i = 0; i < amount; i++) {
                int rec = received.get();
                long messageCount = 0;
                messageCount += receiver.getPendingMessagesCount();
                if (i % 100 == 0) log.info("Send " + i + " recieved: " + rec + " queue: " + messageCount);
                Msg m = new Msg("m", "m", "v" + i, 3000000, true);
                m.setExclusive(true);
                sender.sendMessage(m);
            }
            for (int i = 0; i < broadcastAmount; i++) {
                int rec = received.get();
                long messageCount = receiver.getPendingMessagesCount();
                if (i % 100 == 0) log.info("Send broadcast" + i + " recieved: " + rec + " queue: " + messageCount);
                Msg m = new Msg("m", "m", "v" + i, 3000000, false);
                sender.sendMessage(m);
            }

            while (received.get() != amount + broadcastAmount * 4) {
                int rec = received.get();
                long messageCount = sender.getPendingMessagesCount();
                log.info("Send excl: " + amount + "  brodadcast: " + broadcastAmount + " recieved: " + rec + " queue: " + messageCount + " currently processing: " + (amount + broadcastAmount * 4 - rec - messageCount));
                assert (dups.get() == 0) : "got duplicate message";
                for (StdMessaging m : Arrays.asList(receiver, receiver2, receiver3, receiver4)) {
                    log.info(m.getSenderId() + " active Tasks: " + m.getRunningTasks());
                }
                Thread.sleep(1000);
            }
            int rec = received.get();
            long messageCount = sender.getPendingMessagesCount();
            log.info("Send " + amount + " recieved: " + rec + " queue: " + messageCount);
            assert (received.get() == amount + broadcastAmount * 4) : "should have received " + (amount + broadcastAmount * 4) + " but actually got " + received.get();

            for (String id : recieveCount.keySet()) {
                log.info("Reciever " + id + " message count: " + recieveCount.get(id).get());
            }
            log.info("R1 active: " + receiver.getRunningTasks());
            log.info("R2 active: " + receiver2.getRunningTasks());
            log.info("R3 active: " + receiver3.getRunningTasks());
            log.info("R4 active: " + receiver4.getRunningTasks());
        } finally {
            sender.terminate();
            receiver.terminate();
            receiver2.terminate();
            receiver3.terminate();
            receiver4.terminate();
            morphium2.close();
            morphium3.close();
            morphium4.close();
            morphium5.close();
        }

    }


    @Test
    public void exclusiveMessageStartupTests() throws Exception {
        StdMessaging sender = new StdMessaging(morphium, 100, false);
        StdMessaging receiverNoListener = new StdMessaging(morphium, 100, true);
        try {
            sender.setSenderId("sender");
            morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
            Thread.sleep(100);
            sender.setUseChangeStream(false).start();

            sender.sendMessage(new Msg("test", "test", "test", 30000, true));
            sender.sendMessage(new Msg("test", "test", "test", 30000, true));
            sender.sendMessage(new Msg("test", "test", "test", 30000, true));
            Thread.sleep(1000);
            receiverNoListener.setSenderId("recNL");
            receiverNoListener.setUseChangeStream(false).start();

            assert (morphium.createQueryFor(Msg.class, sender.getCollectionName()).countAll() == 3);
        } finally {
            sender.terminate();
            receiverNoListener.terminate();
        }
    }

    @Test
    public void exclusiveTest() throws Exception {
        morphium.dropCollection(Msg.class);
        StdMessaging sender;
        List<StdMessaging> recs;

        sender = new StdMessaging(morphium, 1000, false);
        sender.setSenderId("sender");
        sender.setUseChangeStream(false).start();
        final AtomicInteger counts = new AtomicInteger();
        recs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            StdMessaging r = new StdMessaging(morphium, 100, false);
            r.setSenderId("r" + i);
            recs.add(r);
            r.setUseChangeStream(false).start();

            r.addListenerForTopic("test", (m, msg) -> {
                counts.incrementAndGet();
                return null;
            });
        }
        try {

            for (int i = 0; i < 50; i++) {
                if (i % 10 == 0) log.info("Msg sent");
                sender.sendMessage(new Msg("name", "msg", "value", 20000000, true));
            }
            while (counts.get() < 50) {
                log.info("Still waiting for incoming messages: " + counts.get());
                Thread.sleep(1000);
            }
            Thread.sleep(2000);
            assert (counts.get() == 50) : "Did get too many? " + counts.get();


            counts.set(0);
            for (int i = 0; i < 10; i++) {
                log.info("Msg sent");
                sender.sendMessage(new Msg("test", "msg", "value", 20000000, false));
            }
            while (counts.get() < 10 * recs.size()) {
                log.info("Still waiting for incoming messages: " + counts.get());
                Thread.sleep(1000);
            }
            Thread.sleep(2000);
            assert (counts.get() == 10 * recs.size()) : "Did get too many? " + counts.get();

        } finally {
            sender.terminate();
            for (StdMessaging r : recs) r.terminate();


        }

    }


    @Test
    public void severalRecipientsTest() throws Exception {
        StdMessaging sender = new StdMessaging(morphium, 100, false);
        sender.setSenderId("sender");
        sender.setUseChangeStream(false).start();

        List<StdMessaging> receivers = new ArrayList<>();
        final List<String> receivedBy = new Vector<>();

        for (int i = 0; i < 10; i++) {
            StdMessaging receiver1 = new StdMessaging(morphium, 100, false);
            receiver1.setSenderId("rec" + i);
            receiver1.setUseChangeStream(false).start();
            receivers.add(receiver1);
            receiver1.addListenerForTopic("test", new MessageListener() {
                @Override
                public Msg onMessage(MorphiumMessaging msg, Msg m)  {
                    if (receivedBy.contains(msg.getSenderId())) {
                        log.error("Receiving msg twice: " + m.getMsgId());
                    }
                    receivedBy.add(msg.getSenderId());
                    return null;
                }
            });
        }

        try {
            Msg m = new Msg("test", "msg", "value");
            m.addRecipient("rec1");
            m.addRecipient("rec2");
            m.addRecipient("rec5");

            sender.sendMessage(m);
            Thread.sleep(1000);

            assert (receivedBy.size() == m.getTo().size());
            for (String r : m.getTo()) {
                assert (receivedBy.contains(r));
            }


            receivedBy.clear();

            m = new Msg("test", "msg", "value");
            m.addRecipient("rec1");
            m.addRecipient("rec2");
            m.addRecipient("rec5");
            m.setExclusive(true);

            sender.sendMessage(m);
            Thread.sleep(1000);
            assert (receivedBy.size() == 1);
            assert (m.getTo().contains(receivedBy.get(0)));
        } finally {
            for (StdMessaging ms : receivers) {
                ms.terminate();
            }
        }

    }

}
