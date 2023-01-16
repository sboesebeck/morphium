package de.caluga.test.mongo.suite.inmem_messaging;

import de.caluga.morphium.ReadAccessType;
import de.caluga.morphium.WriteAccessType;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MessageRejectedException;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MsgLock;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.base.TestUtils;
import de.caluga.test.mongo.suite.inmem.MorphiumInMemTestBase;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 17:34
 * <p/>
 */
@SuppressWarnings("ALL")
public class InMemMessagingTest extends MorphiumInMemTestBase {
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
    public void testMarshallUnmarshall() throws Exception {
        Messaging m = new Messaging(morphium);
        m.start();

        Msg m1=new Msg("name", "msg", "value", 123123, true);
        m1.setTtl(100000000);
        m.sendMessage(m1);
        Thread.sleep(100);

        var msg=morphium.createQueryFor(Msg.class).get();
    }


    @Test
    public void preLockedMessagesTestPreStart() throws Exception {
        AtomicInteger counter=new AtomicInteger();
        Messaging receiver=new Messaging(morphium,100,false);
        receiver.addListenerForMessageNamed("test", (mo,msg)->{
            log.info("Incoming message...");
            counter.incrementAndGet();
            return null;
        });
        Msg m=new Msg("test","msg","value");
        m.setMsgId(new MorphiumId());
        m.setSender("me");
        m.setSenderHost("mo.local");
        MsgLock lck=new MsgLock(m);
        lck.setLockId(receiver.getSenderId());
        morphium.insert(lck,receiver.getLockCollectionName(),null);
        morphium.insert(m,receiver.getCollectionName(),null);
        Thread.sleep(1000);
        assertEquals(0,counter.get());
        log.info("Did not get message beforehand...");
        receiver.start();

        Thread.sleep(100);
        assertEquals(1,counter.get());
        receiver.terminate();

    }

    @Test
    public void preLockedMessagesTest() throws Exception {
        Messaging receiver=new Messaging(morphium,100,false);
        receiver.start();
        AtomicInteger counter=new AtomicInteger();
        receiver.addListenerForMessageNamed("test", (m,msg)->{
            log.info("Incoming message...");
            counter.incrementAndGet();
            return null;
        });

        Msg m=new Msg("test","msg","value");
        m.setMsgId(new MorphiumId());
        m.setSender("me");
        m.setSenderHost("mo.local");
        MsgLock lck=new MsgLock(m);
        lck.setLockId(receiver.getSenderId());
        morphium.insert(lck,receiver.getLockCollectionName(),null);
        morphium.insert(m,receiver.getCollectionName(),null);
        Thread.sleep(100);
        assertEquals(1,counter.get());
        receiver.terminate();

    }


    @Test
    public void testMsgQueName() throws Exception {
        morphium.dropCollection(Msg.class);
        morphium.dropCollection(Msg.class, "mmsg_msg2", null);

        Messaging m = new Messaging(morphium);
        m.setPause(500);
        m.setMultithreadded(false);
        m.setAutoAnswer(false);
        m.setProcessMultiple(true);

        assert (!m.isAutoAnswer());
        assert (!m.isMultithreadded());
        assert (m.getPause() == 500);
        assert (m.isProcessMultiple());
        m.addMessageListener((msg, m1) -> {
            gotMessage1 = true;
            return null;
        });
        m.setUseChangeStream(true).start();

        Messaging m2 = new Messaging(morphium, "msg2", 500, true);
        m2.addMessageListener((msg, m1) -> {
            gotMessage2 = true;
            return null;
        });
        m2.setUseChangeStream(true).start();
        try {
            Msg msg = new Msg("tst", "msg", "value", 30000);
            msg.setExclusive(false);
            m.sendMessage(msg);
            Thread.sleep(10);
            Query<Msg> q = morphium.createQueryFor(Msg.class);
            assert (q.countAll() == 1);
            q.setCollectionName(m2.getCollectionName());
            assert (q.countAll() == 0);

            msg = new Msg("tst2", "msg", "value", 30000);
            msg.setExclusive(false);
            m2.sendMessage(msg);
            Thread.sleep(100);
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
        m.setName("A name");
        morphium.store(m);
        Thread.sleep(500);
        m=morphium.reread(m);
        assert (m.getTimestamp() > 0) : "Timestamp not updated?";

    }

    @SuppressWarnings("Duplicates")
    @Test
    public void multithreaddingTest() throws Exception {
        Messaging producer = new Messaging(morphium, 500, false);
        producer.setUseChangeStream(true).start();
        for (int i = 0; i < 100; i++) {
            Msg m = new Msg("test" + i, "tm", "" + i + System.currentTimeMillis(), 130000);
            producer.sendMessage(m);
        }
        final AtomicInteger count = new AtomicInteger();
        Messaging consumer = new Messaging(morphium, 100, false, true, 1000);
        consumer.addMessageListener((msg, m) -> {
           log.info("Got message!");
            count.incrementAndGet();
            return null;
        });
        long start = System.currentTimeMillis();
        consumer.setUseChangeStream(true).start();
        while (count.get() < 100) {
            log.info("Messages processed: " + count.get());
            Thread.sleep(1000);
        }
        long dur = System.currentTimeMillis() - start;
        log.info("processing 100 multithreaded but single messages took " + dur + "ms == " + (100 / (dur / 1000)) + " msg/sec");

        consumer.terminate();
        log.info("now multithreadded and multiprocessing");
        for (int i = 0; i < 2500; i++) {
            Msg m = new Msg("test" + i, "tm", "" + i + System.currentTimeMillis(), 130000);
            producer.sendMessage(m);
        }
        count.set(0);
        consumer = new Messaging(morphium, 100, true, true, 100);
        consumer.addMessageListener((msg, m) -> {
//            log.info("Got message!");
            count.incrementAndGet();
            return null;
        });
        start = System.currentTimeMillis();
        consumer.setUseChangeStream(true).start();
        while (count.get() < 2500) {
            log.info("Messages processed: " + count.get());
            Thread.sleep(1000);
        }
        dur = System.currentTimeMillis() - start;
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

        final Messaging messaging = new Messaging(morphium, 500, true);
        messaging.setUseChangeStream(true).start();
        Thread.sleep(500);

        messaging.addMessageListener((msg, m) -> {
            log.info("Got Message: " + m.toString());
            gotMessage = true;
            return null;
        });
        messaging.sendMessage(new Msg("Testmessage", "A message", "the value - for now", 5000000));

        Thread.sleep(1000);
        assert (!gotMessage) : "Message recieved from self?!?!?!";
        log.info("Dig not get own message - cool!");

        Msg m = new Msg("meine Message", "The Message", "value is a string", 5000000);
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
        Thread.sleep(1000);
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
        Messaging m1 = new Messaging(morphium, 500, true);
        Messaging m2 = new Messaging(morphium, 500, true);
        m1.setUseChangeStream(true).start();
        m2.setUseChangeStream(true).start();
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

        m1.sendMessage(new Msg("testmsg1", "The message from M1", "Value"));
        Thread.sleep(1000);
        assert (gotMessage2) : "Message not recieved yet?!?!?";
        gotMessage2 = false;

        m2.sendMessage(new Msg("testmsg2", "The message from M2", "Value"));
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

        m4.setUseChangeStream(true).start();
        m1.setUseChangeStream(true).start();
        m2.setUseChangeStream(true).start();
        m3.setUseChangeStream(true).start();
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

        m1.sendMessage(new Msg("testmsg1", "The message from M1", "Value"));
        Thread.sleep(500);
        assert (gotMessage2) : "Message not recieved yet by m2?!?!?";
        assert (gotMessage3) : "Message not recieved yet by m3?!?!?";
        assert (gotMessage4) : "Message not recieved yet by m4?!?!?";
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        m2.sendMessage(new Msg("testmsg2", "The message from M2", "Value"));
        Thread.sleep(500);
        assert (gotMessage1) : "Message not recieved yet by m1?!?!?";
        assert (gotMessage3) : "Message not recieved yet by m3?!?!?";
        assert (gotMessage4) : "Message not recieved yet by m4?!?!?";


        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        m1.sendMessage(new Msg("testmsg_excl", "This is the message", "value", 30000000, true));
        Thread.sleep(500);
        int cnt = 0;
        if (gotMessage1) cnt++;
        if (gotMessage2) cnt++;
        if (gotMessage3) cnt++;
        if (gotMessage4) cnt++;


        Thread.sleep(1000);

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
    public void testRejectException() {
        MessageRejectedException ex = new MessageRejectedException("rejected", true, true);
        assert (ex.isContinueProcessing());
        assert (ex.isSendAnswer());

        ex = new MessageRejectedException("rejected");
        ex.setSendAnswer(true);
        ex.setContinueProcessing(true);
        assert (ex.isContinueProcessing());
        assert (ex.isSendAnswer());

        ex = new MessageRejectedException("rejected", true);
        assert (ex.isContinueProcessing());
        assert (!ex.isSendAnswer());
    }

    @Test
    public void testRejectExclusiveMessage() throws Exception {
        Messaging sender = null;
        Messaging rec1 = null;
        Messaging rec2 = null;
        try {
            sender = new Messaging(morphium, 100, false);
            sender.setSenderId("sender");
            rec1 = new Messaging(morphium, 100, false);
            rec1.setSenderId("rec1");
            rec2 = new Messaging(morphium, 100, false);
            rec2.setSenderId("rec2");
            morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
            Thread.sleep(10);
            sender.setUseChangeStream(true).start();
            rec1.setUseChangeStream(true).start();
            rec2.setUseChangeStream(true).start();
            Thread.sleep(2000);
            final AtomicInteger recFirst = new AtomicInteger(0);

            gotMessage = false;

            rec1.addMessageListener((msg, m) -> {
                if (recFirst.get() == 0) {
                    recFirst.set(1);
                    throw new MessageRejectedException("rejected", true, true);
                }
                gotMessage = true;
                return null;
            });
            rec2.addMessageListener((msg, m) -> {
                if (recFirst.get() == 0) {
                    recFirst.set(1);
                    throw new MessageRejectedException("rejected", true, true);
                }
                gotMessage = true;
                return null;
            });
            sender.addMessageListener((msg, m) -> {
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
            while (!gotMessage) {
                Thread.sleep(500);
            }
            assert (gotMessage);
            assert (gotMessage3);
        } finally {
            sender.terminate();
            rec1.terminate();
            rec2.terminate();
        }


    }


    @Test
    public void testRejectMessage() throws Exception {
        Messaging sender = null;
        Messaging rec1 = null;
        Messaging rec2 = null;
        try {
            sender = new Messaging(morphium, 100, false);
            rec1 = new Messaging(morphium, 100, false);
            rec2 = new Messaging(morphium, 500, false);
            morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
            Thread.sleep(10);
            sender.setUseChangeStream(true).start();
            rec1.setUseChangeStream(true).start();
            rec2.setUseChangeStream(true).start();
            Thread.sleep(2000);
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;

            rec1.addMessageListener((msg, m) -> {
                gotMessage1 = true;
                throw new MessageRejectedException("rejected", true, true);
            });
            rec2.addMessageListener((msg, m) -> {
                gotMessage2 = true;
                log.info("Processing message " + m.getValue());
                return null;
            });
            sender.addMessageListener((msg, m) -> {
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
        final Messaging m1;
        final Messaging m2;
        final Messaging m3;
        m1 = new Messaging(morphium, 100, true);
        m2 = new Messaging(morphium, 100, true);
        m3 = new Messaging(morphium, 100, true);
        try {

            m1.setUseChangeStream(true).start();
            m2.setUseChangeStream(true).start();
            m3.setUseChangeStream(true).start();
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
            m1.sendMessage(new Msg("testmsg1", "The message from M1", "Value"));
            Thread.sleep(3000);
            assert (gotMessage2) : "Message not recieved yet by m2?!?!?";
            assert (gotMessage3) : "Message not recieved yet by m3?!?!?";
            assert (!error);
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            error = false;
            TestUtils.waitForWrites(morphium,log);
            Thread.sleep(2500);
            assert (!gotMessage1) : "Message recieved again by m1?!?!?";
            assert (!gotMessage2) : "Message recieved again by m2?!?!?";
            assert (!gotMessage3) : "Message recieved again by m3?!?!?";
            assert (!error);

            log.info("Sending direct message");
            Msg m = new Msg("testmsg1", "The message from M1", "Value");
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
            m = new Msg("testmsg1", "The message from M1", "Value");
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
        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        m1.setSenderId("m1");
        Messaging m2 = new Messaging(morphium, 10, false, true, 10);
        m2.setSenderId("m2");
        try {
            m1.setUseChangeStream(true).start();
            m2.setUseChangeStream(true).start();

            Msg m = new Msg("test", "ignore me please", "value");
            m1.sendMessage(m);
            Thread.sleep(1000);
            m = morphium.reread(m);
            assert (m.getProcessedBy().size() == 0) : "wrong number of proccessed by entries: " + m.getProcessedBy().size();
        } finally {
            m1.terminate();
            m2.terminate();
        }
    }

    @Test
    public void severalMessagingsTest() throws Exception {

        morphium.dropCollection(Msg.class);
        Thread.sleep(100);

        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        m1.setSenderId("m1");
        Messaging m2 = new Messaging(morphium, 10, false, true, 10);
        m2.setSenderId("m2");
        Messaging m3 = new Messaging(morphium, 10, false, true, 10);
        m3.setSenderId("m3");
        m1.setUseChangeStream(true).start();
        m2.setUseChangeStream(true).start();
        m3.setUseChangeStream(true).start();
        try {
            m3.addListenerForMessageNamed("multisystemtest", (msg, m) -> {
                //log.info("Got message: "+m.getName());
                log.info("Sending answer for " + m.getMsgId());
                return new Msg("multisystemtest", "answer", "value", 600000);
            });

            procCounter.set(0);
            for (int i = 0; i < 180; i++) {
                new Thread() {
                    public void run() {
                        Msg m = new Msg("multisystemtest", "nothing", "value");
                        m.setTtl(60000000);
                        Msg a = m1.sendAndAwaitFirstAnswer(m, 360000);
                        assertNotNull(a);
                        ;
                        procCounter.incrementAndGet();
                    }
                }.start();

            }
            while (procCounter.get() < 180) {
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
                m.setUseChangeStream(true).start();
                systems.add(m);
                MessageListener l = new MessageListener() {
                    final List<String> ids = Collections.synchronizedList(new ArrayList<>());
                    Messaging msg;

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
                systems.get(m).sendMessage(msg);
            }

            long dur = System.currentTimeMillis() - start;
            log.info("Queueing " + numberOfMessages + " messages took " + dur + " ms - now waiting for writes..");
            TestUtils.waitForWrites(morphium,log);
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

        m4.setUseChangeStream(true).start();
        m1.setUseChangeStream(true).start();
        m3.setUseChangeStream(true).start();
        m2.setUseChangeStream(true).start();
        Thread.sleep(300);
        try {
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
        final Messaging producer = new Messaging(morphium, 100, true, false, 10);
        final Messaging consumer = new Messaging(morphium, 100, true, true, 2000);
        producer.setUseChangeStream(true).start();
        consumer.setUseChangeStream(true).start();
        try {
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
            int amount = 1000;
            log.info("------------- sending messages");
            for (int i = 0; i < amount; i++) {
                producer.sendMessage(new Msg("Test " + i, "msg " + i, "value " + i));
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
        final Messaging producer = new Messaging(morphium, 100, true);
        final Messaging consumer = new Messaging(morphium, 10, true);
        producer.setUseChangeStream(true).start();
        consumer.setUseChangeStream(true).start();
        Thread.sleep(2500);
        try {
            final int[] processed = {0};
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

            int amount = 1000;

            for (int i = 0; i < amount; i++) {
                producer.sendMessage(new Msg("Test " + i, "msg " + i, "value " + i));
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
        final Messaging producer = new Messaging(morphium, 100, true);
        final Messaging consumer = new Messaging(morphium, 10, true, true, 2000);
        consumer.setUseChangeStream(true).start();
        producer.setUseChangeStream(true).start();
        Thread.sleep(2500);
        try {
            final AtomicInteger processed = new AtomicInteger();
            final Map<String, AtomicInteger> msgCountById = new ConcurrentHashMap<>();
            consumer.addMessageListener((msg, m) -> {
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
                Msg m = new Msg("msg", "m", "v");
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
        Messaging sender = null;
        Messaging sender2 = null;
        Messaging m1 = null;
        Messaging m2 = null;
        Messaging m3 = null;
        Messaging m4 = null;
        try {
            morphium.dropCollection(Msg.class);

            sender = new Messaging(morphium, "test", 100, false);
            sender.setSenderId("sender1");
            morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
            sender.setUseChangeStream(true).start();
            sender2 = new Messaging(morphium, "test2", 100, false);
            sender2.setSenderId("sender2");
            morphium.dropCollection(Msg.class, sender2.getCollectionName(), null);
            sender2.setUseChangeStream(true).start();
            Thread.sleep(200);
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;

            m1 = new Messaging(morphium, "test", 100, false);
            m1.setSenderId("m1");
            m1.addMessageListener((msg, m) -> {
                gotMessage1 = true;
                log.info("Got message m1");
                return null;
            });
            m2 = new Messaging(morphium, "test", 100, false);
            m2.setSenderId("m2");
            m2.addMessageListener((msg, m) -> {
                gotMessage2 = true;
                log.info("Got message m2");
                return null;
            });
            m3 = new Messaging(morphium, "test2", 100, false);
            m3.setSenderId("m3");
            m3.addMessageListener((msg, m) -> {
                gotMessage3 = true;
                log.info("Got message m3");
                return null;
            });
            m4 = new Messaging(morphium, "test2", 100, false);
            m4.setSenderId("m4");
            m4.addMessageListener((msg, m) -> {
                gotMessage4 = true;
                log.info("Got message m4");
                return null;
            });

            m1.setUseChangeStream(true).start();
            m2.setUseChangeStream(true).start();
            m3.setUseChangeStream(true).start();
            m4.setUseChangeStream(true).start();
            Thread.sleep(200);


            //Sending exclusive Message
            Msg m = new Msg();
            m.setExclusive(true);
            m.setTtl(3000000);
            m.setMsgId(new MorphiumId());
            m.setName("A message");
            log.info("Sending: " + m.getMsgId().toString());
            sender.sendMessage(m);
            Thread.sleep(1500);

            assert (!gotMessage3);
            assert (!gotMessage4);

            int rec = 0;
            long start = System.currentTimeMillis();
            while (true) {
                Thread.sleep(200);

                if (gotMessage1) {
                    rec++;
                }
                if (gotMessage2) {
                    rec++;
                }
                if (rec > 0) break;
                if (System.currentTimeMillis() - start > 500000) {
                    log.error("Timeout!");
                    break;
                }
            }
            assert (rec == 1) : "rec is " + rec;

            gotMessage1 = false;
            gotMessage2 = false;

            m = new Msg();
            m.setExclusive(true);
            m.setName("A message");
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

            for (Messaging ms : Arrays.asList(m1, m2, m3)) {
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
            for (Messaging ms : Arrays.asList(m1, m2, m3)) {
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
        Messaging sender = new Messaging(morphium, 100, false);
        sender.setUseChangeStream(true).start();

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

        m1.setUseChangeStream(true).start();
        m2.setUseChangeStream(true).start();
        m3.setUseChangeStream(true).start();
        try {
            Thread.sleep(100);
            log.info("Sending message");

            Msg m = new Msg();
            m.setExclusive(true);
            m.setName("A message");
            // m.setTtl(10000000);
            // sender.queueMessage(m);
            sender.sendMessage(m);
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
        morphium.dropCollection(Msg.class, "msg", null);
        Thread.sleep(100);
        Messaging m1 = new Messaging(morphium, 1000, false);
        Msg m = new Msg().setMsgId(new MorphiumId()).setMsg("msg").setName("the_name").setValue("a value");
        m1.sendMessage(m);
        Thread.sleep(100);
        List<Msg> lst = morphium.createQueryFor(Msg.class).asList();
        m1.removeMessage(m);
        Thread.sleep(100);

        assertEquals(0, morphium.createQueryFor(Msg.class).countAll());
        m1.terminate();
    }

    @Test
    public void timeoutMessages() throws Exception {
        final AtomicInteger cnt = new AtomicInteger();
        Messaging m1 = new Messaging(morphium, 1000, false);
        m1.addMessageListener((msg, m) -> {
            log.error("ERROR!");
            cnt.incrementAndGet();
            return null;
        });
        m1.setUseChangeStream(true).start();
        Thread.sleep(100);
        Msg m = new Msg().setMsgId(new MorphiumId()).setMsg("msg").setName("timeout_name").setValue("a value").setTtl(-1000);
        m1.sendMessage(m);
        Thread.sleep(200);
        assert (cnt.get() == 0);
        m1.terminate();

    }

    @Test
    public void selfMessages() throws Exception {
        morphium.dropCollection(Msg.class);
        Messaging sender = new Messaging(morphium, 100, false);
        sender.setUseChangeStream(true);
        assert (sender.isReceiveAnswers());
        assert (sender.getReceiveAnswers().equals(Messaging.ReceiveAnswers.ONLY_MINE)); //default!!!
        assert (sender.isUseChangeStream());
        assert (sender.getWindowSize() > 0);
        assert (sender.getQueueName() == null);

        sender.start();
        Thread.sleep(1500);
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
        m1.setUseChangeStream(true).start();
        try {
            sender.sendMessageToSelf(new Msg("testmsg", "Selfmessage", "value"));
            Thread.sleep(1500);
            assert (gotMessage);
            assert (!gotMessage1);

            gotMessage = false;
            sender.queueMessagetoSelf(new Msg("testmsg", "SelfMessage", "val"));
            Thread.sleep(1400);
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
        Messaging sender = new Messaging(morphium, 100, false);
        sender.setUseChangeStream(true).start();

        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        Messaging m3 = new Messaging(morphium, 100, false);
        Messaging m2 = new Messaging(morphium, 100, false);
        Messaging m1 = new Messaging(morphium, 100, false);

        try {
            m3.addMessageListener((msg, m) -> {
                gotMessage3 = true;
                return null;
            });

            m3.setUseChangeStream(true).start();

            Thread.sleep(1500);


            sender.sendMessage(new Msg("test", "testmsg", "testvalue", 120000, false));

            Thread.sleep(1000);
            assert (gotMessage3);
            Thread.sleep(2000);


            m1.addMessageListener((msg, m) -> {
                gotMessage1 = true;
                return null;
            });

            m1.setUseChangeStream(true).start();

            Thread.sleep(1500);
            assert (gotMessage1);


            m2.addMessageListener((msg, m) -> {
                gotMessage2 = true;
                return null;
            });

            m2.setUseChangeStream(true).start();

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
        morphium.getConfig().setMaxWaitTime(1000);
        morphium.dropCollection(Msg.class);
        Thread.sleep(1000);
        Messaging sender = new Messaging(morphium, 100, false, false, 10);
        sender.setUseChangeStream(true).start();

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
        receiver.setUseChangeStream(true).start();
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
        Messaging sender = new Messaging(morphium, 100, false, true, 10);
        sender.setUseChangeStream(true).start();

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
        receiver.setUseChangeStream(true).start();
        try {
            Thread.sleep(500);
            sender.sendMessage(new Msg("test", "test", "test"));
            sender.sendMessage(new Msg("test", "test", "test"));
            Thread.sleep(2000);

            assert (list.size() == 2) : "Size wrong: " + list.size();
        } finally {
            sender.terminate();
            receiver.terminate();
        }
    }


    @Test
    public void priorityTest() throws Exception {
        Messaging sender = new Messaging(morphium, 100, false);
        sender.setUseChangeStream(true).start();

        list.clear();
        //if running multithreadded, the execution order might differ a bit because of the concurrent
        //execution - hence if set to multithreadded, the test will fail!
        Messaging receiver = new Messaging(morphium, 10, false, false, 100);
        try {
            receiver.addMessageListener((msg, m) -> {
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
            receiver.setUseChangeStream(true).start();

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
                sender.sendMessage(m);
            }

            Thread.sleep(1000);
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


    @Test
    public void markExclusiveMessageTest() throws Exception {

        Messaging sender = new Messaging(morphium, 100, false);
        morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
        sender.setUseChangeStream(true).start();
        Messaging receiver = new Messaging(morphium, 10, false, true, 10);
        receiver.setUseChangeStream(true).start();
        Messaging receiver2 = new Messaging(morphium, 10, false, true, 10);
        receiver2.setUseChangeStream(true).start();

        final AtomicInteger pausedReciever = new AtomicInteger(0);

        try {
            Thread.sleep(100);
            receiver.addMessageListener((msg, m) -> {
//                log.info("R1: Incoming message");
                assert (pausedReciever.get() != 1);
                return null;
            });

            receiver2.addMessageListener((msg, m) -> {
//                log.info("R2: Incoming message");
                assert (pausedReciever.get() != 2);
                return null;
            });


            for (int i = 0; i < 200; i++) {
                Msg m = new Msg("test", "test", "value", 3000000, true);
                sender.sendMessage(m);
                if (i == 100) {
                    receiver2.pauseProcessingOfMessagesNamed("test");
                    Thread.sleep(50);
                    pausedReciever.set(2);
                } else if (i == 120) {
                    receiver.pauseProcessingOfMessagesNamed("test");
                    Thread.sleep(50);
                    pausedReciever.set(1);
                } else if (i == 160) {
                    receiver.unpauseProcessingOfMessagesNamed("test");
                    //receiver.findAndProcessPendingMessages("test");
                    receiver2.unpauseProcessingOfMessagesNamed("test");
                    //receiver2.findAndProcessPendingMessages("test");
                    pausedReciever.set(0);
                }

            }

            long start = System.currentTimeMillis();
            Query<Msg> q = morphium.createQueryFor(Msg.class).f(Msg.Fields.name).eq("test").f(Msg.Fields.processedBy).eq(null);
            while (q.countAll() > 0) {
                log.info("Count is still: " + q.countAll());
                Thread.sleep(500);
                if (System.currentTimeMillis()-start > 15000){
                    throw new AssertionError("This took too long! count: "+q.countAll());
                }
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
        morphium.dropCollection(Msg.class);
        Thread.sleep(100);
        Messaging sender = new Messaging(morphium, 1000, false);
        sender.setSenderId("sender");
        morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
        Thread.sleep(100);
        sender.setUseChangeStream(true).start();
        Messaging receiver = new Messaging(morphium, (int) (50 + 100 * Math.random()), true, true, 15);
        receiver.setSenderId("r1");
        receiver.setUseChangeStream(true).start();

        Messaging receiver2 = new Messaging(morphium, (int) (50 + 100 * Math.random()), false, false, 15);
        receiver2.setSenderId("r2");
        receiver2.setUseChangeStream(true).start();

        Messaging receiver3 = new Messaging(morphium, (int) (50 + 100 * Math.random()), true, false, 15);
        receiver3.setSenderId("r3");
        receiver3.setUseChangeStream(true).start();

        Messaging receiver4 = new Messaging(morphium, (int) (50 + 100 * Math.random()), false, true, 15);
        receiver4.setSenderId("r4");
        receiver4.setUseChangeStream(true).start();

        final Map<WriteAccessType, AtomicInteger> wcounts = new ConcurrentHashMap<>();
        final Map<ReadAccessType, AtomicInteger> rcounts = new ConcurrentHashMap<>();

        final AtomicInteger received = new AtomicInteger();
        final AtomicInteger dups = new AtomicInteger();
        final Map<String, Long> ids = new ConcurrentHashMap<>();
        final Map<String, String> recById = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> recieveCount = new ConcurrentHashMap<>();
        Thread.sleep(100);
        try {
            MessageListener messageListener = (msg, m) -> {
                msg.pauseProcessingOfMessagesNamed("m");
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
                msg.unpauseProcessingOfMessagesNamed("m");
                return null;
            };
            receiver.addListenerForMessageNamed("m", messageListener);
            receiver2.addListenerForMessageNamed("m", messageListener);
            receiver3.addListenerForMessageNamed("m", messageListener);
            receiver4.addListenerForMessageNamed("m", messageListener);
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
                for (Messaging m : Arrays.asList(receiver, receiver2, receiver3, receiver4)) {
                    assert (m.getRunningTasks() <= 10) : m.getSenderId() + " runs too many tasks! " + m.getRunningTasks();
                }
                assertEquals(0,dups.get(),"Got duplicate message");
                assertTrue((exclusiveAmount + broadcastAmount * 4 - rec - messageCount)>0);
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


            for (WriteAccessType w : wcounts.keySet()) {
                log.info("Write: " + w.name() + " => " + wcounts.get(w));
            }
            for (ReadAccessType r : rcounts.keySet()) {
                log.info("Read: " + r.name() + " => " + wcounts.get(r));
            }


        } finally {
            sender.terminate();
            receiver.terminate();
            receiver2.terminate();
            receiver3.terminate();
            receiver4.terminate();
        }

    }

    @Test
    public void exclusivityTest() throws Exception {
        Messaging sender = new Messaging(morphium, 100, false);
        sender.setSenderId("sender");
        morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
        Thread.sleep(100);
        sender.setUseChangeStream(true).start();
        Messaging receiver = new Messaging(morphium, 10, true, true, 15);
        receiver.setSenderId("r1");
        receiver.setUseChangeStream(true).start();

        Messaging receiver2 = new Messaging(morphium, 10, false, false, 15);
        receiver2.setSenderId("r2");
        receiver2.setUseChangeStream(true).start();

        Messaging receiver3 = new Messaging(morphium, 10, true, false, 15);
        receiver3.setSenderId("r3");
        receiver3.setUseChangeStream(true).start();

        Messaging receiver4 = new Messaging(morphium, 10, false, true, 15);
        receiver4.setSenderId("r4");
        receiver4.setUseChangeStream(true).start();
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
            receiver.addListenerForMessageNamed("m", messageListener);
            receiver2.addListenerForMessageNamed("m", messageListener);
            receiver3.addListenerForMessageNamed("m", messageListener);
            receiver4.addListenerForMessageNamed("m", messageListener);
            int amount = 20;
            int broadcastAmount = 20;
            for (int i = 0; i < amount; i++) {
                int rec = received.get();
                long messageCount = 0;
                messageCount += receiver.getPendingMessagesCount();
                if (i % 100 == 0) log.info("Send " + i + " recieved: " + rec + " queue: " + messageCount);
                Msg m = new Msg("m", "m", "v" + i, 3000000, true);
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
                for (Messaging m : Arrays.asList(receiver, receiver2, receiver3, receiver4)) {
                    log.info(m.getSenderId() + " active Tasks: " + m.getRunningTasks());
                }
                assertTrue(amount + broadcastAmount * 4 - rec - messageCount>=0);
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
        }

    }


    @Test
    public void exclusiveMessageStartupTests() throws Exception {
        Messaging sender = new Messaging(morphium, 100, false);
        Messaging receiverNoListener = new Messaging(morphium, 100, true);
        try {
            sender.setSenderId("sender");
            morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
            Thread.sleep(100);
            sender.setUseChangeStream(true).start();

            sender.sendMessage(new Msg("test", "test", "test", 30000, true));
            sender.sendMessage(new Msg("test", "test", "test", 30000, true));
            sender.sendMessage(new Msg("test", "test", "test", 30000, true));
            Thread.sleep(1000);
            receiverNoListener.setSenderId("recNL");
            receiverNoListener.setUseChangeStream(true).start();

            assert (morphium.createQueryFor(Msg.class, sender.getCollectionName()).countAll() == 3);
        } finally {
            sender.terminate();
            receiverNoListener.terminate();
        }
    }

    @Test
    public void exclusiveTest() throws Exception {
        morphium.dropCollection(Msg.class);
        Messaging sender;
        List<Messaging> recs;

        sender = new Messaging(morphium, 1000, false);
        sender.setSenderId("sender");
        //sender.setUseChangeStream(true).start();
        final AtomicInteger counts = new AtomicInteger();
        recs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Messaging r = new Messaging(morphium, 100, false);
            r.setSenderId("r" + i);
            recs.add(r);
            r.setUseChangeStream(true).start();

            r.addMessageListener((m, msg) -> {
                counts.incrementAndGet();
                return null;
            });
        }
        try {

            for (int i = 0; i < 50; i++) {
                if (i % 10 == 0) log.info("Msg sent");
                sender.sendMessage(new Msg("excl_name", "msg", "value", 20000000, true));
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
                sender.sendMessage(new Msg("excl_name", "msg", "value", 20000000, false));
            }
            while (counts.get() < 10 * recs.size()) {
                log.info("Still waiting for incoming messages: " + counts.get());
                Thread.sleep(1000);
            }
            Thread.sleep(2000);
            assert (counts.get() == 10 * recs.size()) : "Did get too many? " + counts.get();

        } finally {
            sender.terminate();
            for (Messaging r : recs) r.terminate();


        }

        for (Messaging r : recs) {
            assert (!r.isRunning());
        }

    }


    @Test
    public void severalRecipientsTest() throws Exception {
        Messaging sender = new Messaging(morphium, 1000, false);
        sender.setSenderId("sender");
        sender.setUseChangeStream(true).start();

        List<Messaging> receivers = new ArrayList<>();
        final List<String> receivedBy = new Vector<>();

        try {
            for (int i = 0; i < 10; i++) {
                Messaging receiver1 = new Messaging(morphium, 1000, false);
                receiver1.setSenderId("rec" + i);
                receiver1.setUseChangeStream(true).start();
                receivers.add(receiver1);
                receiver1.addMessageListener(new MessageListener() {
                    @Override
                    public Msg onMessage(Messaging msg, Msg m)  {
                        receivedBy.add(msg.getSenderId());
                        return null;
                    }
                });
            }

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
            sender.terminate();
            for (Messaging ms : receivers) {
                ms.terminate();
            }
        }


    }

}
