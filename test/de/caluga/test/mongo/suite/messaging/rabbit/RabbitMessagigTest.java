package de.caluga.test.mongo.suite.messaging.rabbit;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MessageRejectedException;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.MorphiumTestBase;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 17:34
 */
@SuppressWarnings("Duplicates")
public class RabbitMessagigTest extends MorphiumTestBase {
    public boolean gotMessage = false;

    public boolean gotMessage1 = false;
    public boolean gotMessage2 = false;
    public boolean gotMessage3 = false;
    public boolean gotMessage4 = false;

    public boolean error = false;

    public MorphiumId lastMsgId;

    public AtomicInteger procCounter = new AtomicInteger(0);

    private List<Msg> list = new ArrayList<>();

    private AtomicInteger queueCount = new AtomicInteger(1000);

    @Test
    public void testMsgQueName() throws Exception {

        Messaging m = new Messaging("localhost", morphium, "mmsg1", true);
        m.addMessageListener((msg, m1) -> {
            gotMessage1 = true;
            return null;
        });
        m.start();

        Messaging m2 = new Messaging("localhost", morphium, "mmsg2", true);
        m2.addMessageListener((msg, m1) -> {
            gotMessage2 = true;
            return null;
        });
        m2.start();

        Msg msg = new Msg("tst", "msg", "value", 30000);
        msg.setExclusive(false);
        m.sendMessage(msg);
        Thread.sleep(1);

        msg = new Msg("tst2", "msg", "value", 30000);
        msg.setExclusive(false);
        m2.sendMessage(msg);

        Thread.sleep(200);
        assert (!gotMessage1);
        assert (!gotMessage2);
        m.terminate();
        m2.terminate();
        Thread.sleep(200);
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


    @SuppressWarnings("Duplicates")
    @Test
    public void multithreaddingTest() throws Exception {
        Messaging producer = new Messaging("localhost", morphium, "msg", false);
        Messaging consumer = new Messaging("localhost", morphium, "msg", true);
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

        for (int i = 0; i < 1000; i++) {
            Msg m = new Msg("test" + i, "tm", "" + i + System.currentTimeMillis(), 10000);
            producer.sendMessage(m);
        }


        Thread.sleep(3000);
        consumer.terminate();
        producer.terminate();
        log.info("Messages processed: " + procCounter.get());
        log.info("Messages left: " + consumer.getMessageCount());

    }


    @Test
    public void messagingTest() throws Exception {
        error = false;

        final Messaging messaging = new Messaging("localhost", morphium, "msg", false);
        messaging.start();

        messaging.addMessageListener((msg, m) -> {
            log.info("Got Message: " + m.toString());
            gotMessage = true;
            return null;
        });
        messaging.sendMessage(new Msg("Testmessage", "A message", "the value - for now", 5000));

        Thread.sleep(1000);
        assert (!gotMessage) : "Message recieved from self?!?!?!";
        log.info("Dig not get own message - cool!");

        Msg m = new Msg("meine Message", "The Message", "value is a string", 5000);
        m.setMsgId(new MorphiumId());
        m.setSender("Another sender");

        messaging.sendMessage(m);

        Thread.sleep(100);
        assert (gotMessage) : "Message did not come?!?!?";

        gotMessage = false;
        Thread.sleep(100);
        assert (!gotMessage) : "Got message again?!?!?!";

        messaging.terminate();
        Thread.sleep(500);
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
        final Messaging m1 = new Messaging("localhost", morphium, "msg", false);
        final Messaging m2 = new Messaging("localhost", morphium, "msg", false);
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

        m1.sendMessage(new Msg("testmsg1", "The message from M1", "Value"));
        Thread.sleep(100);
        assert (gotMessage2) : "Message not recieved yet?!?!?";
        gotMessage2 = false;

        m2.sendMessage(new Msg("testmsg2", "The message from M2", "Value"));
        Thread.sleep(100);
        assert (gotMessage1) : "Message not recieved yet?!?!?";
        gotMessage1 = false;
        assert (!error);
        m1.terminate();
        m2.terminate();
        Thread.sleep(100);
        assert (!m1.isAlive()) : "m1 still running?";
        assert (!m2.isAlive()) : "m2 still running?";

    }

    @Test
    public void severalSystemsTest() throws Exception {
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;
        error = false;


        final Messaging m1 = new Messaging("localhost", morphium, "msg", false);
        final Messaging m2 = new Messaging("localhost", morphium, "msg", false);
        final Messaging m3 = new Messaging("localhost", morphium, "msg", false);
        final Messaging m4 = new Messaging("localhost", morphium, "msg", false);

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

        m1.sendMessage(new Msg("testmsg_excl", "This is the message", "value", 30000, true));
        Thread.sleep(500);
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
        Thread.sleep(500);
        assert (!m1.isAlive()) : "M1 still running";
        assert (!m2.isAlive()) : "M2 still running";
        assert (!m3.isAlive()) : "M3 still running";
        assert (!m4.isAlive()) : "M4 still running";


    }


    @Test
    public void testRejectMessage() throws Exception {

        Messaging sender = null;
        Messaging rec1 = null;
        Messaging rec2 = null;
        try {
            sender = new Messaging("localhost", morphium, "msg", false);
            rec1 = new Messaging("localhost", morphium, "msg", false);
            rec2 = new Messaging("localhost", morphium, "msg", false);

            sender.start();
            rec1.start();
            rec2.start();
            Thread.sleep(2000);
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;

            rec1.addMessageListener((msg, m) -> {
                gotMessage1 = true;
                log.info("Rejecting msg " + m.getMsgId().toString());
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

            sender.sendMessage(new Msg("test", "message", "value"));

            Thread.sleep(1000);
            assert (gotMessage1);
            assert (gotMessage2);
            assert (!gotMessage3);
        } finally {
            sender.terminate();
            rec1.terminate();
            rec2.terminate();
//            sender.sendMessage(new Msg("quitting", "quit", "quit", 10, false));
            Thread.sleep(1000);
        }


    }

    @Test
    public void directedMessageTest() throws Exception {

        final Messaging m1;
        final Messaging m2;
        final Messaging m3;
        m1 = new Messaging("localhost", morphium, "msg", false);
        m2 = new Messaging("localhost", morphium, "msg", false);
        m3 = new Messaging("localhost", morphium, "msg", false);
        try {

            m1.start();
            m2.start();
            m3.start();
            Thread.sleep(500);
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
            Thread.sleep(500);
            assert (!gotMessage1) : "Message recieved again by m1?!?!?";
            assert (!gotMessage2) : "Message recieved again by m2?!?!?";
            assert (!gotMessage3) : "Message recieved again by m3?!?!?";
            assert (!error);

            log.info("Sending direct message");
            Msg m = new Msg("testmsg1", "The message from M1", "Value");
            m.addRecipient(m2.getSenderId());
            m1.sendMessage(m);
            Thread.sleep(1000);
            assert (gotMessage2) : "direct Message not received by m2?";
            assert (!gotMessage1) : "direct Message recieved by m1?!?!?";
            assert (!gotMessage3) : "direct Message recieved by m3?!?!?";
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
        Messaging m1 = new Messaging(morphium, 10, false, true, 10);
        m1.setSenderId("m1");
        Messaging m2 = new Messaging(morphium, 10, false, true, 10);
        m2.setSenderId("m2");
        m1.start();
        m2.start();

        Msg m = new Msg("test", "ignore me please", "value");
        m1.sendMessage(m);
        Thread.sleep(1000);
        m = morphium.reread(m);
        assert (m.getProcessedBy().size() == 1) : "wrong number of proccessed by entries: " + m.getProcessedBy().size();
    }

    @Test
    public void severalMessagingsTest() throws Exception {
        Messaging m1 = new Messaging("localhost", morphium, "msg", true);
        m1.setSenderId("m1");
        Messaging m2 = new Messaging("localhost", morphium, "msg", true);
        m2.setSenderId("m2");
        Messaging m3 = new Messaging("localhost", morphium, "msg", true);
        m3.setSenderId("m3");
        m1.start();
        m2.start();
        m3.start();

        m3.addListenerForMessageNamed("test", (msg, m) -> {
            //log.info("Got message: "+m.getName());
            log.info("Sending answer for " + m.getMsgId());
            return new Msg("test", "answer", "value", 600000);
        });

        procCounter.set(0);
        for (int i = 0; i < 180; i++) {
            new Thread() {
                public void run() {
                    Msg m = new Msg("test", "nothing", "value");
                    m.setTtl(60000000);
                    Msg a = m1.sendAndAwaitFirstAnswer(m, 6000);
                    assert (a != null);
                    procCounter.incrementAndGet();
                }
            }.start();

        }
        while (procCounter.get() < 150) {
            Thread.yield();
        }

        m1.terminate();
        m2.terminate();
        m3.terminate();

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
                Messaging m = new Messaging("localhost", morphium, "msg", false);
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
                systems.get(m).sendMessage(msg);
            }

            long dur = System.currentTimeMillis() - start;
            log.info("Queueing " + numberOfMessages + " messages took " + dur + " ms - now waiting for writes..");
            waitForWrites();
            log.info("...all messages persisted!");
            int last = 0;
            assert (!failed[0]);
            Thread.sleep(100);
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
        final Messaging m1 = new Messaging("localhost", morphium, "msg", false);
        final Messaging m2 = new Messaging("localhost", morphium, "msg", false);
        final Messaging m3 = new Messaging("localhost", morphium, "msg", false);
        final Messaging m4 = new Messaging("localhost", morphium, "msg", false);
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
        m1.sendMessage(m);

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
        m4.terminate();

    }

    @Test
    public void messagingSendReceiveThreaddedTest() throws Exception {
        morphium.dropCollection(Msg.class);
        Thread.sleep(2500);
        final Messaging producer = new Messaging("localhost", morphium, "msg", false);
        final Messaging consumer = new Messaging("localhost", morphium, "msg", true);
        producer.start();
        consumer.start();
        Vector<String> processedIds = new Vector<>();
        procCounter.set(0);
        final AtomicInteger parallels = new AtomicInteger();
        consumer.addMessageListener((msg, m) -> {
            procCounter.incrementAndGet();
            parallels.incrementAndGet();
            if (processedIds.contains(m.getMsgId().toString())) {
                log.error("Received msg twice: " + procCounter.get() + "/" + m.getMsgId());
                return null;
            }
            processedIds.add(m.getMsgId().toString());
            //simulate processing
            try {
                Thread.sleep((long) (1000 * Math.random()));
            } catch (InterruptedException e) {

            }
//            log.info("Thread finished");
            parallels.decrementAndGet();
            return null;
        });
        Thread.sleep(2500);
        int amount = 1000;
        log.info("------------- sending messages");
        for (int i = 0; i < amount; i++) {
            producer.sendMessage(new Msg("Test " + i, "msg " + i, "value " + i, 30000, true));
        }
        log.info("PArallels: " + parallels);
        for (int i = 0; i < 30 && procCounter.get() < amount; i++) {
            Thread.sleep(1000);
            log.info("Still processing: " + procCounter.get());
            log.info("PArallels: " + parallels);
        }
        while (parallels.get() > 0) {
            Thread.sleep(100);
        }

    }


    @Test
    public void messagingSendReceiveTest() throws Exception {
        final Messaging producer = new Messaging("localhost", morphium, "msg", false);
        final Messaging consumer = new Messaging("localhost", morphium, "msg", false);
        producer.start();
        consumer.start();
        Thread.sleep(500);
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

        producer.terminate();
        consumer.terminate();
        Thread.sleep(1000);

    }


    @Test
    public void mutlithreaddedMessagingPerformanceTest() throws Exception {
        morphium.clearCollection(Msg.class);
        final Messaging producer = new Messaging("localhost", morphium, "msg", false);
        final Messaging consumer = new Messaging("localhost", morphium, "msg", false);
        consumer.start();
        producer.start();
        Thread.sleep(500);
        final int[] processed = {0};
        final Map<String, Long> msgCountById = new Hashtable<>();
        consumer.addMessageListener((msg, m) -> {
            synchronized (processed) {
                processed[0]++;
            }
            if (processed[0] % 1000 == 0) {
                log.info("Consumed " + processed[0]);
            }
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
            producer.sendMessage(m);
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
            sender = new Messaging("localhost", morphium, "test", false);
            sender.start();
            sender2 = new Messaging("localhost", morphium, "test2", false);
            sender2.start();

            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;

            m1 = new Messaging("localhost", morphium, "test", false);
            m1.addMessageListener((msg, m) -> {
                gotMessage1 = true;
                log.info("Got message m1");
                return null;
            });
            m2 = new Messaging("localhost", morphium, "test", false);
            m2.addMessageListener((msg, m) -> {
                gotMessage2 = true;
                log.info("Got message m2");
                return null;
            });
            m3 = new Messaging("localhost", morphium, "test2", false);
            m3.addMessageListener((msg, m) -> {
                gotMessage3 = true;
                log.info("Got message m3");
                return null;
            });
            Messaging m4 = new Messaging("localhost", morphium, "test2", false);
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
            Thread.sleep(1000);

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
            sender2.sendMessage(m);
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
        Messaging sender = new Messaging("localhost", morphium, "msg", false);
        sender.start();

        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        Messaging m1 = new Messaging("localhost", morphium, "msg", false);
        m1.addMessageListener((msg, m) -> {
            gotMessage1 = true;
            return null;
        });
        Messaging m2 = new Messaging("localhost", morphium, "msg", false);
        m2.addMessageListener((msg, m) -> {
            gotMessage2 = true;
            return null;
        });
        Messaging m3 = new Messaging("localhost", morphium, "msg", false);
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
        m1.terminate();
        m2.terminate();
        m3.terminate();
        sender.terminate();


    }

    @Test
    public void selfMessages() throws Exception {
        Messaging sender = new Messaging("localhost", morphium, "msg", false);
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


    @Test
    public void waitingForMessagesIfNonMultithreadded() throws Exception {

        Messaging sender = new Messaging("localhost", morphium, "msg", false);
        sender.start();

        list.clear();
        Messaging receiver = new Messaging("localhost", morphium, "msg", false);
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
        sender.sendMessage(new Msg("test", "test", "test"));
        sender.sendMessage(new Msg("test", "test", "test"));

        Thread.sleep(500);
        assert (list.size() == 1) : "Size wrong: " + list.size();
        Thread.sleep(2200);
        assert (list.size() == 2);

        sender.terminate();
        receiver.terminate();

    }

    @Test
    public void waitingForMessagesIfMultithreadded() throws Exception {
        log.info("Max threadpool:" + morphium.getConfig().getThreadPoolMessagingCoreSize());
        Thread.sleep(1000);
        Messaging sender = new Messaging("localhost", morphium, "msg", true);
        sender.start();

        list.clear();
        Messaging receiver = new Messaging("localhost", morphium, "msg", true);
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
        sender.sendMessage(new Msg("test", "test", "test"));
        sender.sendMessage(new Msg("test", "test", "test"));
        Thread.sleep(1000);

        assert (list.size() == 2) : "Size wrong: " + list.size();

        sender.terminate();
        receiver.terminate();

    }

//
//    @Test
//    public void priorityTest() throws Exception {
//        Messaging sender =  new Messaging("localhost",morphium, "msg", false);
//        sender.start();
//
//
//        for (int i = 0; i < 10; i++) {
//            Msg m = new Msg("test", "test", "test");
//            m.setPriority((int) (1000.0 * Math.random()));
//            log.info("Stored prio: " + m.getPriority());
//            sender.sendMessage(m);
//        }
//
//        Thread.sleep(1000);
//
//        list.clear();
//        //if running multithreadded, the execution order might differ a bit because of the concurrent
//        //execution - hence if set to multithreadded, the test will fail!
//        Messaging receiver =  new Messaging("localhost",morphium, "msg", false);
//
//        receiver.addMessageListener((msg, m) -> {
//            log.info("Incoming message: prio " + m.getPriority() + "  timestamp: " + m.getTimestamp());
//            list.add(m);
//            return null;
//        });
//
//        receiver.start();
//
//        while (list.size() < 10) {
//            Thread.yield();
//        }
//
//        int lastValue = -888888;
//
//        for (Msg m : list) {
//            log.info("prio: " + m.getPriority());
//            assert (m.getPriority() >= lastValue);
//            lastValue = m.getPriority();
//        }
//
//
//        receiver.pauseProcessingOfMessagesNamed("test");
//        list.clear();
//        for (int i = 0; i < 10; i++) {
//            Msg m = new Msg("test", "test", "test");
//            m.setPriority((int) (10000.0 * Math.random()));
//            log.info("Stored prio: " + m.getPriority());
//            sender.sendMessage(m);
//        }
//
//        Thread.sleep(1000);
//        receiver.unpauseProcessingOfMessagesNamed("test");
//        receiver.findAndProcessPendingMessages("test");
//        while (list.size() < 10) {
//            Thread.yield();
//        }
//
//        lastValue = -888888;
//
//        for (Msg m : list) {
//            log.info("prio: " + m.getPriority());
//            assert (m.getPriority() >= lastValue);
//            lastValue = m.getPriority();
//        }
//
//
//        sender.terminate();
//        receiver.terminate();
//    }
//

}
