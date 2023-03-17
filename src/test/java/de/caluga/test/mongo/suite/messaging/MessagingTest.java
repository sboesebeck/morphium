package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.*;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MessageRejectedException;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 17:34
 * <p/>
 */
@SuppressWarnings("ALL")
public class MessagingTest extends MultiDriverTestBase {
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
    @MethodSource("getMorphiumInstances")
    public void execAfterRelease(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(Msg.class);
            morphium.dropCollection(Msg.class, "mmsg_msg2", null);
            Messaging m = new Messaging(morphium);
            m.setSenderId("sender");
            // m.start();
            AtomicInteger received = new AtomicInteger();
            Messaging rec = new Messaging(morphium);
            rec.setSenderId("rec");
            rec.start();
            rec.addMessageListener((mess, msg)->{
                received.incrementAndGet();
                return null;
            });
            Msg msg = new Msg("test", "msg1", "value1");
            msg.setProcessedBy(Arrays.asList("Paused"));
            msg.setExclusive(true);
            m.sendMessage(msg);
            Thread.sleep(1000);
            assertEquals(0, received.get());
            msg.setProcessedBy(new ArrayList<>());
            morphium.store(msg, m.getCollectionName(), null);
            Thread.sleep(1000);
            assertEquals(1, received.get(), "Did not get message?");
            m.terminate();
            rec.terminate();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testMsgQueName(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(Msg.class);
            morphium.dropCollection(Msg.class, "mmsg_msg2", null);
            Messaging m = new Messaging(morphium);
            m.setPause(500);
            m.setMultithreadded(false);
            m.setAutoAnswer(false);
            m.setProcessMultiple(true);
            assert(!m.isAutoAnswer());
            assert(!m.isMultithreadded());
            assert(m.getPause() == 500);
            assert(m.isProcessMultiple());
            m.addMessageListener((msg, m1)->{
                gotMessage1 = true;
                return null;
            });
            m.start();
            Messaging m2 = new Messaging(morphium, "msg2", 500, true);
            m2.addMessageListener((msg, m1)->{
                gotMessage2 = true;
                return null;
            });
            m2.start();

            try {
                Msg msg = new Msg("tst", "msg", "value", 30000);
                msg.setExclusive(false);
                m.sendMessage(msg);
                Thread.sleep(100);
                Query<Msg> q = morphium.createQueryFor(Msg.class);
                assert(q.countAll() == 1);
                q.setCollectionName(m2.getCollectionName());
                assert(q.countAll() == 0);
                msg = new Msg("tst2", "msg", "value", 30000);
                msg.setExclusive(false);
                m2.sendMessage(msg);
                Thread.sleep(100);
                q = morphium.createQueryFor(Msg.class);
                assert(q.countAll() == 1);
                q.setCollectionName("mmsg_msg2");
                assert(q.countAll() == 1) : "Count is " + q.countAll();
                Thread.sleep(4000);
                assert(!gotMessage1);
                assert(!gotMessage2);
            } finally {
                m.terminate();
                m2.terminate();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testMsgLifecycle(Morphium morphium) throws Exception {
        try(morphium) {
            Msg m = new Msg();
            m.setSender("Meine wunderbare ID " + System.currentTimeMillis());
            m.setMsgId(new MorphiumId());
            m.setName("A name");
            morphium.store(m);
            Thread.sleep(500);
            m = morphium.reread(m);
            assert(m.getTimestamp() > 0) : "Timestamp not updated?";
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void messagingTest(Morphium morphium) throws Exception {
        try (morphium) {
            error = false;
            morphium.dropCollection(Msg.class);
            final Messaging messaging = new Messaging(morphium, 500, true);
            messaging.start();
            Thread.sleep(500);
            messaging.addMessageListener((msg, m)->{
                log.info("Got Message: " + m.toString());
                gotMessage = true;
                return null;
            });
            messaging.sendMessage(new Msg("Testmessage", "A message", "the value - for now", 5000000));
            Thread.sleep(1000);
            assertFalse(gotMessage, "Message recieved from self?!?!?!");;
            log.info("Did not get own message - cool!");
            Msg m = new Msg("meine Message", "The Message", "value is a string", 5000000);
            m.setMsgId(new MorphiumId());
            m.setSender("Another sender");
            morphium.store(m, messaging.getCollectionName(), null);
            long start = System.currentTimeMillis();

            while (!gotMessage) {
                Thread.sleep(100);
                assertTrue(System.currentTimeMillis() - start < 5000, "Message timed out");
            }

            assertTrue(gotMessage);
            gotMessage = false;
            Thread.sleep(1000);
            assertFalse(gotMessage, "Got message again?!?!?!");
            messaging.terminate();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void systemTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(Msg.class);
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;
            error = false;
            morphium.clearCollection(Msg.class);
            Messaging m1 = new Messaging(morphium, 500, true);
            Messaging m2 = new Messaging(morphium, 500, true);
            m1.start();
            m2.start();
            Thread.sleep(100);
            m1.addMessageListener((msg, m)->{
                gotMessage1 = true;
                log.info("M1 got message " + m.toString());

                if (!m.getSender().equals(m2.getSenderId())) {
                    log.error("Sender is not M2?!?!? m2_id: " + m2.getSenderId() + " - message sender: " + m.getSender());
                    error = true;
                }
                return null;
            });
            m2.addMessageListener((msg, m)->{
                gotMessage2 = true;
                log.info("M2 got message " + m.toString());

                if (!m.getSender().equals(m1.getSenderId())) {
                    log.error("Sender is not M1?!?!? m1_id: " + m1.getSenderId() + " - message sender: " + m.getSender());
                    error = true;
                }
                return null;
            });
            m1.sendMessage(new Msg("testmsg1", "The message from M1", "Value"));

            while (!gotMessage2) {
                Thread.sleep(100);
            }

            assertTrue(gotMessage2, "Message not recieved yet?!?!?");
            gotMessage2 = false;
            m2.sendMessage(new Msg("testmsg2", "The message from M2", "Value"));
            Thread.sleep(1000);
            assertTrue(gotMessage1, "Message not recieved yet?!?!?");
            gotMessage1 = false;
            assertFalse(error);
            m1.terminate();
            m2.terminate();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void severalSystemsTest(Morphium morphium) throws Exception {
        try (morphium) {
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
            m1.addMessageListener((msg, m)->{
                gotMessage1 = true;
                log.info("M1 got message " + m.toString());
                return null;
            });
            m2.addMessageListener((msg, m)->{
                gotMessage2 = true;
                log.info("M2 got message " + m.toString());
                return null;
            });
            m3.addMessageListener((msg, m)->{
                gotMessage3 = true;
                log.info("M3 got message " + m.toString());
                return null;
            });
            m4.addMessageListener((msg, m)->{
                gotMessage4 = true;
                log.info("M4 got message " + m.toString());
                return null;
            });
            m1.sendMessage(new Msg("testmsg1", "The message from M1", "Value"));

            while (!gotMessage2 || !gotMessage3 || !gotMessage4) {
                Thread.sleep(100);
                //log.info("Still waiting for messages...");
            }

            assertTrue(gotMessage2, "Message not recieved yet by m2?!?!?");
            assertTrue(gotMessage3, "Message not recieved yet by m3?!?!?");
            assertTrue(gotMessage4, "Message not recieved yet by m4?!?!?");
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;
            m2.sendMessage(new Msg("testmsg2", "The message from M2", "Value"));

            while (!gotMessage1 || !gotMessage3 || !gotMessage4) {
                Thread.sleep(100);
                //log.info("Still waiting for messages...");
            }

            assertTrue(gotMessage1, "Message not recieved yet by m1?!?!?");
            assertTrue(gotMessage3, "Message not recieved yet by m3?!?!?");
            assertTrue(gotMessage4, "Message not recieved yet by m4?!?!?");
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;
            m1.sendMessage(new Msg("testmsg_excl", "This is the message", "value", 30000000, true));

            while (!gotMessage1 && !gotMessage2 && !gotMessage3 && !gotMessage4) {
                Thread.sleep(100);
            }

            int cnt = 0;

            if (gotMessage1) {
                cnt++;
            }

            if (gotMessage2) {
                cnt++;
            }

            if (gotMessage3) {
                cnt++;
            }

            if (gotMessage4) {
                cnt++;
            }

            assertTrue(cnt != 0, "Message was not received");
            assertTrue(cnt == 1, "Message was received too often: " + cnt);
            m1.terminate();
            m2.terminate();
            m3.terminate();
            m4.terminate();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void directedMessageTest(Morphium morphium) throws Exception {
        try (morphium) {
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
                m1.addMessageListener((msg, m)->{
                    gotMessage1 = true;

                    if (m.getTo() != null && !m.getTo().contains(m1.getSenderId())) {
                        log.error("wrongly received message?");
                        error = true;
                    }
                    log.info("DM-M1 got message " + m.toString());
                    //                assert (m.getSender().equals(m2.getSenderId())) : "Sender is not M2?!?!? m2_id: " + m2.getSenderId() + " - message sender: " + m.getSender();
                    return null;
                });
                m2.addMessageListener((msg, m)->{
                    gotMessage2 = true;
                    assert(m.getTo() == null || m.getTo().contains(m2.getSenderId())) : "wrongly received message?";
                    log.info("DM-M2 got message " + m.toString());
                    //                assert (m.getSender().equals(m1.getSenderId())) : "Sender is not M1?!?!? m1_id: " + m1.getSenderId() + " - message sender: " + m.getSender();
                    return null;
                });
                m3.addMessageListener((msg, m)->{
                    gotMessage3 = true;
                    assert(m.getTo() == null || m.getTo().contains(m3.getSenderId())) : "wrongly received message?";
                    log.info("DM-M3 got message " + m.toString());
                    //                assert (m.getSender().equals(m1.getSenderId())) : "Sender is not M1?!?!? m1_id: " + m1.getSenderId() + " - message sender: " + m.getSender();
                    return null;
                });
                //sending message to all
                log.info("Sending broadcast message");
                m1.sendMessage(new Msg("testmsg1", "The message from M1", "Value"));
                Thread.sleep(3000);
                assert(gotMessage2) : "Message not recieved yet by m2?!?!?";
                assert(gotMessage3) : "Message not recieved yet by m3?!?!?";
                assert(!error);
                gotMessage1 = false;
                gotMessage2 = false;
                gotMessage3 = false;
                error = false;
                TestUtils.waitForWrites(morphium, log);
                Thread.sleep(2500);
                assert(!gotMessage1) : "Message recieved again by m1?!?!?";
                assert(!gotMessage2) : "Message recieved again by m2?!?!?";
                assert(!gotMessage3) : "Message recieved again by m3?!?!?";
                assert(!error);
                log.info("Sending direct message");
                Msg m = new Msg("testmsg1", "The message from M1", "Value");
                m.addRecipient(m2.getSenderId());
                m1.sendMessage(m);
                Thread.sleep(1000);
                assert(gotMessage2) : "Message not received by m2?";
                assert(!gotMessage1) : "Message recieved by m1?!?!?";
                assert(!gotMessage3) : "Message  recieved again by m3?!?!?";
                gotMessage1 = false;
                gotMessage2 = false;
                gotMessage3 = false;
                error = false;
                Thread.sleep(1000);
                assert(!gotMessage1) : "Message recieved again by m1?!?!?";
                assert(!gotMessage2) : "Message not recieved again by m2?!?!?";
                assert(!gotMessage3) : "Message not recieved again by m3?!?!?";
                assert(!error);
                log.info("Sending message to 2 recipients");
                log.info("Sending direct message");
                m = new Msg("testmsg1", "The message from M1", "Value");
                m.addRecipient(m2.getSenderId());
                m.addRecipient(m3.getSenderId());
                m1.sendMessage(m);
                Thread.sleep(1000);
                assert(gotMessage2) : "Message not received by m2?";
                assert(!gotMessage1) : "Message recieved by m1?!?!?";
                assert(gotMessage3) : "Message not recieved by m3?!?!?";
                assert(!error);
                gotMessage1 = false;
                gotMessage2 = false;
                gotMessage3 = false;
                Thread.sleep(1000);
                assert(!gotMessage1) : "Message recieved again by m1?!?!?";
                assert(!gotMessage2) : "Message not recieved again by m2?!?!?";
                assert(!gotMessage3) : "Message not recieved again by m3?!?!?";
                assert(!error);
            } finally {
                m1.terminate();
                m2.terminate();
                m3.terminate();
                Thread.sleep(1000);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void ignoringMessagesTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(Msg.class);
            Thread.sleep(100);
            Messaging m1 = new Messaging(morphium, 10, false, true, 10);
            m1.setSenderId("m1");
            Messaging m2 = new Messaging(morphium, 10, false, true, 10);
            m2.setSenderId("m2");

            try {
                m1.start();
                m2.start();
                Thread.sleep(1000);
                Msg m = new Msg("test", "ignore me please", "value");
                m1.sendMessage(m);
                Thread.sleep(1000);
                m = morphium.reread(m);
                assert(m.getProcessedBy().size() == 0) : "wrong number of proccessed by entries: " + m.getProcessedBy().size();
            } finally {
                m1.terminate();
                m2.terminate();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void severalMessagingsTest(Morphium morphium) throws Exception {
        try (morphium) {
            Messaging m1 = new Messaging(morphium, 10, false, true, 10);
            m1.setSenderId("m1");
            Messaging m2 = new Messaging(morphium, 10, false, true, 10);
            m2.setSenderId("m2");
            Messaging m3 = new Messaging(morphium, 10, false, true, 10);
            m3.setSenderId("m3");
            m1.start();
            m2.start();
            m3.start();

            try {
                m3.addListenerForMessageNamed("multisystemtest", (msg, m)->{
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
                            Msg a = m1.sendAndAwaitFirstAnswer(m, 600000);
                            assertNotNull(a);
                            procCounter.incrementAndGet();
                        }
                    }
                    .start();
                }

                long s = System.currentTimeMillis();

                while (procCounter.get() < 180) {
                    Thread.sleep(1000);
                    log.info("Recieved " + procCounter.get());
                    assert(System.currentTimeMillis() - s < 600000);
                }
            } finally {
                m1.terminate();
                m2.terminate();
                m3.terminate();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void massiveMessagingTest(Morphium morphium) throws Exception {
        try (morphium) {
            List<Messaging> systems;
            systems = new ArrayList<>();

            try {
                int numberOfWorkers = 5;
                int numberOfMessages = 50;
                long ttl = 30000; //30 sec
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
                        final List<String> ids = Collections.synchronizedList(new ArrayList<>());
                        @Override
                        public Msg onMessage(Messaging msg, Msg m) {
                            if (ids.contains(msg.getSenderId() + "/" + m.getMsgId())) {
                                failed[0] = true;
                            }

                            assert(!ids.contains(msg.getSenderId() + "/" + m.getMsgId())) : "Re-getting message?!?!? " + m.getMsgId() + " MyId: " + msg.getSenderId();
                            ids.add(msg.getSenderId() + "/" + m.getMsgId());
                            assert(m.getTo() == null || m.getTo().contains(msg.getSenderId())) : "got message not for me?";
                            assert(!m.getSender().equals(msg.getSenderId())) : "Got message from myself?";

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
                    int m = (int)(Math.random() * systems.size());
                    Msg msg = new Msg("test" + i, "The message for msg " + i, "a value", ttl);
                    msg.addAdditional("Additional Value " + i);
                    msg.setExclusive(false);
                    systems.get(m).sendMessage(msg);
                }

                long dur = System.currentTimeMillis() - start;
                log.info("Queueing " + numberOfMessages + " messages took " + dur + " ms - now waiting for writes..");
                TestUtils.waitForWrites(morphium, log);
                log.info("...all messages persisted!");
                int last = 0;
                assert(!failed[0]);
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

                assert(!failed[0]);
                Thread.sleep(1000);
                log.info("done");
                assert(!failed[0]);
                assert(processedMessages.size() == numberOfMessages) : "sent " + numberOfMessages + " messages, but only " + processedMessages.size() + " were recieved?";

                for (MorphiumId id : processedMessages.keySet()) {
                    log.info(id + "---- ok!");
                    assert(processedMessages.get(id) == numberOfWorkers - 1) : "Message " + id + " was not recieved by all " + (numberOfWorkers - 1) + " other workers? only by "
                        + processedMessages.get(id);
                }

                assert(procCounter.get() == numberOfMessages * (numberOfWorkers - 1)) : "Still processing messages?!?!?";
                //Waiting for all messages to be outdated and deleted
            } finally {
                //Stopping all
                for (Messaging m : systems) {
                    m.terminate();
                }

                Thread.sleep(1000);

                for (Messaging m : systems) {
                    assert(!m.isAlive()) : "Thread still running?";
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void broadcastTest(Morphium morphium) throws Exception {
        try (morphium) {
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

            try {
                log.info("m1 ID: " + m1.getSenderId());
                log.info("m2 ID: " + m2.getSenderId());
                log.info("m3 ID: " + m3.getSenderId());
                m1.addMessageListener((msg, m)->{
                    gotMessage1 = true;

                    if (m.getTo() != null && m.getTo().contains(m1.getSenderId())) {
                        log.error("wrongly received message m1?");
                        error = true;
                    }
                    log.info("M1 got message " + m.toString());
                    return null;
                });
                m2.addMessageListener((msg, m)->{
                    gotMessage2 = true;

                    if (m.getTo() != null && !m.getTo().contains(m2.getSenderId())) {
                        log.error("wrongly received message m2?");
                        error = true;
                    }
                    log.info("M2 got message " + m.toString());
                    return null;
                });
                m3.addMessageListener((msg, m)->{
                    gotMessage3 = true;

                    if (m.getTo() != null && !m.getTo().contains(m3.getSenderId())) {
                        log.error("wrongly received message m3?");
                        error = true;
                    }
                    log.info("M3 got message " + m.toString());
                    return null;
                });
                m4.addMessageListener((msg, m)->{
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

                assert(!gotMessage1) : "Got message again?";
                assert(gotMessage4) : "m4 did not get msg?";
                assert(gotMessage2) : "m2 did not get msg?";
                assert(gotMessage3) : "m3 did not get msg";
                assert(!error);
                gotMessage2 = false;
                gotMessage3 = false;
                gotMessage4 = false;
                Thread.sleep(500);
                assert(!gotMessage1) : "Got message again?";
                assert(!gotMessage2) : "m2 did get msg again?";
                assert(!gotMessage3) : "m3 did get msg again?";
                assert(!gotMessage4) : "m4 did get msg again?";
                assert(!error);
            } finally {
                m1.terminate();
                m2.terminate();
                m3.terminate();
                m4.terminate();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void messagingSendReceiveTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(Msg.class);
            Thread.sleep(100);
            final Messaging producer = new Messaging(morphium, 100, true);
            final Messaging consumer = new Messaging(morphium, 10, true);
            producer.start();
            consumer.start();
            Thread.sleep(500);

            try {
                final int[] processed = {0};
                final Vector<String> messageIds = new Vector<>();
                consumer.addMessageListener((msg, m)->{
                    processed[0]++;

                    if (processed[0] % 50 == 1) {
                        log.info(processed[0] + "... Got Message " + m.getName() + " / " + m.getMsg() + " / " + m.getValue());
                    }
                    if (messageIds.contains(m.getMsgId().toString())) {
                        if (m.getProcessedBy().size() != 0 && m.getProcessedBy().contains(consumer.getSenderId())) {
                            log.error("Was already processed by me!");
                        }
                    }
                    assert(!messageIds.contains(m.getMsgId().toString())) : "Duplicate message: " + processed[0];
                    messageIds.add(m.getMsgId().toString());
                    //simulate processing
                    try {
                        Thread.sleep((long)(10 * Math.random()));
                    } catch (InterruptedException e) {
                    }
                    return null;
                });
                int amount = 1000;

                for (int i = 0; i < amount; i++) {
                    if (i % 100 == 0) {
                        log.info("Storing messages... " + i);
                    }

                    producer.sendMessage(new Msg("Test " + i, "msg " + i, "value " + i));
                }

                for (int i = 0; i < 70 && processed[0] < amount; i++) {
                    log.info("Still processing: " + processed[0]);
                    Thread.sleep(1000);
                }

                assert(processed[0] == amount) : "Did process " + processed[0];
            } finally {
                producer.terminate();
                consumer.terminate();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void removeMessageTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(Msg.class, "msg", null);
            Thread.sleep(100);
            Messaging m1 = new Messaging(morphium, 1000, false);

            try {
                Msg m = new Msg().setMsgId(new MorphiumId()).setMsg("msg").setName("the_name").setValue("a value");
                m1.sendMessage(m);
                Thread.sleep(100);
                m1.removeMessage(m);
                long s = System.currentTimeMillis();

                while (true) {
                    Thread.sleep(100);

                    if (morphium.createQueryFor(Msg.class).countAll() == 0) {
                        break;
                    }

                    assert(System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
                }
            } finally {
                m1.terminate();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void timeoutMessages(Morphium morphium) throws Exception {
        try (morphium) {
            final AtomicInteger cnt = new AtomicInteger();
            Messaging m1 = new Messaging(morphium, 1000, false);

            try {
                m1.addMessageListener((msg, m)->{
                    log.error("ERROR!");
                    cnt.incrementAndGet();
                    return null;
                });
                m1.start();
                Thread.sleep(100);
                Msg m = new Msg().setMsgId(new MorphiumId()).setMsg("msg").setName("timeout_name").setValue("a value").setTtl(-1000);
                m1.sendMessage(m);
                Thread.sleep(200);
                assert(cnt.get() == 0);
            } finally {
                m1.terminate();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void selfMessages(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(Msg.class);
            Messaging sender = new Messaging(morphium, 100, false);
            assert(sender.isReceiveAnswers());
            assert(sender.getReceiveAnswers().equals(Messaging.ReceiveAnswers.ONLY_MINE));    //default!!!

            if (!sender.isUseChangeStream()) {
                log.error("Sender does not use changestream??!");
                sender.setUseChangeStream(true);
            }

            assert(sender.getWindowSize() > 0);
            assert(sender.getQueueName() == null);
            sender.start();
            Thread.sleep(2500);
            sender.addMessageListener(((msg, m)->{
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
            m1.addMessageListener((msg, m)->{
                gotMessage1 = true;
                return new Msg(m.getName(), "got message", "value", 5000);
            });
            m1.start();

            try {
                sender.sendMessageToSelf(new Msg("testmsg", "Selfmessage", "value"));
                long s = System.currentTimeMillis();

                while (!gotMessage || gotMessage1) {
                    Thread.sleep(100);
                    assert(System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
                }

                assert(gotMessage);
                assert(!gotMessage1);
                gotMessage = false;
                sender.queueMessagetoSelf(new Msg("testmsg", "SelfMessage", "val"));
                s = System.currentTimeMillis();

                while (!gotMessage || gotMessage1) {
                    Thread.sleep(100);
                    assert(System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
                }

                assert(gotMessage);
                assert(!gotMessage1);
            } finally {
                m1.terminate();
                sender.terminate();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void getPendingMessagesOnStartup(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(Msg.class);
            Thread.sleep(1000);
            Messaging sender = new Messaging(morphium, 100, false);
            sender.start();
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            gotMessage4 = false;
            Messaging m3 = new Messaging(morphium, 100, false);
            Messaging m2 = new Messaging(morphium, 100, false);
            Messaging m1 = new Messaging(morphium, 100, false);

            try {
                m3.addMessageListener((msg, m)->{
                    gotMessage3 = true;
                    return null;
                });
                m3.start();
                Thread.sleep(1500);
                sender.sendMessage(new Msg("test", "testmsg", "testvalue", 120000, false));

                while (!gotMessage3) {
                    Thread.sleep(100);
                }

                assert(gotMessage3);
                Thread.sleep(2000);
                m1.addMessageListener((msg, m)->{
                    gotMessage1 = true;
                    return null;
                });
                m1.start();

                while (!gotMessage1) {
                    Thread.sleep(150);
                }

                assert(gotMessage1);
                m2.addMessageListener((msg, m)->{
                    gotMessage2 = true;
                    return null;
                });
                m2.start();

                while (!gotMessage2) {
                    Thread.sleep(150);
                }

                assert(gotMessage2);
            } finally {
                m1.terminate();
                m2.terminate();
                m3.terminate();
                sender.terminate();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void priorityTest(Morphium morphium) throws Exception {
        try (morphium) {
            log.info("Running with " + morphium.getDriver().getName());
            Messaging sender = new Messaging(morphium, 100, false);
            sender.setSenderId("sender");
            sender.start();
            list.clear();
            //if running multithreadded, the execution order might differ a bit because of the concurrent
            //execution - hence if set to multithreadded, the test will fail!
            Messaging receiver = new Messaging(morphium, 10, false, false, 1);
            receiver.setSenderId("receiver");

            try {
                receiver.addMessageListener((msg, m)->{
                    log.info("Incoming message: prio " + m.getPriority() + "  timestamp: " + m.getTimestamp());
                    list.add(m);
                    return null;
                });

                for (int i = 0; i < 10; i++) {
                    Msg m = new Msg("test", "test", "test", 30000);
                    m.setPriority((int)(1000.0 * Math.random()));
                    log.info("Stored prio: " + m.getPriority());
                    sender.sendMessage(m);
                }

                Thread.sleep(1000);
                receiver.start();

                while (list.size() < 10) {
                    Thread.yield();
                }

                int lastValue = -888888;

                for (Msg m : list) {
                    log.info("prio: " + m.getPriority());
                    assert(m.getPriority() >= lastValue);
                    lastValue = m.getPriority();
                }

                receiver.pauseProcessingOfMessagesNamed("test");
                list.clear();

                for (int i = 0; i < 10; i++) {
                    Msg m = new Msg("test", "test", "test");
                    m.setPriority((int) (10000.0 * Math.random()));
                    m.setTimingOut(true);
                    m.setTtl(122121212);
                    log.info("Stored prio: " + m.getPriority());
                    sender.sendMessage(m);
                }

                Thread.sleep(1000);
                receiver.unpauseProcessingOfMessagesNamed("test");

                while (list.size() < 10) {
                    receiver.triggerCheck();
                    Thread.yield();
                }

                lastValue = -888888;

                for (Msg m : list) {
                    log.info("prio: " + m.getPriority());
                    assert(m.getPriority() >= lastValue);
                    lastValue = m.getPriority();
                }
            } finally {
                sender.terminate();
                receiver.terminate();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void severalRecipientsTest(Morphium morphium) throws Exception {
        try (morphium) {
            Messaging sender = new Messaging(morphium, 1000, false);
            sender.setSenderId("sender");
            sender.start();
            List<Messaging> receivers = new ArrayList<>();
            final List<String> receivedBy = new Vector<>();

            try {
                for (int i = 0; i < 10; i++) {
                    Messaging receiver1 = new Messaging(morphium, 1000, false);
                    receiver1.setSenderId("rec" + i);
                    receiver1.start();
                    receivers.add(receiver1);
                    receiver1.addMessageListener(new MessageListener() {
                        @Override
                        public Msg onMessage(Messaging msg, Msg m) {
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

                while (receivedBy.size() != m.getTo().size()) {
                    Thread.sleep(100);
                }

                assertTrue(receivedBy.size() == m.getTo().size());

                for (String r : m.getTo()) {
                    assert(receivedBy.contains(r));
                }

                receivedBy.clear();
                m = new Msg("test", "msg", "value");
                m.addRecipient("rec1");
                m.addRecipient("rec2");
                m.addRecipient("rec5");
                m.setExclusive(true);
                sender.sendMessage(m);

                while (receivedBy.size() == 0) {
                    Thread.sleep(100);
                }

                assert(receivedBy.size() == 1);
                assert(m.getTo().contains(receivedBy.get(0)));
            } finally {
                sender.terminate();

                for (Messaging ms : receivers) {
                    ms.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void broadCastMultiTest(Morphium morphium) throws Exception {
        try (morphium) {
            Messaging sender = new Messaging(morphium, 10000, false);
            sender.setSenderId("sender");
            sender.start();
            Map<String, List<MorphiumId>> receivedIds = new ConcurrentHashMap<String, List<MorphiumId>>();
            List<Messaging> receivers = new ArrayList<Messaging>();

            for (int i = 0; i < 10; i++) {
                Messaging rec1 = new Messaging(morphium, 10, true);
                rec1.setSenderId("rec" + i);
                receivers.add(rec1);
                rec1.start();
                rec1.addListenerForMessageNamed("bcast", (msg, m)->{
                    receivedIds.putIfAbsent(rec1.getSenderId(), new ArrayList<MorphiumId>());

                    if (receivedIds.get(rec1.getSenderId()).contains(m.getMsgId())) {
                        log.error("Duplicate processing!!!!");
                    } else if (m.isExclusive() && m.getProcessedBy() != null && m.getProcessedBy().size() != 0) {
                        log.error("Duplicate processing Exclusive Message!!!!!");
                    }
                    receivedIds.get(rec1.getSenderId()).add(m.getMsgId());
                    return null;
                });
            }

            int amount = 100;

            for (int i = 0; i < amount; i++) {
                sender.queueMessage(new Msg("bcast", "msg", "value", 4000000));
                sender.queueMessage(new Msg("bcast", "msg", "value", 4000000, true));

                if (i % 10 == 0) {
                    log.info("Sent message #" + i);
                }
            }

            while (true) {
                StringBuilder b = new StringBuilder();
                int totalNum = 0;
                log.info("-------------");

                for (var e : receivedIds.entrySet()) {
                    b.setLength(0);
                    b.append(e.getKey());
                    b.append(": ");

                    for (int i = 0; i < e.getValue().size(); i++) {
                        b.append("*");
                    }

                    totalNum += e.getValue().size();
                    b.append(" -> ");
                    b.append(e.getValue().size());
                    log.info(b.toString());
                }

                if (totalNum == amount * receivers.size() + amount) {
                    break;
                }

                Thread.sleep(1500);
            }

            Thread.sleep(1000);
            log.info("--------");
            StringBuilder b = new StringBuilder();
            int totalNum = 0;
            int bcast = 0;
            int excl = 0;

            for (var e : receivedIds.entrySet()) {
                b.setLength(0);
                b.append(e.getKey());
                b.append(": ");
                int ex = 0;
                int bx = 0;

                for (int i = 0; i < e.getValue().size(); i++) {
                    var m = morphium.findById(Msg.class, e.getValue().get(i));

                    if (m.isExclusive()) {
                        b.append("!");
                        excl++;
                        ex++;
                    } else {
                        b.append("*");
                        bcast++;
                        bx++;
                    }
                }

                totalNum += e.getValue().size();
                b.append(" -> ");
                b.append(e.getValue().size());
                b.append(" ( excl: ");
                b.append(ex);
                b.append(" + bcast: ");
                b.append(bx);
                b.append(")");
                log.info(b.toString());
            }

            log.info("Total processed: " + totalNum);
            log.info("    exclusives : " + excl);
            log.info("    broadcasts : " + bcast);

            for (Messaging m : receivers) {
                m.terminate();
            }

            sender.terminate();
        }
    }
}
