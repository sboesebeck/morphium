package de.caluga.test.mongo.suite.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import de.caluga.morphium.messaging.*;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

public class RejectTests extends MorphiumTestBase {
    private boolean gotMessage = false;
    private boolean gotMessage3 = false;
    private boolean gotMessage2 = false;
    private boolean gotMessage1 = false;
    @Test
    public void allRejectedTest() throws Exception {
        StdMessaging sender = null;
        StdMessaging rec1 = null;
        StdMessaging rec2 = null;

        try {
            sender = new StdMessaging(morphium, 100, false, true, 10);
            sender.setSenderId("sender");
            rec1 = new StdMessaging(morphium, 100, false, true, 10);
            rec1.setSenderId("rec1");
            rec2 = new StdMessaging(morphium, 100, false, true, 10);
            rec2.setSenderId("rec2");
            morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
            Thread.sleep(10);
            sender.start();
            rec1.start();
            rec2.start();
            Thread.sleep(2000);
            rec1.addListenerForMessageNamed("test", new MessageListener<Msg>() {
                @Override
                public Msg onMessage(MorphiumMessaging msg, Msg m)  {
                    gotMessage1 = true;
                    throw new MessageRejectedException("rec1 rejected", true);
                }
            });
            rec2.addListenerForMessageNamed("test", new MessageListener<Msg>() {
                @Override
                public Msg onMessage(MorphiumMessaging msg, Msg m)  {
                    gotMessage2 = true;
                    throw new MessageRejectedException("rec2 rejected", true);
                }
            });
            gotMessage = gotMessage1 = gotMessage2 = gotMessage3 = false;
            Msg m = new Msg("test", "value", "msg", 6000, true);
            sender.sendMessage(m);

            long r = TestUtils.waitForConditionToBecomeTrue(8000, "Was not received by both listeners?", () -> gotMessage1 && gotMessage2);
            log.info("Both tried processing! ms: " + r);
            gotMessage = gotMessage1 = gotMessage2 = gotMessage3 = false;
            Thread.sleep(2000);
            //Messages will be reprocessed!
            assertTrue(gotMessage1 || gotMessage2);
        } finally {
            if (sender != null) { sender.terminate(); }

            if (rec1 != null) { rec1.terminate(); }

            if (rec2 != null) { rec2.terminate(); }
        }

        // MessageRejectedException ex = new MessageRejectedException("rejected", true, false);
    }

    @Test
    public void lotsofclientsTest() throws Exception {
        StdMessaging sender = null;
        List<StdMessaging> clients = new ArrayList<>();
        AtomicInteger recs = new AtomicInteger();
        try {
            sender = new StdMessaging(morphium, 100, false);
            sender.setSenderId("sender");
            log.info("Creating listeners...");
            for (int i = 0; i < 10; i++) {
                StdMessaging m = new StdMessaging(morphium, 100, false);
                m.setSenderId("Rec" + i);
                log.info(m.getSenderId());
                m.start();
                m.addListenerForMessageNamed("test", new MessageListener<Msg>() {
                    Map<MorphiumId, AtomicInteger> cnt = new HashMap<>();
                    @Override
                    public Msg onMessage(MorphiumMessaging msg, Msg m) {
                        recs.incrementAndGet();
                        cnt.putIfAbsent(m.getMsgId(), new AtomicInteger());
                        if (cnt.get(m.getMsgId()).incrementAndGet() == 1) {
                            throw new MessageRejectedException("rejecting at first: " + msg.getSenderId(), true);
                        }
                        return null;
                    }

                });
                clients.add(m);
            }
            Thread.sleep(2500);
            log.info("done - sending message");
            Msg m = new Msg("Test", "value", "msg");
            m.setDeleteAfterProcessing(true);
            m.setDeleteAfterProcessingTime(0);
            m.setExclusive(true);

            sender.sendMessage(m);
            TestUtils.waitForConditionToBecomeTrue(5000, "did not receive messages?", () -> recs.get() > 0);
            Thread.sleep(1000);
            //mst not raise
            assertTrue(recs.get() > 1, "should have been received more than once: " + recs.get());

        } finally {
            sender.terminate();
            for (var m : clients) m.terminate();
        }
    }

    @Test
    public void testRejectException() {
        MessageRejectedException ex = new MessageRejectedException("rejected", true, true);
        assertTrue(ex.isContinueProcessing());
        assertTrue(ex.isSendAnswer());
        ex = new MessageRejectedException("rejected");
        ex.setSendAnswer(true);
        ex.setContinueProcessing(true);
        assertTrue(ex.isContinueProcessing());
        assertTrue(ex.isSendAnswer());
        ex = new MessageRejectedException("rejected", true);
        assertTrue(ex.isContinueProcessing());
        assertFalse(ex.isSendAnswer());
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
            sender.start();
            rec1.start();
            rec2.start();
            Thread.sleep(2000);
            final AtomicInteger recFirst = new AtomicInteger(0);
            gotMessage = false;
            rec1.addListenerForMessageNamed("test", (msg, m)-> {
                if (recFirst.get() == 0) {
                    recFirst.set(1);
                    throw new MessageRejectedException("rejected", true, true);
                }
                gotMessage = true;
                return null;
            });
            rec2.addListenerForMessageNamed("test", (msg, m)-> {
                if (recFirst.get() == 0) {
                    recFirst.set(1);
                    throw new MessageRejectedException("rejected", true, true);
                }
                gotMessage = true;
                return null;
            });
            sender.addListenerForMessageNamed("test", (msg, m)-> {
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
            sender.sendMessage(new Msg("test", "message", "value", 3000, true));

            while (!gotMessage) {
                Thread.sleep(500);
            }

            assert(gotMessage);
            assert(gotMessage3);
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
            sender.start();
            rec1.start();
            rec2.start();
            Thread.sleep(2000);
            gotMessage1 = false;
            gotMessage2 = false;
            gotMessage3 = false;
            rec1.addListenerForMessageNamed("test", (msg, m)-> {
                gotMessage1 = true;
                throw new MessageRejectedException("rejected", true, true);
            });
            rec2.addListenerForMessageNamed("test", (msg, m)-> {
                gotMessage2 = true;
                log.info("Processing message " + m.getValue());
                return null;
            });
            sender.addListenerForMessageNamed("test", (msg, m)-> {
                if (m.getInAnswerTo() == null) {
                    log.error("Message is not an answer! ERROR!");
                    return null;
                }
                gotMessage3 = true;
                log.info("Receiver rejected message");
                return null;
            });
            sender.sendMessage(new Msg("test", "message", "value"));
        } finally {
            sender.terminate();
            rec1.terminate();
            rec2.terminate();
        }
    }

}
