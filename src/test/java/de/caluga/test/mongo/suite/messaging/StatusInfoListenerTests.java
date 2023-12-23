package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.StatusInfoListener;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

import static org.junit.jupiter.api.Assertions.*;


public class StatusInfoListenerTests extends MorphiumTestBase {

    @Test
    public void disablingEnablingStatusListener() throws Exception {
        Messaging m1 = new Messaging(morphium, 100, true);
        m1.setSenderId("m1");
        m1.setMultithreadded(true);
        m1.start();
        Messaging m2 = new Messaging(morphium, 100, true);
        m2.setSenderId("m2");
        m2.setMultithreadded(false);
        m2.start();

        addListeners(m1, m2);
        Messaging sender = new Messaging(morphium, 100, true);
        sender.start();
        Thread.sleep(5250);
        List<Msg> lst = sender.sendAndAwaitAnswers(new Msg(sender.getStatusInfoListenerName(), "status", StatusInfoListener.StatusInfoLevel.PING.name()), 2, 1000);
        assertEquals(2, lst.size());

        m1.disableStatusInfoListener();
        lst = sender.sendAndAwaitAnswers(new Msg(sender.getStatusInfoListenerName(), "status", StatusInfoListener.StatusInfoLevel.PING.name()), 2, 1000);
        assertEquals(1, lst.size());
        m2.disableStatusInfoListener();
        lst = sender.sendAndAwaitAnswers(new Msg(sender.getStatusInfoListenerName(), "status", StatusInfoListener.StatusInfoLevel.PING.name()), 2, 1000);

        assertTrue(lst == null || lst.size() == 0);

        m1.enableStatusInfoListener();
        m2.enableStatusInfoListener();
        lst = sender.sendAndAwaitAnswers(new Msg(sender.getStatusInfoListenerName(), "status", StatusInfoListener.StatusInfoLevel.PING.name()), 2, 1000);
        assertEquals(2, lst.size());

        m1.terminate();
        m2.terminate();
        sender.terminate();


    }

    @Test
    public void getStatusInfo() throws Exception {
        Messaging m1 = new Messaging(morphium, 100, true);
        m1.setSenderId("m1");
        m1.setMultithreadded(true);
        m1.start();
        Messaging m2 = new Messaging(morphium, 100, true);
        m2.setSenderId("m2");
        m2.setMultithreadded(false);
        m2.start();

        addListeners(m1, m2);

        Thread.sleep(1500);
        Messaging sender = new Messaging(morphium, 100, true);
        sender.start();
        log.info("Getting standard stauts (should be Messaging only)");
        List<Msg> lst = sender.sendAndAwaitAnswers(new Msg(sender.getStatusInfoListenerName(), "status", "value"), 2, 5000);
        assertNotNull(lst);
        assertEquals(2, lst.size());
        for (Msg m : lst) {
            Map<String, Object> mapValue = m.getMapValue();
            assertFalse(mapValue.containsKey(StatusInfoListener.morphiumConfigKey));
            assertFalse(mapValue.containsKey(StatusInfoListener.morphiumCachestatsKey));
            checkMessagingStats(m, mapValue);
        }
        log.info("Getting all status");

        lst = sender.sendAndAwaitAnswers(new Msg(sender.getStatusInfoListenerName(), "status", StatusInfoListener.StatusInfoLevel.ALL.name()), 2, 1000);
        assertNotNull(lst);
        assertEquals(2, lst.size());
        for (Msg m : lst) {
            Map<String, Object> mapValue = m.getMapValue();
            assertTrue(mapValue.containsKey(StatusInfoListener.morphiumConfigKey));
            assertTrue(mapValue.containsKey(StatusInfoListener.morphiumCachestatsKey));
            checkMessagingStats(m, mapValue);
        }
        log.info("just ping");

        lst = sender.sendAndAwaitAnswers(new Msg(sender.getStatusInfoListenerName(), "status", StatusInfoListener.StatusInfoLevel.PING.name()), 2, 1000);
        assertNotNull(lst);
        assertEquals(2, lst.size());
        for (Msg m : lst) {
            Map<String, Object> mapValue = m.getMapValue();
            assertEquals(0, mapValue.size());
        }

        log.info("Getting morphium stats only");

        lst = sender.sendAndAwaitAnswers(new Msg(sender.getStatusInfoListenerName(), "status", StatusInfoListener.StatusInfoLevel.MORPHIUM_ONLY.name()), 2, 1000);
        assertNotNull(lst);
        assertEquals(2, lst.size());
        for (Msg m : lst) {
            Map<String, Object> mapValue = m.getMapValue();
            assertTrue(mapValue.containsKey(StatusInfoListener.morphiumConfigKey));
            assertTrue(mapValue.containsKey(StatusInfoListener.morphiumCachestatsKey));
            assertFalse(mapValue.containsKey(StatusInfoListener.messageListenersbyNameKey));
            assertFalse(mapValue.containsKey(StatusInfoListener.globalListenersKey));
        }
        log.info("all fine... exiting");


        m1.terminate();
        m2.terminate();
        sender.terminate();
    }

    private void addListeners(Messaging m1, Messaging m2) {
        m1.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m)  {
                return null;
            }
        });
        m1.addListenerForMessageNamed("test1", new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m)  {
                return null;
            }
        });

        m2.addListenerForMessageNamed("test1", new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m)  {
                return null;
            }
        });

        m2.addListenerForMessageNamed("test2", new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) {
                return null;
            }
        });
    }


    @Test
    public void testStatusInfoListener() throws Exception {
        Messaging m = new Messaging(morphium);
        m.setWindowSize(1);
        m.setProcessMultiple(false);
        m.setMultithreadded(false);
        m.setUseChangeStream(false);
        m.start();
        assertTrue(m.isStatusInfoListenerEnabled());
        m.addMessageListener((msg, m1) -> {
            log.info("Incoming message! " + m1.getMsgId());
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {

            }
            return null;
        });

        Messaging sender = new Messaging(morphium);
        sender.start();
        Thread.sleep(3000);
        ArrayList<Msg> lst = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            var msg = new Msg("something else", "no", "");
            msg.setSender(sender.getSenderId());
            msg.setTimingOut(false);
            msg.setPriority(500);
            msg.setDeleteAfterProcessing(true);
            msg.setDeleteAfterProcessingTime(0);
            msg.setSenderHost("localhost");
            lst.add(msg);
        }

        morphium.storeList(lst);
        for (int i = 0; i < 10; i++) {
            Thread.sleep(100);
            var msg = new Msg(sender.getStatusInfoListenerName(), "PING", "");
            msg.setPriority(10);
            msg.setTtl(12030303);
            msg.setMsgId(new MorphiumId());
            log.info("Sending ping: " + msg.getMsgId().toString());
            var answer = sender.sendAndAwaitFirstAnswer(msg, 1500000, true);
            log.info("Got answer - " + i);
        }
    }


    @Test
    public void prioQueueTest() throws Exception {
        PriorityBlockingQueue<Messaging.ProcessingQueueElement> qu = new PriorityBlockingQueue<>();

        Messaging.ProcessingQueueElement el = new Messaging.ProcessingQueueElement(500, 123, new MorphiumId());
        qu.add(el);
        el = new Messaging.ProcessingQueueElement(500, 124, new MorphiumId());
        qu.add(el);
        el = new Messaging.ProcessingQueueElement(500, 124, new MorphiumId());
        qu.add(el);
        el = new Messaging.ProcessingQueueElement(500, 125, new MorphiumId());
        qu.add(el);
        el = new Messaging.ProcessingQueueElement(5, 128, new MorphiumId());
        qu.add(el);
        el = new Messaging.ProcessingQueueElement(5, 138, new MorphiumId());
        qu.add(el);
        int sz = qu.size();
        log.info("Queue size: " + sz);
        for (int i = 0; i < sz; i++) {
            var e = qu.take();
            log.info(i + ". element: " + e.getPriority() + " / " + e.getTimestamp());
        }
    }

    private void checkMessagingStats(Msg m, Map<String, Object> mapValue) {
        if (m.getSender().equals("m1")) {
            assertTrue(mapValue.containsKey(StatusInfoListener.messageListenersbyNameKey));
            assertTrue(mapValue.containsKey(StatusInfoListener.globalListenersKey));
            assertEquals(1, ((List) mapValue.get(StatusInfoListener.globalListenersKey)).size());
            assertEquals(2, ((Map) mapValue.get(StatusInfoListener.messageListenersbyNameKey)).size()); //own listener + statusinfo
            assertTrue(((Map) mapValue.get(StatusInfoListener.messageListenersbyNameKey)).containsKey("test1"));
            assertFalse(((Map) mapValue.get(StatusInfoListener.messageListenersbyNameKey)).containsKey("test2"));
            assertTrue(mapValue.containsKey(StatusInfoListener.messagingThreadpoolstatsKey));

        } else if (m.getSender().equals("m2")) {
            assertTrue(mapValue.containsKey(StatusInfoListener.messageListenersbyNameKey));
            assertTrue(mapValue.containsKey(StatusInfoListener.globalListenersKey));
            assertEquals(0, ((List) mapValue.get(StatusInfoListener.globalListenersKey)).size());
            assertEquals(3, ((Map) mapValue.get(StatusInfoListener.messageListenersbyNameKey)).size());
            assertTrue(((Map) mapValue.get(StatusInfoListener.messageListenersbyNameKey)).containsKey("test1"));
            assertTrue(((Map) mapValue.get(StatusInfoListener.messageListenersbyNameKey)).containsKey("test2"));
            assertFalse(mapValue.containsKey(StatusInfoListener.messagingThreadpoolstatsKey));

        } else {
            throw new RuntimeException("WRONG SENDER " + m.getSender());
        }
    }

}
