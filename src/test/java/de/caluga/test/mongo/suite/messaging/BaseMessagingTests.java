package de.caluga.test.mongo.suite.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MsgLock;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

public class BaseMessagingTests extends MorphiumTestBase {

    @Test
    public void simpleMsgLockTest() throws Exception {
        MorphiumId id = new MorphiumId();
        MsgLock l = new MsgLock(id);
        l.setLockId("someone");
        morphium.insert(l);
        log.info("worksl");
        var l2 = new MsgLock(id);
        l2.setLockId("other");
        var ex=assertThrows(RuntimeException.class, ()->{
            morphium.insert(l2);
        });
        log.info("Exception as expected!",ex);
    }
    @Test
    public void simpleBroadcastTest() throws Exception {
        log.info("Running simple broadcast test");
        morphium.dropCollection(Msg.class);
        TestUtils.waitForCollectionToBeDeleted(morphium, Msg.class);
        Messaging sender = new Messaging(morphium);
        sender.setSenderId("sender");
        sender.start();
        Messaging rec1 = new Messaging(morphium);
        /* rec1.setUseChangeStream(false);
         * rec1.setPause(10); */
        rec1.setSenderId("rec1");
        rec1.start();
        Messaging rec2 = new Messaging(morphium);
        // rec2.setUseChangeStream(false);
        // rec2.setPause(10);
        rec2.setSenderId("rec2");
        rec2.start();
        AtomicInteger count = new AtomicInteger(0);
        MessageListener ml = new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) {
                count.incrementAndGet();
                return null;
            }
        };
        rec1.addMessageListener(ml);
        rec2.addMessageListener(ml);
        int amount = 25;

        for (int i = 0; i < amount; i++) {
            Msg m = new Msg("test", "test", "test");
            m.setTtl(6000000);
            // m.setDeleteAfterProcessing(true);
            sender.sendMessage(m);
            assertEquals(0, morphium.createQueryFor(Msg.class).setCollectionName("msg_lck").countAll());
        }

        while (count.get() != amount * 2) {
            assertEquals(0, morphium.createQueryFor(Msg.class).setCollectionName("msg_lck").countAll());
            log.info("count is " + count.get());
            Thread.sleep(1000);
        }

        Thread.sleep(2000);
        assertEquals(0, morphium.createQueryFor(Msg.class).setCollectionName("msg_lck").countAll());
        assertEquals(amount * 2, count.get());
    }

    @Test
    public void exclusiveProcessedByTest() throws Exception {
        log.info("Running simple processedBy test");
        morphium.dropCollection(Msg.class);
        morphium.dropCollection(MsgLock.class, "msg_lck", null);
        TestUtils.waitForCollectionToBeDeleted(morphium, Msg.class);
        Messaging sender = new Messaging(morphium);
        sender.setSenderId("sender");
        sender.start();
        Messaging rec1 = new Messaging(morphium);
        /* rec1.setUseChangeStream(false);
         * rec1.setPause(10); */
        rec1.setSenderId("rec1");
        rec1.start();
        Messaging rec2 = new Messaging(morphium);
        // rec2.setUseChangeStream(false);
        // rec2.setPause(10);
        rec2.setSenderId("rec2");
        rec2.start();
        AtomicInteger count = new AtomicInteger(0);
        MessageListener ml = new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) {
                count.incrementAndGet();
                try {
                    Thread.sleep((long)(Math.random()*1900.0));
                } catch (InterruptedException e) {
                }
                return null;
            }
        };
        rec1.addMessageListener(ml);
        rec2.addMessageListener(ml);
        int amount = 25;

        for (int i = 0; i < amount; i++) {
            Msg m = new Msg("test", "test", "test");
            m.setTtl(6000000);
            m.setDeleteAfterProcessing(true);
            m.setExclusive(true);
            m.setProcessedBy(Arrays.asList("someone"));
            sender.sendMessage(m);
        }

        Thread.sleep(2000);
        assertEquals(0, morphium.createQueryFor(Msg.class).setCollectionName("msg_lck").countAll());
        assertEquals(0, count.get());
    }
    @Test
    public void simpleExclusiveTest() throws Exception {
        log.info("Simple exclusive message test");
        morphium.dropCollection(Msg.class);
        TestUtils.waitForCollectionToBeDeleted(morphium, Msg.class);
        Messaging sender = new Messaging(morphium);
        sender.setSenderId("sender");
        sender.start();
        Messaging rec1 = new Messaging(morphium);
        // rec1.setUseChangeStream(false);
        rec1.setMultithreadded(true);
        rec1.setWindowSize(10);
        rec1.setPause(100);
        rec1.setSenderId("rec1");
        rec1.start();
        Messaging rec2 = new Messaging(morphium);
        // rec2.setUseChangeStream(false);
        rec2.setMultithreadded(true);
        rec2.setWindowSize(10);
        rec2.setPause(100);
        rec2.setSenderId("rec2");
        rec2.start();
        AtomicInteger count = new AtomicInteger(0);
        MessageListener<Msg> ml = new MessageListener<Msg>() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) {
                count.incrementAndGet();
                try {
                    Thread.sleep((long)(Math.random()*1000.0));
                } catch (InterruptedException e) {
                }
                return null;
            }
        };
        rec1.addMessageListener(ml);
        rec2.addMessageListener(ml);
        int amount = 250;

        for (int i = 0; i < amount; i++) {
            Msg m = new Msg("test", "test", "test");
            m.setTtl(6000000);
            m.setDeleteAfterProcessing(false);
            m.setExclusive(true);
            sender.queueMessage(m);
        }

        while (count.get() != amount) {
            log.info("count is " + count.get());
            Thread.sleep(1000);
        }

        Thread.sleep(2000);
        assertEquals(amount, count.get());
        assertEquals(0, morphium.createQueryFor(Msg.class).setCollectionName("msg_lck").countAll());
    }
}