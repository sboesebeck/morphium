package de.caluga.test.mongo.suite.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MsgLock;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

public class BaseMessagingTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void simpleMsgLockTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumId id = new MorphiumId();
            MsgLock l = new MsgLock(id);
            l.setLockId("someone");
            morphium.insert(l);
            log.info("worksl");
            var l2 = new MsgLock(id);
            l2.setLockId("other");
            var ex = assertThrows(RuntimeException.class, ()->{
                morphium.insert(l2);
            });
            log.info("Exception as expected!", ex);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void simpleBroadcastTest(Morphium morphium) throws Exception {
        try (morphium) {
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
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void nonExMessageTest(Morphium morphium) throws Exception {
        try (morphium) {
            Messaging sender = new Messaging(morphium, 100, true, true, 10);
            sender.setUseChangeStream(true);
            sender.setSenderId("sender");
            // sender.start();
            Messaging rec1 = new Messaging(morphium, 100, true, true, 13);
            rec1.setSenderId("rec1");
            rec1.setUseChangeStream(true).start();
            Messaging rec2 = new Messaging(morphium, 100, true, true, 7);
            rec2.setSenderId("rec2");
            rec2.setUseChangeStream(true).start();
            AtomicInteger counts = new AtomicInteger(0);
            AtomicInteger errors = new AtomicInteger();
            MessageListener ml = (m, msg)->{
                // log.info(m.getSenderId()+": incoming");
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException e) {
                }

                counts.incrementAndGet();
                // log.info(m.getSenderId()+": done processing");
                return null;
            };
            rec1.addListenerForMessageNamed("test", ml);;
            rec2.addListenerForMessageNamed("test", ml);;
            int amount = 74;

            for (int i = 0; i < amount; i++) {
                sender.sendMessage(new Msg("test", "msgt", "value", 30000, false));
            }

            while (counts.get() < 2 * amount) {
                log.info("Msg rec: " + counts.get());
                Thread.sleep(500);
            }

            assertEquals(0, errors.get());;
            log.info("FINISHED! Msg rec: " + counts.get());
            var cnt = morphium.createQueryFor(MsgLock.class, sender.getLockCollectionName()).countAll();
            assertEquals(0, cnt);
            log.info("Done!");
            // assertEquals(0,morphium.createQueryFor(MsgLock.class,sender.getLockCollectionName()).countAll());
            sender.terminate();
            rec1.terminate();
            rec2.terminate();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void exclusiveMessageTest(Morphium morphium) throws Exception {
        try (morphium) {
            Messaging sender = new Messaging(morphium, 100, true, true, 10);
            sender.setUseChangeStream(true);
            sender.setSenderId("sender");
            // sender.start();
            Messaging rec1 = new Messaging(morphium, 100, true, true, 13);
            rec1.setSenderId("rec1");
            rec1.setUseChangeStream(true).start();
            Messaging rec2 = new Messaging(morphium, 100, true, true, 7);
            rec2.setSenderId("rec2");
            rec2.setUseChangeStream(true).start();
            Map<MorphiumId, Long> counts = new HashMap<>();
            AtomicInteger errors = new AtomicInteger();
            Hashtable processing = new Hashtable();
            MessageListener ml = (m, msg)->{
                // log.info(m.getSenderId()+": incoming");
                if (processing.containsKey(msg.getMsgId())) {
                    errors.incrementAndGet();
                    log.info(m.getSenderId() + ": Is just being processed by " + processing.get(msg.getMsgId()));
                } else {
                    processing.put(msg.getMsgId(), m.getSenderId());
                }

                try {
                    Thread.sleep(1500);
                } catch (InterruptedException e) {
                }

                synchronized (counts) {
                    if (counts.containsKey(msg.getMsgId())) {
                        log.error(m.getSenderId() + ": Already processeed: " + (System.currentTimeMillis() - counts.get(msg.getMsgId())) + "ms ago");
                        log.error(m.getSenderId() + ": Msg: processed_by# " + msg.getProcessedBy().size());
                        var cnt = morphium.createQueryFor(MsgLock.class, m.getLockCollectionName()).f("_id").eq(msg.getMsgId()).get();

                        if (cnt != null) {
                            log.error(m.getSenderId() + ": Locked by: " + cnt.getLockId());
                        }

                        errors.incrementAndGet();
                    } else {
                        counts.put(msg.getMsgId(), System.currentTimeMillis());
                    }

                    processing.remove(msg.getMsgId());
                }
                // log.info(m.getSenderId()+": done processing");
                return null;
            };
            rec1.addListenerForMessageNamed("test", ml);;
            rec2.addListenerForMessageNamed("test", ml);;
            int amount = 74;

            for (int i = 0; i < amount; i++) {
                sender.sendMessage(new Msg("test", "msgt", "value", 30000, true));
            }

            while (counts.size() < amount) {
                log.info("Msg rec: " + counts.size());
                Thread.sleep(500);
            }

            assertEquals(0, errors.get());;
            log.info("FINISHED! Msg rec: " + counts.size());

            while (true) {
                var cnt = morphium.createQueryFor(MsgLock.class, sender.getLockCollectionName()).countAll();

                if (cnt == 0) {
                    break;
                }

                Thread.sleep(1000);
                log.info("Locks: " + cnt);;
            }

            log.info("Done!");
            // assertEquals(0,morphium.createQueryFor(MsgLock.class,sender.getLockCollectionName()).countAll());
            sender.terminate();
            rec1.terminate();
            rec2.terminate();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void exclusiveProcessedByTest(Morphium morphium) throws Exception {
        try (morphium) {
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
                        Thread.sleep((long)(Math.random() * 1900.0));
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
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void simpleExclusiveTest(Morphium morphium) throws Exception {
        try (morphium) {
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
                        Thread.sleep((long)(Math.random() * 1000.0));
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
                m.setTtl(600000);
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
            assertEquals(amount, morphium.createQueryFor(Msg.class).setCollectionName("msg_lck").countAll());
        }
    }
}
