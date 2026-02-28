package de.caluga.test.morphium.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.test.OutputHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.SingleCollectionMessaging;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

import org.junit.jupiter.api.Tag;

// @Disabled
@Tag("messaging")
public class MultithreaddedMessagingTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    // public void basicSendReceiveTest(Morphium morphium) throws Exception {
    public void messagingSendReceiveThreaddedTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            for (String msgImpl : MultiDriverTestBase.messagingsToTest) {
                OutputHelper.figletOutput(log, msgImpl);
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

                try (Morphium morph = new Morphium(cfg)) {
                    morph.dropCollection(Msg.class);
                    Thread.sleep(500);

                    MorphiumMessaging sender = morph.createMessaging();
                    sender.setSenderId("sender");
                    MorphiumMessaging receiver = morph.createMessaging();
                    receiver.setSenderId("receiver");
                    receiver.setMultithreadded(true);

                    AtomicInteger msgCount = new AtomicInteger(0);
                    Set<Msg> receivedMessages = ConcurrentHashMap.newKeySet();

                    receiver.addListenerForTopic("test", (msg, m) -> {
                        msgCount.incrementAndGet();
                        receivedMessages.add(m);
                        try {
                            Thread.sleep((long) (200 * Math.random()));
                        } catch (InterruptedException e) {
                        }
                        return null;
                    });

                    sender.start();
                    receiver.start();
                    assertTrue(sender.waitForReady(30, TimeUnit.SECONDS), "sender not ready");
                    assertTrue(receiver.waitForReady(30, TimeUnit.SECONDS), "receiver not ready");
                    Thread.sleep(1000); // Allow topic listener to register

                    // Test basic send/receive
                    Msg testMsg = new Msg("test", "Basic message", "value1");
                    sender.sendMessage(testMsg);

                    TestUtils.waitForConditionToBecomeTrue(5000, "Did not receive message", () -> msgCount.get() >= 1);
                    assertEquals(1, msgCount.get());
                    assertEquals(1, receivedMessages.size());
                    assertEquals("Basic message", receivedMessages.toArray(new Msg[] {})[0].getMsg());
                    assertEquals("value1", receivedMessages.toArray(new Msg[] {})[0].getValue());
                    log.info("Got message ... now sending more");
                    int amount = 500;
                    // Test multiple messages
                    for (int i = 0; i < amount; i++) {
                        sender.sendMessage(new Msg("test", "Message " + i, "value" + i));
                    }

                    TestUtils.waitForConditionToBecomeTrue(5000, (dur, e)-> {log.info("Did not receive all messages after {}", dur);}, () -> msgCount.get() >= amount + 1, (dur)-> {log.info("Waiting for messages...{}", msgCount.get());}, (dur)-> {log.info("got all messages after {}ms", dur);});
                    assertEquals(amount + 1, msgCount.get());
                    assertEquals(amount + 1, receivedMessages.size());

                    sender.terminate();
                    receiver.terminate();
                }
            }
        }
    }

    // @Test
    // @org.junit.jupiter.api.Disabled("Performance tests don't provide meaningful
    // assertions for test coverage")
    // public void mutlithreaddedMessagingPerformanceTest() throws Exception {
    // morphium.clearCollection(Msg.class);
    // for (String msgImpl :
    // de.caluga.test.mongo.suite.base.MultiDriverTestBase.messagingsToTest) {
    // MorphiumConfig cfg = morphium.getConfig().createCopy();
    // cfg.messagingSettings().setMessagingImplementation(msgImpl);
    // cfg.encryptionSettings()
    // .setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
    // cfg.encryptionSettings().setCredentialsDecryptionKey(
    // morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
    // cfg.encryptionSettings().setCredentialsEncryptionKey(
    // morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
    // try (Morphium m = new Morphium(cfg)) {
    // final MorphiumMessaging producer = m.createMessaging();
    // final MorphiumMessaging consumer = m.createMessaging();
    // consumer.start();
    // producer.start();
    // Thread.sleep(500);

    // try {
    // final AtomicInteger processed = new AtomicInteger();
    // final Map<String, AtomicInteger> msgCountById = new ConcurrentHashMap<>();
    // consumer.addListenerForTopic("test", (msg, message) -> {
    // processed.incrementAndGet();

    // if (processed.get() % 1000 == 0) {
    // log.info("Consumed " + processed.get());
    // }
    // org.junit.jupiter.api.Assertions
    // .assertFalse(msgCountById.containsKey(message.getMsgId().toString()));
    // msgCountById.putIfAbsent(message.getMsgId().toString(), new AtomicInteger());
    // msgCountById.get(message.getMsgId().toString()).incrementAndGet();
    // // simulate processing
    // try {
    // Thread.sleep((long) (10 * Math.random()));
    // } catch (InterruptedException e) {
    // e.printStackTrace();
    // }
    // return null;
    // });
    // int numberOfMessages = 300;

    // for (int i = 0; i < numberOfMessages; i++) {
    // Msg message = new Msg("test", "m", "v");
    // message.setTtl(5 * 60 * 1000);

    // if (i % 1000 == 0) {
    // log.info("created msg " + i + " / " + numberOfMessages);
    // }

    // producer.sendMessage(message);
    // }

    // long start = System.currentTimeMillis();

    // while (processed.get() < numberOfMessages) {
    // // ThreadMXBean thbean = ManagementFactory.getThreadMXBean();
    // // log.info("Running threads: " + thbean.getThreadCount());
    // log.info("Processed " + processed.get());
    // Thread.sleep(1500);
    // }

    // long dur = System.currentTimeMillis() - start;
    // log.info("Processing took " + dur + " ms");
    // assert (processed.get() == numberOfMessages);

    // for (String id : msgCountById.keySet()) {
    // org.junit.jupiter.api.Assertions.assertEquals(1, msgCountById.get(id).get());
    // }
    // } finally {
    // producer.terminate();
    // consumer.terminate();
    // }
    // }
    // }
    // }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void waitingForMessagesIfNonMultithreadded(Morphium morphium) throws Exception {
        String tstName = new Object() {
        } .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            for (String msgImpl : MultiDriverTestBase.messagingsToTest) {
                OutputHelper.figletOutput(log, msgImpl);
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings()
                   .setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(
                                   morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(
                                   morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

                try (Morphium morph = new Morphium(cfg)) {
                    final List<Msg> list = new ArrayList<>();
                    morph.dropCollection(Msg.class);
                    Thread.sleep(1000);
                    MorphiumMessaging sender = morph.createMessaging();
                    sender.setMultithreadded(false).setWindowSize(10).start();
                    assertTrue(sender.waitForReady(30, TimeUnit.SECONDS), "sender not ready");
                    list.clear();
                    MorphiumMessaging receiver = morph.createMessaging();
                    receiver.setMultithreadded(false).setWindowSize(10);
                    receiver.addListenerForTopic("test", (msg, m) -> {
                        list.add(m);

                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                        }

                        return null;
                    });
                    receiver.start();
                    assertTrue(receiver.waitForReady(30, TimeUnit.SECONDS), "receiver not ready");

                    try {
                        Thread.sleep(1000); // Allow topic listener to register
                        sender.sendMessage(new Msg("test", "test", "test"));
                        sender.sendMessage(new Msg("test", "test", "test"));

                        while (list.size() == 0) {
                            Thread.sleep(500);
                        }

                        org.junit.jupiter.api.Assertions.assertEquals(1, list.size(), "Size wrong: " + list.size());
                        Thread.sleep(2200);
                        org.junit.jupiter.api.Assertions.assertEquals(2, list.size());
                    } finally {
                        sender.terminate();
                        receiver.terminate();
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void waitingForMessagesIfMultithreadded(Morphium morphium) throws Exception {
        try(morphium) {
            final List<Msg> list = new ArrayList<>();
            morphium.dropCollection(Msg.class);
            morphium.getConfig().messagingSettings().setThreadPoolMessagingCoreSize(5);
            log.info("Max threadpool:" + morphium.getConfig().messagingSettings().getThreadPoolMessagingCoreSize());
            Thread.sleep(1000);
            MorphiumMessaging sender = morphium.createMessaging();
            sender.setMultithreadded(true).setWindowSize(10).start();
            assertTrue(sender.waitForReady(30, TimeUnit.SECONDS), "sender not ready");
            list.clear();
            MorphiumMessaging receiver = morphium.createMessaging();
            receiver.setMultithreadded(true).setWindowSize(10);
            receiver.addListenerForTopic("test", (msg, m) -> {
                log.info("Incoming message...");
                list.add(m);

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                }

                return null;
            });
            receiver.start();
            assertTrue(receiver.waitForReady(30, TimeUnit.SECONDS), "receiver not ready");

            try {
                Thread.sleep(1000); // Allow topic listener to register
                sender.sendMessage(new Msg("test", "test", "test"));
                sender.sendMessage(new Msg("test", "test", "test"));

                while (list.size() < 2) {
                    Thread.sleep(100);
                }

                Thread.sleep(100);
                org.junit.jupiter.api.Assertions.assertEquals(2, list.size(), "Size wrong: " + list.size());
            } finally {
                sender.terminate();
                receiver.terminate();
            }
        }
    }

    // @Test
    // @org.junit.jupiter.api.Disabled("Performance tests don't provide meaningful assertions for test coverage")
    // public void multithreaddingTestSingle() throws Exception {
    //     int amount = 65;
    //     MorphiumMessaging producer = morphium.createMessaging();
    //     producer.setPause(500);
    //     producer.start();

    //     for (int i = 0; i < amount; i++) {
    //         if (i % 10 == 0) {
    //             log.info("Messages sent: " + i);
    //         }

    //         Msg m = new Msg("test", "tm", "" + i + System.currentTimeMillis(), 30000);
    //         producer.sendMessage(m);
    //     }

    //     final AtomicInteger count = new AtomicInteger();
    //     MorphiumMessaging consumer = morphium.createMessaging();
    //     consumer.setMultithreadded(true).setWindowSize(1000).setPause(100);
    //     consumer.addListenerForTopic("test", (msg, m) -> {
    //         // log.info("Got message!");
    //         count.incrementAndGet();
    //         return null;
    //     });
    //     long start = System.currentTimeMillis();
    //     consumer.start();

    //     while (count.get() < amount) {
    //         log.info("Messages processed: " + count.get());
    //         Thread.sleep(1000);

    //         if (System.currentTimeMillis() - start > 20000) {
    //             throw new RuntimeException("Timeout");
    //         }
    //     }

    //     long dur = System.currentTimeMillis() - start;
    //     log.info("processing " + amount + " multithreaded but single messages took " + dur + "ms == "
    //              + (amount / (dur / 1000)) + " msg/sec");
    //     consumer.terminate();
    //     producer.terminate();
    // }

    // @Test
    // @org.junit.jupiter.api.Disabled("Performance tests don't provide meaningful assertions for test coverage")
    // public void multithreaddingTestMultiple() throws Exception {
    //     int amount = 650;
    //     SingleCollectionMessaging producer = new SingleCollectionMessaging(morphium, 500, false);
    //     producer.start();
    //     log.info("now multithreadded and multiprocessing");

    //     for (int i = 0; i < amount; i++) {
    //         if (i % 10 == 0) {
    //             log.info("Messages sent: " + i + "/" + amount);
    //         }

    //         Msg m = new Msg("test", "tm", "" + i + System.currentTimeMillis(), 300000);
    //         producer.sendMessage(m);
    //     }

    //     final AtomicInteger count = new AtomicInteger();
    //     count.set(0);
    //     SingleCollectionMessaging consumer = new SingleCollectionMessaging(morphium, 100, true, true, 121);
    //     consumer.addListenerForTopic("test", (msg, m) -> {
    //         // log.info("Got message!");
    //         count.incrementAndGet();
    //         return null;
    //     });
    //     long start = System.currentTimeMillis();
    //     consumer.start();

    //     while (count.get() < amount) {
    //         log.info("Messages processed: " + count.get() + "/" + amount);
    //         Thread.sleep(1000);

    //         if (System.currentTimeMillis() - start > 20000) {
    //             throw new RuntimeException("Timeout!");
    //         }

    //         // for (var e:morphium.getDriver().getDriverStats().entrySet()){
    //         // log.info("Stats: "+e.getKey()+" - " + e.getValue());
    //         // }
    //         var stats = morphium.getDriver().getDriverStats();
    //         log.info("Connections in Pool : " + stats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_IN_POOL));
    //         log.info("Connections opened  : " + stats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_OPENED));
    //         log.info("Connections borrowed: " + stats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_BORROWED));
    //         log.info("Connections released: " + stats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_RELEASED));
    //         log.info("Connections in use  : " + stats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_IN_USE));
    //         log.info("-------------------------------------");
    //     }

    //     long dur = System.currentTimeMillis() - start;
    //     log.info("processing " + amount + " multithreaded and multiprocessing messages took " + dur + "ms == "
    //              + (amount / (dur / 1000)) + " msg/sec");
    //     log.info("Messages processed: " + count.get());
    //     log.info("Messages left: " + consumer.getPendingMessagesCount());

    //     try {
    //         consumer.terminate();
    //     } catch (Exception e) {
    //         log.info("Error stopping consumer");
    //     }

    //     try {
    //         producer.terminate();
    //     } catch (Exception e) {
    //         log.info("Error stopping producer");
    //     }
    // }

}
