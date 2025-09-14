package de.caluga.test.morphium.messaging;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
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
import de.caluga.morphium.messaging.StdMessaging;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;

// @Disabled
public class MultithreaddedMessagingTests extends MorphiumTestBase {

    @ParameterizedTest
    @MethodSource("de.caluga.test.mongo.suite.base.MultiDriverTestBase#getMorphiumInstancesNoSingle")
    public void messagingSendReceiveThreaddedTest(Morphium morphium) throws Exception {
        String tstName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            for (String msgImpl : MorphiumTestBase.messagingsToTest) {
                OutputHelper.figletOutput(log, msgImpl);
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings()
                        .setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(
                        morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(
                        morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
                try (Morphium m = new Morphium(cfg)) {
                    AtomicInteger procCounter = new AtomicInteger(0);
                    final MorphiumMessaging producer = m.createMessaging();
                    final MorphiumMessaging consumer = m.createMessaging();
                    producer.start();
                    consumer.start();
                    Thread.sleep(500);

                    try {
                        Vector<String> processedIds = new Vector<>();
                        Hashtable<String, Long> processedAt = new Hashtable<>();
                        procCounter.set(0);
                        consumer.addListenerForTopic("test", (msg, message) -> {
                            procCounter.incrementAndGet();

                            if (processedIds.contains(message.getMsgId().toString())) {
                                log.error("Received msg twice: " + procCounter.get() + "/" + message.getMsgId() + "  - "
                                        + (System.currentTimeMillis() - processedAt.get(message.getMsgId().toString()))
                                        + "ms ago");
                                return null;
                            }
                            processedIds.add(message.getMsgId().toString());
                            processedAt.put(message.getMsgId().toString(), System.currentTimeMillis());
                            // simulate processing
                            try {
                                Thread.sleep((long) (100 * Math.random()));
                            } catch (InterruptedException e) {
                            }
                            return null;
                        });
                        Thread.sleep(2500);
                        int amount = 500;
                        log.info("------------- sending messages");

                        for (int i = 0; i < amount; i++) {
                            if (i % 1000 == 0) {
                                log.info("Sending message {} of {} / Processed: {}", i, amount, procCounter.get());
                            }

                            producer.sendMessage(new Msg("test ", "msg " + i, "value " + i, 30000, false));
                        }

                        log.info("--- sending finished");

                        for (int i = 0; i < 30 && procCounter.get() < amount; i++) {
                            Thread.sleep(1000);
                            log.info("Still processing: " + procCounter.get());
                        }

                        org.junit.jupiter.api.Assertions.assertEquals(amount, procCounter.get(),
                                "Did process " + procCounter.get());
                    } finally {
                        producer.terminate();
                        consumer.terminate();
                    }
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
    // de.caluga.test.mongo.suite.base.MorphiumTestBase.messagingsToTest) {
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
    @MethodSource("de.caluga.test.mongo.suite.base.MultiDriverTestBase#getMorphiumInstancesNoSingle")
    public void waitingForMessagesIfNonMultithreadded(Morphium morphium) throws Exception {
        String tstName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            for (String msgImpl : MorphiumTestBase.messagingsToTest) {
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

                    try {
                        Thread.sleep(500);
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

    @Test
    public void waitingForMessagesIfMultithreadded() throws Exception {
        final List<Msg> list = new ArrayList<>();
        morphium.dropCollection(Msg.class);
        morphium.getConfig().setThreadPoolMessagingCoreSize(5);
        log.info("Max threadpool:" + morphium.getConfig().getThreadPoolMessagingCoreSize());
        Thread.sleep(1000);
        MorphiumMessaging sender = morphium.createMessaging();
        sender.setMultithreadded(true).setWindowSize(10).start();
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

        try {
            Thread.sleep(500);
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

    @Test
    @org.junit.jupiter.api.Disabled("Performance tests don't provide meaningful assertions for test coverage")
    public void multithreaddingTestSingle() throws Exception {
        int amount = 65;
        MorphiumMessaging producer = morphium.createMessaging();
        producer.setPause(500);
        producer.start();

        for (int i = 0; i < amount; i++) {
            if (i % 10 == 0) {
                log.info("Messages sent: " + i);
            }

            Msg m = new Msg("test", "tm", "" + i + System.currentTimeMillis(), 30000);
            producer.sendMessage(m);
        }

        final AtomicInteger count = new AtomicInteger();
        MorphiumMessaging consumer = morphium.createMessaging();
        consumer.setMultithreadded(true).setWindowSize(1000).setPause(100);
        consumer.addListenerForTopic("test", (msg, m) -> {
            // log.info("Got message!");
            count.incrementAndGet();
            return null;
        });
        long start = System.currentTimeMillis();
        consumer.start();

        while (count.get() < amount) {
            log.info("Messages processed: " + count.get());
            Thread.sleep(1000);

            if (System.currentTimeMillis() - start > 20000) {
                throw new RuntimeException("Timeout");
            }
        }

        long dur = System.currentTimeMillis() - start;
        log.info("processing " + amount + " multithreaded but single messages took " + dur + "ms == "
                + (amount / (dur / 1000)) + " msg/sec");
        consumer.terminate();
        producer.terminate();
    }

    @Test
    @org.junit.jupiter.api.Disabled("Performance tests don't provide meaningful assertions for test coverage")
    public void multithreaddingTestMultiple() throws Exception {
        int amount = 650;
        StdMessaging producer = new StdMessaging(morphium, 500, false);
        producer.start();
        log.info("now multithreadded and multiprocessing");

        for (int i = 0; i < amount; i++) {
            if (i % 10 == 0) {
                log.info("Messages sent: " + i + "/" + amount);
            }

            Msg m = new Msg("test", "tm", "" + i + System.currentTimeMillis(), 300000);
            producer.sendMessage(m);
        }

        final AtomicInteger count = new AtomicInteger();
        count.set(0);
        StdMessaging consumer = new StdMessaging(morphium, 100, true, true, 121);
        consumer.addListenerForTopic("test", (msg, m) -> {
            // log.info("Got message!");
            count.incrementAndGet();
            return null;
        });
        long start = System.currentTimeMillis();
        consumer.start();

        while (count.get() < amount) {
            log.info("Messages processed: " + count.get() + "/" + amount);
            Thread.sleep(1000);

            if (System.currentTimeMillis() - start > 20000) {
                throw new RuntimeException("Timeout!");
            }

            // for (var e:morphium.getDriver().getDriverStats().entrySet()){
            // log.info("Stats: "+e.getKey()+" - " + e.getValue());
            // }
            var stats = morphium.getDriver().getDriverStats();
            log.info("Connections in Pool : " + stats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_IN_POOL));
            log.info("Connections opened  : " + stats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_OPENED));
            log.info("Connections borrowed: " + stats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_BORROWED));
            log.info("Connections released: " + stats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_RELEASED));
            log.info("Connections in use  : " + stats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_IN_USE));
            log.info("-------------------------------------");
        }

        long dur = System.currentTimeMillis() - start;
        log.info("processing " + amount + " multithreaded and multiprocessing messages took " + dur + "ms == "
                + (amount / (dur / 1000)) + " msg/sec");
        log.info("Messages processed: " + count.get());
        log.info("Messages left: " + consumer.getPendingMessagesCount());

        try {
            consumer.terminate();
        } catch (Exception e) {
            log.info("Error stopping consumer");
        }

        try {
            producer.terminate();
        } catch (Exception e) {
            log.info("Error stopping producer");
        }
    }

}
