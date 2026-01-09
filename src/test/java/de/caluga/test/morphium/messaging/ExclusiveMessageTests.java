package de.caluga.test.morphium.messaging;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import de.caluga.morphium.messaging.*;

import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.oracle.truffle.js.nodes.access.GetPrototypeFromConstructorNode;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Tag;
import de.caluga.test.mongo.suite.base.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("messaging")
@Tag("slow")  // Timing-sensitive messaging tests - may be flaky under high parallel load
public class ExclusiveMessageTests extends MultiDriverTestBase {
    private boolean gotMessage1 = false;
    private boolean gotMessage2 = false;
    private boolean gotMessage3 = false;
    private boolean gotMessage4 = false;

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void ignoringExclusiveMessagesTest(Morphium morphium) throws Exception  {
        for (String msgImpl : de.caluga.test.mongo.suite.base.MultiDriverTestBase.messagingsToTest) {
            de.caluga.test.OutputHelper.figletOutput(log, msgImpl);
            log.info("Using messaging implementation: {}", msgImpl);
            var cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImpl);
            // Propagate encryption/auth settings that are not exported by createCopy
            cfg.encryptionSettings()
               .setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(
                               morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(
                               morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                Thread.sleep(100);
                MorphiumMessaging m1 = m.createMessaging();
                m1.setPause(10).setMultithreadded(true).setWindowSize(10);
                m1.setSenderId("m1");
                MorphiumMessaging m2 = m.createMessaging();
                m2.setPause(10).setMultithreadded(true).setWindowSize(10);
                m2.setSenderId("m2");
                MorphiumMessaging m3 = m.createMessaging();
                m3.setPause(10).setMultithreadded(true).setWindowSize(10);
                m3.setSenderId("m3");
                m3.addListenerForTopic("test", (msg, mm) -> null);
                try {
                    m1.start();
                    m2.start();
                    m3.start();
                    Thread.sleep(250);
                    for (int i = 0; i < 10; i++) {
                        Msg mm = new Msg("test", "ignore me please", "value", 20000, true);
                        m1.sendMessage(mm);
                        long start = System.currentTimeMillis();
                        final String collName = m1.getCollectionName(mm);
                        final MorphiumId msgId = mm.getMsgId();
                        while (true) {
                            Thread.sleep(50);
                            // Use query to fetch by ID to handle replication lag
                            mm = m.createQueryFor(Msg.class, collName).f("_id").eq(msgId).get();
                            if (mm == null) {
                                assertTrue(System.currentTimeMillis() - start < 10000, "timeout waiting for message to be visible");
                                continue;
                            }
                            if (mm.getProcessedBy() != null && mm.getProcessedBy().size() != 0)
                                break;
                            assertTrue(System.currentTimeMillis() - start < 10000, "timeout waiting for processing");
                        }
                        assertNotNull(mm);
                        assertEquals(1, mm.getProcessedBy().size());
                    }
                } finally {
                    try {
                        m1.terminate();
                    } catch (Exception ignored) {
                    }
                    try {
                        m2.terminate();
                    } catch (Exception ignored) {
                    }
                    try {
                        m3.terminate();
                    } catch (Exception ignored) {
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    @Disabled
    public void deleteAfterProcessingTest(Morphium morphium) throws Exception  {
        morphium.dropCollection(Msg.class);
        TestUtils.waitForConditionToBecomeTrue(1000, "Collection did not drop", () -> !morphium.exists(Msg.class));
        MorphiumMessaging sender = morphium.createMessaging();
        sender.setPause(100).setMultithreadded(true).setWindowSize(1);
        sender.setQueueName("t1");
        sender.start();
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;
        MorphiumMessaging m1 = morphium.createMessaging();
        m1.setPause(100).setMultithreadded(true).setWindowSize(1);
        m1.setQueueName("t1");
        m1.addListenerForTopic("A message", (msg, m) -> {
            gotMessage1 = true;
            return null;
        });
        MorphiumMessaging m2 = morphium.createMessaging();
        m2.setPause(100).setMultithreadded(true).setWindowSize(1);
        m2.setQueueName("t1");
        m2.addListenerForTopic("A message", (msg, m) -> {
            // gotMessage2 = true;
            return null;
        });
        MorphiumMessaging m3 = morphium.createMessaging();
        m3.setPause(100).setMultithreadded(true).setWindowSize(1);
        m3.setQueueName("t1");
        m3.addListenerForTopic("A message", (msg, m) -> {
            gotMessage3 = true;
            return null;
        });
        m1.start();
        m2.start();
        m3.start();

        try {
            Thread.sleep(2000);
            Msg m = new Msg();
            m.setExclusive(true);
            m.setDeleteAfterProcessing(true);
            m.setDeleteAfterProcessingTime(0);
            m.setTopic("A message");
            m.setProcessedBy(Arrays.asList("someone_else"));
            sender.sendMessage(m);
            Thread.sleep(1000);
            // Message should NOT be received since processedBy contains someone_else
            assertFalse(gotMessage1 || gotMessage2 || gotMessage3 || gotMessage4);
            morphium.setInEntity(m, m1.getCollectionName(), Map.of(Msg.Fields.processedBy, new ArrayList<String>()));
            Thread.sleep(100);
            long s = System.currentTimeMillis();

            while (true) {
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

                if (rec == 1) {
                    break;
                }

                assertThat(rec).isLessThanOrEqualTo(1);
                Thread.sleep(50);
                assertThat(System.currentTimeMillis() - s).isLessThan(morphium.getConfig().getMaxWaitTime());
            }

            TestUtils.waitForConditionToBecomeTrue(10000, "Messages not processed", () -> m1.getNumberOfMessages() == 0);
            TestUtils.waitForConditionToBecomeTrue(5000, "Msg collection not empty", () -> morphium.createQueryFor(Msg.class, m1.getCollectionName()).countAll() == 0);
            TestUtils.waitForConditionToBecomeTrue(5000, "MsgLock collection not empty", () -> morphium.createQueryFor(MsgLock.class, m1.getLockCollectionName()).countAll() == 0);
        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
            sender.terminate();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    @Disabled
    public void exclusiveMessageTest(Morphium morphium) throws Exception  {
        morphium.dropCollection(Msg.class);
        TestUtils.waitForConditionToBecomeTrue(1000, "Collection did not drop", () -> !morphium.exists(Msg.class));
        MorphiumMessaging sender = morphium.createMessaging();
        sender.setPause(100).setMultithreadded(true).setWindowSize(1);
        sender.start();
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;
        MorphiumMessaging m1 = morphium.createMessaging();
        m1.setPause(100).setMultithreadded(true).setWindowSize(1);
        m1.addListenerForTopic("A message", (msg, m) -> {
            gotMessage1 = true;
            return null;
        });
        MorphiumMessaging m2 = morphium.createMessaging();
        m2.setPause(100).setMultithreadded(true).setWindowSize(1);
        m2.addListenerForTopic("A message", (msg, m) -> {
            // gotMessage2 = true;
            return null;
        });
        MorphiumMessaging m3 = morphium.createMessaging();
        m3.setPause(100).setMultithreadded(true).setWindowSize(1);
        m3.addListenerForTopic("A message", (msg, m) -> {
            gotMessage3 = true;
            return null;
        });
        m1.start();
        m2.start();
        m3.start();

        try {
            Thread.sleep(2000);
            Msg m = new Msg();
            m.setExclusive(true);
            m.setTopic("A message");
            sender.sendMessage(m);
            long s = System.currentTimeMillis();

            while (true) {
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

                if (rec == 1) {
                    break;
                }

                assertThat(rec).isLessThanOrEqualTo(1);
                Thread.sleep(50);
                assertThat(System.currentTimeMillis() - s).isLessThan(2 * morphium.getConfig().getMaxWaitTime());
            }

            TestUtils.waitForConditionToBecomeTrue(5000, "Messages not processed", () -> m1.getNumberOfMessages() == 0);
        } finally {
            m1.terminate();
            m2.terminate();
            m3.terminate();
            sender.terminate();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void exclusiveMessageCustomQueueTest(Morphium morphium) throws Exception  {
        for (String msgImpl : MultiDriverTestBase.messagingsToTest) {
            de.caluga.test.OutputHelper.figletOutput(log, msgImpl);
            log.info("Using messaging implementation: {}", msgImpl);
            var cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImpl);
            cfg.encryptionSettings()
               .setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(
                               morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(
                               morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
            MorphiumMessaging sender = null;
            MorphiumMessaging sender2 = null;
            MorphiumMessaging m1 = null;
            MorphiumMessaging m2 = null;
            MorphiumMessaging m3 = null;
            MorphiumMessaging m4 = null;

            try (Morphium mx = new Morphium(cfg)) {
                mx.dropCollection(Msg.class);
                sender = mx.createMessaging();
                sender.setQueueName("test").setPause(100).setMultithreadded(false).setWindowSize(1);
                sender.setSenderId("sender1");
                mx.dropCollection(Msg.class, sender.getCollectionName(), null);
                sender.start();
                sender2 = mx.createMessaging();
                sender2.setQueueName("test2").setPause(100).setMultithreadded(false).setWindowSize(1);
                sender2.setSenderId("sender2");
                mx.dropCollection(Msg.class, sender2.getCollectionName(), null);
                sender2.start();
                Thread.sleep(200);
                gotMessage1 = false;
                gotMessage2 = false;
                gotMessage3 = false;
                gotMessage4 = false;
                m1 = mx.createMessaging();
                m1.setQueueName("test").setPause(100).setMultithreadded(false).setWindowSize(1);
                m1.setSenderId("m1");
                m1.addListenerForTopic("A message", (msg, m) -> {
                    gotMessage1 = true;
                    log.info("Got message m1");
                    return null;
                });
                m2 = mx.createMessaging();
                m2.setQueueName("test").setPause(100).setMultithreadded(false).setWindowSize(1);
                m2.setSenderId("m2");
                m2.addListenerForTopic("A message", (msg, m) -> {
                    gotMessage2 = true;
                    log.info("Got message m2");
                    return null;
                });
                m3 = mx.createMessaging();
                m3.setQueueName("test2").setPause(100).setMultithreadded(false).setWindowSize(1);
                m3.setSenderId("m3");
                m3.addListenerForTopic("A message", (msg, m) -> {
                    gotMessage3 = true;
                    log.info("Got message m3");
                    return null;
                });
                m4 = mx.createMessaging();
                m4.setQueueName("test2").setPause(100).setMultithreadded(false).setWindowSize(1);
                m4.setSenderId("m4");
                m4.addListenerForTopic("A message", (msg, m) -> {
                    gotMessage4 = true;
                    log.info("Got message m4");
                    return null;
                });
                m1.start();
                m2.start();
                m3.start();
                m4.start();
                Thread.sleep(2200);
                // Sending exclusive Message
                Msg m = new Msg();
                m.setExclusive(true);
                m.setTtl(3000000);
                m.setMsgId(new MorphiumId());
                m.setTopic("A message");
                log.info("Sending: " + m.getMsgId().toString());
                sender.sendMessage(m);
                org.junit.jupiter.api.Assertions.assertFalse(gotMessage3);
                org.junit.jupiter.api.Assertions.assertFalse(gotMessage4);
                long s = System.currentTimeMillis();

                while (!gotMessage1 && !gotMessage2) {
                    Thread.sleep(200);
                    log.info("Still did not get all messages: m1=" + gotMessage1 + " m2=" + gotMessage2);
                    assertThat(System.currentTimeMillis() - s).isLessThan(morphium.getConfig().getMaxWaitTime());
                }

                int rec = 0;

                if (gotMessage1) {
                    rec++;
                }

                if (gotMessage2) {
                    rec++;
                }

                org.junit.jupiter.api.Assertions.assertEquals(1, rec, "rec is " + rec);
                gotMessage1 = false;
                gotMessage2 = false;
                m = new Msg();
                m.setExclusive(true);
                m.setTopic("A message");
                m.setTtl(3000000);
                sender2.sendMessage(m);
                Thread.sleep(500);
                org.junit.jupiter.api.Assertions.assertFalse(gotMessage1);
                org.junit.jupiter.api.Assertions.assertFalse(gotMessage2);
                rec = 0;
                s = System.currentTimeMillis();

                while (rec == 0) {
                    if (gotMessage3) {
                        rec++;
                    }

                    if (gotMessage4) {
                        rec++;
                    }

                    Thread.sleep(100);
                    org.junit.jupiter.api.Assertions
                    .assertTrue(System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
                }

                org.junit.jupiter.api.Assertions.assertEquals(1, rec, "rec is " + rec);
                Thread.sleep(2500);

                for (MorphiumMessaging ms : Arrays.asList(m1, m2, m3)) {
                    if (ms.getNumberOfMessages() > 0) {
                        Query<Msg> q1 = mx.createQueryFor(Msg.class, ms.getCollectionName());
                        q1.f(Msg.Fields.sender).ne(ms.getSenderId());
                        q1.f(Msg.Fields.processedBy).ne(ms.getSenderId());
                        List<Msg> ret = q1.asList();

                        for (Msg f : ret) {
                            log.info("Found elements for " + ms.getSenderId() + ": " + f.toString());
                        }
                    }
                }

                for (MorphiumMessaging ms : Arrays.asList(m1, m2, m3)) {
                    org.junit.jupiter.api.Assertions.assertEquals(0, ms.getNumberOfMessages(),
                                                    "Number of messages " + ms.getSenderId() + " is " + ms.getNumberOfMessages());
                }
            } finally {
                if (m1 != null)
                    m1.terminate();
                if (m2 != null)
                    m2.terminate();
                if (m3 != null)
                    m3.terminate();
                if (m4 != null)
                    m4.terminate();
                if (sender != null)
                    sender.terminate();
                if (sender2 != null)
                    sender2.terminate();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    @Tag("external")
    public void exclusivityTest(Morphium morphium) throws Exception  {
        for (String msgImpl : MultiDriverTestBase.messagingsToTest) {
            de.caluga.test.OutputHelper.figletOutput(log, msgImpl);
            log.info("Using messaging implementation: {}", msgImpl);
            var cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImpl);
            cfg.encryptionSettings()
               .setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(
                               morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(
                               morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
            try (Morphium mx = new Morphium(cfg)) {
                MorphiumMessaging sender = mx.createMessaging();
                sender.setPause(100).setMultithreadded(true).setWindowSize(1);
                sender.setSenderId("sender");
                mx.dropCollection(Msg.class, sender.getCollectionName(), null);
                mx.dropCollection(MsgLock.class, sender.getLockCollectionName(), null);
                Thread.sleep(2000);
                sender.start();
                // additional Morphium instances
                MorphiumConfig c2 = MorphiumConfig.fromProperties(mx.getConfig().asProperties());
                c2.setCredentialsEncryptionKey(mx.getConfig().getCredentialsEncryptionKey());
                c2.setCredentialsDecryptionKey(mx.getConfig().getCredentialsDecryptionKey());
                Morphium morphium2 = new Morphium(c2);
                morphium2.getConfig().setThreadPoolMessagingMaxSize(10);
                morphium2.getConfig().setThreadPoolMessagingCoreSize(5);
                morphium2.getConfig().setThreadPoolAsyncOpMaxSize(10);
                MorphiumMessaging receiver = morphium2.createMessaging();
                receiver.setPause(10).setMultithreadded(true).setWindowSize(15);
                receiver.setSenderId("r1");
                receiver.start();
                MorphiumConfig c3 = MorphiumConfig.fromProperties(mx.getConfig().asProperties());
                c3.setCredentialsEncryptionKey(mx.getConfig().getCredentialsEncryptionKey());
                c3.setCredentialsDecryptionKey(mx.getConfig().getCredentialsDecryptionKey());
                Morphium morphium3 = new Morphium(c3);
                morphium3.getConfig().setThreadPoolMessagingMaxSize(10);
                morphium3.getConfig().setThreadPoolMessagingCoreSize(5);
                morphium3.getConfig().setThreadPoolAsyncOpMaxSize(10);
                MorphiumMessaging receiver2 = morphium3.createMessaging();
                receiver2.setPause(10).setMultithreadded(false).setWindowSize(1);
                receiver2.setSenderId("r2");
                receiver2.start();
                MorphiumConfig c4 = MorphiumConfig.fromProperties(mx.getConfig().asProperties());
                c4.setCredentialsEncryptionKey(mx.getConfig().getCredentialsEncryptionKey());
                c4.setCredentialsDecryptionKey(mx.getConfig().getCredentialsDecryptionKey());
                Morphium morphium4 = new Morphium(c4);
                morphium4.getConfig().setThreadPoolMessagingMaxSize(10);
                morphium4.getConfig().setThreadPoolMessagingCoreSize(5);
                morphium4.getConfig().setThreadPoolAsyncOpMaxSize(10);
                MorphiumMessaging receiver3 = morphium4.createMessaging();
                receiver3.setPause(10).setMultithreadded(false).setWindowSize(15);
                receiver3.setSenderId("r3");
                receiver3.start();
                MorphiumConfig c5 = MorphiumConfig.fromProperties(mx.getConfig().asProperties());
                c5.setCredentialsEncryptionKey(mx.getConfig().getCredentialsEncryptionKey());
                c5.setCredentialsDecryptionKey(mx.getConfig().getCredentialsDecryptionKey());
                Morphium morphium5 = new Morphium(c5);
                morphium5.getConfig().setThreadPoolMessagingMaxSize(10);
                morphium5.getConfig().setThreadPoolMessagingCoreSize(5);
                morphium5.getConfig().setThreadPoolAsyncOpMaxSize(10);
                MorphiumMessaging receiver4 = morphium5.createMessaging();
                receiver4.setPause(10).setMultithreadded(true).setWindowSize(1);
                receiver4.setSenderId("r4");
                receiver4.start();
                final AtomicInteger received = new AtomicInteger();
                final AtomicInteger dups = new AtomicInteger();
                final Map<String, Long> ids = new ConcurrentHashMap<>();
                final Map<String, String> recById = new ConcurrentHashMap<>();
                final Map<String, AtomicInteger> recieveCount = new ConcurrentHashMap<>();
                final Map<String, List<MorphiumId>> recIdsByReceiver = new ConcurrentHashMap<>();
                log.info("All receivers initialized... starting");
                Thread.sleep(2000);

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
                            log.error("Duplicate recieved message " + msg.getSenderId() + " "
                                      + (System.currentTimeMillis() - ids.get(m.getMsgId().toString())) + "ms ago");

                            if (recById.get(m.getMsgId().toString()).equals(msg.getSenderId())) {
                                log.error("--- duplicate was processed before by me!");
                            } else {
                                log.error("--- duplicate processed by someone else");
                            }
                            log.error("Duplicate processing: ", new RuntimeException());
                            dups.incrementAndGet();
                        } else if (!m.isExclusive()) {
                            recIdsByReceiver.putIfAbsent(msg.getSenderId(), new ArrayList<>());

                            if (recIdsByReceiver.get(msg.getSenderId()).contains(m.getMsgId())) {
                                log.error(msg.getSenderId()
                                          + ": Duplicate processing of broadcast message of same receiver! ");
                                m = msg.getMorphium().reread(m);

                                if (m == null) {
                                    log.error("... is deleted???");
                                } else if (m.getProcessedBy().contains(msg.getSenderId())) {
                                    log.error("... but is properly marked!");
                                }

                                dups.incrementAndGet();
                            } else {
                                recIdsByReceiver.get(msg.getSenderId()).add(m.getMsgId());
                            }
                        }

                        // log.info("Processed msg excl: " + m.isExclusive());
                        ids.put(m.getMsgId().toString(), System.currentTimeMillis());
                        recById.put(m.getMsgId().toString(), msg.getSenderId());
                        return null;
                    };
                    receiver.addListenerForTopic("m", messageListener);
                    receiver2.addListenerForTopic("m", messageListener);
                    receiver3.addListenerForTopic("m", messageListener);
                    receiver4.addListenerForTopic("m", messageListener);
                    int amount = 150;
                    int broadcastAmount = 50;

                    for (int i = 0; i < amount; i++) {
                        int rec = received.get();
                        long messageCount = receiver.getPendingMessagesCount();

                        if (i % 10 == 0) {
                            log.info("Send " + i + " recieved: " + rec + " queue: " + messageCount);
                        }

                        Msg m = new Msg("m", "m", "v" + i, 30000, true);
                        m.setExclusive(true);
                        sender.sendMessage(m);
                    }

                    for (int i = 0; i < broadcastAmount; i++) {
                        int rec = received.get();
                        long messageCount = receiver.getPendingMessagesCount();

                        if (i % 100 == 0) {
                            log.info("Send broadcast " + i + " recieved: " + rec + " queue: " + messageCount);
                        }

                        Msg m = new Msg("m", "m", "v" + i, 30000, false);
                        sender.sendMessage(m);
                    }

                    long waitUntil = System.currentTimeMillis() + 300000; // 5 minutes - allows for slower backends like MorphiumServer

                    while (received.get() != amount + broadcastAmount * 4) {
                        int rec = received.get();
                        long messageCount = receiver.getPendingMessagesCount();
                        log.info(String.format(
                                                 "Send excl: %d  brodadcast: %d recieved: %d queue: %d currently processing: %d", amount,
                                                 broadcastAmount, rec, messageCount,
                                                 (amount + broadcastAmount * 4 - rec - messageCount)));
                        log.info(String.format("Number of ids: %d", ids.size()));
                        org.junit.jupiter.api.Assertions.assertEquals(0, dups.get(), "got duplicate message");

                        for (MorphiumMessaging m : Arrays.asList(receiver, receiver2, receiver3, receiver4)) {
                            log.info(m.getSenderId() + " active Tasks: " + m.getRunningTasks());
                        }

                        Thread.sleep(1000);
                        assertTrue(System.currentTimeMillis() < waitUntil, "Took too long!");
                    }

                    int rec = received.get();
                    long messageCount = sender.getPendingMessagesCount();
                    log.info("Send " + amount + " recieved: " + rec + " queue: " + messageCount);
                    org.junit.jupiter.api.Assertions.assertEquals(amount + broadcastAmount * 4, received.get(),
                                                    "should have received " + (amount + broadcastAmount * 4) + " but actually got "
                                                    + received.get());

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
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void exclusiveMessageStartupTests(Morphium morphium) throws Exception  {
        SingleCollectionMessaging sender = new SingleCollectionMessaging(morphium, 100, true, 1);
        MorphiumMessaging receiverNoListener = morphium.createMessaging();
        receiverNoListener.setPause(100).setMultithreadded(true).setWindowSize(10);

        try {
            sender.setSenderId("sender");
            morphium.dropCollection(Msg.class, sender.getCollectionName(), null);
            morphium.dropCollection(MsgLock.class, sender.getLockCollectionName(), null);
            Thread.sleep(2000);
            sender.start();
            sender.sendMessage(new Msg("test", "test", "test", 30000, true));
            sender.sendMessage(new Msg("test", "test", "test", 30000, true));
            sender.sendMessage(new Msg("test", "test", "test", 30000, true));
            Thread.sleep(1000);
            receiverNoListener.setSenderId("recNL");
            receiverNoListener.start();
            org.junit.jupiter.api.Assertions.assertEquals(3,
                    morphium.createQueryFor(Msg.class, sender.getCollectionName()).countAll());
        } finally {
            sender.terminate();
            receiverNoListener.terminate();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void exclusiveMessageDelAfterProcessingTimeOffsetTest(Morphium morphium) throws Exception  {
        MorphiumMessaging sender = morphium.createMessaging();
        sender.setPause(100).setMultithreadded(true).setWindowSize(1);
        sender.setSenderId("Sender");
        // sender.start();
        Msg m = new Msg("test", "msg", "value");
        m.setExclusive(true);
        m.setTimingOut(false);
        m.setDeleteAfterProcessing(true);
        m.setDeleteAfterProcessingTime(10);
        sender.sendMessage(m);
        Thread.sleep(200);
        AtomicInteger recCount = new AtomicInteger(0);
        MorphiumMessaging receiver = morphium.createMessaging();
        receiver.setPause(100).setMultithreadded(true);
        receiver.addListenerForTopic("test2", (messaging, msg) -> {
            recCount.incrementAndGet();
            return null;
        });
        receiver.addListenerForTopic("test", (messaging, msg) -> {
            recCount.incrementAndGet();
            return null;
        });
        receiver.start();

        try {
            TestUtils.waitForConditionToBecomeTrue(10000, "Message not received", () -> recCount.get() == 1);
            log.info("waiting for mongo to delete...");
            long start = System.currentTimeMillis();

            while (true) {
                var cnt = morphium.createQueryFor(Msg.class, sender.getCollectionName()).countAll();
                log.info("... still " + cnt);

                if (cnt == 0) {
                    break;
                }

                Thread.sleep(4000);
                // MongoDB's TTL monitor runs periodically; allow enough time for real clusters.
                assertTrue(System.currentTimeMillis() - start < 180000);
            }

            log.info("Deleted after: " + (System.currentTimeMillis() - start));
            assertEquals(0, morphium.createQueryFor(MsgLock.class, sender.getLockCollectionName()).countAll());
            ;
        } finally {
            sender.terminate();
            receiver.terminate();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void exclusiveMessageCheckOnStartTest(Morphium morphium) throws Exception  {
        MorphiumMessaging sender = morphium.createMessaging();
        sender.setPause(100).setMultithreadded(true).setWindowSize(1);
        sender.setSenderId("Sender");
        // sender.start();
        Msg m = new Msg("test", "msg", "value");
        m.setExclusive(true);
        m.setTimingOut(false);
        m.setDeleteAfterProcessing(true);
        m.setDeleteAfterProcessingTime(0);
        sender.sendMessage(m);
        Thread.sleep(200);
        AtomicInteger recCount = new AtomicInteger(0);
        MorphiumMessaging receiver = morphium.createMessaging();
        receiver.setPause(100).setMultithreadded(true).setWindowSize(10);
        receiver.addListenerForTopic("test2", (messaging, msg) -> {
            recCount.incrementAndGet();
            return null;
        });
        receiver.addListenerForTopic("test", (messaging, msg) -> {
            recCount.incrementAndGet();
            return null;
        });
        receiver.start();

        try {
            TestUtils.waitForConditionToBecomeTrue(10000, "Message not received", () -> recCount.get() == 1);
        } finally {
            sender.terminate();
            receiver.terminate();
        }
    }



    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void msgLockTest(Morphium morphium) throws Exception  {

        int amount = 130;
        List<Msg> msgs = new Vector<>();
        List<Msg> locked = new Vector<>();
        MorphiumMessaging msgn = morphium.createMessaging();
        for (int i = 0; i < amount; i++) {

            Msg m = new Msg("test", "value", "value", 33000);
            m.setSender("me");
            m.setSenderHost("localhost");
            morphium.store(m, msgn.getCollectionName(m));
            msgs.add(m);

        }


        log.info("Created {} msgs", amount);

        List<Thread> thr = new ArrayList<>();
        for (int i = 0; i < amount * 2; i++) {
            String tn = "Thr" + i;
            var thread = Thread.ofVirtual().start(()-> {

                int idx = 0;
                int cycle = 0;
                while (true) {
                    if (msgn.lockMessage(msgs.get(idx), tn, null)) {
                        locked.add(msgs.get(idx));
                        break;
                    }
                    idx++;
                    if (idx >= msgs.size()) {
                        idx = 0;
                        cycle++;
                    }
                    if (cycle >= 2) {
                        log.info("Did not get lock at all");
                        break;
                    }
                }
                log.info("{} got lock for #{} - exiting", tn, idx);
                return;
            });
            thr.add(thread);
        }
        int idx = 0;
        for (var t : thr) {
            t.join();
            log.info("Thread {} joined...", idx++);
        }
        assertEquals(msgs.size(), locked.size());

    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void exclusiveTest(Morphium morphium) throws Exception  {
        // for (String msgImpl : MultiDriverTestBase.messagingsToTest) {
        // for (String msgImpl : List.of("MultiCollectionMessaging")) {
        for (String msgImpl : List.of("StandardMessaging")) {
            de.caluga.test.OutputHelper.figletOutput(log, msgImpl);
            log.info("Using messaging implementation: {}", msgImpl);
            final int listeners = 14;
            final int exclusiveMessages = 137;
            final int broadcastAmount = 72;
            final AtomicInteger insertEvents = new AtomicInteger(0);
            var cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImpl);
            cfg.encryptionSettings()
               .setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(
                               morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(
                               morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

            cfg.messagingSettings().setMessagingMultithreadded(true);
            cfg.messagingSettings().setMessagingWindowSize(25);
            cfg.messagingSettings().setUseChangeStream(true);

            try (Morphium mx = new Morphium(cfg)) {
                mx.dropCollection(Msg.class);
                MorphiumMessaging sender = mx.createMessaging();
                sender.setPause(1000).setMultithreadded(true).setWindowSize(1);
                sender.setSenderId("sender");
                final AtomicInteger counts = new AtomicInteger();
                List<MorphiumMessaging> recs = new ArrayList<>();
                List<MorphiumId> msgIds = new ArrayList<>();
                List<MorphiumId> sent = new ArrayList<>();
                Map<String, List<Msg>> recByRecId = new ConcurrentHashMap<>();
                List<Thread> threads = new ArrayList();
                for (int i = 0; i < listeners; i++) {
                    MorphiumMessaging r = mx.createMessaging();
                    r.setPause(10).setMultithreadded(true).setWindowSize(10);
                    r.setSenderId("r" + i);
                    r.setUseChangeStream(true);
                    recs.add(r);
                    r.start();

                    r.addListenerForTopic("excl_name", (m, msg) -> {
                        counts.incrementAndGet();
                        if (msg.isExclusive()) {
                            if (msgIds.contains(msg.getMsgId())) {
                                log.error("Duplicate processing of msg {}", msgIds.indexOf(msg.getMsgId()));
                                if (msg.getProcessedBy().contains(m.getSenderId())) {
                                    log.info("{} -> processed by me {}", msg.getProcessedBy(), m.getSenderId());
                                } else {
                                    log.info("{} -> was processed by someone else than me {}", msg.getProcessedBy(), m.getSenderId());
                                }
                            }
                        } else {
                            recByRecId.putIfAbsent(m.getSenderId(), new Vector<>());
                            recByRecId.get(m.getSenderId()).add(msg);
                        }
                        msgIds.add(msg.getMsgId());
                        try {
                            Thread.sleep(150);
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return null;
                    });
                }
                List<Map<String, Object>> pipeline = new ArrayList<>();
                Map<String, Object> match = new LinkedHashMap<>();
                Map<String, Object> in = new LinkedHashMap<>();
                in.put("$in", Arrays.asList("insert"));
                match.put("operationType", in);
                pipeline.add(UtilsMap.of("$match", match));
                var changeStreamMonitor = new ChangeStreamMonitor(mx, sender.getCollectionName("excl_name"), false, 100, pipeline);

                changeStreamMonitor.addListener((evt)-> {

                    insertEvents.incrementAndGet();
                    return true;
                });

                changeStreamMonitor.start();


                TestUtils.wait("waiting for receivers", 10);
                try {
                    for (int i = 0; i < exclusiveMessages; i++) {
                        if (i % 10 == 0)
                            log.info("Sent exclusive Msg {}", i);
                        var msg = new Msg("excl_name", "msg", "value", 20000000, true)
                        .setDeleteAfterProcessing(true).setDeleteAfterProcessingTime(0);
                        sender.sendMessage(msg);
                        sent.add(msg.getMsgId());
                    }

                    var q = mx.createQueryFor(Msg.class, sender.getCollectionName("excl_name"));
                    TestUtils.waitForConditionToBecomeTrue(exclusiveMessages * 200, "Did not reach message count",
                    () -> counts.get() >= exclusiveMessages, (dur) -> {
                        log.info("Waiting to reach {}, still at {}, mongo {}", exclusiveMessages, counts.get(),
                                 q.countAll());
                        StringBuffer b = new StringBuffer();
                        if (counts.get() > exclusiveMessages - 5) {
                            for (MorphiumId id : sent) {
                                if (id == null)
                                    continue;
                                // log.info(id.toString());
                                if (!msgIds.contains(id)) {
                                    log.info("Missing sent msg #{}", sent.indexOf(id));
                                    b.append("x");
                                } else {
                                    b.append("-");
                                }
                            }
                            log.info("Stats: {}", b.toString());
                        }
                    });
                    log.info("All messages received");
                    TestUtils.wait("Waiting some time", 5);
                    log.info("Now we got {} messages", counts.get());
                    assertEquals(exclusiveMessages, counts.get());
                    counts.set(0);
                    insertEvents.set(0);

                    for (int i = 0; i < broadcastAmount; i++) {
                        if (i % 5 == 0)
                            log.info("Sent broadcast message {}", i);
                        sender.sendMessage(new Msg("excl_name", "msg", "value", 20000000, false));
                    }

                    TestUtils.waitForConditionToBecomeTrue(broadcastAmount * 350, "Did not reach message count",
                    () -> counts.get() >= listeners * broadcastAmount, (dur) -> {
                        log.info("sent {} message, not yet {} messages received, still got {}, mongo {} - events {}", broadcastAmount, listeners * broadcastAmount,
                                 counts.get(), q.countAll(), insertEvents.get());
                        for (var entry : recByRecId.entrySet()) {
                            if (entry.getValue().size() < broadcastAmount) {
                                log.info("Rec {} has only {} msg received", entry.getKey(), entry.getValue().size());
                                var qu = mx.createQueryFor(Msg.class, sender.getCollectionName("excl_name")).f(Msg.Fields.exclusive).eq(false).f(Msg.Fields.processedBy).ne(entry.getKey());
                                log.info("Unprocessed messages in mongo for rec {}: {}", entry.getKey(), qu.countAll());
                            }

                            Map<String, AtomicInteger> cnts = new HashMap<>();
                            for (var msg : entry.getValue()) {
                                cnts.putIfAbsent(msg.getMsgId().toString(), new AtomicInteger(0));
                                cnts.get(msg.getMsgId().toString()).incrementAndGet();
                            }
                            for (var key : cnts.keySet()) {
                                if (cnts.get(key).get() > 1) {
                                    log.error("Sender {} received msg {} {}x", entry.getKey(), key, cnts.get(key));
                                }
                            }

                        }
                    });
                    TestUtils.wait("Waiting some time", 5);
                    org.junit.jupiter.api.Assertions.assertEquals(listeners * broadcastAmount, counts.get(),
                                                    "Did get too many? " + counts.get());
                } finally {
                    Thread.ofVirtual().start(() -> {
                        sender.terminate();
                    });
                    for (MorphiumMessaging r : recs) {
                        Thread.ofVirtual().start(() -> {
                            r.terminate();
                        });
                    }
                }

            }
        }
    }

}
