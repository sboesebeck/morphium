package de.caluga.test.morphium.messaging;

import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import de.caluga.morphium.messaging.*;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumId;
import org.junit.jupiter.api.Tag;
import de.caluga.test.mongo.suite.base.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Basic exclusive message tests - simpler tests for exclusive message delivery.
 * Split from ExclusiveMessageTests for better parallelization.
 */
@Tag("messaging")
@Tag("slow")
public class ExclusiveMessageBasicTests extends MultiDriverTestBase {
    private boolean gotMessage1 = false;
    private boolean gotMessage2 = false;
    private boolean gotMessage3 = false;

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void ignoringExclusiveMessagesTest(Morphium morphium) throws Exception {
        for (String msgImpl : de.caluga.test.mongo.suite.base.MultiDriverTestBase.messagingsToTest) {
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
                    Thread.sleep(2000);
                    for (int i = 0; i < 10; i++) {
                        Msg mm = new Msg("test", "ignore me please", "value", 20000, true);
                        m1.sendMessage(mm);
                        Thread.sleep(100);
                        long start = System.currentTimeMillis();
                        final String collName = m1.getCollectionName(mm);
                        final MorphiumId msgId = mm.getMsgId();
                        while (true) {
                            Thread.sleep(100);
                            mm = m.createQueryFor(Msg.class, collName).f("_id").eq(msgId).get();
                            if (mm == null) {
                                assertTrue(System.currentTimeMillis() - start < 120000, "timeout waiting for message to be visible");
                                continue;
                            }
                            if (mm.getProcessedBy() != null && mm.getProcessedBy().size() != 0)
                                break;
                            assertTrue(System.currentTimeMillis() - start < 120000, "timeout waiting for processing");
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
    public void exclusiveMessageTest(Morphium morphium) throws Exception {
        morphium.dropCollection(Msg.class);
        TestUtils.waitForConditionToBecomeTrue(1000, "Collection did not drop", () -> !morphium.exists(Msg.class));
        MorphiumMessaging sender = morphium.createMessaging();
        sender.setPause(100).setMultithreadded(true).setWindowSize(1);
        sender.start();
        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        MorphiumMessaging m1 = morphium.createMessaging();
        m1.setPause(100).setMultithreadded(true).setWindowSize(1);
        m1.addListenerForTopic("A message", (msg, m) -> {
            gotMessage1 = true;
            return null;
        });
        MorphiumMessaging m2 = morphium.createMessaging();
        m2.setPause(100).setMultithreadded(true).setWindowSize(1);
        m2.addListenerForTopic("A message", (msg, m) -> {
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
                assertThat(System.currentTimeMillis() - s).isLessThan(3 * morphium.getConfig().getMaxWaitTime());
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
    public void exclusiveMessageStartupTests(Morphium morphium) throws Exception {
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
    public void exclusiveMessageDelAfterProcessingTimeOffsetTest(Morphium morphium) throws Exception {
        MorphiumMessaging sender = morphium.createMessaging();
        sender.setPause(100).setMultithreadded(true).setWindowSize(1);
        sender.setSenderId("Sender");
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
                assertTrue(System.currentTimeMillis() - start < 180000);
            }

            log.info("Deleted after: " + (System.currentTimeMillis() - start));
            assertEquals(0, morphium.createQueryFor(MsgLock.class, sender.getLockCollectionName()).countAll());
        } finally {
            sender.terminate();
            receiver.terminate();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void exclusiveMessageCheckOnStartTest(Morphium morphium) throws Exception {
        MorphiumMessaging sender = morphium.createMessaging();
        sender.setPause(100).setMultithreadded(true).setWindowSize(1);
        sender.setSenderId("Sender");
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
}
