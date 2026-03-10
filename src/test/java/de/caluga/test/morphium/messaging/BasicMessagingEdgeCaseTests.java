package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.OutputHelper;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import de.caluga.test.mongo.suite.base.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge case messaging tests - timeout behavior and messages without listeners.
 * Split from BasicMessagingTests for better parallelization.
 */
@Tag("messaging")
public class BasicMessagingEdgeCaseTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void timeoutTest(Morphium morphium) throws Exception {
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
                    MorphiumMessaging sender = morph.createMessaging();
                    sender.setSenderId("sender");
                    MorphiumMessaging receiver = morph.createMessaging();
                    receiver.setSenderId("receiver");

                    // Clean slate for each messaging implementation
                    String collName = sender.getCollectionName("test");
                    log.info("Using collection: {} for messaging implementation: {}", collName, msgImpl);
                    morph.dropCollection(Msg.class, collName, null);
                    Thread.sleep(500);
                    
                    morph.ensureIndicesFor(Msg.class, sender.getLockCollectionName("test"));
                    AtomicInteger msgCount = new AtomicInteger(0);
                    receiver.addListenerForTopic("test", (msg, mm) -> {
                        msgCount.incrementAndGet();
                        return null;
                    });

                    sender.start();
                    receiver.start();
                    // Wait for messaging to be fully ready
                    assertTrue(sender.waitForReady(15, TimeUnit.SECONDS), "sender not ready");
                    assertTrue(receiver.waitForReady(15, TimeUnit.SECONDS), "receiver not ready");
                    // Small delay for topic listeners to be fully registered
                    Thread.sleep(1000);

                    // Test messages that should not timeout
                    for (int i = 0; i < 5; i++) {
                        var msg = new Msg("test", "value" + i, "" + i).setTimingOut(false);
                        sender.sendMessage(msg);
                    }

                    TestUtils.waitForConditionToBecomeTrue(10000, "Did not get all messages", () -> msgCount.get() == 5);
                    assertEquals(5, msgCount.get());

                    // Verify messages are processed and not timed out
                    TestUtils.waitWithMessage(35000, 5000, (dur)->log.info("Waiting...{}s", dur / 1000));
                    String countCollection = sender.getCollectionName("test");
                    long count = morph.createQueryFor(Msg.class, countCollection).countAll();
                    log.info("Count in collection '{}': {} (expected 5)", countCollection, count);
                    if (count != 5) {
                        // Debug: check what's actually in the collection
                        var msgs = morph.createQueryFor(Msg.class, countCollection).asList();
                        log.error("Found {} messages in collection: {}", msgs.size(), msgs.stream().map(m -> m.getMsgId() + "/" + m.getDeleteAt()).toList());
                    }
                    assertEquals(5, count, "Messages should be processed and not removed");
                    log.info("Test {} using {}/{} finished", tstName, msgImpl, morphium.getDriver().getName());
                    sender.terminate();
                    receiver.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void noListenerTest(Morphium morphium) throws Exception {
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
                    sender.start();
                    assertTrue(sender.waitForReady(15, TimeUnit.SECONDS), "sender not ready");

                    // Send message without any listeners
                    Msg msg = new Msg("orphan", "No listener for this", "value");
                    msg.setTtl(2000);
                    morph.ensureIndicesFor(Msg.class, sender.getCollectionName(msg) ); //ensure TTL index
                    sender.sendMessage(msg);
                    Thread.sleep(500);

                    // Verify message exists in collection
                    long count = morph.createQueryFor(Msg.class, sender.getCollectionName(msg)).countAll();
                    assertEquals(1, count, "Message should exist in collection");

                    // Wait for TTL to expire
                    Thread.sleep(3000);

                    // Message should be removed by TTL
                    // MongoDB's TTL monitor runs periodically; allow enough time for real clusters.
                    TestUtils.waitForConditionToBecomeTrue(120000, "Message should be removed by TTL",
                                                           () -> morph.createQueryFor(Msg.class, sender.getCollectionName(msg)).countAll() == 0);

                    log.info("Test {} using {}/{} finished", tstName, msgImpl, morphium.getDriver().getName());
                    sender.terminate();
                }
            }
        }
    }
}
