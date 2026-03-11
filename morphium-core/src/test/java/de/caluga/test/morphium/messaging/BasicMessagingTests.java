package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.OutputHelper;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import de.caluga.test.mongo.suite.base.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Consolidated basic messaging tests providing maximum coverage with minimum number of tests.
 * Tests core messaging functionality: send/receive, timeouts, multithreading, custom queues.
 */
@Tag("messaging")
public class BasicMessagingTests extends MultiDriverTestBase {

    private boolean gotMessage1 = false;
    private boolean gotMessage2 = false;
    private boolean gotMessage3 = false;
    private boolean error = false;

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void basicSendReceiveTest(Morphium morphium) throws Exception {
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

                    AtomicInteger msgCount = new AtomicInteger(0);
                    List<Msg> receivedMessages = new ArrayList<>();

                    receiver.addListenerForTopic("test", (msg, m) -> {
                        synchronized (receivedMessages) {
                            receivedMessages.add(m);
                            msgCount.incrementAndGet();
                        }
                        return null;
                    });

                    sender.start();
                    receiver.start();
                    // Wait for messaging to be fully ready
                    assertTrue(sender.waitForReady(15, TimeUnit.SECONDS), "sender not ready");
                    assertTrue(receiver.waitForReady(15, TimeUnit.SECONDS), "receiver not ready");
                    // Small delay for topic listeners to be fully registered
                    Thread.sleep(1000);

                    // Test basic send/receive
                    Msg testMsg = new Msg("test", "Basic message", "value1");
                    sender.sendMessage(testMsg);

                    TestUtils.waitForConditionToBecomeTrue(5000, "Did not receive message", () -> msgCount.get() >= 1);
                    assertEquals(1, msgCount.get());
                    synchronized (receivedMessages) {
                        assertEquals("Basic message", receivedMessages.get(0).getMsg());
                        assertEquals("value1", receivedMessages.get(0).getValue());
                    }

                    // Test multiple messages
                    for (int i = 0; i < 5; i++) {
                        sender.sendMessage(new Msg("test", "Message " + i, "value" + i));
                    }

                    TestUtils.waitForConditionToBecomeTrue(5000, "Did not receive all messages", () -> msgCount.get() >= 6);
                    assertEquals(6, msgCount.get());

                    log.info("Test {} using {}/{} finished", tstName, msgImpl, morphium.getDriver().getName());
                    sender.terminate();
                    receiver.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void multithreadedProcessingTest(Morphium morphium) throws Exception {
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

                    final MorphiumMessaging producer = morph.createMessaging();
                    final MorphiumMessaging consumer = morph.createMessaging();
                    producer.setSenderId("producer");
                    consumer.setSenderId("consumer");
                    consumer.setMultithreadded(true);
                    producer.start();
                    consumer.start();
                    // Wait for messaging to be fully ready
                    assertTrue(producer.waitForReady(15, TimeUnit.SECONDS), "producer not ready");
                    assertTrue(consumer.waitForReady(15, TimeUnit.SECONDS), "consumer not ready");

                    AtomicInteger processed = new AtomicInteger(0);
                    consumer.addListenerForTopic("test", (msg, message) -> {
                        processed.incrementAndGet();
                        // Simulate some processing time
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return null;
                    });
                    // Small delay for topic listeners to be fully registered
                    Thread.sleep(1000);
                    int numberOfMessages = 20;

                    // Send messages
                    for (int i = 0; i < numberOfMessages; i++) {
                        Msg message = new Msg("test", "m" + i, "v" + i);
                        message.setTtl(60000);
                        producer.sendMessage(message);
                    }

                    TestUtils.waitForConditionToBecomeTrue(15000,
                                                           "Did not process all messages: " + processed.get() + "/" + numberOfMessages,
                                                           () -> processed.get() >= numberOfMessages);

                    assertEquals(numberOfMessages, processed.get());
                    log.info("Test using {}/{} finished", msgImpl, morphium.getDriver().getName());

                    producer.terminate();
                    consumer.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void customQueueTest(Morphium morphium) throws Exception {
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

                    // Test different queues
                    MorphiumMessaging sender1 = morph.createMessaging();
                    sender1.setQueueName("queue1").setSenderId("sender1");
                    MorphiumMessaging receiver1 = morph.createMessaging();
                    receiver1.setQueueName("queue1").setSenderId("receiver1");

                    MorphiumMessaging sender2 = morph.createMessaging();
                    sender2.setQueueName("queue2").setSenderId("sender2");
                    MorphiumMessaging receiver2 = morph.createMessaging();
                    receiver2.setQueueName("queue2").setSenderId("receiver2");

                    AtomicInteger queue1Messages = new AtomicInteger(0);
                    AtomicInteger queue2Messages = new AtomicInteger(0);

                    receiver1.addListenerForTopic("test", (msg, m) -> {
                        queue1Messages.incrementAndGet();
                        return null;
                    });

                    receiver2.addListenerForTopic("test", (msg, m) -> {
                        queue2Messages.incrementAndGet();
                        return null;
                    });

                    sender1.start();
                    receiver1.start();
                    sender2.start();
                    receiver2.start();
                    // Wait for messaging to be fully ready
                    // These can be a bit flaky on CI/loaded machines; allow more time for changestream init
                    assertTrue(sender1.waitForReady(60, TimeUnit.SECONDS), "sender1 not ready");
                    assertTrue(receiver1.waitForReady(60, TimeUnit.SECONDS), "receiver1 not ready");
                    assertTrue(sender2.waitForReady(60, TimeUnit.SECONDS), "sender2 not ready");
                    assertTrue(receiver2.waitForReady(60, TimeUnit.SECONDS), "receiver2 not ready");
                    // Small delay for topic listeners to be fully registered
                    Thread.sleep(1000);

                    // Send messages to different queues
                    sender1.sendMessage(new Msg("test", "Queue1 message", "value1"));
                    sender2.sendMessage(new Msg("test", "Queue2 message", "value2"));

                    TestUtils.waitForConditionToBecomeTrue(5000, "Messages not received",
                                                           () -> queue1Messages.get() >= 1 && queue2Messages.get() >= 1);

                    assertEquals(1, queue1Messages.get());
                    assertEquals(1, queue2Messages.get());
                    log.info("Test {} using {}/{} finished", tstName, msgImpl, morphium.getDriver().getName());

                    sender1.terminate();
                    receiver1.terminate();
                    sender2.terminate();
                    receiver2.terminate();
                }
            }
        }
    }
}
