package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.MessageRejectedException;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class TopicRegistryTest extends MultiDriverTestBase {

    private void waitUntilSendAccepted(long timeoutMs, Runnable sendAttempt) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        MessageRejectedException last = null;

        while (System.currentTimeMillis() < deadline) {
            try {
                sendAttempt.run();
                return;
            } catch (MessageRejectedException e) {
                last = e;
                Thread.sleep(50);
            }
        }

        if (last != null) {
            throw last;
        }
        throw new MessageRejectedException("Timed out waiting for send to be accepted");
    }

    private void waitUntilSendRejected(long timeoutMs, Runnable sendAttempt) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            try {
                sendAttempt.run();
                Thread.sleep(50);
            } catch (MessageRejectedException e) {
                return;
            }
        }

        throw new MessageRejectedException("Timed out waiting for send to be rejected");
    }

    @ParameterizedTest
    @MethodSource("de.caluga.test.mongo.suite.base.MultiDriverTestBase#getMorphiumInstancesNoSingle")
    public void testThrowOnNoListeners(Morphium morphium) throws Exception {
        try (morphium) {
            for (String msgImpl : MorphiumTestBase.messagingsToTest) {
                log.info("Running test with messaging implementation: " + msgImpl);
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.messagingSettings().setMessagingRegistryEnabled(true);
                cfg.messagingSettings().setMessagingRegistryCheckTopics(de.caluga.morphium.config.MessagingSettings.TopicCheck.THROW);
                cfg.messagingSettings().setMessagingRegistryUpdateInterval(1);
                try (Morphium m = new Morphium(cfg)) {
                    MorphiumMessaging messaging = m.createMessaging();
                    messaging.start();

                    Thread.sleep(1500); // wait for registry to run once

                    assertThrows(MessageRejectedException.class, () -> {
                        log.info("Sending message to topic with no listeners...");
                        messaging.sendMessage(new Msg("no-listener-topic", "msg", "value"));
                    });

                    messaging.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("de.caluga.test.mongo.suite.base.MultiDriverTestBase#getMorphiumInstancesNoSingle")
    public void testSuccessfulSendWithListener(Morphium morphium) throws Exception {
        log.info("Running test: testSuccessfulSendWithListener");
        try (morphium) {
            for (String msgImpl : MorphiumTestBase.messagingsToTest) {
                log.info("Running test with messaging implementation: " + msgImpl);
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.messagingSettings().setMessagingRegistryEnabled(true);
                cfg.messagingSettings().setMessagingRegistryCheckTopics(de.caluga.morphium.config.MessagingSettings.TopicCheck.THROW);
                cfg.messagingSettings().setMessagingRegistryUpdateInterval(1);

                try (Morphium m1 = new Morphium(cfg); Morphium m2 = new Morphium(cfg)) {
                    MorphiumMessaging sender = m1.createMessaging();
                    sender.start();

                    MorphiumMessaging receiver = m2.createMessaging();
                    java.util.concurrent.atomic.AtomicBoolean received = new java.util.concurrent.atomic.AtomicBoolean(false);
                    receiver.addListenerForTopic("listener-topic", (msg, m) -> {
                        log.info("Receiver got message!");
                        received.set(true);
                        return null;
                    });
                    receiver.start();

                    log.info("Waiting for network discovery (registry update)...");
                    waitUntilSendAccepted(15_000, () -> sender.sendMessage(new Msg("listener-topic", "warmup", "value")));

                    log.info("Sending message to topic with listener...");
                    sender.sendMessage(new Msg("listener-topic", "msg", "value"));

                    Thread.sleep(1000); // Wait for message processing
                    assert (received.get());

                    sender.terminate();
                    receiver.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("de.caluga.test.mongo.suite.base.MultiDriverTestBase#getMorphiumInstancesNoSingle")
    public void testWarnOnNoListeners(Morphium morphium) throws Exception {
        log.info("Running test: testWarnOnNoListeners");
        try (morphium) {
            for (String msgImpl : MorphiumTestBase.messagingsToTest) {
                log.info("Running test with messaging implementation: " + msgImpl);
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.messagingSettings().setMessagingRegistryEnabled(true);
                cfg.messagingSettings().setMessagingRegistryCheckTopics(de.caluga.morphium.config.MessagingSettings.TopicCheck.WARN);
                cfg.messagingSettings().setMessagingRegistryUpdateInterval(1);
                try (Morphium m = new Morphium(cfg)) {
                    MorphiumMessaging messaging = m.createMessaging();
                    messaging.start();

                    Thread.sleep(1500); // wait for registry to run once

                    // Should just log a warning, not throw
                    log.info("Sending message that should trigger a warning...");
                    messaging.sendMessage(new Msg("no-listener-topic-warn", "msg", "value"));
                    log.info("Message sent.");

                    Thread.sleep(500);
                    messaging.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("de.caluga.test.mongo.suite.base.MultiDriverTestBase#getMorphiumInstancesNoSingle")
    public void testRecipientCheck(Morphium morphium) throws Exception {
        log.info("Running test: testRecipientCheck");
        try (morphium) {
            for (String msgImpl : MorphiumTestBase.messagingsToTest) {
                log.info("Running test with messaging implementation: " + msgImpl);
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.messagingSettings().setMessagingRegistryEnabled(true);
                cfg.messagingSettings().setMessagingRegistryCheckRecipients(de.caluga.morphium.config.MessagingSettings.RecipientCheck.THROW);
                cfg.messagingSettings().setMessagingRegistryParticipantTimeout(2000);
                cfg.messagingSettings().setMessagingRegistryUpdateInterval(1);

                try (Morphium m1 = new Morphium(cfg)) {
                    MorphiumMessaging sender = m1.createMessaging();
                    sender.start();

                    String receiverId;
                    try (Morphium m2 = new Morphium(cfg)) {
                        MorphiumMessaging receiver = m2.createMessaging();
                        receiverId = receiver.getSenderId();
                        receiver.start();
                        try {
                            log.info("Waiting for initial discovery...");
                            log.info("Receiver ID: " + receiverId);
                            // Should work (wait for registry to see receiver)
                            waitUntilSendAccepted(15_000, () -> {
                                Msg directMsg = new Msg("direct", "msg", "value");
                                directMsg.addRecipient(receiverId);
                                sender.sendMessage(directMsg);
                            });
                            log.info("Sent message to active recipient.");
                        } finally {
                            receiver.terminate();
                        }
                        // Receiver is terminated automatically by try-with-resources
                    }

                    log.info("Receiver is now terminated. Waiting for participant timeout...");
                    waitUntilSendRejected(15_000, () -> {
                        Msg directMsg = new Msg("direct", "msg", "value");
                        directMsg.addRecipient(receiverId);
                        log.info("Sending message to inactive recipient...");
                        sender.sendMessage(directMsg);
                    });
                    log.info("Successfully caught exception for inactive recipient.");
                    sender.terminate();
                }
            }
        }
    }
}
