package de.caluga.test.morphium.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.DualChannelMessaging;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.SingleCollectionMessaging;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

@Tag("messaging")
public class TopicFilterChangeStreamTest extends MultiDriverTestBase {

    private static final List<String> IMPLEMENTATIONS = List.of(SingleCollectionMessaging.NAME, DualChannelMessaging.NAME);

    private MorphiumConfig configFor(Morphium base, String impl) {
        MorphiumConfig cfg = base.getConfig().createCopy();
        cfg.messagingSettings().setMessagingImplementation(impl);
        cfg.encryptionSettings().setCredentialsEncrypted(base.getConfig().encryptionSettings().getCredentialsEncrypted());
        cfg.encryptionSettings().setCredentialsDecryptionKey(base.getConfig().encryptionSettings().getCredentialsDecryptionKey());
        cfg.encryptionSettings().setCredentialsEncryptionKey(base.getConfig().encryptionSettings().getCredentialsEncryptionKey());
        return cfg;
    }

    private Set<String> csFilterTopics(MorphiumMessaging messaging) {
        if (messaging instanceof SingleCollectionMessaging scm) return scm.getCsFilterTopics();
        if (messaging instanceof DualChannelMessaging dcm) return dcm.getCsFilterTopics();
        throw new IllegalArgumentException("unexpected messaging implementation: " + messaging.getClass());
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void listenedTopicDeliveredForeignTopicNot(Morphium morphium) throws Exception {
        try (morphium) {
            for (String impl : IMPLEMENTATIONS) {
                log.info("=====> listenedTopicDeliveredForeignTopicNot with " + impl);

                try (Morphium m = new Morphium(configFor(morphium, impl))) {
                    m.dropCollection(Msg.class);
                    MorphiumMessaging sender = m.createMessaging();
                    MorphiumMessaging receiver = m.createMessaging();
                    AtomicInteger gotListened = new AtomicInteger(0);
                    AtomicInteger gotForeign = new AtomicInteger(0);

                    try {
                        sender.start();
                        assertTrue(sender.waitForReady(30, TimeUnit.SECONDS), "sender not ready");
                        receiver.start();
                        assertTrue(receiver.waitForReady(30, TimeUnit.SECONDS), "receiver not ready");

                        receiver.addListenerForTopic("tf_listened", (mm, msg) -> {
                            gotListened.incrementAndGet();
                            return null;
                        });

                        sender.sendMessage(new Msg("tf_foreign", "msg", "value"));
                        sender.sendMessage(new Msg("tf_listened", "msg", "value"));

                        TestUtils.waitForConditionToBecomeTrue(15000, "listened topic not delivered (" + impl + ")",
                            () -> gotListened.get() == 1);
                        // the foreign message was sent BEFORE the listened one and both took the same
                        // path - if it were going to be delivered, it would have arrived by now
                        Thread.sleep(1000);
                        assertEquals(0, gotForeign.get(), "message without listener must not be delivered (" + impl + ")");
                        assertEquals(1, gotListened.get(), "listened message delivered exactly once (" + impl + ")");
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
    public void filterRebuildsOnLateListenerRegistration(Morphium morphium) throws Exception {
        try (morphium) {
            for (String impl : IMPLEMENTATIONS) {
                log.info("=====> filterRebuildsOnLateListenerRegistration with " + impl);

                try (Morphium m = new Morphium(configFor(morphium, impl))) {
                    m.dropCollection(Msg.class);
                    MorphiumMessaging sender = m.createMessaging();
                    MorphiumMessaging receiver = m.createMessaging();
                    AtomicInteger gotB = new AtomicInteger(0);

                    try {
                        sender.start();
                        assertTrue(sender.waitForReady(30, TimeUnit.SECONDS), "sender not ready");
                        receiver.start();
                        assertTrue(receiver.waitForReady(30, TimeUnit.SECONDS), "receiver not ready");

                        receiver.addListenerForTopic("tf_a", (mm, msg) -> null);
                        TestUtils.waitForConditionToBecomeTrue(15000, "filter not rebuilt for tf_a (" + impl + ")",
                            () -> csFilterTopics(receiver).contains("tf_a"));

                        // message sent before tf_b is registered - must be picked up on registration
                        sender.sendMessage(new Msg("tf_b", "msg", "early"));
                        Thread.sleep(500);
                        assertEquals(0, gotB.get(), "tf_b has no listener yet (" + impl + ")");

                        receiver.addListenerForTopic("tf_b", (mm, msg) -> {
                            gotB.incrementAndGet();
                            return null;
                        });
                        TestUtils.waitForConditionToBecomeTrue(15000, "pre-registration tf_b message not picked up (" + impl + ")",
                            () -> gotB.get() == 1);
                        TestUtils.waitForConditionToBecomeTrue(15000, "filter not rebuilt for tf_b (" + impl + ")",
                            () -> csFilterTopics(receiver).contains("tf_b"));

                        // now the rebuilt change stream must deliver new tf_b messages
                        sender.sendMessage(new Msg("tf_b", "msg", "late"));
                        TestUtils.waitForConditionToBecomeTrue(15000, "post-rebuild tf_b message not delivered (" + impl + ")",
                            () -> gotB.get() == 2);

                        Set<String> topics = csFilterTopics(receiver);
                        assertTrue(topics.contains("tf_a") && topics.contains("tf_b"),
                            "filter topics must track registered listeners (" + impl + "), got: " + topics);
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
    public void broadcastAnswerBypassesTopicFilter(Morphium morphium) throws Exception {
        try (morphium) {
            for (String impl : IMPLEMENTATIONS) {
                log.info("=====> broadcastAnswerBypassesTopicFilter with " + impl);

                try (Morphium m = new Morphium(configFor(morphium, impl))) {
                    m.dropCollection(Msg.class);
                    MorphiumMessaging requester = m.createMessaging();
                    MorphiumMessaging responder = m.createMessaging();

                    try {
                        requester.start();
                        assertTrue(requester.waitForReady(30, TimeUnit.SECONDS), "requester not ready");
                        responder.start();
                        assertTrue(responder.waitForReady(30, TimeUnit.SECONDS), "responder not ready");

                        responder.addListenerForTopic("tf_req", (mm, msg) -> {
                            // craft a BROADCAST answer: inAnswerTo set, no recipient, and a topic
                            // the requester has no listener for - must still reach its waiter
                            Msg ans = new Msg("tf_unrelated", "answer", "value");
                            ans.setInAnswerTo(msg.getMsgId());
                            mm.sendMessage(ans);
                            return null;
                        });

                        Msg answer = requester.sendAndAwaitFirstAnswer(new Msg("tf_req", "question", "value"), 15000);
                        assertTrue(answer != null && "tf_unrelated".equals(answer.getTopic()),
                            "broadcast answer on unlistened topic must reach the waiter (" + impl + ")");
                    } finally {
                        requester.terminate();
                        responder.terminate();
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void filterShrinksOnListenerRemoval(Morphium morphium) throws Exception {
        try (morphium) {
            for (String impl : IMPLEMENTATIONS) {
                log.info("=====> filterShrinksOnListenerRemoval with " + impl);

                try (Morphium m = new Morphium(configFor(morphium, impl))) {
                    m.dropCollection(Msg.class);
                    MorphiumMessaging receiver = m.createMessaging();
                    MessageListener<Msg> listener = (mm, msg) -> null;

                    try {
                        receiver.start();
                        assertTrue(receiver.waitForReady(30, TimeUnit.SECONDS), "receiver not ready");

                        receiver.addListenerForTopic("tf_tmp", listener);
                        TestUtils.waitForConditionToBecomeTrue(15000, "filter not rebuilt after add (" + impl + ")",
                            () -> csFilterTopics(receiver).contains("tf_tmp"));

                        receiver.removeListenerForTopic("tf_tmp", listener);
                        TestUtils.waitForConditionToBecomeTrue(15000, "filter not rebuilt after remove (" + impl + ")",
                            () -> !csFilterTopics(receiver).contains("tf_tmp"));
                    } finally {
                        receiver.terminate();
                    }
                }
            }
        }
    }
}
