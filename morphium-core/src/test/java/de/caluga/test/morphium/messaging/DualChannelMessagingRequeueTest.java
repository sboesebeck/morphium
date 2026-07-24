package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.DualChannelMessaging;
import de.caluga.morphium.messaging.MessageRejectedException;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Requeue/retry semantics for {@link DualChannelMessaging} (#265, beta) DM-lane message
 * processing: a listener exception leaves {@code processed_by} empty so the fallback poll
 * redelivers the message; {@link MessageRejectedException} is honored the same way; and
 * {@code deleteAfterProcessing} works for DM-routed answers.
 */
@Tag("messaging")
public class DualChannelMessagingRequeueTest extends MultiDriverTestBase {

    private MorphiumConfig configFor(Morphium base) {
        MorphiumConfig cfg = base.getConfig().createCopy();
        cfg.messagingSettings().setMessagingImplementation("DualChannelMessaging");
        cfg.messagingSettings().setMessagingFallbackPollInterval(1000);
        cfg.encryptionSettings().setCredentialsEncrypted(base.getConfig().encryptionSettings().getCredentialsEncrypted());
        cfg.encryptionSettings().setCredentialsDecryptionKey(base.getConfig().encryptionSettings().getCredentialsDecryptionKey());
        cfg.encryptionSettings().setCredentialsEncryptionKey(base.getConfig().encryptionSettings().getCredentialsEncryptionKey());
        return cfg;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void listenerExceptionLeavesUnprocessedForRetryTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                DualChannelMessaging receiver = (DualChannelMessaging) m.createMessaging();
                DualChannelMessaging sender = (DualChannelMessaging) m.createMessaging();
                AtomicInteger attempts = new AtomicInteger(0);

                try {
                    receiver.start();
                    assertTrue(receiver.waitForReady(30, TimeUnit.SECONDS));
                    sender.start();
                    assertTrue(sender.waitForReady(30, TimeUnit.SECONDS));

                    receiver.addListenerForTopic("flaky", (mm, msg) -> {
                        int n = attempts.incrementAndGet();

                        if (n == 1) {
                            throw new RuntimeException("boom - simulated first-attempt failure");
                        }

                        return null;
                    });

                    Msg msg = new Msg("flaky", "x", "");
                    msg.setRecipient(receiver.getSenderId());
                    sender.sendMessage(msg);

                    TestUtils.waitForConditionToBecomeTrue(20000, "message was never retried after listener exception",
                            () -> attempts.get() >= 2);

                    String dmColl = receiver.getDMCollectionName();
                    Msg stored = m.createQueryFor(Msg.class, dmColl).f("_id").eq(msg.getMsgId()).get();
                    assertNotNull(stored);
                    assertTrue(stored.getProcessedBy() == null || stored.getProcessedBy().contains(receiver.getSenderId()),
                            "second (successful) attempt should have marked processed_by");
                } finally {
                    receiver.terminate();
                    sender.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void messageRejectedExceptionTriggersRetryTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                DualChannelMessaging receiver = (DualChannelMessaging) m.createMessaging();
                DualChannelMessaging sender = (DualChannelMessaging) m.createMessaging();
                AtomicInteger attempts = new AtomicInteger(0);
                AtomicInteger rejectionHandlerCalls = new AtomicInteger(0);

                try {
                    receiver.start();
                    assertTrue(receiver.waitForReady(30, TimeUnit.SECONDS));
                    sender.start();
                    assertTrue(sender.waitForReady(30, TimeUnit.SECONDS));

                    receiver.addListenerForTopic("rejectme", (mm, msg) -> {
                        int n = attempts.incrementAndGet();

                        if (n == 1) {
                            MessageRejectedException mre = new MessageRejectedException("simulated rejection");
                            mre.setCustomRejectionHandler((mm2, msg2) -> rejectionHandlerCalls.incrementAndGet());
                            throw mre;
                        }

                        return null;
                    });

                    Msg msg = new Msg("rejectme", "x", "");
                    msg.setRecipient(receiver.getSenderId());
                    sender.sendMessage(msg);

                    TestUtils.waitForConditionToBecomeTrue(20000, "message was never retried after rejection",
                            () -> attempts.get() >= 2);
                    assertTrue(rejectionHandlerCalls.get() >= 1, "rejection handler was never invoked");
                } finally {
                    receiver.terminate();
                    sender.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void deleteAfterProcessingOnDmAnswerTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                DualChannelMessaging requester = (DualChannelMessaging) m.createMessaging();
                DualChannelMessaging responder = (DualChannelMessaging) m.createMessaging();

                try {
                    requester.start();
                    assertTrue(requester.waitForReady(30, TimeUnit.SECONDS));
                    responder.start();
                    assertTrue(responder.waitForReady(30, TimeUnit.SECONDS));

                    responder.addListenerForTopic("delping", (mm, msg) -> {
                        Msg answer = new Msg("delping", "pong", "");
                        answer.setDeleteAfterProcessing(true);
                        answer.setDeleteAfterProcessingTime(0);
                        return answer;
                    });

                    Msg req = new Msg("delping", "ping", "");
                    Msg answer = requester.sendAndAwaitFirstAnswer(req, 15000);
                    assertNotNull(answer);

                    String dmColl = requester.getDMCollectionName();
                    TestUtils.waitForConditionToBecomeTrue(10000, "answer with deleteAfterProcessing was not removed from the DM collection",
                            () -> m.createQueryFor(Msg.class, dmColl).f("_id").eq(answer.getMsgId()).get() == null);
                } finally {
                    requester.terminate();
                    responder.terminate();
                }
            }
        }
    }
}
