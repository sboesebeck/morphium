package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for {@code MorphiumMessaging#sendMessages(List)} and the default {@code
 * sendAnswers(Msg, List)} built on top of it - genuine client-side batching (one or more real
 * bulk-insert wire calls, grouped by whatever target collection each implementation's routing
 * needs), not {@code @WriteBuffer}. See docs/v5-vs-v6-performance.md "Batch Send Throughput"
 * for why this exists: {@code @WriteBuffer}'s poll-and-WAIT batching turned out to be a
 * throughput ceiling, not a booster, for messaging - this is the mechanism that actually helps.
 */
@Tag("messaging")
public class SendMessagesBulkTest extends MultiDriverTestBase {

    private MorphiumConfig configFor(Morphium base, String impl) {
        MorphiumConfig cfg = base.getConfig().createCopy();
        cfg.messagingSettings().setMessagingImplementation(impl);
        cfg.encryptionSettings().setCredentialsEncrypted(base.getConfig().encryptionSettings().getCredentialsEncrypted());
        cfg.encryptionSettings().setCredentialsDecryptionKey(base.getConfig().encryptionSettings().getCredentialsDecryptionKey());
        cfg.encryptionSettings().setCredentialsEncryptionKey(base.getConfig().encryptionSettings().getCredentialsEncryptionKey());
        return cfg;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void sendMessagesBroadcastTest(Morphium morphium) throws Exception {
        try (morphium) {
            for (String impl : MultiDriverTestBase.messagingsToTest) {
                MorphiumConfig cfg = configFor(morphium, impl);

                try (Morphium m = new Morphium(cfg)) {
                    m.dropCollection(Msg.class);
                    MorphiumMessaging sender = m.createMessaging();
                    MorphiumMessaging receiver = m.createMessaging();
                    List<String> received = new CopyOnWriteArrayList<>();

                    try {
                        sender.start();
                        assertTrue(sender.waitForReady(30, TimeUnit.SECONDS));
                        receiver.start();
                        assertTrue(receiver.waitForReady(30, TimeUnit.SECONDS));

                        receiver.addListenerForTopic("bulk_broadcast", (MessageListener<Msg>) (mm, msg) -> {
                            received.add(msg.getMsg());
                            return null;
                        });

                        List<Msg> batch = new ArrayList<>();
                        for (int i = 0; i < 20; i++) {
                            batch.add(new Msg("bulk_broadcast", "m" + i, "v", 30000));
                        }
                        sender.sendMessages(batch);

                        TestUtils.waitForConditionToBecomeTrue(15000,
                                impl + ": not all bulk-sent broadcast messages arrived",
                                () -> received.size() >= 20);
                        assertEquals(20, received.size(), impl + ": unexpected message count");

                        for (int i = 0; i < 20; i++) {
                            assertTrue(received.contains("m" + i), impl + ": missing m" + i);
                        }
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
    public void sendMessagesGroupsByRecipientTest(Morphium morphium) throws Exception {
        try (morphium) {
            for (String impl : MultiDriverTestBase.messagingsToTest) {
                MorphiumConfig cfg = configFor(morphium, impl);

                try (Morphium m = new Morphium(cfg)) {
                    m.dropCollection(Msg.class);
                    MorphiumMessaging sender = m.createMessaging();
                    MorphiumMessaging receiverA = m.createMessaging();
                    MorphiumMessaging receiverB = m.createMessaging();
                    List<String> receivedA = new CopyOnWriteArrayList<>();
                    List<String> receivedB = new CopyOnWriteArrayList<>();
                    List<String> receivedBroadcastByA = new CopyOnWriteArrayList<>();

                    try {
                        sender.start();
                        assertTrue(sender.waitForReady(30, TimeUnit.SECONDS));
                        receiverA.start();
                        assertTrue(receiverA.waitForReady(30, TimeUnit.SECONDS));
                        receiverB.start();
                        assertTrue(receiverB.waitForReady(30, TimeUnit.SECONDS));

                        receiverA.addListenerForTopic("bulk_directed", (MessageListener<Msg>) (mm, msg) -> {
                            receivedA.add(msg.getMsg());
                            return null;
                        });
                        receiverB.addListenerForTopic("bulk_directed", (MessageListener<Msg>) (mm, msg) -> {
                            receivedB.add(msg.getMsg());
                            return null;
                        });
                        receiverA.addListenerForTopic("bulk_directed_broadcast", (MessageListener<Msg>) (mm, msg) -> {
                            receivedBroadcastByA.add(msg.getMsg());
                            return null;
                        });

                        // one bulk call, mixing: 2 directed-to-A, 2 directed-to-B, 1 broadcast -
                        // exercises the per-target-collection grouping in one shot
                        List<Msg> batch = new ArrayList<>();
                        Msg toA1 = new Msg("bulk_directed", "toA1", "v", 30000);
                        toA1.setExclusive(false);
                        toA1.setRecipient(receiverA.getSenderId());
                        batch.add(toA1);
                        Msg toA2 = new Msg("bulk_directed", "toA2", "v", 30000);
                        toA2.setExclusive(false);
                        toA2.setRecipient(receiverA.getSenderId());
                        batch.add(toA2);
                        Msg toB1 = new Msg("bulk_directed", "toB1", "v", 30000);
                        toB1.setExclusive(false);
                        toB1.setRecipient(receiverB.getSenderId());
                        batch.add(toB1);
                        Msg toB2 = new Msg("bulk_directed", "toB2", "v", 30000);
                        toB2.setExclusive(false);
                        toB2.setRecipient(receiverB.getSenderId());
                        batch.add(toB2);
                        Msg broadcast = new Msg("bulk_directed_broadcast", "bcast", "v", 30000);
                        batch.add(broadcast);

                        sender.sendMessages(batch);

                        TestUtils.waitForConditionToBecomeTrue(15000,
                                impl + ": directed/broadcast bulk messages did not all arrive",
                                () -> receivedA.size() >= 2 && receivedB.size() >= 2 && receivedBroadcastByA.size() >= 1);

                        assertEquals(2, receivedA.size(), impl + ": receiver A got wrong message count");
                        assertTrue(receivedA.contains("toA1") && receivedA.contains("toA2"), impl + ": receiver A missing its messages");
                        assertFalse(receivedA.contains("toB1") || receivedA.contains("toB2"), impl + ": receiver A got B's messages");

                        assertEquals(2, receivedB.size(), impl + ": receiver B got wrong message count");
                        assertTrue(receivedB.contains("toB1") && receivedB.contains("toB2"), impl + ": receiver B missing its messages");
                        assertFalse(receivedB.contains("toA1") || receivedB.contains("toA2"), impl + ": receiver B got A's messages");

                        assertEquals(1, receivedBroadcastByA.size(), impl + ": broadcast not delivered exactly once");
                    } finally {
                        sender.terminate();
                        receiverA.terminate();
                        receiverB.terminate();
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void sendAnswersDeliversAllAsAnswersToOriginalTest(Morphium morphium) throws Exception {
        try (morphium) {
            for (String impl : MultiDriverTestBase.messagingsToTest) {
                MorphiumConfig cfg = configFor(morphium, impl);

                try (Morphium m = new Morphium(cfg)) {
                    m.dropCollection(Msg.class);
                    MorphiumMessaging requester = m.createMessaging();
                    MorphiumMessaging responder = m.createMessaging();
                    List<Msg> receivedAnswers = new CopyOnWriteArrayList<>();
                    AtomicInteger requestsSeen = new AtomicInteger(0);

                    try {
                        requester.start();
                        assertTrue(requester.waitForReady(30, TimeUnit.SECONDS));
                        responder.start();
                        assertTrue(responder.waitForReady(30, TimeUnit.SECONDS));

                        // requester listens for answers targeted at it (recipient == requester)
                        requester.addListenerForTopic("bulk_answers", (MessageListener<Msg>) (mm, msg) -> {
                            if (msg.isAnswer()) {
                                receivedAnswers.add(msg);
                            }
                            return null;
                        });

                        responder.addListenerForTopic("bulk_answers", (MessageListener<Msg>) (mm, req) -> {
                            if (req.isAnswer()) {
                                return null; // ignore the answers we sent, in case of loopback
                            }
                            requestsSeen.incrementAndGet();

                            // simulate a chunked/streamed response: many answers to one request,
                            // sent from a single thread in one bulk call
                            List<Msg> answers = new ArrayList<>();
                            for (int i = 0; i < 10; i++) {
                                answers.add(new Msg("bulk_answers", "chunk" + i, "v", 30000));
                            }
                            responder.sendAnswers(req, answers);
                            return null;
                        });

                        Msg request = new Msg("bulk_answers", "give me chunks", "v", 30000);
                        request.setExclusive(false);
                        requester.sendMessage(request);

                        TestUtils.waitForConditionToBecomeTrue(15000,
                                impl + ": not all bulk-sent answers arrived",
                                () -> receivedAnswers.size() >= 10);

                        assertEquals(10, receivedAnswers.size(), impl + ": unexpected answer count");
                        for (Msg answer : receivedAnswers) {
                            assertEquals(request.getMsgId(), answer.getInAnswerTo(),
                                    impl + ": answer not linked to the original request");
                        }

                        List<String> chunkValues = new ArrayList<>();
                        for (Msg answer : receivedAnswers) {
                            chunkValues.add(answer.getMsg());
                        }
                        for (int i = 0; i < 10; i++) {
                            assertTrue(chunkValues.contains("chunk" + i), impl + ": missing chunk" + i);
                        }
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
    public void sendMessagesEmptyOrNullListIsNoopTest(Morphium morphium) throws Exception {
        try (morphium) {
            m_dropAndRun(morphium, "StandardMessaging", messaging -> {
                assertDoesNotThrow(() -> messaging.sendMessages(null));
                assertDoesNotThrow(() -> messaging.sendMessages(List.of()));
            });
        }
    }

    private void m_dropAndRun(Morphium morphium, String impl, java.util.function.Consumer<MorphiumMessaging> body) throws Exception {
        MorphiumConfig cfg = configFor(morphium, impl);

        try (Morphium m = new Morphium(cfg)) {
            m.dropCollection(Msg.class);
            MorphiumMessaging messaging = m.createMessaging();

            try {
                messaging.start();
                assertTrue(messaging.waitForReady(30, TimeUnit.SECONDS));
                body.accept(messaging);
            } finally {
                messaging.terminate();
            }
        }
    }
}
