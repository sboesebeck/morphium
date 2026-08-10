package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.DualChannelMessaging;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic functional coverage for {@link DualChannelMessaging} (#265, beta): broadcast still works
 * exactly like {@code StandardMessaging}, directed messages physically land in the recipient's
 * per-recipient DM collection (not the shared main collection), and all the sendAndAwait*
 * variants work over the DM lane.
 */
@Tag("messaging")
public class DualChannelMessagingBasicTest extends MultiDriverTestBase {

    private MorphiumConfig configFor(Morphium base, String queueName) {
        MorphiumConfig cfg = base.getConfig().createCopy();
        cfg.messagingSettings().setMessagingImplementation("DualChannelMessaging");
        if (queueName != null) {
            cfg.messagingSettings().setMessageQueueName(queueName);
        }
        cfg.encryptionSettings().setCredentialsEncrypted(base.getConfig().encryptionSettings().getCredentialsEncrypted());
        cfg.encryptionSettings().setCredentialsDecryptionKey(base.getConfig().encryptionSettings().getCredentialsDecryptionKey());
        cfg.encryptionSettings().setCredentialsEncryptionKey(base.getConfig().encryptionSettings().getCredentialsEncryptionKey());
        return cfg;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void broadcastStillWorksTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium, null);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                MorphiumMessaging sender = m.createMessaging();
                MorphiumMessaging r1 = m.createMessaging();
                MorphiumMessaging r2 = m.createMessaging();
                AtomicInteger got1 = new AtomicInteger(0);
                AtomicInteger got2 = new AtomicInteger(0);
                AtomicInteger gotSender = new AtomicInteger(0);

                try {
                    sender.start();
                    assertTrue(sender.waitForReady(30, TimeUnit.SECONDS));
                    r1.start();
                    assertTrue(r1.waitForReady(30, TimeUnit.SECONDS));
                    r2.start();
                    assertTrue(r2.waitForReady(30, TimeUnit.SECONDS));

                    r1.addListenerForTopic("bcast", (mm, msg) -> {
                        got1.incrementAndGet();
                        return null;
                    });
                    r2.addListenerForTopic("bcast", (mm, msg) -> {
                        got2.incrementAndGet();
                        return null;
                    });
                    sender.addListenerForTopic("bcast", (mm, msg) -> {
                        gotSender.incrementAndGet();
                        return null;
                    });

                    Msg msg = new Msg("bcast", "hello", "world");
                    msg.setExclusive(false);
                    sender.sendMessage(msg);

                    TestUtils.waitForConditionToBecomeTrue(10000, "broadcast not delivered to both receivers",
                            () -> got1.get() > 0 && got2.get() > 0);

                    Thread.sleep(500);
                    assertEquals(0, gotSender.get(), "sender must not receive its own broadcast");
                    assertEquals(1, got1.get());
                    assertEquals(1, got2.get());
                } finally {
                    sender.terminate();
                    r1.terminate();
                    r2.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void directedMessageLandsInDmCollectionTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium, "dcbasic");

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                DualChannelMessaging sender = (DualChannelMessaging) m.createMessaging();
                DualChannelMessaging receiver = (DualChannelMessaging) m.createMessaging();
                AtomicInteger received = new AtomicInteger(0);

                try {
                    sender.start();
                    assertTrue(sender.waitForReady(30, TimeUnit.SECONDS));
                    receiver.start();
                    assertTrue(receiver.waitForReady(30, TimeUnit.SECONDS));

                    receiver.addListenerForTopic("direct", (mm, msg) -> {
                        received.incrementAndGet();
                        return null;
                    });

                    Msg msg = new Msg("direct", "hi", "there");
                    msg.setExclusive(false);
                    msg.setRecipient(receiver.getSenderId());
                    sender.sendMessage(msg);

                    TestUtils.waitForConditionToBecomeTrue(10000, "directed message not delivered",
                            () -> received.get() > 0);

                    // Physically verify: the message must never have been visible in the shared
                    // main collection (only in the recipient's own DM collection).
                    String mainColl = sender.getCollectionName();
                    String dmColl = sender.getDMCollectionName(receiver.getSenderId());
                    assertNotEquals(mainColl, dmColl);

                    long inMain = m.createQueryFor(Msg.class, mainColl).f(Msg.Fields.topic).eq("direct").countAll();
                    assertEquals(0, inMain, "directed message must not be stored in the main collection");
                } finally {
                    sender.terminate();
                    receiver.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void sendAndAwaitFirstAnswerTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium, null);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                MorphiumMessaging requester = m.createMessaging();
                MorphiumMessaging responder = m.createMessaging();

                try {
                    requester.start();
                    assertTrue(requester.waitForReady(30, TimeUnit.SECONDS));
                    responder.start();
                    assertTrue(responder.waitForReady(30, TimeUnit.SECONDS));

                    responder.addListenerForTopic("ping", (mm, msg) -> new Msg(msg.getTopic(), "pong", "value"));

                    Msg req = new Msg("ping", "ping", "");
                    Msg answer = requester.sendAndAwaitFirstAnswer(req, 10000);
                    assertNotNull(answer, "no answer received via DM lane");
                    assertEquals("pong", answer.getMsg());
                } finally {
                    requester.terminate();
                    responder.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void sendAndAwaitAnswersMultipleRespondersTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium, null);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                MorphiumMessaging requester = m.createMessaging();
                MorphiumMessaging r1 = m.createMessaging();
                MorphiumMessaging r2 = m.createMessaging();
                MorphiumMessaging r3 = m.createMessaging();

                try {
                    requester.start();
                    assertTrue(requester.waitForReady(30, TimeUnit.SECONDS));
                    r1.start();
                    assertTrue(r1.waitForReady(30, TimeUnit.SECONDS));
                    r2.start();
                    assertTrue(r2.waitForReady(30, TimeUnit.SECONDS));
                    r3.start();
                    assertTrue(r3.waitForReady(30, TimeUnit.SECONDS));

                    r1.addListenerForTopic("survey", (mm, msg) -> new Msg(msg.getTopic(), "answer", "r1"));
                    r2.addListenerForTopic("survey", (mm, msg) -> new Msg(msg.getTopic(), "answer", "r2"));
                    r3.addListenerForTopic("survey", (mm, msg) -> new Msg(msg.getTopic(), "answer", "r3"));

                    Msg req = new Msg("survey", "survey", "");
                    req.setExclusive(false);
                    List<Msg> answers = requester.sendAndAwaitAnswers(req, 3, 10000);
                    assertEquals(3, answers.size(), "expected answers from all 3 responders");
                } finally {
                    requester.terminate();
                    r1.terminate();
                    r2.terminate();
                    r3.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void sendAndAwaitAsyncCallbackTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium, null);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                MorphiumMessaging requester = m.createMessaging();
                MorphiumMessaging responder = m.createMessaging();
                AtomicInteger callbackCount = new AtomicInteger(0);

                try {
                    requester.start();
                    assertTrue(requester.waitForReady(30, TimeUnit.SECONDS));
                    responder.start();
                    assertTrue(responder.waitForReady(30, TimeUnit.SECONDS));

                    responder.addListenerForTopic("asyncping", (mm, msg) -> new Msg(msg.getTopic(), "pong-async", ""));

                    Msg req = new Msg("asyncping", "ping", "");
                    requester.sendAndAwaitAsync(req, 10000, incoming -> callbackCount.incrementAndGet());

                    TestUtils.waitForConditionToBecomeTrue(10000, "async callback never fired",
                            () -> callbackCount.get() > 0);
                    assertEquals(1, callbackCount.get());
                } finally {
                    requester.terminate();
                    responder.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void sendMessageToSelfTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium, null);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                MorphiumMessaging me = m.createMessaging();
                AtomicInteger got = new AtomicInteger(0);

                try {
                    me.start();
                    assertTrue(me.waitForReady(30, TimeUnit.SECONDS));
                    me.addListenerForTopic("self", (mm, msg) -> {
                        got.incrementAndGet();
                        return null;
                    });

                    Msg msg = new Msg("self", "hi", "myself");
                    me.sendMessageToSelf(msg);

                    TestUtils.waitForConditionToBecomeTrue(10000, "self message never delivered",
                            () -> got.get() > 0);
                    assertEquals(1, got.get());
                } finally {
                    me.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void dmNamingIncludesQueuePrefixTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg1 = configFor(morphium, "queueA");
            MorphiumConfig cfg2 = configFor(morphium, "queueB");

            try (Morphium m1 = new Morphium(cfg1); Morphium m2 = new Morphium(cfg2)) {
                m1.dropCollection(Msg.class);
                m2.dropCollection(Msg.class);
                DualChannelMessaging a = (DualChannelMessaging) m1.createMessaging();
                DualChannelMessaging b = (DualChannelMessaging) m2.createMessaging();
                a.setSenderId("sameSenderId");
                b.setSenderId("sameSenderId");

                try {
                    a.start();
                    assertTrue(a.waitForReady(30, TimeUnit.SECONDS));
                    b.start();
                    assertTrue(b.waitForReady(30, TimeUnit.SECONDS));

                    String dmA = a.getDMCollectionName();
                    String dmB = b.getDMCollectionName();
                    assertNotEquals(dmA, dmB, "same sender id on different queues must not share a DM collection");
                    assertTrue(dmA.startsWith(a.getCollectionName() + "_dm_"));
                    assertTrue(dmB.startsWith(b.getCollectionName() + "_dm_"));
                } finally {
                    a.terminate();
                    b.terminate();
                }
            }
        }
    }
}
