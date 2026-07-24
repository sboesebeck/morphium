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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Polling-only coverage for {@link DualChannelMessaging} (#265, beta) with
 * {@code setUseChangeStream(false)}: broadcast, directed messages and answers must all still be
 * delivered purely via the fallback poll (both lanes), external requeue via a direct
 * {@code processed_by := []} update must trigger redelivery, and messages inserted before an
 * instance starts must be picked up (the change-stream "watch established -> catch-up poll"
 * pattern doesn't apply here, but the periodic/immediate poll must still find them).
 */
@Tag("messaging")
public class DualChannelMessagingFallbackTest extends MultiDriverTestBase {

    private MorphiumConfig configFor(Morphium base) {
        MorphiumConfig cfg = base.getConfig().createCopy();
        cfg.messagingSettings().setMessagingImplementation("DualChannelMessaging");
        cfg.messagingSettings().setUseChangeStream(false);
        cfg.messagingSettings().setMessagingPollPause(200);
        cfg.encryptionSettings().setCredentialsEncrypted(base.getConfig().encryptionSettings().getCredentialsEncrypted());
        cfg.encryptionSettings().setCredentialsDecryptionKey(base.getConfig().encryptionSettings().getCredentialsDecryptionKey());
        cfg.encryptionSettings().setCredentialsEncryptionKey(base.getConfig().encryptionSettings().getCredentialsEncryptionKey());
        return cfg;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void broadcastDmAndAnswerViaPollingOnlyTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                MorphiumMessaging a = m.createMessaging();
                MorphiumMessaging b = m.createMessaging();
                AtomicInteger broadcastReceived = new AtomicInteger(0);
                AtomicInteger directReceived = new AtomicInteger(0);

                try {
                    a.start();
                    assertTrue(a.waitForReady(30, TimeUnit.SECONDS));
                    b.start();
                    assertTrue(b.waitForReady(30, TimeUnit.SECONDS));
                    assertFalse(a.isUseChangeStream());
                    assertFalse(b.isUseChangeStream());

                    b.addListenerForTopic("bcast", (mm, msg) -> {
                        broadcastReceived.incrementAndGet();
                        return null;
                    });
                    b.addListenerForTopic("direct", (mm, msg) -> {
                        directReceived.incrementAndGet();
                        return null;
                    });
                    b.addListenerForTopic("ping", (mm, msg) -> new Msg("ping", "pong", ""));

                    Msg bcast = new Msg("bcast", "x", "");
                    bcast.setExclusive(false);
                    a.sendMessage(bcast);

                    Msg direct = new Msg("direct", "x", "");
                    direct.setRecipient(b.getSenderId());
                    a.sendMessage(direct);

                    TestUtils.waitForConditionToBecomeTrue(15000, "broadcast not delivered via polling",
                            () -> broadcastReceived.get() > 0);
                    TestUtils.waitForConditionToBecomeTrue(15000, "directed message not delivered via polling",
                            () -> directReceived.get() > 0);

                    Msg req = new Msg("ping", "ping", "");
                    Msg answer = a.sendAndAwaitFirstAnswer(req, 15000);
                    assertNotNull(answer, "answer not delivered via polling-only DM lane");
                    assertEquals("pong", answer.getMsg());
                } finally {
                    a.terminate();
                    b.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void requeueViaProcessedByResetTriggersRedeliveryTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                DualChannelMessaging receiver = (DualChannelMessaging) m.createMessaging();
                DualChannelMessaging sender = (DualChannelMessaging) m.createMessaging();
                AtomicInteger deliveries = new AtomicInteger(0);

                try {
                    receiver.start();
                    assertTrue(receiver.waitForReady(30, TimeUnit.SECONDS));
                    sender.start();
                    assertTrue(sender.waitForReady(30, TimeUnit.SECONDS));

                    receiver.addListenerForTopic("requeue", (mm, msg) -> {
                        deliveries.incrementAndGet();
                        return null;
                    });

                    Msg msg = new Msg("requeue", "x", "");
                    msg.setRecipient(receiver.getSenderId());
                    sender.sendMessage(msg);

                    TestUtils.waitForConditionToBecomeTrue(15000, "first delivery never happened",
                            () -> deliveries.get() >= 1);

                    String dmColl = receiver.getDMCollectionName();
                    m.createQueryFor(Msg.class, dmColl).f("_id").eq(msg.getMsgId())
                            .set(Msg.Fields.processedBy, new java.util.ArrayList<String>());

                    TestUtils.waitForConditionToBecomeTrue(15000, "requeue via processed_by reset never redelivered",
                            () -> deliveries.get() >= 2);
                } finally {
                    receiver.terminate();
                    sender.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void dmInsertBeforeStartIsPickedUpAfterStartTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = configFor(morphium);

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                DualChannelMessaging receiver = (DualChannelMessaging) m.createMessaging();
                AtomicInteger deliveries = new AtomicInteger(0);

                // Insert directly into the (not-yet-existing-as-collection, but Mongo/InMem create
                // on write) DM collection before the instance is started - simulates a message
                // that arrived while the recipient was down.
                String dmColl = receiver.getDMCollectionName();
                Msg preExisting = new Msg("catchup", "x", "");
                preExisting.setRecipient(receiver.getSenderId());
                preExisting.setSender("someone-else");
                m.insert(preExisting, dmColl, null);

                try {
                    receiver.addListenerForTopic("catchup", (mm, msg) -> {
                        deliveries.incrementAndGet();
                        return null;
                    });
                    receiver.start();
                    assertTrue(receiver.waitForReady(30, TimeUnit.SECONDS));

                    TestUtils.waitForConditionToBecomeTrue(15000, "pre-existing DM was never picked up after start",
                            () -> deliveries.get() >= 1);
                } finally {
                    receiver.terminate();
                }
            }
        }
    }
}
