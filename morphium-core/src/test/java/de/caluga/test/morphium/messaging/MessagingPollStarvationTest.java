package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
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
 * Regression tests for the poll-window starvation bug: {@code getMessagesForProcessing()} fetches
 * candidates sorted by (priority, timestamp) with limit(windowSize). A message for a topic this
 * instance has no listener for is skipped during processing WITHOUT a processed_by mark
 * (deliberately - a listener registered later must still receive it). Pre-fix, such messages
 * re-entered every poll and - being older than any new arrival - permanently occupied window
 * slots: enough of them starved deliverable messages until TTL expiry.
 * <p>
 * The fix filters the poll query itself by the currently registered topics (answers pass
 * regardless), so unlistened messages never enter the window but stay pending in the collection.
 * The "listener registered later" tests pin the crucial safety property: skipped messages must
 * NOT be marked processed - they must still be delivered once a listener shows up
 * (addListenerForTopic bumps the poll trigger).
 * <p>
 * Change streams are disabled to force the poll path - with a change stream, its server-side
 * relevance filter (buildMainCsPipeline) masks the poll behavior for broadcasts.
 */
@Tag("messaging")
public class MessagingPollStarvationTest extends MultiDriverTestBase {

    private static final int WINDOW_SIZE = 5;

    private MorphiumConfig configFor(Morphium base, String impl) {
        MorphiumConfig cfg = base.getConfig().createCopy();
        cfg.messagingSettings().setMessagingImplementation(impl);
        cfg.messagingSettings().setUseChangeStream(false);
        cfg.messagingSettings().setMessagingPollPause(200);
        cfg.messagingSettings().setMessagingWindowSize(WINDOW_SIZE);
        cfg.encryptionSettings().setCredentialsEncrypted(base.getConfig().encryptionSettings().getCredentialsEncrypted());
        cfg.encryptionSettings().setCredentialsDecryptionKey(base.getConfig().encryptionSettings().getCredentialsDecryptionKey());
        cfg.encryptionSettings().setCredentialsEncryptionKey(base.getConfig().encryptionSettings().getCredentialsEncryptionKey());
        return cfg;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void unlistenedTopicMessagesDoNotStarveThePollWindowTest(Morphium morphium) throws Exception {
        try (morphium) {
            for (String impl : new String[] {"SingleCollectionMessaging", "DualChannelMessaging"}) {
                MorphiumConfig cfg = configFor(morphium, impl);

                try (Morphium m = new Morphium(cfg)) {
                    m.dropCollection(Msg.class);
                    MorphiumMessaging sender = m.createMessaging();
                    MorphiumMessaging receiver = m.createMessaging();
                    AtomicInteger received = new AtomicInteger(0);

                    try {
                        sender.start();
                        assertTrue(sender.waitForReady(30, TimeUnit.SECONDS));
                        receiver.start();
                        assertTrue(receiver.waitForReady(30, TimeUnit.SECONDS));

                        receiver.addListenerForTopic("wanted", (mm, msg) -> {
                            received.incrementAndGet();
                            return null;
                        });

                        // 3x windowSize broadcasts for a topic NOBODY listens to. Long TTL so the
                        // starvation cannot resolve itself via expiry within the assertion window.
                        for (int i = 0; i < 3 * WINDOW_SIZE; i++) {
                            Msg orphan = new Msg("orphan_topic", "m" + i, "v", 120000);
                            orphan.setExclusive(false);
                            sender.sendMessage(orphan);
                        }

                        // make sure the deliverable message sorts strictly AFTER the orphans
                        Thread.sleep(300);
                        Msg wanted = new Msg("wanted", "hello", "v", 120000);
                        wanted.setExclusive(false);
                        sender.sendMessage(wanted);

                        TestUtils.waitForConditionToBecomeTrue(15000,
                                impl + ": message for a listened topic was starved out of the poll "
                                + "window by older messages for topics without a listener",
                                () -> received.get() > 0);
                    } finally {
                        sender.terminate();
                        receiver.terminate();
                    }
                }
            }
        }
    }

    /**
     * Safety guard against the "obvious" wrong fix (marking skipped messages as processed): a
     * broadcast that arrives BEFORE any listener for its topic exists must still be delivered
     * once a listener is registered later.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void lateRegisteredListenerStillReceivesEarlierBroadcastTest(Morphium morphium) throws Exception {
        try (morphium) {
            for (String impl : new String[] {"SingleCollectionMessaging", "DualChannelMessaging"}) {
                MorphiumConfig cfg = configFor(morphium, impl);

                try (Morphium m = new Morphium(cfg)) {
                    m.dropCollection(Msg.class);
                    MorphiumMessaging sender = m.createMessaging();
                    MorphiumMessaging receiver = m.createMessaging();
                    AtomicInteger lateReceived = new AtomicInteger(0);

                    try {
                        sender.start();
                        assertTrue(sender.waitForReady(30, TimeUnit.SECONDS));
                        receiver.start();
                        assertTrue(receiver.waitForReady(30, TimeUnit.SECONDS));

                        // some unrelated listener, so the poll runs its regular (non-empty) branch
                        receiver.addListenerForTopic("other_topic", (mm, msg) -> null);

                        Msg early = new Msg("late_topic", "early", "v", 120000);
                        early.setExclusive(false);
                        sender.sendMessage(early);

                        // several poll ticks: without a listener the message must NOT be delivered...
                        Thread.sleep(1500);
                        assertEquals(0, lateReceived.get(),
                                impl + ": message delivered although no listener was registered for its topic");

                        // ...but it must not be lost either: registering the listener afterwards
                        // must deliver the earlier message
                        receiver.addListenerForTopic("late_topic", (mm, msg) -> {
                            lateReceived.incrementAndGet();
                            return null;
                        });

                        TestUtils.waitForConditionToBecomeTrue(15000,
                                impl + ": message that arrived before its listener was registered got lost",
                                () -> lateReceived.get() > 0);
                    } finally {
                        sender.terminate();
                        receiver.terminate();
                    }
                }
            }
        }
    }

    /**
     * Same "listener comes later" guarantee for DIRECTED messages (DualChannelMessaging routes
     * these through its DM lane, which gets the equivalent poll filter).
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void lateRegisteredListenerStillReceivesEarlierDirectedMessageTest(Morphium morphium) throws Exception {
        try (morphium) {
            for (String impl : new String[] {"SingleCollectionMessaging", "DualChannelMessaging"}) {
                MorphiumConfig cfg = configFor(morphium, impl);

                try (Morphium m = new Morphium(cfg)) {
                    m.dropCollection(Msg.class);
                    MorphiumMessaging sender = m.createMessaging();
                    MorphiumMessaging receiver = m.createMessaging();
                    AtomicInteger lateReceived = new AtomicInteger(0);

                    try {
                        sender.start();
                        assertTrue(sender.waitForReady(30, TimeUnit.SECONDS));
                        receiver.start();
                        assertTrue(receiver.waitForReady(30, TimeUnit.SECONDS));

                        receiver.addListenerForTopic("other_topic", (mm, msg) -> null);

                        Msg direct = new Msg("late_direct", "early", "v", 120000);
                        direct.setExclusive(false);
                        direct.setRecipient(receiver.getSenderId());
                        sender.sendMessage(direct);

                        Thread.sleep(1500);
                        assertEquals(0, lateReceived.get(),
                                impl + ": directed message delivered although no listener was registered for its topic");

                        receiver.addListenerForTopic("late_direct", (mm, msg) -> {
                            lateReceived.incrementAndGet();
                            return null;
                        });

                        TestUtils.waitForConditionToBecomeTrue(15000,
                                impl + ": directed message that arrived before its listener was registered got lost",
                                () -> lateReceived.get() > 0);
                    } finally {
                        sender.terminate();
                        receiver.terminate();
                    }
                }
            }
        }
    }
}
