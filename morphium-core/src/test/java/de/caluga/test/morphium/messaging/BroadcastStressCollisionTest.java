package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression backstop for #405: a broadcast was lost when the topic change-stream monitor and the
 * interval fallback poll raced over {@code locallyProcessedBroadcastIds} vs {@code processingMessages}
 * - one path claimed the message, the other left a broadcast marker that made the claim holder drop
 * the message, so no listener ever ran.
 *
 * <p>A deterministic reproduction would need an injection point between claim and marker, which does
 * not exist, so this is deliberately a statistical stress test: many request/reply broadcast round
 * trips with the topic monitor and the poll both live (the exact topology of the flake, which also
 * applies with {@code setUseChangeStream(false)} - {@code addListenerForTopic} always starts the
 * topic monitor). It cannot prove correct ordering on a single run, but over {@link #BROADCASTS}
 * round trips any ordering regression that drops a broadcast shows up as a null answer, and it stays
 * green when the order is correct. It is the honest guard that the claim-first order does not slip
 * back.
 */
@Tag("messaging")
public class BroadcastStressCollisionTest extends MultiDriverTestBase {

    private static final int BROADCASTS = 300;
    // A lost message bears no timeout like this; a merely slow one (CI-parallel scheduling) still
    // has ample room, so the assertion only trips on a broadcast that was actually dropped.
    private static final long ANSWER_TIMEOUT_MS = 30_000;

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void noBroadcastLostUnderMonitorPollCollision(Morphium morphium) throws Exception {
        try (morphium) {
            for (String msgImpl : MultiDriverTestBase.messagingsToTest) {
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.messagingSettings().setMessagingPollPause(50); // fast polls -> wider race window

                Morphium morph = new Morphium(cfg);
                morph.dropCollection(Msg.class);
                MorphiumMessaging sender = null;
                MorphiumMessaging receiver = null;
                try {
                    sender = morph.createMessaging();
                    sender.setSenderId("sender");
                    sender.setUseChangeStream(false);
                    sender.start();

                    receiver = morph.createMessaging();
                    receiver.setSenderId("receiver");
                    // Register the listener before start() so the topic watch is active from the start.
                    receiver.addListenerForTopic("stress", (msg, m) -> m.createAnswerMsg());
                    receiver.setUseChangeStream(false);
                    receiver.start();

                    assertTrue(sender.waitForReady(60, TimeUnit.SECONDS), "sender not ready (" + msgImpl + ")");
                    assertTrue(receiver.waitForReady(60, TimeUnit.SECONDS), "receiver not ready (" + msgImpl + ")");

                    for (int i = 0; i < BROADCASTS; i++) {
                        Msg q = new Msg("stress", "q" + i, "v" + i);
                        q.setPriority(5);
                        Msg answer = sender.sendAndAwaitFirstAnswer(q, ANSWER_TIMEOUT_MS, false);
                        assertNotNull(answer,
                            "lost broadcast answer for " + q.getMsgId() + " (" + msgImpl + ") - "
                                + "the monitor/poll collision dropped the message before any listener ran");
                        assertEquals(q.getMsgId(), answer.getInAnswerTo(),
                            "wrong inAnswerTo (" + msgImpl + ")");
                    }
                } finally {
                    // Terminate both messaging instances BEFORE closing the driver: their
                    // decouple_thr- poll loops keep running against a closed driver otherwise
                    // (suite convention, cf. SendMessagesBulkTest). Null-guard in case the
                    // construction above failed partway.
                    if (sender != null) {
                        sender.terminate();
                    }
                    if (receiver != null) {
                        receiver.terminate();
                    }
                    morph.close();
                }
            }
        }
    }
}