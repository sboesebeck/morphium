package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.DualChannelMessaging;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.SingleCollectionMessaging;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Cements the deliberate mixed-cluster decision for {@link DualChannelMessaging} (#265, beta),
 * documented on the class javadoc: a legacy {@code StandardMessaging} responder can still answer
 * a {@code DualChannelMessaging} requester (the forked main lane keeps SCM's answer-handling path
 * as a compatibility side effect), but NOT the other way around - a {@code StandardMessaging}
 * requester awaiting an answer from a {@code DualChannelMessaging} responder times out, because
 * the answer is routed into the requester's (never-read-by-legacy-code) DM collection.
 */
@Tag("messaging")
public class DualChannelMessagingCompatTest extends MultiDriverTestBase {

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
    public void standardResponderAnswersDualChannelRequesterTest(Morphium morphium) throws Exception {
        try (morphium) {
            // Both sides must share one collection layout to talk to each other at all: the
            // requester is DualChannelMessaging but the REQUEST itself (no recipients set, a
            // plain topic call) still goes through the unmodified main lane/collection, which the
            // legacy StandardMessaging responder shares.
            MorphiumConfig requesterCfg = configFor(morphium, "DualChannelMessaging");

            try (Morphium m = new Morphium(requesterCfg)) {
                m.dropCollection(Msg.class);
                MorphiumMessaging requester = m.createMessaging();

                MorphiumConfig responderCfg = configFor(morphium, "StandardMessaging");
                responderCfg.connectionSettings().setDatabase(requesterCfg.connectionSettings().getDatabase());

                try (Morphium m2 = new Morphium(responderCfg)) {
                    MorphiumMessaging responder = m2.createMessaging();

                    try {
                        requester.start();
                        assertTrue(requester.waitForReady(30, TimeUnit.SECONDS));
                        responder.start();
                        assertTrue(responder.waitForReady(30, TimeUnit.SECONDS));

                        responder.addListenerForTopic("compat", (mm, msg) -> new Msg("compat", "pong", "from-legacy"));

                        Msg req = new Msg("compat", "ping", "");
                        Msg answer = requester.sendAndAwaitFirstAnswer(req, 15000, false);
                        assertNotNull(answer, "DualChannelMessaging requester should receive the legacy StandardMessaging responder's answer");
                        assertEquals("from-legacy", answer.getValue());
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
    public void standardRequesterTimesOutAgainstDualChannelResponderTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig requesterCfg = configFor(morphium, "StandardMessaging");

            try (Morphium m = new Morphium(requesterCfg)) {
                m.dropCollection(Msg.class);
                MorphiumMessaging requester = m.createMessaging();

                MorphiumConfig responderCfg = configFor(morphium, "DualChannelMessaging");
                responderCfg.connectionSettings().setDatabase(requesterCfg.connectionSettings().getDatabase());

                try (Morphium m2 = new Morphium(responderCfg)) {
                    DualChannelMessaging responder = (DualChannelMessaging) m2.createMessaging();

                    try {
                        requester.start();
                        assertTrue(requester.waitForReady(30, TimeUnit.SECONDS));
                        responder.start();
                        assertTrue(responder.waitForReady(30, TimeUnit.SECONDS));

                        responder.addListenerForTopic("compat2", (mm, msg) -> new Msg("compat2", "pong", "from-dcm"));

                        Msg req = new Msg("compat2", "ping", "");
                        // throwExceptionOnTimeout=false: we WANT the timeout to happen quietly and
                        // assert on the null return, not treat it as a test failure.
                        Msg answer = requester.sendAndAwaitFirstAnswer(req, 8000, false);
                        assertNull(answer, "legacy StandardMessaging requester must NOT receive an answer "
                                + "from a DualChannelMessaging responder (answer is routed to the requester's "
                                + "DM collection, which legacy code never reads) - this is the documented, "
                                + "deliberate mixed-cluster limitation");

                        // Verify the answer really was stored - just in the requester's DM collection,
                        // not lost - proving this is a routing/visibility gap, not a processing failure.
                        String requesterDmColl = responder.getDMCollectionName(requester.getSenderId());
                        long answersInDmColl = m2.createQueryFor(Msg.class, requesterDmColl)
                                .f(Msg.Fields.inAnswerTo).eq(req.getMsgId()).countAll();
                        assertEquals(1, answersInDmColl,
                                "answer should be sitting in the requester's DM collection, unread by the legacy requester");
                    } finally {
                        requester.terminate();
                        responder.terminate();
                    }
                }
            }
        }
    }
}
