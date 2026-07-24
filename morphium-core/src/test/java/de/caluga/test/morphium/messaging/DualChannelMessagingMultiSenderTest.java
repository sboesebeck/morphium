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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Multi-instance cross-talk coverage for {@link DualChannelMessaging} (#265, beta): several
 * instances issuing concurrent, criss-crossing request/reply calls must not see each other's
 * answers (each instance's DM collection/cursor is strictly its own), and the number of DM
 * collections/cursors in play matches the number of participating instances.
 */
@Tag("messaging")
public class DualChannelMessagingMultiSenderTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void crossWiseRequestReplyNoAnswerCrosstalkTest(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation("DualChannelMessaging");
            cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

            try (Morphium m = new Morphium(cfg)) {
                m.dropCollection(Msg.class);
                int n = 4;
                List<DualChannelMessaging> instances = new ArrayList<>();

                try {
                    for (int i = 0; i < n; i++) {
                        DualChannelMessaging inst = (DualChannelMessaging) m.createMessaging();
                        inst.setSenderId("node" + i);
                        instances.add(inst);
                    }

                    for (DualChannelMessaging inst : instances) {
                        inst.start();
                        assertTrue(inst.waitForReady(30, TimeUnit.SECONDS));
                    }

                    // Every node answers pings addressed to it with its own id embedded in the value,
                    // so a requester can verify the answer actually came from - and only from - the
                    // node it addressed.
                    for (DualChannelMessaging inst : instances) {
                        String myId = inst.getSenderId();
                        inst.addListenerForTopic("ping", (mm, msg) -> new Msg("ping", "pong", myId));
                    }

                    // Distinct DM collections/cursors: one per instance.
                    long distinctDmCollections = instances.stream()
                            .map(DualChannelMessaging::getDMCollectionName)
                            .distinct()
                            .count();
                    assertEquals(n, distinctDmCollections, "expected one DM collection per instance");

                    // Fire n*(n-1) concurrent cross-requests (every node -> every other node) and
                    // verify each requester only ever gets the answer from the node it addressed.
                    Map<String, Boolean> crosstalkDetected = new ConcurrentHashMap<>();
                    AtomicInteger completed = new AtomicInteger(0);
                    List<Thread> workers = new ArrayList<>();

                    for (int i = 0; i < n; i++) {
                        final DualChannelMessaging requester = instances.get(i);

                        for (int j = 0; j < n; j++) {
                            if (i == j) continue;
                            final DualChannelMessaging responder = instances.get(j);
                            final String expectedResponderId = responder.getSenderId();

                            Thread t = Thread.ofPlatform().start(() -> {
                                try {
                                    Msg req = new Msg("ping", "ping", "");
                                    req.setRecipient(expectedResponderId);
                                    Msg answer = requester.sendAndAwaitFirstAnswer(req, 15000);

                                    if (answer == null || !expectedResponderId.equals(answer.getValue())) {
                                        crosstalkDetected.put(requester.getSenderId() + "->" + expectedResponderId, true);
                                    }
                                } catch (Exception e) {
                                    crosstalkDetected.put(requester.getSenderId() + "->" + expectedResponderId, true);
                                } finally {
                                    completed.incrementAndGet();
                                }
                            });
                            workers.add(t);
                        }
                    }

                    for (Thread t : workers) {
                        t.join(30000);
                    }

                    assertEquals(n * (n - 1), completed.get(), "not all cross-requests completed");
                    assertTrue(crosstalkDetected.isEmpty(),
                            "answer crosstalk or missing/incorrect answers detected: " + crosstalkDetected.keySet());
                } finally {
                    for (MorphiumMessaging inst : instances) {
                        inst.terminate();
                    }
                }
            }
        }
    }
}
