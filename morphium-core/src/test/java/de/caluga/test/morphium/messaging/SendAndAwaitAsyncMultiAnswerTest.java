package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.OutputHelper;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * sendAndAwaitAsync must deliver ALL answers to a non-exclusive (broadcast) request via the
 * callback — one per responder — and only stop after the first answer for exclusive requests.
 * Guards the contract stated in the sendAndAwaitAsync javadoc for every messaging implementation
 * (MultiCollectionMessaging used to deregister the callback after the first answer).
 */
@Tag("messaging")
public class SendAndAwaitAsyncMultiAnswerTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void broadcastDeliversAllAnswersToCallback(Morphium morphium) throws Exception {
        log.info("Running with driver " + morphium.getDriver().getName());

        try (morphium) {
            for (String msgImpl : MultiDriverTestBase.messagingsToTest) {
                OutputHelper.figletOutput(log, msgImpl);
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
                Morphium morph = new Morphium(cfg);

                MorphiumMessaging sender = morph.createMessaging();
                MorphiumMessaging responder1 = morph.createMessaging();
                MorphiumMessaging responder2 = morph.createMessaging();
                sender.setSenderId("sender");
                responder1.setSenderId("responder1");
                responder2.setSenderId("responder2");

                try {
                    // responder2 answers noticeably later: a callback that (wrongly) deregisters
                    // after the first answer then reliably misses the second one, instead of both
                    // answers racing past the deregistration in the same instant.
                    responder1.addListenerForTopic("multi_answer", (msg, m) -> m.createAnswerMsg());
                    responder2.addListenerForTopic("multi_answer", (msg, m) -> {
                        try {
                            Thread.sleep(800);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return m.createAnswerMsg();
                    });

                    sender.start();
                    responder1.start();
                    responder2.start();
                    Thread.sleep(2000);

                    // broadcast: both responders answer, both answers must reach the callback
                    Set<String> answeredBy = ConcurrentHashMap.newKeySet();
                    Msg broadcast = new Msg("multi_answer", "q", "v", 10000);
                    broadcast.setExclusive(false);
                    sender.sendAndAwaitAsync(broadcast, 10000, a -> answeredBy.add(a.getSender()));
                    try {
                        TestUtils.waitForConditionToBecomeTrue(8000,
                                "did not receive answers from both responders",
                                () -> answeredBy.size() == 2);
                    } catch (AssertionError e) {
                        throw new AssertionError(e.getMessage() + " - got: " + answeredBy, e);
                    }

                    // exclusive: only one responder processes the request → exactly one answer
                    Set<String> answeredExclusive = ConcurrentHashMap.newKeySet();
                    Msg exclusive = new Msg("multi_answer", "q", "v", 10000);
                    exclusive.setExclusive(true);
                    sender.sendAndAwaitAsync(exclusive, 10000, a -> answeredExclusive.add(a.getSender()));
                    TestUtils.waitForConditionToBecomeTrue(8000,
                            "exclusive request got no answer",
                            () -> answeredExclusive.size() >= 1);
                    Thread.sleep(1500);   // grace period: a second answer would arrive in here
                    assertEquals(1, answeredExclusive.size(),
                            "exclusive request must yield exactly one answer, got: " + answeredExclusive);
                } finally {
                    sender.terminate();
                    responder1.terminate();
                    responder2.terminate();
                    morph.close();
                }
            }
        }
    }
}
