package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.MorphiumDriver.DriverStatsKey;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import de.caluga.test.mongo.suite.base.TestUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static de.caluga.test.mongo.suite.base.TestUtils.waitForConditionToBecomeTrue;

@Tag("messaging")
public class AdvancedMessagingTests extends MultiDriverTestBase {
    private final Map<MorphiumId, Integer> counts = new ConcurrentHashMap<>();


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void messageAnswerTest(Morphium baseMorphium) throws Exception {
        try (baseMorphium) {
            for (String msgImpl : MultiDriverTestBase.messagingsToTest) {
                MorphiumConfig cfg = baseMorphium.getConfig().createCopy();
                cfg.driverSettings().setSharedConnectionPool(true);
                cfg.driverSettings().setInMemorySharedDatabases(true);
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(baseMorphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(baseMorphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(baseMorphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
                cfg.encryptionSettings().setCredentialsEncrypted(baseMorphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(baseMorphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(baseMorphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

                try (Morphium morphium = new Morphium(cfg)) {
                    morphium.dropCollection(Msg.class, "msg", null);
                    counts.clear();

                    MorphiumMessaging m1 = morphium.createMessaging();
                    cfg.driverSettings().setInMemorySharedDatabases(true);
                    m1.start();

                    MorphiumConfig cfg2 = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
                    cfg2.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
                    cfg2.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                    cfg2.driverSettings().setInMemorySharedDatabases(true);
                    Morphium morphium2 = new Morphium(cfg2);
                    MorphiumMessaging m2 = morphium2.createMessaging();
                    m2.start();

                    cfg2 = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
                    cfg2.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
                    cfg2.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                    cfg2.driverSettings().setInMemorySharedDatabases(true);
                    Morphium morphium3 = new Morphium(cfg2);
                    MorphiumMessaging m3 = morphium3.createMessaging();
                    m3.start();

                    cfg2 = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
                    cfg2.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
                    cfg2.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                    cfg2.driverSettings().setInMemorySharedDatabases(true);
                    Morphium morphium4 = new Morphium(cfg2);
                    MorphiumMessaging m4 = morphium4.createMessaging();
                    m4.start();

                    // Wait for all messaging instances to be fully ready (change streams initialized)
                    assertTrue(m1.waitForReady(15, TimeUnit.SECONDS), "m1 not ready");
                    assertTrue(m2.waitForReady(15, TimeUnit.SECONDS), "m2 not ready");
                    assertTrue(m3.waitForReady(15, TimeUnit.SECONDS), "m3 not ready");
                    assertTrue(m4.waitForReady(15, TimeUnit.SECONDS), "m4 not ready");

                    MessageListener<Msg> msgMessageListener = (msg, m) -> {
                        Msg answer = m.createAnswerMsg();
                        answer.setTopic("test_answer");
                        return answer;
                    };

                    try {
                        m2.addListenerForTopic("test", msgMessageListener);
                        m3.addListenerForTopic("test", msgMessageListener);
                        m4.addListenerForTopic("test", msgMessageListener);

                        // Small delay for topic listeners to be fully registered
                        // Real MongoDB changestreams need more time for subscription propagation
                        Thread.sleep(2000);

                        for (int i = 0; i < 5; i++) {
                            Msg query = new Msg("test", "test query", "query");
                            query.setExclusive(true);
                            List<Msg> ans = m1.sendAndAwaitAnswers(query, 3, 15000);
                            assertEquals(1, ans.size(), "more than one answer for exclusive message");
                        }

                        for (int i = 0; i < 5; i++) {
                            Msg query = new Msg("test", "test query", "query");
                            query.setExclusive(false);
                            List<Msg> ans = m1.sendAndAwaitAnswers(query, 3, 15000);
                            assertEquals(3, ans.size(), "not enough answers for non-exclusive message");
                        }
                    } finally {
                        m1.terminate();
                        m2.terminate();
                        m3.terminate();
                        m4.terminate();
                        morphium2.close();
                        morphium3.close();
                        morphium4.close();
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void answerWithDifferentNameTest(Morphium baseMorphium) throws Exception {
        try (baseMorphium) {
            for (String msgImpl : MultiDriverTestBase.messagingsToTest) {
                MorphiumConfig cfg = baseMorphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(baseMorphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(baseMorphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(baseMorphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
                cfg.encryptionSettings().setCredentialsEncrypted(baseMorphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(baseMorphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(baseMorphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

                try (Morphium morphium = new Morphium(cfg)) {
                    MorphiumMessaging producer = morphium.createMessaging();
                    producer.setUseChangeStream(true);
                    producer.start();
                    MorphiumMessaging consumer = morphium.createMessaging();
                    consumer.setUseChangeStream(true);
                    consumer.start();

                    // Wait for both messaging instances to be fully ready (change streams initialized)
                    assertTrue(producer.waitForReady(15, TimeUnit.SECONDS), "producer not ready");
                    assertTrue(consumer.waitForReady(15, TimeUnit.SECONDS), "consumer not ready");

                    try {
                        consumer.addListenerForTopic("testDiff", new MessageListener() {
                            @Override
                            public Msg onMessage(MorphiumMessaging msg, Msg m) {
                                Msg answer = m.createAnswerMsg();
                                answer.setTopic("answer");
                                return answer;
                            }
                        });
                        Msg answer = producer.sendAndAwaitFirstAnswer(new Msg("testDiff", "query", "value"), 10000);
                        assertNotNull(answer);
                        assertEquals("answer", answer.getTopic());
                    } finally {
                        producer.terminate();
                        consumer.terminate();
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void ownAnsweringHandler(Morphium baseMorphium) throws Exception {
        try (baseMorphium) {
            for (String msgImpl : MultiDriverTestBase.messagingsToTest) {
                MorphiumConfig cfg = baseMorphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(baseMorphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(baseMorphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(baseMorphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
                cfg.encryptionSettings().setCredentialsEncrypted(baseMorphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(baseMorphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(baseMorphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

                try (Morphium morphium = new Morphium(cfg)) {
                    MorphiumMessaging producer = morphium.createMessaging();
                    producer.start();
                    MorphiumMessaging consumer = morphium.createMessaging();
                    consumer.start();

                    // Wait for both messaging instances to be fully ready (change streams initialized)
                    assertTrue(producer.waitForReady(15, TimeUnit.SECONDS), "producer not ready");
                    assertTrue(consumer.waitForReady(15, TimeUnit.SECONDS), "consumer not ready");

                    counts.clear();
                    consumer.addListenerForTopic("testAnswering", (msg, m) -> {
                        Msg answer = m.createAnswerMsg();
                        answer.setTopic("answerForTestAnswering");
                        return answer;
                    });
                    MorphiumId msgId = new MorphiumId();
                    producer.addListenerForTopic("answerForTestAnswering", (msg, m) -> {
                        assertEquals(msgId, m.getInAnswerTo());
                        counts.put(msgId, 1);
                        return null;
                    });
                    Msg msg = new Msg("testAnswering", "query", "value");
                    msg.setMsgId(msgId);
                    producer.sendMessage(msg);

                    // Wait for the answer to be received
                    waitForConditionToBecomeTrue(10000, "Answer not received in counts map",
                                                 () -> counts.containsKey(msgId));

                    assertTrue(counts.containsKey(msgId));
                    producer.terminate();
                    consumer.terminate();
                }
            }
        }
    }
}
