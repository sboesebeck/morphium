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
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Tag("messaging")
public class AdvancedMessagingTests extends MultiDriverTestBase {
    private final Map<MorphiumId, Integer> counts = new ConcurrentHashMap<>();

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testExclusiveXTimes(Morphium baseMorphium) throws Exception {
        try (baseMorphium) {
            for (String msgImpl : MorphiumTestBase.messagingsToTest) {
                MorphiumConfig cfg = baseMorphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(baseMorphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(baseMorphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(baseMorphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
                cfg.encryptionSettings().setCredentialsEncrypted(baseMorphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(baseMorphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(baseMorphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

                try (Morphium morphium = new Morphium(cfg)) {
                    for (int i = 0; i < 3; i++) {
                        runExclusiveMessagesTest(morphium, (int) (Math.random() * 200 + 50), (int) (5 * Math.random()) + 2);
                    }
                }
            }
        }
    }

    private void runExclusiveMessagesTest(Morphium morphium, int amount, int receivers) throws Exception {
        morphium.createQueryFor(Msg.class).delete();
        TestUtils.waitForConditionToBecomeTrue(10000, "MsgClass not deleted", () -> morphium.createQueryFor(Msg.class).countAll() == 0);

        List<Morphium> morphiums = new ArrayList<>();
        List<MorphiumMessaging> messagings = new ArrayList<>();
        MorphiumMessaging sender;
        sender = morphium.createMessaging();
        sender.setSenderId("amsender");
        AtomicBoolean error = new AtomicBoolean(false);

        try {
            counts.clear();
            MessageListener<Msg> msgMessageListener = (msg, m) -> {
                counts.putIfAbsent(m.getMsgId(), 0);
                counts.put(m.getMsgId(), counts.get(m.getMsgId()) + 1);

                if (counts.get(m.getMsgId()) > 1) {
                    error.set(true);
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }

                return null;
            };

            for (int i = 0; i < receivers; i++) {
                Morphium m;
                if (morphium.getDriver().getName().equals(InMemoryDriver.driverName)) {
                    morphiums.add(morphium);
                    m = morphium;
                } else {
                    MorphiumConfig cfg2 = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
                    cfg2.setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
                    cfg2.setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                    m = new Morphium(cfg2);
                    m.getConfig().getCache().setHouskeepingIntervalPause(100);
                    morphiums.add(m);
                }

                MorphiumMessaging msg = m.createMessaging();
                msg.setSenderId("msg" + i);
                msg.setUseChangeStream(true).start();
                messagings.add(msg);
                msg.addListenerForTopic("test", msgMessageListener);
            }

            for (int i = 0; i < amount; i++) {
                Msg m = new Msg("test", "test msg" + i, "value" + i);
                m.setMsgId(new MorphiumId());
                m.setExclusive(true);
                m.setTtl(600000);
                sender.sendMessage(m);
            }

            int lastCount = counts.size();
            while (counts.size() < amount) {
                for (MorphiumId id : counts.keySet()) {
                    assertTrue(counts.get(id) <= 1, "processed more than once: " + id);
                }

                Thread.sleep(1000);
                assertNotEquals(lastCount, counts.size());
                lastCount = counts.size();
                assertFalse(error.get(), "duplicate processing detected");
            }
        } finally {
            // terminate sender and all receivers synchronously
            try { sender.terminate(); } catch (Exception ignored) {}
            for (MorphiumMessaging m : messagings) {
                try { m.terminate(); } catch (Exception ignored) {}
            }

            // close additional Morphium instances (not needed for in‑memory)
            if (!morphium.getDriver().getName().equals(InMemoryDriver.driverName)) {
                for (Morphium m : morphiums) {
                    try { m.close(); } catch (Exception ignored) {}
                }
            } else {
                try { morphium.getDriver().close(); morphium.getDriver().connect(); } catch (Exception ignored) {}
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void messageAnswerTest(Morphium baseMorphium) throws Exception {
        try (baseMorphium) {
            for (String msgImpl : MorphiumTestBase.messagingsToTest) {
                MorphiumConfig cfg = baseMorphium.getConfig().createCopy();
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
                    m1.start();

                    MorphiumConfig cfg2 = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
                    cfg2.setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
                    cfg2.setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                    Morphium morphium2 = new Morphium(cfg2);
                    MorphiumMessaging m2 = morphium2.createMessaging();
                    m2.start();

                    cfg2 = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
                    cfg2.setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
                    cfg2.setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                    Morphium morphium3 = new Morphium(cfg2);
                    MorphiumMessaging m3 = morphium3.createMessaging();
                    m3.start();

                    cfg2 = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
                    cfg2.setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
                    cfg2.setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                    Morphium morphium4 = new Morphium(cfg2);
                    MorphiumMessaging m4 = morphium4.createMessaging();
                    m4.start();

                    Thread.sleep(500);
                    MessageListener<Msg> msgMessageListener = (msg, m) -> {
                        Msg answer = m.createAnswerMsg();
                        answer.setTopic("test_answer");
                        return answer;
                    };

                    try {
                        m2.addListenerForTopic("test", msgMessageListener);
                        m3.addListenerForTopic("test", msgMessageListener);
                        m4.addListenerForTopic("test", msgMessageListener);

                        for (int i = 0; i < 5; i++) {
                            Msg query = new Msg("test", "test query", "query");
                            query.setExclusive(true);
                            List<Msg> ans = m1.sendAndAwaitAnswers(query, 3, 1000);
                            assertEquals(1, ans.size(), "more than one answer for exclusive message");
                        }

                        for (int i = 0; i < 5; i++) {
                            Msg query = new Msg("test", "test query", "query");
                            query.setExclusive(false);
                            List<Msg> ans = m1.sendAndAwaitAnswers(query, 3, 1000);
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
            for (String msgImpl : MorphiumTestBase.messagingsToTest) {
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
                    Thread.sleep(500);
                    try {
                        consumer.addListenerForTopic("testDiff", new MessageListener() {
                            @Override
                            public Msg onMessage(MorphiumMessaging msg, Msg m) {
                                Msg answer = m.createAnswerMsg();
                                answer.setTopic("answer");
                                return answer;
                            }
                        });
                        Msg answer = producer.sendAndAwaitFirstAnswer(new Msg("testDiff", "query", "value"), 4000);
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
            for (String msgImpl : MorphiumTestBase.messagingsToTest) {
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
                    Thread.sleep(500);
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
                    Thread.sleep(1500);
                    assertTrue(counts.containsKey(msgId));
                    producer.terminate();
                    consumer.terminate();
                }
            }
        }
    }
}
