package de.caluga.test.morphium.messaging;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.messaging.*;
import de.caluga.morphium.query.Query;
import de.caluga.test.OutputHelper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.test.mongo.suite.base.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings("ALL")
@Tag("messaging")
public class MessagingTest extends MultiDriverTestBase {
    public boolean gotMessage = false;
    public boolean gotMessage1 = false;
    public boolean gotMessage2 = false;
    public boolean gotMessage3 = false;
    public boolean gotMessage4 = false;
    public boolean error = false;
    public MorphiumId lastMsgId;
    public AtomicInteger procCounter = new AtomicInteger(0);
    private final List<Msg> list = new ArrayList<>();
    private final AtomicInteger queueCount = new AtomicInteger(1000);


    @Test
    @org.junit.jupiter.api.Timeout(value = 60, unit = java.util.concurrent.TimeUnit.SECONDS)
    public void runMessageRoundtripForEver() throws Exception {
        // This used to be an infinite soak test against a hard-coded local replica set.
        // For CI we keep it bounded and use the central TestConfig (external RS if configured).
        var baseCfg = de.caluga.test.support.TestConfig.load().createCopy();
        baseCfg.driverSettings().setDriverName(PooledDriver.driverName);
        baseCfg.connectionSettings().setMaxConnections(10);
        baseCfg.connectionSettings().setMaxWaitTime(10000);
        baseCfg.connectionSettings().setConnectionTimeout(2000);
        baseCfg.connectionSettings().setRetriesOnNetworkError(3);
        baseCfg.connectionSettings().setSleepBetweenNetworkErrorRetries(500);

        // Use an isolated DB name for this test run
        baseCfg.connectionSettings().setDatabase("morphium_msg_roundtrip_" + System.currentTimeMillis());

        try (Morphium m = new Morphium(baseCfg)) {
            MorphiumMessaging sender = m.createMessaging();
            sender.setSenderId("sender");
            sender.start();
            assertTrue(sender.waitForReady(30, TimeUnit.SECONDS), "sender not ready");

            MorphiumMessaging receiver = m.createMessaging();
            receiver.setSenderId("receiver");
            receiver.start();
            assertTrue(receiver.waitForReady(30, TimeUnit.SECONDS), "receiver not ready");

            AtomicInteger received = new AtomicInteger(0);
            receiver.addListenerForTopic("test", (msgs, msg) -> {
                received.incrementAndGet();
                return null;
            });

            // Send a small bounded number of messages and assert we receive them.
            int toSend = 5;
            for (int i = 0; i < toSend; i++) {
                sender.sendMessage(new Msg("test", "test", "test" + i));
            }

            TestUtils.waitForConditionToBecomeTrue(30_000, "Did not receive all messages",
                    () -> received.get() >= toSend);

            assertEquals(toSend, received.get());
        }
    }

    @ParameterizedTest
    @MethodSource("de.caluga.test.mongo.suite.base.MultiDriverTestBase#getMorphiumInstancesNoSingle")
    public void testBackwardCompatibility(Morphium morphium) throws Exception {
        try(morphium) {
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(SingleCollectionMessaging.NAME);
            cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

            //Test Sending V5 and receiving V6
            //
            MorphiumMessaging receiver = morphium.createMessaging(cfg.messagingSettings());
            receiver.start();
            assertTrue(receiver.waitForReady(30, TimeUnit.SECONDS), "receiver not ready");
            AtomicBoolean received = new AtomicBoolean(false);
            receiver.addListenerForTopic("test", (m, msg)-> {
                OutputHelper.figletOutput(log, "Indcoming...");
                received .set( true);
                return msg.createAnswerMsg();
            });

            Thread.sleep(1000); // Allow topic listener to register
            //simulating write of V5
            //
            var v5msg = Map.of("name", (Object)"test", "value", "test", "processed_by", new ArrayList<String>(), "sender", "v5", "sender_host", "self", "timestamp", System.currentTimeMillis(), "ttl", 30000, "delete_at", new Date(System.currentTimeMillis() + 30000), "priority", 100);
            morphium.storeMap(receiver.getCollectionName(), v5msg);
            TestUtils.waitForBooleanToBecomeTrue(5000, "Did not get message", received, (dur)->log.info("Still waiting"));
            OutputHelper.figletOutput(log, "Got V5 Msg");

            //checking  answer - wait for answer to be visible on replica sets
            final String collName = receiver.getCollectionName();
            TestUtils.waitForConditionToBecomeTrue(10000, "Answer not visible",
                                                   () -> morphium.createQueryFor(Msg.class, collName).f(Msg.Fields.inAnswerTo).ne(null).countAll() >= 1);
            var q = morphium.createQueryFor(Msg.class, receiver.getCollectionName());
            q.f(Msg.Fields.inAnswerTo).ne(null);
            var answers = q.asMapList();
            assertEquals(1, answers.size());
            assertNotNull(answers.get(0).get("name"));

        }
    }


    @ParameterizedTest
    @MethodSource("de.caluga.test.mongo.suite.base.MultiDriverTestBase#getMorphiumInstancesNoSingle")
    public void testMsgQueName(Morphium morphium) throws Exception {
        try (morphium) {
            String method = new Object() {} .getClass().getEnclosingMethod().getName();
            log.info("Running Test {} with {}", method, morphium.getDriver().getName());

            for (String msgImpl : MultiDriverTestBase.messagingsToTest) {
                OutputHelper.figletOutput(log, msgImpl);
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

                try (Morphium morph = new Morphium(cfg)) {
                    MorphiumMessaging messaging1 = morph.createMessaging();
                    messaging1.setPause(500);
                    messaging1.setMultithreadded(false);
                    messaging1.setAutoAnswer(false);
                    assertFalse(messaging1.isAutoAnswer());
                    assertFalse(messaging1.isMultithreadded());
                    assertEquals(500, messaging1.getPause());
                    assertNotEquals(1, messaging1.getWindowSize());
                    messaging1.addListenerForTopic("tst", (msg, m1) -> {
                        gotMessage1 = true;
                        return null;
                    });
                    morph.dropCollection(Msg.class, messaging1.getCollectionName("test"), null);
                    messaging1.start();
                    MorphiumMessaging messaging2 = morph.createMessaging();
                    messaging2.setQueueName("msg2");
                    messaging2.setPause(500);
                    messaging2.addListenerForTopic("test", (msg, m1) -> {
                        gotMessage2 = true;
                        return null;
                    });
                    morph.dropCollection(Msg.class, messaging2.getCollectionName("test"), null);
                    messaging2.start();
                    try {
                        Msg msg = new Msg("test", "msg", "value", 30000);
                        msg.setExclusive(false);
                        messaging1.sendMessage(msg);
                        Thread.sleep(100);
                        Query<Msg> q = morph.createQueryFor(Msg.class, messaging1.getCollectionName(msg));
                        assertEquals(1, q.countAll()); //stored in the colleciton for messaging1
                        TestUtils.check(log, "Message stored and found in messaging1");
                        q.setCollectionName(messaging2.getCollectionName(msg));
                        assertEquals(0, q.countAll()); //not found in collection for messaging2
                        TestUtils.check(log, "Message stored and not found in messaging2");
                        msg = new Msg("test", "msg", "value", 30000);
                        msg.setExclusive(false);
                        messaging2.sendMessage(msg);
                        Thread.sleep(100);
                        q = morph.createQueryFor(Msg.class, messaging2.getCollectionName(msg));
                        assertEquals(1, q.countAll());  //stored in collection for messaging2
                        TestUtils.check(log, "Message stored and found in messaging2");
                        q = morph.createQueryFor(Msg.class, messaging1.getCollectionName(msg));
                        assertEquals(1, q.countAll());  //not stored in collection for messaging1 - still 1
                        TestUtils.check(log, "Message stored and not found in messaging1");
                        // Messages should NOT be received - different queues, sleep to ensure nothing arrives
                        Thread.sleep(500);
                        assertFalse(gotMessage1);
                        assertFalse(gotMessage2);
                        TestUtils.check(log, "Messages should not be received");

                    } finally {
                        messaging1.terminate();
                        messaging2.terminate();
                    }
                }
            }
        }
    }

    // NOTE: Remaining tests from the original class can be migrated similarly if desired.
}
