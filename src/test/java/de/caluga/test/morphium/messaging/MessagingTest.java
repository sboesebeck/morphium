package de.caluga.test.morphium.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.*;
import de.caluga.morphium.query.Query;
import de.caluga.test.OutputHelper;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.jupiter.api.Tag;
import de.caluga.test.mongo.suite.base.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings("ALL")
@Tag("messaging")
public class MessagingTest extends MorphiumTestBase {
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

    @ParameterizedTest
    @MethodSource("de.caluga.test.mongo.suite.base.MultiDriverTestBase#getMorphiumInstancesNoSingle")
    public void execAfterRelease(Morphium morphium) throws Exception {
        try (morphium) {
            String method = new Object() {} .getClass().getEnclosingMethod().getName();
            log.info("Running Test {} with {}", method, morphium.getDriver().getName());

            for (String msgImpl : MorphiumTestBase.messagingsToTest) {
                OutputHelper.figletOutput(log, msgImpl);
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

                try (Morphium morph = new Morphium(cfg)) {
                    morph.dropCollection(Msg.class);
                    morph.dropCollection(Msg.class, "mmsg_msg2", null);

                    MorphiumMessaging m = morph.createMessaging();
                    m.setSenderId("sender");
                    AtomicInteger received = new AtomicInteger();
                    MorphiumMessaging rec = morph.createMessaging();
                    rec.setSenderId("rec");
                    rec.start();
                    rec.addListenerForTopic("test", (mess, msg) -> {
                        received.incrementAndGet();
                        return null;
                    });
                    Thread.sleep(500);
                    Msg msg = new Msg("test", "msg1", "value1");
                    msg.setProcessedBy(Arrays.asList("Paused"));
                    msg.setExclusive(true);
                    m.sendMessage(msg);
                    Thread.sleep(200); // Give some time to ensure message is not processed
                    assertEquals(0, received.get(), "Message should not be received when processed by 'Paused'");
                    msg.setProcessedBy(new ArrayList<>());
                    morph.store(msg, m.getCollectionName(), null);
                    TestUtils.waitForConditionToBecomeTrue(5000, "Did not get message",
                        () -> received.get() == 1);
                    m.terminate();
                    rec.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("de.caluga.test.mongo.suite.base.MultiDriverTestBase#getMorphiumInstancesNoSingle")
    public void testMsgQueName(Morphium morphium) throws Exception {
        try (morphium) {
            String method = new Object() {} .getClass().getEnclosingMethod().getName();
            log.info("Running Test {} with {}", method, morphium.getDriver().getName());

            for (String msgImpl : MorphiumTestBase.messagingsToTest) {
                OutputHelper.figletOutput(log, msgImpl);
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());

                try (Morphium morph = new Morphium(cfg)) {
                    morph.dropCollection(Msg.class);
                    morph.dropCollection(Msg.class, "mmsg_msg2", null);
                    MorphiumMessaging m = morph.createMessaging();
                    m.setPause(500);
                    m.setMultithreadded(false);
                    m.setAutoAnswer(false);
                    assertFalse(m.isAutoAnswer());
                    assertFalse(m.isMultithreadded());
                    assertEquals(500, m.getPause());
                    assertNotEquals(1, m.getWindowSize());
                    m.addListenerForTopic("tst", (msg, m1) -> {
                        gotMessage1 = true;
                        return null;
                    });
                    m.start();
                    MorphiumMessaging m2 = morph.createMessaging();
                    m2.setQueueName("msg2");
                    m2.setPause(500);
                    m2.addListenerForTopic("test", (msg, m1) -> {
                        gotMessage2 = true;
                        return null;
                    });
                    m2.start();
                    try {
                        Msg msg = new Msg("test", "msg", "value", 30000);
                        msg.setExclusive(false);
                        m.sendMessage(msg);
                        Thread.sleep(100);
                        Query<Msg> q = morph.createQueryFor(Msg.class);
                        assertEquals(1, q.countAll());
                        q.setCollectionName(m2.getCollectionName());
                        assertEquals(0, q.countAll());
                        msg = new Msg("test", "msg", "value", 30000);
                        msg.setExclusive(false);
                        m2.sendMessage(msg);
                        Thread.sleep(100);
                        q = morph.createQueryFor(Msg.class);
                        assertEquals(1, q.countAll());
                        q.setCollectionName("mmsg_msg2");
                        assertEquals(1, q.countAll(), "Count mismatch");
                        Thread.sleep(1500);
                        assertFalse(gotMessage1);
                        assertFalse(gotMessage2);
                    } finally {
                        m.terminate();
                        m2.terminate();
                    }
                }
            }
        }
    }

    // NOTE: Remaining tests from the original class can be migrated similarly if desired.
}
