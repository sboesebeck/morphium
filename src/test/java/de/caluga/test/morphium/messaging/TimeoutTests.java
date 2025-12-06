package de.caluga.test.morphium.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.test.OutputHelper;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

@Tag("messaging")
public class TimeoutTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void timeOutTests(Morphium morphium) throws Exception {
        String tstName = new Object() {
        } .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(Msg.class);
            TestUtils.waitForCollectionToBeDeleted(morphium, Msg.class);
            for (String msgImpl : MorphiumTestBase.messagingsToTest) {
                OutputHelper.figletOutput(log, msgImpl);
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings()
                   .setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(
                                   morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(
                                   morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
                try (Morphium m = new Morphium(cfg)) {
                    MorphiumMessaging m1 = m.createMessaging();
                    m1.setSenderId("sender");
                    m1.start();
                    MorphiumMessaging m2 = m.createMessaging();
                    m2.setSenderId("receiver");
                    m2.start();
                    AtomicInteger msgCount = new AtomicInteger(0);
                    m2.addListenerForTopic("test", (msg, mm) -> {
                        msgCount.incrementAndGet();
                        return null;
                    });

                    for (int i = 0; i < 50; i++) {
                        var msg = new Msg("test", "value" + i, "" + i).setTimingOut(false);
                        m1.sendMessage(msg);
                    }

                    TestUtils.waitForConditionToBecomeTrue(20000, "Did not get all messages?",
                                                           () -> msgCount.get() == 50);
                    assertEquals(50, m.createQueryFor(Msg.class, m1.getCollectionName("test")).countAll());

                    for (Msg mm : m.createQueryFor(Msg.class, m1.getCollectionName("test")).asIterable()) {
                        assertNull(mm.getDeleteAt());
                        assertEquals(0, mm.getTtl());
                    }

                    TestUtils.wait(5);
                    assertEquals(50, m.createQueryFor(Msg.class, m1.getCollectionName("test")).countAll());
                    m1.terminate();
                    m2.terminate();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void timeoutAfterProcessing(Morphium morphium) throws Exception {
        String tstName = new Object() {
        } .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(Msg.class);
            TestUtils.waitForCollectionToBeDeleted(morphium, Msg.class);
            for (String msgImpl : MorphiumTestBase.messagingsToTest) {
                OutputHelper.figletOutput(log, msgImpl);
                MorphiumConfig cfg = morphium.getConfig().createCopy();
                cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings()
                   .setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(
                                   morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(
                                   morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
                try (Morphium m = new Morphium(cfg)) {
                    MorphiumMessaging m1 = m.createMessaging();
                    m1.setSenderId("sender");
                    m1.setUseChangeStream(true);
                    m1.start();
                    m1.sendMessage(new Msg("test", "value0", "").setExclusive(true).setTimingOut(false)
                                   .setDeleteAfterProcessing(true).setDeleteAfterProcessingTime(15000));
                    TestUtils.wait(1);
                    final AtomicBoolean gotmsg = new AtomicBoolean(false);
                    assertEquals(1, m.createQueryFor(Msg.class, m1.getCollectionName("test")).countAll());
                    MorphiumMessaging m2 = m.createMessaging();
                    m2.setUseChangeStream(true);
                    m2.setSenderId("recevier");
                    m2.addListenerForTopic("test", (n, mm) -> { log.info("Got message"); gotmsg.set(true); return null;});
                    m2.start();

                    // morphium.ensureIndicesFor(Msg.class, m1.getCollectionName("test"));
                    log.info("Indexes: {}", morphium.getIndexesFromMongo(m1.getCollectionName("test")));
                    TestUtils.waitForConditionToBecomeTrue(5000, "Message not stored within timeout",
                                                           () -> m.createQueryFor(Msg.class, m1.getCollectionName("test")).countAll() == 1);
                    TestUtils.waitForBooleanToBecomeTrue(5000, "Message not received?", gotmsg, (dur)-> {log.info("Waiting for receiver...{}ms", dur);});
                    TestUtils.wait(2);
                    var theMsg = morphium.createQueryFor(Msg.class, m1.getCollectionName("test")).get();
                    log.info("Delete at is set to {}", theMsg.getDeleteAt());
                    TestUtils.waitForConditionToBecomeTrue(120000, "Message not deleted within timeout",
                                                           () -> m.createQueryFor(Msg.class, m1.getCollectionName("test")).countAll() == 0, (dur)-> {log.info("Still waiting for message do be deleted");});
                    m1.terminate();
                    m2.terminate();
                }
            }
        }
    }

    // standardBehaviour test removed (was disabled and duplicated coverage)

}
