package de.caluga.test.morphium.messaging;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.TestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * User: Stephan Bösebeck
 * Date: 11.05.18
 * Time: 22:22
 * <p>
 * TODO: Add documentation here
 */
@Tag("messaging")
public class CustomMsgTest extends MultiDriverTestBase {

    AtomicBoolean received = new AtomicBoolean(false);

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testCustomMsgSending(Morphium morphium) throws Exception  {
        morphium.dropCollection(Msg.class);
        for (String msgImpl : de.caluga.test.mongo.suite.base.MultiDriverTestBase.messagingsToTest) {
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImpl);
            cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
            try (Morphium m = new Morphium(cfg)) {
                MorphiumMessaging m1 = m.createMessaging();
                m1.setUseChangeStream(true);
                MorphiumMessaging m2 = m.createMessaging();
                m2.addListenerForTopic("test", (MessageListener<Msg>) (msg, mm) -> {
                    log.info("Got mesage");
                    received.set(true);
                    return null;
                });
                m2.setUseChangeStream(true);
                m1.start();
                m2.start();
                Thread.sleep(250);
                CustomMsg cm = new CustomMsg();
                cm.setCustomBuiltValue("test a avalue");
                cm.setTopic("test");
                m1.sendMessage(cm);
                // Wait for message to be stored and visible on replica sets
                final String collName = m1.getCollectionName(cm);
                // Use findById to query by exact msgId to avoid race conditions
                final var msgId = cm.getMsgId();
                TestUtils.waitForConditionToBecomeTrue(10000, "Message not stored",
                    () -> m.createQueryFor(CustomMsg.class, collName).f("_id").eq(msgId).get() != null);

                TestUtils.check(log, "Message stored");
                var msg = m.createQueryFor(CustomMsg.class, collName).f("_id").eq(msgId).get();
                assertEquals(1, m.createQueryFor(CustomMsg.class, collName).countAll());
                assertEquals(msg.getMsgId(), cm.getMsgId());
                TestUtils.check(log, "Message equal");

                m1.sendMessage(new Msg("test", "who cares", "darn"));
                TestUtils.waitForBooleanToBecomeTrue(30000, "did not receive message", received, (dur)-> {log.info("Still waiting after " + dur + "ms...");});
                m1.terminate();
                m2.terminate();
                received.set(false);
            }
        }
    }

    @Entity(typeId = "cmsg", polymorph = true)
    public static class CustomMsg extends Msg {
        private String customBuiltValue;

        public String getCustomBuiltValue() {
            return customBuiltValue;
        }

        public void setCustomBuiltValue(String customBuiltValue) {
            this.customBuiltValue = customBuiltValue;
        }
    }
}
