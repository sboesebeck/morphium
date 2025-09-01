package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.jupiter.api.Test;

/**
 * User: Stephan Bösebeck
 * Date: 11.05.18
 * Time: 22:22
 * <p>
 * TODO: Add documentation here
 */
public class CustomMsgTest extends MorphiumTestBase {

    boolean received = false;

    @Test
    public void testCustomMsgSending() throws Exception {
        morphium.dropCollection(Msg.class);
        for (String msgImpl : de.caluga.test.mongo.suite.base.MorphiumTestBase.messagingsToTest) {
            MorphiumConfig cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImpl);
            cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
            cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
            cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
            try (Morphium m = new Morphium(cfg)) {
                MorphiumMessaging m1 = m.createMessaging();
                MorphiumMessaging m2 = m.createMessaging();
                m2.addListenerForTopic("test", (MessageListener<CustomMsg>) (msg, mm) -> {
                    received = true;
                    return null;
                });
                m1.start();
                m2.start();
                Thread.sleep(250);
                CustomMsg cm = new CustomMsg();
                cm.setCustomBuiltValue("test a avalue");
                cm.setTopic("test");
                m1.sendMessage(cm);
                Thread.sleep(300);
                org.junit.jupiter.api.Assertions.assertTrue(received);
                m1.terminate();
                m2.terminate();
                received = false;
            }
        }
    }

    // @Entity(typeId="cmsg", polymorph = true)
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
