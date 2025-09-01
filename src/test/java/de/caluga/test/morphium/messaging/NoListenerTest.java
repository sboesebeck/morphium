package de.caluga.test.morphium.messaging;

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.jupiter.api.Test;

public class NoListenerTest extends MorphiumTestBase {

    @Test
    public void removeListenerTest() throws Exception {
        for (String msgImpl : de.caluga.test.mongo.suite.base.MorphiumTestBase.messagingsToTest) {
            var cfg = morphium.getConfig().createCopy();
            cfg.messagingSettings().setMessagingImplementation(msgImpl);
                cfg.encryptionSettings().setCredentialsEncrypted(morphium.getConfig().encryptionSettings().getCredentialsEncrypted());
                cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
                cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
            try (de.caluga.morphium.Morphium m = new de.caluga.morphium.Morphium(cfg)) {
                MorphiumMessaging sender = m.createMessaging();
                sender.start();
                MorphiumMessaging r1 = m.createMessaging();
                r1.start();
                Thread.sleep(200);
                MessageListener ml = (msg, mm) -> null;
                r1.addListenerForTopic("test_listener", ml);
                Msg mm = new Msg("test_listener", "a test", "42");
                sender.sendMessage(mm);
                Thread.sleep(300);
                m.reread(mm);
                org.junit.jupiter.api.Assertions.assertEquals(1, mm.getProcessedBy().size());
                r1.removeListenerForTopic("test_listener", ml);
                mm = new Msg("test_listener", "a test", "42");
                sender.sendMessage(mm);
                Thread.sleep(200);
                m.reread(mm);
                org.junit.jupiter.api.Assertions.assertTrue(mm.getProcessedBy() == null || mm.getProcessedBy().size() == 0);
                sender.terminate();
                r1.terminate();
            }
        }
    }

}
