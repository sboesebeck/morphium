package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.jupiter.api.Test;

public class NoListenerTest extends MorphiumTestBase {

    @Test
    public void removeListenerTest() throws Exception {
        Messaging sender = new Messaging(morphium, 10, false);
        sender.start();

        Messaging r1 = new Messaging(morphium, 10, false);
        r1.start();
        Thread.sleep(2000);

        MessageListener ml = (msg, m) -> {
            log.info("Got Message");
            return null;
        };

        r1.addListenerForMessageNamed("test_listener", ml);

        Msg m = new Msg("test_listener", "a test", "42");
        sender.sendMessage(m);
        Thread.sleep(2000);
        morphium.reread(m);

        assert (m.getProcessedBy().size() == 1);
        r1.removeListenerForMessageNamed("test_listener", ml);
        m = new Msg("test_listener", "a test", "42");
        sender.sendMessage(m);
        morphium.reread(m);
        assert (m.getProcessedBy() == null || m.getProcessedBy().size() == 0);

    }

}
