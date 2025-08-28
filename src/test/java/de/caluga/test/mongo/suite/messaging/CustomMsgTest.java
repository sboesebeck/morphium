package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.StdMessaging;
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
        StdMessaging m1 = new StdMessaging(morphium, 100, false);
        StdMessaging m2 = new StdMessaging(morphium, 100, false);
        m2.addListenerForTopic("test", (MessageListener<CustomMsg>) (msg, m) -> {
            received = true;
            log.info("incoming message: " + m.getCustomBuiltValue());
            return null;
        });
        m1.start();
        m2.start();
        Thread.sleep(2500);
        CustomMsg cm = new CustomMsg();
        cm.setCustomBuiltValue("test a avalue");
        cm.setTopic("test");
        m1.sendMessage(cm);

        Thread.sleep(2000);
        assert (received);
        m1.terminate();
        m2.terminate();
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
