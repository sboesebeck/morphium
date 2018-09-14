package de.caluga.test.mongo.suite;

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 11.05.18
 * Time: 22:22
 * <p>
 * TODO: Add documentation here
 */
public class CustomMsgTest extends MongoTest {

    boolean received = false;

    @Test
    public void testCustomMsgSending() throws Exception {
        morphium.dropCollection(Msg.class);
        Messaging m1 = new Messaging(morphium, 100, false);
        Messaging m2 = new Messaging(morphium, 100, false);
        m2.addMessageListener((MessageListener<CustomMsg>) (msg, m) -> {
            received = true;
            log.info("incoming message: " + m.getCustomBuiltValue());
            return null;
        });
        m1.start();
        m2.start();
        Thread.sleep(2500);
        CustomMsg cm = new CustomMsg();
        cm.setCustomBuiltValue("test a avalue");
        cm.setName("test");
        m1.storeMessage(cm);

        Thread.sleep(1000);
        assert (received);
        m1.terminate();
        m2.terminate();
    }

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
