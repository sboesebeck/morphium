package de.caluga.test.mongo.suite.inmem.messaging;

import de.caluga.morphium.Utils;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.MorphiumTestBase;
import de.caluga.test.mongo.suite.inmem.MorphiumInMemTestBase;
import org.junit.Test;

public class InMemBigMessagesTest extends MorphiumInMemTestBase {

    @Test
    public void testBigMessage() throws Exception {
        Messaging sender = new Messaging(morphium, 100, true, true, 10);
        sender.start();
        Messaging receiver = new Messaging(morphium);
        receiver.start();
        try {
            receiver.addListenerForMessageNamed("bigMsg", (msg, m) -> {
                long dur = System.currentTimeMillis() - m.getTimestamp();
                long dur2 = System.currentTimeMillis() - (Long) m.getMapValue().get("ts");
                log.info("Received #" + m.getMapValue().get("msgNr") + " after " + dur + "ms Dur2: " + dur2);
                return null;
            });

            StringBuilder txt = new StringBuilder();
            txt.append("Test");
            for (int i = 0; i < 12; i++) {
                txt.append(txt.toString() + "/" + txt.toString());
            }
            log.info("Text Size: " + txt.length());
            for (int i = 0; i < 300; i++) {
                Msg big = new Msg();
                big.setName("bigMsg");
                big.setTtl(30000);
                big.setValue(txt.toString());
                big.setMapValue(Utils.getMap("msgNr", i));
                big.getMapValue().put("ts", System.currentTimeMillis());
                big.setTimestamp(System.currentTimeMillis());
                sender.sendMessage(big);
            }
        } finally {
            sender.terminate();
            receiver.terminate();
        }

    }
}
