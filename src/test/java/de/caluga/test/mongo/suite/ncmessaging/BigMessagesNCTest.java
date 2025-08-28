package de.caluga.test.mongo.suite.ncmessaging;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.messaging.StdMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

@Disabled
public class BigMessagesNCTest extends MorphiumTestBase {

    @Test
    public void testBigMessage() throws Exception {
        final AtomicInteger count = new AtomicInteger();
        morphium.dropCollection(Msg.class, "msg", null);
        Thread.sleep(1000);
        StdMessaging sender = new StdMessaging(morphium, 100,  true, 10);
        StdMessaging receiver = new StdMessaging(morphium);

        try {
            sender.setUseChangeStream(false).start();
            receiver.setUseChangeStream(false).start();
            receiver.addListenerForTopic("bigMsg", (msg, m) -> {
                long dur = System.currentTimeMillis() - m.getTimestamp();
                long dur2 = System.currentTimeMillis() - (Long) m.getMapValue().get("ts");
                log.info("Received #" + m.getMapValue().get("msgNr") + " after " + dur + "ms Dur2: " + dur2);
                count.incrementAndGet();
                return null;
            });
            int amount = 25;

            for (int i = 0; i < amount; i++) {
                StringBuilder txt = new StringBuilder();
                txt.append("Test");

                for (int t = 0; t < 6 * Math.random() + 5; t++) {
                    txt.append(txt.toString() + "/" + txt.toString());
                }

                log.info("Text Size: " + txt.length());
                Msg big = new Msg();
                big.setTopic("bigMsg");
                big.setTtl(3000000);
                big.setValue(txt.toString());
                big.setMapValue(UtilsMap.of("msgNr", i));
                big.getMapValue().put("ts", System.currentTimeMillis());
                big.setTimestamp(System.currentTimeMillis());
                sender.sendMessage(big);
            }

            while (count.get() < amount) {
                if (count.get() % 10 == 0) {
                    log.info("still waiting... messages recieved: " + count.get());
                }

                Thread.sleep(500);
            }
        } finally {
            sender.terminate();
            receiver.terminate();
        }
    }
}
