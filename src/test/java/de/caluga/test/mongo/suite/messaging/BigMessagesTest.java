package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.messaging.StdMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

@Disabled
public class BigMessagesTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testBigMessage(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            final AtomicInteger count = new AtomicInteger();
            morphium.dropCollection(Msg.class, "msg", null);
            Thread.sleep(1000);
            StdMessaging sender = new StdMessaging(morphium, 100, true, 10);
            StdMessaging receiver = new StdMessaging(morphium);

            try {
                sender.setUseChangeStream(true).start();
                receiver.setUseChangeStream(true).start();
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

                    log.info(i + ". Text Size: " + txt.length());
                    Thread.yield();
                    Msg big = new Msg();
                    big.setTopic("bigMsg");
                    big.setTtl(3000000);
                    big.setValue(txt.toString());
                    big.setMapValue(UtilsMap.of("msgNr", i));
                    big.getMapValue().put("ts", System.currentTimeMillis());
                    big.setTimestamp(System.currentTimeMillis());
                    sender.sendMessage(big);
                }

                long start = System.currentTimeMillis();

                while (count.get() < amount) {
                    if (count.get() % 10 == 0) {
                        log.info("... messages recieved: " + count.get());
                    }

                    Thread.sleep(500);

                    if (System.currentTimeMillis() - start > 10000) {
                        log.error("Message was lost");
                        log.info("Messagecount: " + morphium.createQueryFor(Msg.class).countAll());

                        for (Msg m : morphium.createQueryFor(Msg.class).asIterable()) {
                            log.info("Msg: " + m.getMsgId());

                            if (m.getProcessedBy() != null) {
                                for (String pb : m.getProcessedBy()) {
                                    log.info("Processed by: " + pb);
                                }
                            }
                        }
                    }

                    assertTrue(System.currentTimeMillis() - start < 35000);
                }
            } finally {
                sender.terminate();
                receiver.terminate();
            }
        }
    }
}
