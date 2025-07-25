package de.caluga.test.mongo.suite.messaging;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

public class MessagingSeveralMessagingTest extends MultiDriverTestBase {
    private AtomicInteger procCounter = new AtomicInteger();
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesPooledOnly")
    public void severalMessagingsTest(Morphium morphium) throws Exception {
        try (morphium) {
            String method = new Object() {
            }
            .getClass().getEnclosingMethod().getName();
            log.info(String.format("=====================> Running Test %s with %s <===============================", method, morphium.getDriver().getName()));
            Messaging m1 = new Messaging(morphium, 10,  true, 1);
            m1.setSenderId("m1");
            Messaging m2 = new Messaging(morphium, 10,  true, 1);
            m2.setSenderId("m2");
            Messaging m3 = new Messaging(morphium, 10,  true, 1);
            m3.setSenderId("m3");
            m1.start();
            m2.start();
            m3.start();
            Thread.sleep(2000);

            try {
                m3.addListenerForMessageNamed("multisystemtest", (msg, m) -> {
                    //log.info("Got message: "+m.getName());
                    log.info("Sending answer for " + m.getMsgId());
                    return new Msg("multisystemtest", "answer", "value", 60000);
                });
                procCounter.set(0);

                for (int i = 0; i < 180; i++) {
                    final int num = i;
                    new Thread() {
                        public void run() {
                            Msg m = new Msg("multisystemtest", "nothing", "value");
                            m.setTtl(10000);

                            try {
                                Msg a = m1.sendAndAwaitFirstAnswer(m, 5000);
                                assertNotNull(a);
                                procCounter.incrementAndGet();
                            } catch (Exception e) {
                                log.error("Did not receive answer for msg " + num);
                            }
                        }
                    }
                    .start();
                }

                long s = System.currentTimeMillis();

                while (procCounter.get() < 180) {
                    Thread.sleep(1000);
                    log.info("Recieved " + procCounter.get());
                    assert(System.currentTimeMillis() - s < 60000);
                }
            } finally {
                try {
                    m1.terminate();
                    m2.terminate();
                    m3.terminate();
                } catch (Exception e) {
                    //swallow
                }
            }

            log.info(method + "() finished with " + morphium.getDriver().getName());
        }
    }

}
