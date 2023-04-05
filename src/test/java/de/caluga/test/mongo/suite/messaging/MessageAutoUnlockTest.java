package de.caluga.test.mongo.suite.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

public class MessageAutoUnlockTest extends MorphiumTestBase {


    @Test
    public void testAutoReleaseLockMulti() throws Exception {
        AtomicInteger msgCount = new AtomicInteger(0);
        Messaging prod = new Messaging(morphium);
        Messaging consumer = new Messaging(morphium);
        Messaging consumer2 = new Messaging(morphium);
        Vector<MorphiumId> ids = new Vector<>();
        Map<MorphiumId,String> consumerByMsgId=new ConcurrentHashMap<>();

        try {
            prod.setSenderId("prod");
            prod.setPause(100);
            prod.start();
            MessageListener l = new MessageListener<Msg>() {

                @Override
                public boolean markAsProcessedBeforeExec() {
                    return true;
                }

                public Msg onMessage(Messaging m, Msg msg) {
                    msgCount.incrementAndGet();

                    if (ids.contains(msg.getMsgId())) {
                        log.info("Was processed by "+consumerByMsgId.get(msg.getMsgId()));
                        log.info(m.getSenderId()+": duplicat processing" );
                        throw new RuntimeException("duplicate processing");
                    }
                    consumerByMsgId.put(msg.getMsgId(),m.getSenderId());
                    ids.add(msg.getMsgId());
                    return null;
                }
            };
            consumer2.setPause(100);
            consumer2.setSenderId("c2");
            consumer2.addMessageListener(l);
            consumer2.start();

            consumer.setSenderId("c1");
            consumer.setPause(100);
            assertTrue(msgCount.get()<10);
            consumer.addMessageListener(l);
            consumer.start();
            Thread.sleep(2000);
            //creating several locked Messages
            for (int i = 1; i <= 10; i++) {
                var m = new Msg("test", "message", "value");
                m.setExclusive(true);
                prod.sendMessage(m);
            }

            Thread.sleep(100);
            assertEquals(0, msgCount.get());
            Thread.sleep(1000);
            log.info("MessageCount: "+msgCount.get());
            long tm = TestUtils.waitForConditionToBecomeTrue(5000, "not reached?", ()->msgCount.get() == 5);
            log.info("It took " + tm + "ms");
            assertTrue(tm > 3000 && tm < 5000, "time should be between 3 and 5s but was "+tm);
            Thread.sleep(1000);
            log.info("MessageCount: "+msgCount.get());
            Thread.sleep(1000);
            log.info("MessageCount: "+msgCount.get());
            Thread.sleep(1000);
            log.info("MessageCount: "+msgCount.get());
            assertTrue(msgCount.get()<10);
            Thread.sleep(1000);
            log.info("MessageCount: "+msgCount.get());
            assertTrue(msgCount.get()<10);
            Thread.sleep(1000);
            log.info("MessageCount: "+msgCount.get());
            assertTrue(msgCount.get()<10);
            Thread.sleep(1000);
            log.info("MessageCount: "+msgCount.get());
            Thread.sleep(1000);
            log.info("MessageCount: "+msgCount.get());
            // tm = TestUtils.waitForConditionToBecomeTrue(5000, "not reached?", ()->msgCount.get() == 10);
            // assertTrue(tm>3000 && tm< 5000, "time wrong "+tm);
        } finally {
            prod.terminate();
            consumer.terminate();
            consumer2.terminate();
        }
    }

}
