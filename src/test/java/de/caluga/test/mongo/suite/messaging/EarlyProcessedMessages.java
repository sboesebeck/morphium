package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class EarlyProcessedMessages extends MorphiumTestBase {

    @Test
    public void testEarlyProcessedMessagesBroadcast() throws Exception {
        log.info("Running singlethreadded...");
        runTest(false, false);

    }

    @Test
    public void testEarlyProcessedMessagesBroadcastMultithreadded() throws Exception {

        log.info("Running multithreadded...");
        runTest(true, false);

    }


    @Test
    public void testEarlyProcessedMessagesExcl() throws Exception {
        log.info("Running singlethreadded...");
        runTest(false, true);

    }

    @Test
    public void testEarlyProcessedMessagesExclMultithreadded() throws Exception {

        log.info("Running multithreadded...");
        runTest(true, true);

    }


    public void runTest(boolean multithreadded, boolean exclusive) throws Exception {

        final Map<String, AtomicInteger> count = new HashMap<>();
        Messaging m1 = new Messaging(morphium, 100, true);
        m1.start();
        Messaging m2 = new Messaging(morphium, 100, true, multithreadded, 10);
        m2.setSenderId("m2");
        m2.start();

        Messaging m3 = new Messaging(morphium, 100, true, multithreadded, 10);
        m3.setSenderId("m3");
        m3.start();
        Messaging m4 = new Messaging(morphium, 100, true, multithreadded, 10);
        m4.setSenderId("m4");
        m4.start();
        Thread.sleep(2500);
        MessageListener l = new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) {
                count.putIfAbsent(msg.getSenderId(), new AtomicInteger());
                log.info(msg.getSenderId() + ": Got message: " + count.get(msg.getSenderId()).incrementAndGet());
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    //swallow
                }
                return null;
            }

            @Override
            public boolean markAsProcessedBeforeExec() {
                return true;
            }
        };
        m2.addMessageListener(l);
        m3.addMessageListener(l);
        m4.addMessageListener(l);


        for (int i = 0; i < 10; i++) {
            m1.sendMessage(new Msg("testmessage", "msg", "value", 15000, exclusive));
        }

        long start = System.currentTimeMillis();
        while (true) {
            int sum = 0;
            for (AtomicInteger c : count.values()) sum = sum + c.get();

            if (sum == 30 && !exclusive) break;
            if (sum == 10 && exclusive) break;
            Thread.sleep(1000);
            if (System.currentTimeMillis() - start > 15000) {
                break;
            }
        }
        Thread.sleep(2000);
        int sum = 0;
        for (AtomicInteger c : count.values()) sum = sum + c.get();
        if (exclusive) {
            log.info("Exclusive Message count: max 10");
            assertEquals(10, sum);
        } else {
            log.info("NonExclusive Message count: max 10*num Nodes = 30");

            assertEquals(30, sum);
        }
        m1.terminate();
        m2.terminate();
        m3.terminate();
        m4.terminate();
    }


}
