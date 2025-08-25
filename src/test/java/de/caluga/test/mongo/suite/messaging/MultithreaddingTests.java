package de.caluga.test.mongo.suite.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.StdMessaging;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;

public class MultithreaddingTests extends MorphiumTestBase {

    @Test
    public void multithreaddingMessagingTest() throws Exception {
        log.info("Starting test");
        StdMessaging sender = new StdMessaging(morphium, 100,  false, 1); //no Multithreadding
        sender.setSenderId("sender");
        sender.start();
        StdMessaging rec = new StdMessaging(morphium, 100,  true, 4);
        rec.setSenderId("rec");
        rec.start();
        AtomicInteger parallelThreads = new AtomicInteger(0);
        rec.addListenerForMessageNamed("test", new MessageListener<Msg>() {
            @Override
            public Msg onMessage(MorphiumMessaging msg, Msg m) {
                parallelThreads.incrementAndGet();

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                }

                parallelThreads.decrementAndGet();
                return null;
            }
        });
        Thread.sleep(2000);
        List<Msg> toSend = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            var m = new Msg("test", "msg", "value");
            m.setSenderHost("localhost");
            m.setSender(sender.getSenderId());
            toSend.add(m);
        }

        morphium.insert(toSend);
        long start = System.currentTimeMillis();

        while (parallelThreads.get() == 0) {
            Thread.sleep(100);
            assertTrue(System.currentTimeMillis() - start < 60000);
        }

        int maxParallel = 0;
        int minParallel = 1000;

        while (parallelThreads.get() != 0) {
            log.info("Parallel threads: " + parallelThreads.get());
            ;

            if (parallelThreads.get() > maxParallel) maxParallel = parallelThreads.get();

            if (parallelThreads.get() < minParallel) minParallel = parallelThreads.get();

            Thread.sleep(1000);
        }

        assertEquals(5, maxParallel);
        assertTrue(minParallel != 0);
    }

}
