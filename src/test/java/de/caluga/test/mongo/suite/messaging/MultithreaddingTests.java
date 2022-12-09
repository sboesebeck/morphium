package de.caluga.test.mongo.suite.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;

public class MultithreaddingTests extends MorphiumTestBase {

    @Test
    public void multithreaddingMessagingTest() throws Exception {
        Messaging sender = new Messaging(morphium, 100, false, false, 1); //no Multithreadding
        sender.setSenderId("sender");
        sender.start();
        Messaging rec = new Messaging(morphium, 100, true, true, 4);
        rec.setSenderId("rec");
        rec.start();
        AtomicInteger parallelThreads = new AtomicInteger(0);
        rec.addMessageListener(new MessageListener<Msg>() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) {
                parallelThreads.incrementAndGet();

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                }

                parallelThreads.decrementAndGet();
                return null;
            }
        });
        Thread.sleep(1000);
        List<Msg> toSend = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            var m = new Msg("msg", "msg", "value");
            m.setSenderHost("localhost");
            m.setSender(sender.getSenderId());
            toSend.add(m);
        }

        morphium.insert(toSend);
        Thread.sleep(1000);
        int maxParallel=0;
        int minParallel=1000;
        while (parallelThreads.get()!=0) {
            log.info("Parallel threads: " + parallelThreads.get());;
            if (parallelThreads.get()>maxParallel) maxParallel=parallelThreads.get();
            if (parallelThreads.get()<minParallel) minParallel=parallelThreads.get();
            Thread.sleep(1000);
        }
        assertEquals(5,maxParallel);
        assertTrue(minParallel!=0);



    }

}
