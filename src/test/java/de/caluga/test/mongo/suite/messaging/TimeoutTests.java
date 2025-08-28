package de.caluga.test.mongo.suite.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.atomic.AtomicInteger;

import de.caluga.morphium.messaging.StdMessaging;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

public class TimeoutTests extends MorphiumTestBase {

    @Test
    public void timeOutTests() throws Exception {
        morphium.dropCollection(Msg.class);
        TestUtils.waitForCollectionToBeDeleted(morphium, Msg.class);
        StdMessaging m1 = new StdMessaging(morphium, 100,  true, 1);
        m1.setSenderId("sender");
        m1.setUseChangeStream(true);
        m1.start();
        StdMessaging m2 = new StdMessaging(morphium, 100,  true, 1);
        m2.setUseChangeStream(true);
        m2.setSenderId("recevier");
        m2.start();
        AtomicInteger msgCount = new AtomicInteger(0);
        m2.addListenerForTopic("test", (msg, m)-> {
            log.info("Got message! " + m.getMsg());
            msgCount.incrementAndGet();
            return null;
        }
                              );

        for (int i = 0; i < 100; i++) {
            var msg = new Msg("test", "value" + i, "" + i).setTimingOut(false);
            m1.sendMessage(msg);
        }

        TestUtils.waitForConditionToBecomeTrue(20000, "Did not get all messages?", ()->msgCount.get() == 100);

        for (Msg m : morphium.createQueryFor(Msg.class).asIterable()) {
            assertNull(m.getDeleteAt());
            assertEquals(0, m.getTtl());
        }

        log.info("Waiting for timeout (which should not occur)");
        TestUtils.wait(90);
        assertEquals(100, morphium.createQueryFor(Msg.class).countAll());
        m1.terminate();
        m2.terminate();
    }

    @Test
    public void timeoutAfterProcessing() throws Exception {
        morphium.dropCollection(Msg.class);
        TestUtils.waitForCollectionToBeDeleted(morphium, Msg.class);
        StdMessaging m1 = new StdMessaging(morphium, 100,  true, 1);
        m1.setSenderId("sender");
        m1.setUseChangeStream(true);
        m1.start();
        m1.sendMessage(new Msg("test", "value0", "").setExclusive(true).setTimingOut(false).setDeleteAfterProcessing(true).setDeleteAfterProcessingTime(1000));
        log.info("Message sent -waiting a couple of seconds");
        TestUtils.wait(3);
        assertEquals(1, morphium.createQueryFor(Msg.class).countAll());
        log.info("Msg not deleted yet - good");
        StdMessaging m2 = new StdMessaging(morphium, 100,  true, 1);
        m2.setUseChangeStream(true);
        m2.setSenderId("recevier");
        m2.addListenerForTopic("test", (n, m)-> {
            log.info("Message incoming");
            return null;
        });
        m2.start();
        Thread.sleep(900);
        assertEquals(1, morphium.createQueryFor(Msg.class).countAll());
        log.info("Msg still there after processing - at least for a second! Waiting a minute....");
        TestUtils.wait(600);
        assertEquals(0, morphium.createQueryFor(Msg.class).countAll());
        m1.terminate();
        m2.terminate();
    }

    @Test
    @Disabled
    public void standardBehaviour() throws Exception {
        morphium.dropCollection(Msg.class);
        TestUtils.waitForCollectionToBeDeleted(morphium, Msg.class);
        StdMessaging m1 = new StdMessaging(morphium, 100,  true, 1);
        m1.setSenderId("sender");
        m1.setUseChangeStream(true);
        m1.start();
        m1.sendMessage(new Msg("test", "value0", "").setExclusive(true));
        TestUtils.wait(3);
        assertEquals(1, morphium.createQueryFor(Msg.class).countAll());
        log.info("Msg not deleted yet - good... waiting for timeout");
        TestUtils.wait("Waiting for timeout ", 60);
        TestUtils.waitForConditionToBecomeTrue(30000, "Msg was not deleted - timeout failed", ()->morphium.createQueryFor(Msg.class).countAll() == 0);
        assertEquals(0, morphium.createQueryFor(Msg.class).countAll());
        log.info("Was deleted...");
        m1.sendMessage(new Msg("test", "value0", "").setExclusive(true));
        StdMessaging m2 = new StdMessaging(morphium, 100,  true, 1);
        m2.setUseChangeStream(true);
        m2.setSenderId("recevier");
        m2.addListenerForTopic("test", (n, m)-> {
            log.info("Message incoming");
            return null;
        });
        m2.start();
        Thread.sleep(900);
        assertEquals(1, morphium.createQueryFor(Msg.class).countAll());
        log.info("Msg still there after processing - at least for a second! Waiting a minute....");
        TestUtils.wait(60);
        TestUtils.waitForConditionToBecomeTrue(5000, "Msg was not deleted - timeout failed", ()->morphium.createQueryFor(Msg.class).countAll() == 0);
        log.info("finished");
        m1.terminate();
        m2.terminate();
    }

}
