package de.caluga.test.mongo.suite.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import de.caluga.morphium.messaging.*;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageRejectedException.RejectionHandler;
import de.caluga.morphium.messaging.StdMessaging;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

public class ComplexMessageQueueTests extends MorphiumTestBase {

    @Test
    public void longRunningExclusivesWithIgnore() throws Exception {
        StdMessaging sender = new StdMessaging(morphium);
        sender.setSenderId("sender");
        sender.start();
        Thread.sleep(2000);
        StdMessaging rec = new StdMessaging(morphium);
        rec.setSenderId("rec");
        rec.start();
        AtomicInteger messageCount = new AtomicInteger();
        AtomicInteger totalCount = new AtomicInteger();
        rec.addListenerForMessageNamed("name", (msg, m) -> {
            if (messageCount.get() == 0) {
                messageCount.incrementAndGet();
                var ex = new MessageRejectedException("nah.. not now", true);
                ex.setCustomRejectionHandler(new RejectionHandler() {
                    @Override
                    public void handleRejection(MorphiumMessaging msg, Msg m) throws Exception {
                        m.setProcessedBy(new ArrayList<>());
                        morphium.save(m);
                    }
                });
                throw ex;
            }
            totalCount.incrementAndGet();
            messageCount.decrementAndGet();

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
            return null;
        });

        for (int i = 0; i < 10; i++) {
            Msg m = new Msg("name", "msg" + i, "value");
            m.setTimingOut(false);
            m.setDeleteAfterProcessing(true);
            m.setDeleteAfterProcessingTime(0);
            sender.sendMessage(m);
        }

        TestUtils.waitForConditionToBecomeTrue(110000, "did not get all messages?", () -> totalCount.get() == 10,
                                               () -> log.info("Not there yet: " + totalCount.get()));
    }

    @Test
    @Disabled
    public void releaseLockonMessage() throws Exception {
        StdMessaging sender = new StdMessaging(morphium);
        sender.setSenderId("sender");
        sender.start();
        Thread.sleep(2000);
        Vector<StdMessaging> clients = new Vector<>();
        Vector<MorphiumId> processedMessages = new Vector<>();

        try {
            MessageListener<Msg> lst = new MessageListener<Msg>() {
                @Override
                public boolean markAsProcessedBeforeExec() {
                    return true;
                }
                @Override
                public Msg onMessage(MorphiumMessaging msg, Msg m) {
                    if (processedMessages.contains(m.getMsgId())) {
                        log.error("Duplicate processing!");
                    }

                    processedMessages.add(m.getMsgId());
                    return null;
                }
            };

            for (int i = 0; i < 5; i++) {
                StdMessaging cl = new StdMessaging(morphium);
                cl.setSenderId("cl" + i);
                cl.addListenerForMessageNamed("test", lst);
                cl.start();
                clients.add(cl);
            }

            // //releasing lock _after_ sending
            Msg m = new Msg("test", "msg", "value", 1000, true);
            m.setMsgId(new MorphiumId());
            m.setTimingOut(false);
            MsgLock l = new MsgLock(m);
            l.setLockId("someone");
            morphium.save(l, "msg_lck", null);
            sender.sendMessage(m);
            Thread.sleep(2000);
            assertEquals(0, processedMessages.size());
            var q = morphium.createQueryFor(MsgLock.class).setCollectionName("msg_lck").f("_id").eq(m.getMsgId());
            q.remove();
            for (StdMessaging ms : clients) ms.triggerCheck();
            //now it should be processed...
            Thread.sleep(3000);
            assertEquals(1, processedMessages.size(), "not processed");
        } finally {
            for (StdMessaging m : clients) {
                m.terminate();
            }

            sender.terminate();
        }
    }


}
