package de.caluga.test.mongo.suite.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MessageRejectedException;
import de.caluga.morphium.messaging.MessageRejectedException.RejectionHandler;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MsgLock;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;

public class ComplexMessageQueueTests extends MorphiumTestBase {

    @Test
    public void longRunningExclusivesWithIgnore() throws Exception {
        Messaging sender = new Messaging(morphium);
        sender.setSenderId("sender");
        sender.start();
        Thread.sleep(2000);
        Messaging rec = new Messaging(morphium);
        rec.setSenderId("rec");
        rec.start();
        AtomicInteger messageCount = new AtomicInteger();
        AtomicInteger totalCount = new AtomicInteger();
        rec.addMessageListener((msg, m) -> {
            if (messageCount.get() == 0) {
                messageCount.incrementAndGet();
                var ex = new MessageRejectedException("nah.. not now", true);
                ex.setCustomRejectionHandler(new RejectionHandler() {
                    @Override
                    public void handleRejection(Messaging msg, Msg m) throws Exception {
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
        Messaging sender = new Messaging(morphium);
        sender.setSenderId("sender");
        sender.start();
        Thread.sleep(2000);
        Vector<Messaging> clients = new Vector<>();
        Vector<MorphiumId> processedMessages = new Vector<>();

        try {
            MessageListener<Msg> lst = new MessageListener<Msg>() {
                @Override
                public boolean markAsProcessedBeforeExec() {
                    return true;
                }
                @Override
                public Msg onMessage(Messaging msg, Msg m) {
                    if (processedMessages.contains(m.getMsgId())) {
                        log.error("Duplicate processing!");
                    }

                    processedMessages.add(m.getMsgId());
                    return null;
                }
            };

            for (int i = 0; i < 5; i++) {
                Messaging cl = new Messaging(morphium);
                cl.setSenderId("cl" + i);
                cl.addMessageListener(lst);
                cl.start();
                clients.add(cl);
            }

            // //releasing lock _after_ sending
            Msg m = new Msg("test", "msg", "value", 1000, true);
            m.setMsgId(new MorphiumId());
            m.setTimingOut(false);
            MsgLock l=new MsgLock(m);
            l.setLockId("someone");
            morphium.save(l,"msg_lck",null);
            sender.sendMessage(m);
            Thread.sleep(2000);
            assertEquals(0, processedMessages.size());
            var q = morphium.createQueryFor(MsgLock.class).setCollectionName("msg_lck").f("_id").eq(m.getMsgId());
            q.remove();
            for (Messaging ms:clients) ms.triggerCheck();
            //now it should be processed...
            Thread.sleep(3000);
            assertEquals(1, processedMessages.size(), "not processed");
        } finally {
            for (Messaging m : clients) {
                m.terminate();
            }

            sender.terminate();
        }
    }


}
