package de.caluga.test.mongo.suite.inmem_messaging;

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.inmem.MorphiumInMemTestBase;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class InMemMessagingSpeedTests extends MorphiumInMemTestBase {

    @Test
    public void writeSpeed() throws Exception {
        morphium.clearCollection(Msg.class);
        Messaging msg = new Messaging(morphium, 100, false, true, 10);
        msg.start();

        try {
            final long dur = 1000;
            final long start = System.currentTimeMillis();

            for (int i = 0; i < 25; i++) {
                new Thread() {
                    public void run() {
                        Msg m = new Msg("test", "test", "testval", 30000);

                        while (System.currentTimeMillis() < start + dur) {
                            msg.sendMessage(m);
                            m.setMsgId(null);
                        }
                    }
                }
                .start();
            }

            while (System.currentTimeMillis() < start + dur) {
                Thread.sleep(10);
            }

            long cnt = morphium.createQueryFor(Msg.class).countAll();
            log.info("stored msg: " + cnt + " in " + dur + "ms");
        } finally {
            try {
                msg.terminate();
            } catch (Exception e) {
                log.error("Termination of messaging failed....");
            }
        }
    }

    @Test
    public void writeRecSpeed() throws Exception {
        morphium.clearCollection(Msg.class);
        //        morphium.getConfig().setThreadPoolAsyncOpCoreSize(1000);
        Messaging sender = new Messaging(morphium, 100, false, true, 10);
        sender.start();
        Messaging receiver = new Messaging(morphium, 100, true, true, 100);
        receiver.start();

        try {
            final AtomicInteger recCount = new AtomicInteger();
            receiver.addMessageListener(new MessageListener() {
                @Override
                public Msg onMessage(Messaging msg, Msg m) {
                    recCount.incrementAndGet();
                    return null;
                }
            });
            final long dur = 1000;
            final long start = System.currentTimeMillis();

            for (int i = 0; i < 15; i++) {
                new Thread() {
                    public void run() {
                        Msg m = new Msg("test", "test", "testval", 30000);

                        while (System.currentTimeMillis() < start + dur) {
                            sender.sendMessage(m);
                            m.setMsgId(null);
                        }
                    }
                }
                .start();
            }

            while (System.currentTimeMillis() < start + dur) {
                Thread.sleep(10);
            }

            long cnt = morphium.createQueryFor(Msg.class).countAll();
            log.info("Messages sent: " + cnt + " received: " + recCount.get() + " in " + dur + "ms");
        } finally {
            try {
                sender.terminate();
                receiver.terminate();
            } catch (Exception e) {
                log.error("Termination of messaging failed....");
            }
        }
    }

    @Test
    public void writeExclusiveRec() throws Exception {
        //        morphium.getConfig().setThreadPoolAsyncOpCoreSize(1000);
        morphium.clearCollection(Msg.class);
        Messaging sender = new Messaging(morphium, 100, false, true, 10);
        sender.start();
        Messaging receiver = new Messaging(morphium, 100, true, true, 100);
        receiver.start();
        Messaging receiver2 = new Messaging(morphium, 100, true, true, 100);
        receiver2.start();

        try {
            final AtomicInteger recCount = new AtomicInteger();
            receiver.addMessageListener((msg, m)->{
                recCount.incrementAndGet();
                return null;
            });
            receiver2.addMessageListener((msg, m)->{
                recCount.incrementAndGet();
                return null;
            });
            final long dur = 1000;
            final long start = System.currentTimeMillis();

            for (int i = 0; i < 15; i++) {
                new Thread() {
                    public void run() {
                        Msg m = new Msg("test", "test", "testval", 30000);
                        m.setExclusive(true);

                        while (System.currentTimeMillis() < start + dur) {
                            sender.sendMessage(m);
                            m.setMsgId(null);
                        }
                    }
                }
                .start();
            }

            while (System.currentTimeMillis() < start + dur) {
                Thread.sleep(10);
            }

            long cnt = morphium.createQueryFor(Msg.class).countAll();
            log.info("Messages sent: " + cnt + " received: " + recCount.get() + " in " + dur + "ms");
        } finally {
            try {
                sender.terminate();
                receiver.terminate();
                receiver2.terminate();
            } catch (Exception e) {
                log.error("Termination of messaging failed....");
            }
        }
    }

}
