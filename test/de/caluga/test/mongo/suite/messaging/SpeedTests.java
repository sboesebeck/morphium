package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.MorphiumTestBase;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class SpeedTests extends MorphiumTestBase {

    @Test
    public void writeSpeed() throws Exception {
        Messaging msg = new Messaging(morphium, 100, false, true, 10);
        msg.start();


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
            }.start();
        }
        while (System.currentTimeMillis() < start + dur) {
            Thread.sleep(10);
        }
        long cnt = msg.getMessageCount();
        log.info("stored msg: " + cnt + " in " + dur + "ms");
        msg.terminate();
    }

    @Test
    public void writeRecSpeed() throws Exception {
//        morphium.getConfig().setThreadPoolAsyncOpCoreSize(1000);
        Messaging sender = new Messaging(morphium, 100, false, true, 10);
        sender.start();
        Messaging receiver = new Messaging(morphium, 100, true, true, 100);
        receiver.start();
        final AtomicInteger recCount = new AtomicInteger();

        receiver.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
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
            }.start();
        }

        while (System.currentTimeMillis() < start + dur) {
            Thread.sleep(10);
        }
        long cnt = sender.getMessageCount();
        log.info("Messages sent: " + cnt + " received: " + recCount.get() + " in " + dur + "ms");
        sender.terminate();
        receiver.terminate();
    }

    @Test
    public void writeExclusiveRec() throws Exception {
//        morphium.getConfig().setThreadPoolAsyncOpCoreSize(1000);
        Messaging sender = new Messaging(morphium, 100, false, true, 10);
        sender.start();
        Messaging receiver = new Messaging(morphium, 100, true, true, 100);
        receiver.start();
        Messaging receiver2 = new Messaging(morphium, 100, true, true, 100);
        receiver2.start();
        final AtomicInteger recCount = new AtomicInteger();

        receiver.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                recCount.incrementAndGet();
                return null;
            }
        });
        receiver2.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
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
                    m.setExclusive(true);
                    while (System.currentTimeMillis() < start + dur) {
                        sender.sendMessage(m);
                        m.setMsgId(null);
                    }
                }
            }.start();
        }

        while (System.currentTimeMillis() < start + dur) {
            Thread.sleep(10);
        }
        long cnt = sender.getMessageCount();
        log.info("Messages sent: " + cnt + " received: " + recCount.get() + " in " + dur + "ms");
        sender.terminate();
        receiver.terminate();
    }


}
