package de.caluga.test.mongo.suite.ncmessaging;

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.StdMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

@Disabled
@Tag("messaging")
public class SpeedNCTests extends MorphiumTestBase {

    @Test
    public void writeSpeed() throws Exception {
        morphium.clearCollection(Msg.class);
        StdMessaging msg = new StdMessaging(morphium, 100, false, true, 10);
        msg.setUseChangeStream(false).start();


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
            } .start();
        }
        while (System.currentTimeMillis() < start + dur) {
            Thread.sleep(10);
        }
        long cnt = morphium.createQueryFor(Msg.class).countAll();
        log.info("stored msg: " + cnt + " in " + dur + "ms");
        msg.terminate();
    }

    @Test
    public void writeRecSpeed() throws Exception {
        morphium.clearCollection(Msg.class);
//        morphium.getConfig().setThreadPoolAsyncOpCoreSize(1000);
        StdMessaging sender = new StdMessaging(morphium, 100, false, true, 10);
        sender.setUseChangeStream(false).start();
        StdMessaging receiver = new StdMessaging(morphium, 100, true, true, 100);
        receiver.setUseChangeStream(false).start();
        final AtomicInteger recCount = new AtomicInteger();

        receiver.addListenerForTopic("test", new MessageListener() {
            @Override
            public Msg onMessage(MorphiumMessaging msg, Msg m)  {
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
            } .start();
        }

        while (System.currentTimeMillis() < start + dur) {
            Thread.sleep(10);
        }
        long cnt = morphium.createQueryFor(Msg.class).countAll();
        log.info("Messages sent: " + cnt + " received: " + recCount.get() + " in " + dur + "ms");
        sender.terminate();
        receiver.terminate();
    }

    @Test
    public void writeExclusiveRec() throws Exception {
//        morphium.getConfig().setThreadPoolAsyncOpCoreSize(1000);
        morphium.clearCollection(Msg.class);
        StdMessaging sender = new StdMessaging(morphium, 100, false, true, 10);
        sender.setUseChangeStream(false).start();
        StdMessaging receiver = new StdMessaging(morphium, 100, true, true, 100);
        receiver.setUseChangeStream(false).start();
        StdMessaging receiver2 = new StdMessaging(morphium, 100, true, true, 100);
        receiver2.setUseChangeStream(false).start();
        final AtomicInteger recCount = new AtomicInteger();

        receiver.addListenerForTopic("test", new MessageListener() {
            @Override
            public Msg onMessage(MorphiumMessaging msg, Msg m)  {
                recCount.incrementAndGet();
                return null;
            }
        });
        receiver2.addListenerForTopic("test", new MessageListener() {
            @Override
            public Msg onMessage(MorphiumMessaging msg, Msg m)  {
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
            } .start();
        }

        while (System.currentTimeMillis() < start + dur) {
            Thread.sleep(10);
        }
        long cnt = morphium.createQueryFor(Msg.class).countAll();
        log.info("Messages sent: " + cnt + " received: " + recCount.get() + " in " + dur + "ms");
        sender.terminate();
        receiver.terminate();
    }


}
