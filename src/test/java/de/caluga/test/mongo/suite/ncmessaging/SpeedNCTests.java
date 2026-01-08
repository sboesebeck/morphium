package de.caluga.test.mongo.suite.ncmessaging;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.SingleCollectionMessaging;
import de.caluga.morphium.messaging.Msg;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

@Disabled
@Tag("messaging")
public class SpeedNCTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void writeSpeed(Morphium morphium) throws Exception  {
        morphium.clearCollection(Msg.class);
        SingleCollectionMessaging msg = new SingleCollectionMessaging(morphium, 100, false, true, 10);
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

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void writeRecSpeed(Morphium morphium) throws Exception  {
        morphium.clearCollection(Msg.class);
//        morphium.getConfig().setThreadPoolAsyncOpCoreSize(1000);
        SingleCollectionMessaging sender = new SingleCollectionMessaging(morphium, 100, false, true, 10);
        sender.setUseChangeStream(false).start();
        SingleCollectionMessaging receiver = new SingleCollectionMessaging(morphium, 100, true, true, 100);
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

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void writeExclusiveRec(Morphium morphium) throws Exception  {
//        morphium.getConfig().setThreadPoolAsyncOpCoreSize(1000);
        morphium.clearCollection(Msg.class);
        SingleCollectionMessaging sender = new SingleCollectionMessaging(morphium, 100, false, true, 10);
        sender.setUseChangeStream(false).start();
        SingleCollectionMessaging receiver = new SingleCollectionMessaging(morphium, 100, true, true, 100);
        receiver.setUseChangeStream(false).start();
        SingleCollectionMessaging receiver2 = new SingleCollectionMessaging(morphium, 100, true, true, 100);
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
