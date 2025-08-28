package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.StdMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.atomic.AtomicInteger;

public class SpeedTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesPooledOnly")
    public void writeSpeed(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.clearCollection(Msg.class);
            StdMessaging msg = new StdMessaging(morphium, 100,  true, 1);
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
                    } .start();
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
                    log.warn("Messaging termination failed!");
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void writeRecSpeed(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.clearCollection(Msg.class);
            //        morphium.getConfig().setThreadPoolAsyncOpCoreSize(1000);
            StdMessaging sender = new StdMessaging(morphium, 100,  true, 1);
            sender.start();
            StdMessaging receiver = new StdMessaging(morphium, 100,  true, 100);
            receiver.start();

            try {
                final AtomicInteger recCount = new AtomicInteger();
                receiver.addListenerForTopic("test", new MessageListener() {
                    @Override
                    public Msg onMessage(MorphiumMessaging msg, Msg m) {
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
            } finally {
                try {
                    sender.terminate();
                    receiver.terminate();
                } catch (Exception e) {
                    log.warn("Messageing termination failed!");
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void writeExclusiveRec(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            //        morphium.getConfig().setThreadPoolAsyncOpCoreSize(1000);
            morphium.clearCollection(Msg.class);
            StdMessaging sender = new StdMessaging(morphium, 100, true, 1);
            sender.start();
            StdMessaging receiver = new StdMessaging(morphium, 100,  true, 100);
            receiver.start();
            StdMessaging receiver2 = new StdMessaging(morphium, 100,  true, 100);
            receiver2.start();

            try {
                final AtomicInteger recCount = new AtomicInteger();
                receiver.addListenerForTopic("test", (msg, m) -> {
                    recCount.incrementAndGet();
                    return null;
                });
                receiver2.addListenerForTopic("test", (msg, m) -> {
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
                    } .start();
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
                    log.warn("Messageing termination failed!");
                }
            }
        }
    }


}
