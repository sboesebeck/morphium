package de.caluga.test.mongo.suite.base;


import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.SingleCollectionMessaging;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.changestream.ChangeStreamEvent;
import de.caluga.morphium.changestream.ChangeStreamListener;
import de.caluga.morphium.config.CollectionCheckSettings.CappedCheck;
import de.caluga.morphium.config.CollectionCheckSettings.IndexCheck;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.server.MorphiumServer;
import de.caluga.test.mongo.suite.data.UncachedObject;

@Disabled
public class MorphiumServerTest {
    private Logger log = LoggerFactory.getLogger(MorphiumServerTest.class);


    @Test
    public void messagingPerformanceTest()throws Exception {
        var srv = new MorphiumServer(17017, "localhost", 20, 1);
        srv.start();
        Morphium morphium = new Morphium("localhost:17017", "test");
        SingleCollectionMessaging msg1 = new SingleCollectionMessaging(morphium, 100, true);
        msg1.setUseChangeStream(true);
        AtomicInteger received = new AtomicInteger();
        SingleCollectionMessaging msg2 = new SingleCollectionMessaging(morphium, 100, true);
        msg2.setUseChangeStream(true);
        msg2.addListenerForTopic("test", (msg, m)-> {
            received.incrementAndGet();
            return null;
        });
        msg2.start();
        msg1.start();
        long start = System.currentTimeMillis();
        int amount = 10000;

        for (int i = 0; i < amount; i++) {
            Msg m = new Msg("test", "msg", "value");
            msg1.sendMessage(m);
        }

        log.info("Sent {} msgs, took {}ms - already got {}", amount, System.currentTimeMillis() - start, received.get());

        while (received.get() < amount) {
            log.info("not there {} yet: {}", amount, received.get());
            Thread.sleep(1000);
        }

        msg1.terminate();
        msg2.terminate();
        morphium.close();
        srv.terminate();
    }

    @Test
    public void singleConnectToServerTest()throws Exception {
        var srv = new MorphiumServer(17017, "localhost", 20, 1);
        srv.start();
        SingleMongoConnectDriver drv = new SingleMongoConnectDriver();
        drv.setHostSeed("localhost:17017");
        drv.setMaxConnections(10);
        drv.setHeartbeatFrequency(250);
        drv.setMaxWaitTime(0);
        drv.connect();
        log.info("connection established");
        SingleMongoConnectDriver drv2 = new SingleMongoConnectDriver();
        drv2.setHostSeed("localhost:17017");
        drv2.setMaxWaitTime(0);
        drv2.setMaxConnections(10);
        drv2.setHeartbeatFrequency(250);
        drv2.connect();
        log.info("connection established");
        drv.close();
        drv2.close();
        srv.terminate();
    }


    @Test
    public void testConnectionPool() throws Exception {
        var srv = new MorphiumServer(17017, "localhost", 10, 1);
        srv.start();
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:17017");
        cfg.setDatabase("srvtst");
        cfg.setMaxConnections(5);
        cfg.setMinConnections(2);
        cfg.setMaxConnectionIdleTime(1000);
        cfg.setMaxConnectionLifeTime(2000);
        Morphium morphium = new Morphium(cfg);
        // for (int i = 0; i < 15; i++) {
        //     log.info("PoolSize: {}", srv.getConnectionCount());
        //     morphium.store(new UncachedObject("Hello", i));
        //     log.info("Server Connections: {}", srv.getConnectionCount());
        //     assertEquals(i + 1, morphium.createQueryFor(UncachedObject.class).asList().size());
        //     assertEquals(2, srv.getConnectionCount());
        //     Thread.sleep(1230);
        // }
        // Messaging msg = new Messaging(morphium, 100, true);
        // msg.setUseChangeStream(true);
        // msg.start();
        //
        AtomicBoolean running = new AtomicBoolean(true);
        new Thread(()-> {
            while (running.get()) {
                try {
                    morphium.watch(UncachedObject.class, true, new ChangeStreamListener() {
                        @Override
                        public boolean incomingData(ChangeStreamEvent evt) {
                            return running.get();
                        }
                    });
                } catch (Exception e) {
                }
            }

            log.info("Thread finished!");

        }).start();

        for (int i = 0; i < 15; i++) {
            log.info("PoolSize: {}", srv.getConnectionCount());
            // msg.sendMessage(new Msg("test", "Ignore", "no listener"));
            Thread.sleep(1500);
        }

        running.set(false);
        morphium.close();
        srv.terminate();
    }

    @Test
    public void testServerMessaging() throws Exception {
        var srv = new MorphiumServer(17017, "localhost", 100, 1);
        srv.start();
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:17017");
        cfg.setDatabase("srvtst");
        cfg.setMaxConnections(10);
        Morphium morphium = new Morphium(cfg);
        final AtomicLong recTime = new AtomicLong(0l);

        try(morphium) {
            //Messaging test
            var msg1 = new SingleCollectionMessaging(morphium);
            msg1.start();
            var msg2 = new SingleCollectionMessaging(morphium);
            msg2.start();
            msg2.addListenerForTopic("tstmsg", new MessageListener() {
                @Override
                public Msg onMessage(MorphiumMessaging msg, Msg m) {
                    recTime.set(System.currentTimeMillis());
                    log.info("incoming mssage");
                    return null;
                }
            });
            long start = System.currentTimeMillis();
            msg1.sendMessage(new Msg("tstmsg", "hello", "value"));

            while (recTime.get() == 0L) {
                Thread.yield();
            }

            long dur = recTime.get() - start;
            log.info("Got message after {}ms", dur);
        }

        srv.terminate();
    }

    @Test
    public void testServerMessagingMoreThanOne() throws Exception {
        var srv = new MorphiumServer(17017, "localhost", 100, 1);
        srv.start();
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:17017");
        cfg.setDatabase("srvtst");
        cfg.setMaxConnections(10);
        Morphium morphium = new Morphium(cfg);
        MorphiumConfig cfg2 = new MorphiumConfig();
        cfg2.setHostSeed("localhost:17017");
        cfg2.setDatabase("srvtst");
        cfg2.setMaxConnections(10);
        Morphium morphium2 = new Morphium(cfg2);
        final AtomicLong recTime = new AtomicLong(0l);
        Thread.sleep(2500);

        try(morphium) {
            //Messaging test
            var msg1 = new SingleCollectionMessaging(morphium, 100, true);
            msg1.setUseChangeStream(true);
            msg1.start();
            var msg2 = new SingleCollectionMessaging(morphium2, 10, true, true, 1000);
            msg2.setUseChangeStream(true);
            msg2.start();
            msg2.addListenerForTopic("tstmsg", new MessageListener() {
                @Override
                public Msg onMessage(MorphiumMessaging msg, Msg m) {
                    recTime.set(System.currentTimeMillis());
                    log.info("incoming mssage");
                    return null;
                }
            });
            long start = System.currentTimeMillis();
            msg1.sendMessage(new Msg("tstmsg", "hello", "value"));

            while (recTime.get() == 0L) {
                Thread.yield();
            }

            long dur = recTime.get() - start;
            log.info("Got message after {}ms", dur);
        }

        srv.terminate();
    }

    @Test
    public void testServer() throws Exception {
        var srv = new MorphiumServer(17017, "localhost", 100, 1);
        srv.start();
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:17017");
        cfg.setDatabase("srvtst");
        cfg.setMaxConnections(10);
        cfg.setIndexCheck(IndexCheck.CREATE_ON_STARTUP);
        cfg.setCappedCheck(CappedCheck.CREATE_ON_STARTUP);
        Morphium morphium = new Morphium(cfg);
        final AtomicLong recTime = new AtomicLong(0l);
        new Thread() {
            public void run() {
                morphium.watch(UncachedObject.class, true, new ChangeStreamListener() {
                    @Override
                    public boolean incomingData(ChangeStreamEvent evt) {
                        log.info("Incoming....{} {} {}", evt.getCollectionName(), evt.getNs(), evt.getFullDocument());
                        return true;
                    }
                });
            }
        } .start();

        try(morphium) {
            for (int i = 0; i < 100; i++) {
                UncachedObject uc = new UncachedObject();
                uc.setCounter(i);
                uc.setStrValue("Counter-" + i);
                morphium.store(uc);
                log.info("Stored {}", i);
                Thread.sleep(250);
            }

            var lst = morphium.createQueryFor(UncachedObject.class).asList();
            assertEquals(100, lst.size());
        }

        srv.terminate();
    }


    @Test
    public void multithreaddedMessagingTest() throws Exception {
        var srv = new MorphiumServer(17017, "localhost", 100, 1);
        srv.start();
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:17017");
        cfg.setDatabase("srvtst");
        cfg.setMaxConnections(10);
        Morphium morphium = new Morphium(cfg);
        MorphiumConfig cfg2 = new MorphiumConfig();
        cfg2.setHostSeed("localhost:17017");
        cfg2.setDatabase("srvtst");
        cfg2.setMaxConnections(10);
        Morphium morphium2 = new Morphium(cfg2);
        final AtomicLong recAmount = new AtomicLong(0l);
        final Map<MorphiumId, Long> sendTimes = new ConcurrentHashMap<>();
        final Map<MorphiumId, Long> recTimes = new ConcurrentHashMap<>();
        Thread.sleep(2500);

        try(morphium) {
            //Messaging test
            var msg1 = morphium.createMessaging();
            msg1.setPause(1000).setMultithreadded(true);
            msg1.setUseChangeStream(true);
            msg1.start();
            var msg2 = morphium2.createMessaging();
            msg2.setPause(10).setMultithreadded(true).setWindowSize(1000);
            msg2.setUseChangeStream(true);
            msg2.start();
            // Thread.sleep(2500);
            msg2.addListenerForTopic("tstmsg", new MessageListener() {
                @Override
                public Msg onMessage(MorphiumMessaging msg, Msg m) {
                    recAmount.incrementAndGet();
                    recTimes.put(m.getMsgId(), System.currentTimeMillis());
                    log.info("incoming mssage after {}ms", System.currentTimeMillis() - m.getTimestamp());
                    return null;
                }
            });
            long start = System.currentTimeMillis();

            for (int i = 0; i < 100; i++) {
                var msg = new Msg("tstmsg", "hello" + i, "value" + i);
                msg1.sendMessage(msg);
                sendTimes.put(msg.getMsgId(), System.currentTimeMillis());
            }

            long dur = System.currentTimeMillis() - start;
            log.info("Sending took {}ms", dur);

            while (recAmount.get() != 100L) {
                Thread.yield();
            }

            dur = System.currentTimeMillis() - start;
            log.info("Got messages after {}ms", dur);
            long minDur = 999999;
            long maxDur = 0;
            long first = System.currentTimeMillis();
            long last = 0;

            for (var id : sendTimes.keySet()) {
                long d = recTimes.get(id) - sendTimes.get(id);

                if (d < minDur) minDur = d;

                if (d > maxDur) maxDur = d;

                if (start - recTimes.get(id) < first) {
                    first = start - recTimes.get(id);
                }

                if (start - recTimes.get(id) > last) {
                    last = start - recTimes.get(id);
                }
            }

            log.info("First Message after {}ms - last message after {}ms", first, last);
            log.info("Longest roundtrip {}ms - quickest {}ms", maxDur, minDur);
        }

        srv.terminate();
    }

    @Test
    public void multithreaddedMessaging() throws Exception {
        var srv = new MorphiumServer(17017, "localhost", 100, 1);
        srv.start();
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:17017");
        cfg.setDatabase("srvtst");
        cfg.setMaxConnections(10);
        Morphium morphium = new Morphium(cfg);
        MorphiumConfig cfg2 = new MorphiumConfig();
        cfg2.setHostSeed("localhost:17017");
        cfg2.setDatabase("srvtst");
        cfg2.setMaxConnections(10);
        Morphium morphium2 = new Morphium(cfg2);
        final AtomicLong recAmount = new AtomicLong(0l);
        Thread.sleep(2500);

        try(morphium) {
            //Messaging test
            var msg1 = morphium.createMessaging();
            msg1.setPause(100).setMultithreadded(true);
            msg1.setUseChangeStream(true);
            msg1.start();
            var msg2 = morphium2.createMessaging();
            msg2.setPause(10).setMultithreadded(true).setWindowSize(1000);
            msg2.setUseChangeStream(true);
            msg2.start();
            // Thread.sleep(2500);
            msg2.addListenerForTopic("tstmsg", new MessageListener() {
                @Override
                public Msg onMessage(MorphiumMessaging msg, Msg m) {
                    recAmount.incrementAndGet();

                    synchronized (recAmount) {
                        recAmount.notifyAll();
                    }

                    log.info("incoming mssage after {}ms", System.currentTimeMillis() - m.getTimestamp());
                    return null;
                }
            });

            for (int i = 0; i < 200; i++) {
                var msg = new Msg("tstmsg", "hello" + i, "value" + i);
                long start = System.currentTimeMillis();
                msg1.sendMessage(msg);
                log.info("Message sent...");

                while (recAmount.get() != i + 1) {
                    log.info("Waiting....{} != {}", i + 1, recAmount.get());

                    synchronized (recAmount) {
                        recAmount.wait();
                    }
                }

                log.info("Got it! {}", System.currentTimeMillis() - start);
                Thread.sleep(250);
            }
        }

        srv.terminate();
    }

    // @Test
    // public void mutlithreaddedMessagingPerformanceTest() throws Exception {
    //     var srv = new MorphiumServer(17017, "localhost", 100, 1);
    //     srv.start();
    //     MorphiumConfig cfg = new MorphiumConfig();
    //     cfg.setHostSeed("localhost:17017");
    //     cfg.setDatabase("srvtst");
    //     cfg.setMaxConnections(10);
    //     Morphium morphium = new Morphium(cfg);
    //     Thread.sleep(2500);
    //     morphium.clearCollection(Msg.class);
    //     final Messaging producer = new Messaging(morphium, 100, true);
    //     final Messaging consumer = new Messaging(morphium, 10, true, true, 2000);
    //     producer.setUseChangeStream(true);
    //     consumer.setUseChangeStream(true);
    //     consumer.start();
    //     producer.start();
    //     Thread.sleep(2500);
    //
    //     try {
    //         final AtomicInteger processed = new AtomicInteger();
    //         final Map<String, AtomicInteger> msgCountById = new ConcurrentHashMap<>();
    //         consumer.addListenerForMessageNamed("msg", (msg, m) -> {
    //             log.info("Incoming message...");
    //             processed.incrementAndGet();
    //
    //             if (processed.get() % 1000 == 0) {
    //                 log.info("Consumed " + processed.get());
    //             }
    //             assert(!msgCountById.containsKey(m.getMsgId().toString()));
    //             msgCountById.putIfAbsent(m.getMsgId().toString(), new AtomicInteger());
    //             msgCountById.get(m.getMsgId().toString()).incrementAndGet();
    //             //simulate processing
    //             try {
    //                 Thread.sleep((long)(10 * Math.random()));
    //             } catch (InterruptedException e) {
    //                 // e.printStackTrace();
    //             }
    //             return null;
    //         });
    //         int numberOfMessages = 1000;
    //
    //         for (int i = 0; i < numberOfMessages; i++) {
    //             Msg m = new Msg("msg", "m", "v");
    //             m.setTtl(5 * 60 * 1000);
    //             // if (i % 1000 == 0) {
    //             log.info("created msg " + i + " / " + numberOfMessages);
    //             // }
    //             producer.sendMessage(m);
    //         }
    //
    //         long start = System.currentTimeMillis();
    //
    //         while (processed.get() < numberOfMessages) {
    //             //            ThreadMXBean thbean = ManagementFactory.getThreadMXBean();
    //             //            log.info("Running threads: " + thbean.getThreadCount());
    //             log.info("Processed " + processed.get());
    //             Thread.sleep(1500);
    //         }
    //
    //         long dur = System.currentTimeMillis() - start;
    //         log.info("Processing took " + dur + " ms");
    //         assert(processed.get() == numberOfMessages);
    //
    //         for (String id : msgCountById.keySet()) {
    //             assert(msgCountById.get(id).get() == 1);
    //         }
    //     } finally {
    //         producer.terminate();
    //         consumer.terminate();
    //     }
    // }
}
