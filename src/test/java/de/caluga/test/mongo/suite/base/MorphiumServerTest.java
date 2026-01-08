package de.caluga.test.mongo.suite.base;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Map;
import java.util.List;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.SingleCollectionMessaging;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
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

@Tag("server")
public class MorphiumServerTest {
    private Logger log = LoggerFactory.getLogger(MorphiumServerTest.class);
    private static final AtomicInteger PORT = new AtomicInteger(18000);

    private int nextPort() {
        // try to find a free port to avoid BindException when previous server still running
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (Exception e) {
            return PORT.incrementAndGet();
        }
    }


    @Test
    public void messagingPerformanceTest()throws Exception {
        int port = nextPort();
        var srv = new MorphiumServer(port, "localhost", 220, 20);
        startServer(srv, port);
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.clusterSettings().addHostToSeed("localhost:" + port);
        cfg.connectionSettings().setDatabase("test");
        cfg.connectionSettings().setMaxConnections(200);
        cfg.cacheSettings().setBufferedWritesEnabled(false);
        Morphium morphium = new Morphium(cfg);
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
        int amount = 2000;

        for (int i = 0; i < amount; i++) {
            Msg m = new Msg("test", "msg", "value");
            msg1.sendMessage(m);
            if (i % 100 == 0) {
                double dur = (double)(System.currentTimeMillis() - start);
                double speed = dur / (double)(i + 1);

                log.info("Msg #{} sent- duration {}ms per message", i, speed);
            }
        }

        log.info("Sent {} msgs, took {}ms - already got {}", amount, System.currentTimeMillis() - start, received.get());

        long deadline = System.currentTimeMillis() + 120_000;
        while (received.get() < amount && System.currentTimeMillis() < deadline) {
            double dur = (double)(System.currentTimeMillis() - start);
            double speed = dur / (double)received.get();
            log.info("not there {} yet: {} - roundtrip {}ms per message", amount, received.get(), speed);

            Thread.sleep(1000);
        }
        assertEquals(amount, received.get(), "Timed out waiting for all messages to be received");

        try {
            msg1.terminate();
            msg2.terminate();
            morphium.close();
        } finally {
            srv.shutdown();
        }
    }

    @Test
    public void singleConnectToServerTest()throws Exception {
        int port = nextPort();
        var srv = new MorphiumServer(port, "localhost", 20, 1);
        startServer(srv, port);
        SingleMongoConnectDriver drv = new SingleMongoConnectDriver();
        drv.setHostSeed("localhost:" + port);
        drv.setMaxConnections(10);
        drv.setHeartbeatFrequency(250);
        drv.setMaxWaitTime(0);
        drv.connect();
        log.info("connection established");
        SingleMongoConnectDriver drv2 = new SingleMongoConnectDriver();
        drv2.setHostSeed("localhost:" + port);
        drv2.setMaxWaitTime(0);
        drv2.setMaxConnections(10);
        drv2.setHeartbeatFrequency(250);
        drv2.connect();
        log.info("connection established");
        drv.close();
        drv2.close();
        srv.shutdown();
    }


    @Test
    public void testConnectionPool() throws Exception {
        int port = nextPort();
        var srv = new MorphiumServer(port, "localhost", 10, 1);
        startServer(srv, port);
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:" + port);
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
        srv.shutdown();
    }

    @Test
    public void testServerMessaging() throws Exception {
        int port = nextPort();
        var srv = new MorphiumServer(port, "localhost", 100, 1);
        startServer(srv, port);
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:" + port);
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
        } finally {
            srv.shutdown();
        }
    }

    @Test
    public void testServerMessagingMoreThanOne() throws Exception {
        int port = nextPort();
        var srv = new MorphiumServer(port, "localhost", 100, 1);
        startServer(srv, port);
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:" + port);
        cfg.setDatabase("srvtst");
        cfg.setMaxConnections(10);
        Morphium morphium = new Morphium(cfg);
        MorphiumConfig cfg2 = new MorphiumConfig();
        cfg2.setHostSeed("localhost:" + port);
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
        } finally {
            srv.shutdown();
        }
    }

    @Test
    public void testServer() throws Exception {
        int port = nextPort();
        var srv = new MorphiumServer(port, "localhost", 100, 1);
        startServer(srv, port);
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:" + port);
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
        } finally {
            srv.shutdown();
        }
    }


    @Test
    public void multithreaddedMessagingTest() throws Exception {
        int port = nextPort();
        var srv = new MorphiumServer(port, "localhost", 100, 60);  // 60 second idle timeout
        startServer(srv, port);
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:" + port);
        cfg.setDatabase("srvtst");
        cfg.setMaxConnections(10);
        Morphium morphium = new Morphium(cfg);
        MorphiumConfig cfg2 = new MorphiumConfig();
        cfg2.setHostSeed("localhost:" + port);
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
        } finally {
            srv.shutdown();
        }
    }

    @Test
    public void multithreaddedMessaging() throws Exception {
        int port = PORT.incrementAndGet();
        var srv = new MorphiumServer(port, "localhost", 100, 60);  // 60 second idle timeout
        startServer(srv, port);
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:" + port);
        cfg.setDatabase("srvtst");
        cfg.setMaxConnections(10);
        Morphium morphium = new Morphium(cfg);
        MorphiumConfig cfg2 = new MorphiumConfig();
        cfg2.setHostSeed("localhost:" + port);
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
            msg1.terminate();
            msg2.terminate();

        } finally {
            morphium.close();
            morphium2.close();
            srv.shutdown();
        }
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
    //         producer.shutdown();
    //         consumer.shutdown();
    //     }
    // }

    @Test
    public void priorityBasedPrimarySelection() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        MorphiumServer s1 = new MorphiumServer(port1, "localhost", 10, 1);
        MorphiumServer s2 = new MorphiumServer(port2, "localhost", 10, 1);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2);
        var prio = Map.of("localhost:" + port1, 200, "localhost:" + port2, 100);
        s1.configureReplicaSet("rsTest", hosts, prio);
        s2.configureReplicaSet("rsTest", hosts, prio);
        startServer(s1, port1);
        startServer(s2, port2);
        Thread.sleep(500);
        assertTrue(s1.isPrimary());
        assertFalse(s2.isPrimary());
        s1.shutdown();
        s2.shutdown();
    }

    // Test removed - stepDown functionality no longer exists in simplified MorphiumServer
    // @Test
    // public void stepDownPromotesNextPriority() throws Exception { ... }

    private void startServer(MorphiumServer srv, int port) throws Exception {
        srv.start();
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("localhost", port), 250);
                return;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(50);
            }
        }
    }

    @Test
    public void testReplicaSetDataReplication() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();

        // Start primary server
        MorphiumServer primary = new MorphiumServer(port1, "localhost", 10, 1);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2);
        var prio = Map.of("localhost:" + port1, 300, "localhost:" + port2, 100);
        primary.configureReplicaSet("rsDataTest", hosts, prio);
        startServer(primary, port1);

        // Connect to primary and store some data BEFORE secondary starts
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:" + port1);
        cfg.setDatabase("replicatest");
        cfg.setMaxConnections(5);
        Morphium morphiumPrimary = new Morphium(cfg);

        // Store 10 documents on primary
        for (int i = 0; i < 10; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i);
            uc.setStrValue("PreSync-" + i);
            morphiumPrimary.store(uc);
        }
        log.info("Stored 10 documents on primary before secondary started");

        // Verify data exists on primary
        var primaryCount = morphiumPrimary.createQueryFor(UncachedObject.class).countAll();
        assertEquals(10, primaryCount, "Primary should have 10 documents");

        // Now start secondary server
        MorphiumServer secondary = new MorphiumServer(port2, "localhost", 10, 1);
        secondary.configureReplicaSet("rsDataTest", hosts, prio);
        startServer(secondary, port2);

        // Give time for initial sync to complete
        Thread.sleep(3000);

        // Connect to secondary
        MorphiumConfig cfg2 = new MorphiumConfig();
        cfg2.setHostSeed("localhost:" + port2);
        cfg2.setDatabase("replicatest");
        cfg2.setMaxConnections(5);
        Morphium morphiumSecondary = new Morphium(cfg2);

        // Verify data was replicated to secondary
        var secondaryCount = morphiumSecondary.createQueryFor(UncachedObject.class).countAll();
        log.info("Secondary has {} documents after initial sync", secondaryCount);
        assertEquals(10, secondaryCount, "Secondary should have 10 documents after initial sync");

        // Verify the actual data is correct
        var docs = morphiumSecondary.createQueryFor(UncachedObject.class).sort("counter").asList();
        assertEquals(10, docs.size());
        for (int i = 0; i < 10; i++) {
            assertEquals(i, docs.get(i).getCounter());
            assertEquals("PreSync-" + i, docs.get(i).getStrValue());
        }
        log.info("All documents verified on secondary!");

        // Clean up
        morphiumPrimary.close();
        morphiumSecondary.close();
        primary.shutdown();
        secondary.shutdown();
    }

    @Test
    public void testReplicaSetOngoingReplication() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();

        // Start both servers
        MorphiumServer primary = new MorphiumServer(port1, "localhost", 10, 1);
        MorphiumServer secondary = new MorphiumServer(port2, "localhost", 10, 1);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2);
        var prio = Map.of("localhost:" + port1, 300, "localhost:" + port2, 100);
        primary.configureReplicaSet("rsOngoing", hosts, prio);
        secondary.configureReplicaSet("rsOngoing", hosts, prio);
        startServer(primary, port1);
        startServer(secondary, port2);

        // Give time for replication to set up
        Thread.sleep(2000);

        // Connect to primary
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:" + port1);
        cfg.setDatabase("ongoingtest");
        cfg.setMaxConnections(5);
        Morphium morphiumPrimary = new Morphium(cfg);

        // Connect to secondary
        MorphiumConfig cfg2 = new MorphiumConfig();
        cfg2.setHostSeed("localhost:" + port2);
        cfg2.setDatabase("ongoingtest");
        cfg2.setMaxConnections(5);
        Morphium morphiumSecondary = new Morphium(cfg2);

        // Store documents on primary AFTER secondary is running
        for (int i = 0; i < 5; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i);
            uc.setStrValue("Ongoing-" + i);
            morphiumPrimary.store(uc);
        }
        log.info("Stored 5 documents on primary");

        // Wait for change stream replication
        Thread.sleep(2000);

        // Verify data was replicated to secondary via change stream
        var secondaryCount = morphiumSecondary.createQueryFor(UncachedObject.class).countAll();
        log.info("Secondary has {} documents after change stream replication", secondaryCount);
        assertEquals(5, secondaryCount, "Secondary should have 5 documents from change stream");

        // Clean up
        morphiumPrimary.close();
        morphiumSecondary.close();
        primary.shutdown();
        secondary.shutdown();
    }

    @Test
    public void testLateJoiningNodeInitialSync() throws Exception {
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        MorphiumServer primary = new MorphiumServer(port1, "localhost", 10, 1);
        MorphiumServer secondary = new MorphiumServer(port2, "localhost", 10, 1);
        MorphiumServer lateJoiner = new MorphiumServer(port3, "localhost", 10, 1);
        var hosts = List.of("localhost:" + port1, "localhost:" + port2, "localhost:" + port3);
        var prio = Map.of("localhost:" + port1, 300, "localhost:" + port2, 200, "localhost:" + port3, 100);
        primary.configureReplicaSet("rsLateJoin", hosts, prio);
        secondary.configureReplicaSet("rsLateJoin", hosts, prio);
        lateJoiner.configureReplicaSet("rsLateJoin", hosts, prio);
        startServer(primary, port1);
        startServer(secondary, port2);

        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:" + port1);
        cfg.setDatabase("latejoin");
        cfg.setMaxConnections(5);
        Morphium morphiumPrimary = new Morphium(cfg);

        for (int i = 0; i < 7; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i);
            uc.setStrValue("LateJoin-" + i);
            morphiumPrimary.store(uc);
        }

        var primaryCount = morphiumPrimary.createQueryFor(UncachedObject.class).countAll();
        assertEquals(7, primaryCount, "Primary should have inserted documents");

        startServer(lateJoiner, port3);
        Thread.sleep(3000);

        MorphiumConfig cfgLate = new MorphiumConfig();
        cfgLate.setHostSeed("localhost:" + port3);
        cfgLate.setDatabase("latejoin");
        cfgLate.setMaxConnections(5);
        Morphium morphiumLate = new Morphium(cfgLate);

        var lateCount = morphiumLate.createQueryFor(UncachedObject.class).countAll();
        assertEquals(7, lateCount, "Late joining node should replicate existing data via initial sync");

        morphiumPrimary.close();
        morphiumLate.close();
        primary.shutdown();
        secondary.shutdown();
        lateJoiner.shutdown();
    }
}
