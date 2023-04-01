package de.caluga.test.morphium.driver.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.ClusterConnectionMode;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.commands.CountMongoCommand;
import de.caluga.morphium.driver.commands.DropDatabaseMongoCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.HelloCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.ShutdownCommand;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.writer.MorphiumWriterImpl;
import de.caluga.test.mongo.suite.data.UncachedObject;

public class PooledDriverTest {
    private Logger log = LoggerFactory.getLogger(PooledDriverTest.class);
    private int amount = 1000;

    private Object sync = new Object();

    @Test
    public void testPooledConnections() throws Exception {
        PooledDriver drv = getDriver();
        log.info("Connecting...");
        drv.connect();
        Thread.sleep(1000);
        log.info("Checking status");

        for (var e : drv.getNumConnectionsByHost().entrySet()) {
            log.info("Host: " + e.getKey() + " connections: " + e.getValue());
            assertTrue(e.getValue() <= 10 && e.getValue() >= 2);
        }

        drv.close();
    }

    @Test
    public void comparePooledDriverMongoDriver() throws Exception {
        testCRUDPooledDriver();
        crudTestMongoDriver();
    }

    public void testCRUDPooledDriver() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl();
        long start = System.currentTimeMillis();
        var drv = getDriver();
        drv.setMaxConnectionLifetime(45000);
        drv.setMaxConnectionIdleTime(44000);

        try (drv) {
            drv.connect();
            //dropping testdb
            var con = drv.getPrimaryConnection(null);
            DropDatabaseMongoCommand cmd = new DropDatabaseMongoCommand(con);
            cmd.setDb("morphium_test");
            cmd.execute();
            drv.releaseConnection(con);

            for (int i = 0; i < amount; i++) {
                if (i % 100 == 0) {
                    log.info(String.format("Stored %s/%s", i, amount));
                }

                con = drv.getPrimaryConnection(null);
                InsertMongoCommand insert = new InsertMongoCommand(con);
                insert.setDocuments(Arrays.asList(om.serialize(new UncachedObject("value_" + (i + amount), i + amount)), om.serialize(new UncachedObject("value_" + i, i))));
                insert.setColl("uncached_object").setDb("morphium_test");
                var m = insert.execute();
                assertEquals(2, m.get("n"), "should insert 2 elements");
                //log.info(Utils.toJsonString(m));
                drv.releaseConnection(con);
            }

            drv.releaseConnection(con);
            long d = System.currentTimeMillis() - start;
            log.info("Writing took: " + d);
            start = System.currentTimeMillis();
            //            log.info("Waiting for reconnects due to idle / max age connections");
            //            Thread.sleep(1000); //forcing idle reconnects
            //reading
            var c = drv.getReadConnection(ReadPreference.secondaryPreferred());

            for (int i = 0; i < amount; i++) {
                //log.info("got connection to " + c.getConnectedTo());
                FindCommand fnd = new FindCommand(c).setLimit(10).setColl("uncached_object").setDb("morphium_test").setFilter(Doc.of("counter", i));
                var ret = fnd.execute();
                assertNotNull(ret);
                assertEquals(ret.size(), 1, "Should find exactly one element");
            }

            drv.releaseConnection(con);
        }

        long dur = System.currentTimeMillis() - start;
        log.info("PooledDriver took " + dur + "ms");
        drv.close();
    }

    private com.mongodb.WriteConcern toMongoWriteConcern(de.caluga.morphium.driver.WriteConcern w) {
        com.mongodb.WriteConcern wc = w.getW() > 0 ? com.mongodb.WriteConcern.ACKNOWLEDGED : com.mongodb.WriteConcern.UNACKNOWLEDGED;

        if (w.getW() > 0) {
            if (w.getWtimeout() > 0) {
                wc.withWTimeout(w.getWtimeout(), TimeUnit.MILLISECONDS);
            }
        }

        return wc;
    }

    public void crudTestMongoDriver() {
        long start = System.currentTimeMillis();
        MongoClientSettings.Builder o = MongoClientSettings.builder();
        o.writeConcern(toMongoWriteConcern(de.caluga.morphium.driver.WriteConcern.getWc(1, true, 1000)));
        //read preference check
        o.credential(MongoCredential.createCredential("test", "admin", "test".toCharArray()));
        o.retryReads(true);
        o.retryWrites(true);
        o.applyToSocketSettings(socketSettings->{
            socketSettings.connectTimeout(1000, TimeUnit.MILLISECONDS);
            socketSettings.readTimeout(10000, TimeUnit.MILLISECONDS);
        });
        o.applyToConnectionPoolSettings(connectionPoolSettings->{
            connectionPoolSettings.maxConnectionIdleTime(500, TimeUnit.MILLISECONDS);
            connectionPoolSettings.maxConnectionLifeTime(1000, TimeUnit.MILLISECONDS);
            connectionPoolSettings.maintenanceFrequency(2000, TimeUnit.MILLISECONDS);
            connectionPoolSettings.maxSize(10);
            connectionPoolSettings.minSize(2);
            connectionPoolSettings.maxWaitTime(10000, TimeUnit.MILLISECONDS);
        });
        o.applyToClusterSettings(clusterSettings->{
            clusterSettings.serverSelectionTimeout(1000, TimeUnit.MILLISECONDS);
            clusterSettings.mode(ClusterConnectionMode.MULTIPLE);
            List<ServerAddress> hosts = new ArrayList<>();
            hosts.add(new ServerAddress("localhost", 27017));
            hosts.add(new ServerAddress("localhost", 27018));
            hosts.add(new ServerAddress("localhost", 27019));

            clusterSettings.hosts(hosts);
            clusterSettings.serverSelectionTimeout(1000, TimeUnit.MILLISECONDS);
            clusterSettings.localThreshold(100, TimeUnit.MILLISECONDS);
        });
        var mongo = MongoClients.create(o.build());
        var col = mongo.getDatabase("morphium_test").getCollection("uncached_object");
        col.drop();
        ObjectMapperImpl om = new ObjectMapperImpl();
        //Write

        for (int i = 0; i < amount; i++) {
            if (i % 100 == 0) {
                log.info(String.format("Stored %s/%s", i, amount));
            }

            col = mongo.getDatabase("morphium_test").getCollection("uncached_object");
            var ret =
             col.insertMany(Arrays.asList(new Document(om.serialize(new UncachedObject("value_" + (i + amount), i + amount))), new Document(om.serialize(new UncachedObject("value_" + i, i)))));
            assertEquals(2, ret.getInsertedIds().size(), "should insert 2 elements");
        }

        long d = System.currentTimeMillis() - start;
        log.info("Writing took " + d);
        start = System.currentTimeMillis();

        //reading
        for (int i = 0; i < amount; i++) {
            var it = mongo.getDatabase("morphium_test").getCollection("uncached_object").find(new Document(Doc.of("counter", i)));
            int cnt = 0;

            for (var m : it) {
                cnt++;
            }

            assertEquals(1, cnt);
        }

        long dur = System.currentTimeMillis() - start;
        log.info("MongoClient took " + dur + "ms");
    }

    private PooledDriver getDriver() throws MorphiumDriverException {
        var drv = new PooledDriver();
        drv.setCredentials("admin", "test", "test");
        drv.setHostSeed("127.0.0.1:27017", "127.0.0.1:27018", "127.0.0.1:27019");
        drv.setMaxConnectionsPerHost(105);
        drv.setMinConnectionsPerHost(2);
        drv.setConnectionTimeout(5000);
        drv.setMaxConnectionLifetime(1000);
        drv.setMaxConnectionIdleTime(500);
        drv.setDefaultReadPreference(ReadPreference.nearest());
        drv.setHeartbeatFrequency(1000);
        return drv;
    }

    @Test
    public void testMultithreaddedConnectionPool() throws Exception {
        var drv = getDriver();
        drv.connect();

        while (!drv.isConnected()) {
            Thread.sleep(500);
        }

        log.info("Connected...");
        int loops = 25;
        int readThreads = 25;
        int primaryThreads = 20;
        var error = new AtomicInteger(0);
        List<Thread> threads = new ArrayList<>();
        var primaryRun = new Runnable() {
            public void run() {
                for (int i = 0; i < loops; i++) {
                    try {
                        var con = drv.getPrimaryConnection(null);
                        Thread.sleep((long)(1000 * Math.random()));
                        var res = new HelloCommand(con);
                        res.setIncludeClient(false);
                        var r = res.execute();
                        res.releaseConnection();

                        con=drv.getPrimaryConnection(null);
                        InsertMongoCommand cmd=new InsertMongoCommand(con);
                        cmd.setDb("testdb").setColl("testcoll").setDocuments(List.of(Doc.of("value","Hello world")));
                        cmd.execute();
                        cmd.releaseConnection();

                    } catch (Exception e) {
                        log.error("error", e);
                        error.incrementAndGet();
                    }
                }
            }
        };

        for (int i = 0; i < primaryThreads; i++) {
            Thread thr = new Thread(primaryRun);
            threads.add(thr);
        }

        var runnable = new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < loops; i++) {
                    try {
                        var con = drv.getReadConnection(null);
                        Thread.sleep((long)(1000 * Math.random()));
                        var res = new HelloCommand(con);
                        res.setIncludeClient(false);
                        var r = res.execute();
                        res.releaseConnection();
                    } catch (Exception e) {
                        log.error("error", e);
                        error.incrementAndGet();
                    }
                }
            }
        };

        for (int i = 0; i < readThreads; i++) {
            Thread thr = new Thread(runnable);
            threads.add(thr);
        }

        for (Thread t : threads) {
            t.start();
        }

        log.info("All threads started...");

        for (var t : threads) {
            t.join();
            log.info("Thread finished...");
        }

        log.info("All threads finished!");
        assertEquals(0, error.get(), "Too man errors");
        Map<MorphiumDriver.DriverStatsKey, Double> driverStats = drv.getDriverStats();

        for (var e : driverStats.entrySet()) {
            log.info(e.getKey() + "  =  " + e.getValue());
        }

        assertEquals(driverStats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_OPENED).doubleValue(),
         driverStats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_CLOSED) + driverStats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_IN_POOL), 0);
        drv.close();
    }

    @Test
    public void testPrimaryFailover() throws Exception {
        var drv = getDriver();
        drv.setHeartbeatFrequency(500);
        drv.connect();

        while (!drv.isConnected()) {
            Thread.sleep(500);
        }

        DropDatabaseMongoCommand drop = new DropDatabaseMongoCommand(drv.getPrimaryConnection(null));
        drop.setDb("test");
        drop.setComment("Killing it");
        drop.execute();
        drop.releaseConnection();
        Thread.sleep(1000);
        //generating some Testdata

        for (int i = 0; i < 100; i++) {
            InsertMongoCommand insert = new InsertMongoCommand(drv.getPrimaryConnection(null));
            insert.setDocuments(List.of(Map.of("_id", new ObjectId(), "val", i, "str", "Str" + i), Map.of("_id", new ObjectId(), "val", i + 100, "str", "Str" + (i + 100))));
            insert.setDb("test");
            insert.setColl("tst_data");
            log.info("Writing to: " + insert.getConnection().getConnectedTo());
            insert.execute();
            insert.releaseConnection();
        }

        log.info("Testdata created!");
        CountMongoCommand count = new CountMongoCommand(drv.getPrimaryConnection(null));
        var cnt = count.setDb("test").setColl("tst_data").setQuery(Map.of()).getCount();
        assertEquals(200, cnt);
        count.releaseConnection();
        //recovering on primary fail...
        ShutdownCommand shutdown = new ShutdownCommand(drv.getPrimaryConnection(null));
        log.info("Shutting down: " + shutdown.getConnection().getConnectedTo());
        shutdown.setDb("admin");
        shutdown.executeAsync(); //needs to be async, as command never returns
        shutdown.releaseConnection();
        //one secondary down
        Thread.sleep(1000);
        var errors = 0;

        for (int i = 1000; i < 1100; i++) {
            InsertMongoCommand insert = null;

            try {
                insert = new InsertMongoCommand(drv.getPrimaryConnection(null));
                insert.setDocuments(List.of(Map.of("_id", new ObjectId(), "val", i, "str", "Str" + i), Map.of("_id", new ObjectId(), "val", i + 100, "str", "Str" + (i + 100))));
                insert.setDb("test");
                insert.setColl("tst_data");
                log.info("Writing to: " + insert.getConnection().getConnectedTo());
                insert.execute();
                insert.releaseConnection();
            } catch (Exception e) {
                log.error("find failed " + e.getMessage());
                e.printStackTrace();
                errors++;
            }

            if (insert != null) {
                insert.releaseConnection();
            }
        }

        log.info(String.format("Number of errors during write: %s", errors));

        for (String h : drv.getHostSeed()) {
            log.info("Host: " + h);
        }

        log.info("Waiting for you to restart node...");
        Thread.sleep(5000);
        drv.close();
    }

    // @Test
    // public void testSecondaryFailover() throws Exception {
    //     var drv = getDriver();
    //     drv.setHeartbeatFrequency(500);
    //     drv.connect();
    //
    //     while (!drv.isConnected()) {
    //         Thread.sleep(500);
    //     }
    //
    //     DropDatabaseMongoCommand drop = new DropDatabaseMongoCommand(drv.getPrimaryConnection(null));
    //     drop.setDb("test");
    //     drop.setComment("Killing it");
    //     drop.execute();
    //     drop.releaseConnection();
    //     Thread.sleep(1000);
    //     //generating some Testdata
    //
    //     for (int i = 0; i < 100; i++) {
    //         InsertMongoCommand insert = new InsertMongoCommand(drv.getPrimaryConnection(null));
    //         insert.setDocuments(List.of(Map.of("_id", new ObjectId(), "val", i, "str", "Str" + i), Map.of("_id", new ObjectId(), "val", i + 100, "str", "Str" + (i + 100))));
    //         insert.setDb("test");
    //         insert.setColl("tst_data");
    //         insert.execute();
    //         insert.releaseConnection();
    //     }
    //
    //     log.info("Testdata created!");
    //     CountMongoCommand count = new CountMongoCommand(drv.getPrimaryConnection(null));
    //     var cnt = count.setDb("test").setColl("tst_data").setQuery(Map.of()).getCount();
    //     assertEquals(200, cnt);
    //     count.releaseConnection();
    //     //recovering on secondary fail...
    //     ShutdownCommand shutdown = new ShutdownCommand(drv.getReadConnection(ReadPreference.secondary()));
    //     log.info("Shutting down: " + shutdown.getConnection().getConnectedTo());
    //     shutdown.setDb("admin");
    //     shutdown.executeAsync(); //needs to be async, as command never returns
    //     shutdown.releaseConnection();
    //     //one secondary down
    //     Thread.sleep(1000);
    //     var errors = 0;
    //
    //     for (int i = 0; i < 100; i++) {
    //         FindCommand fnd = null;
    //
    //         try {
    //             log.info("finding...");
    //             fnd = new FindCommand(drv.getReadConnection(ReadPreference.secondary()));
    //             log.info("Reading from "+fnd.getConnection().getConnectedTo());;
    //             fnd.setFilter(Doc.of("str", "Str" + i));
    //             fnd.setColl("tst_data").setDb("test");
    //             var res = fnd.execute();
    //
    //             if (res == null || res.size() == 0) {
    //                 errors++;
    //             }
    //             log.info("Got results:"+res.size());
    //         } catch (Exception e) {
    //             log.error("find failed "+e.getMessage());
    //             e.printStackTrace();
    //             errors++;
    //         }
    //
    //         if (fnd != null) {
    //             fnd.releaseConnection();
    //         }
    //     }
    //     log.info(String.format("Number of errors during read: %s", errors));
    //
    //     for (String h:drv.getHostSeed()){
    //         log.info("Host: "+h);
    //     }
    //     assertEquals(2,drv.getHostSeed().size());
    //
    //     log.info("Waiting for you to restart node...");
    //     var start=System.currentTimeMillis();
    //     while (true){
    //         if(drv.getHostSeed().size()==3){
    //             break;
    //         }
    //         assertTrue(System.currentTimeMillis()-start < 60000);
    //
    //     }
    //     for (String h:drv.getHostSeed()){
    //         log.info("Host: "+h);
    //     }
    //
    //
    //     drv.close();
    // }

    @Test
    public void idleTimeoutTest() throws Exception {
        var drv = getDriver();
        drv.setMinConnections(2);
        drv.setHeartbeatFrequency(250);
        drv.setMaxConnectionLifetime(10000);
        drv.setMaxConnectionIdleTime(5000);
        drv.connect();

        try {
            while (!drv.isConnected()) {
                Thread.sleep(500);
            }

            log.info("Connected...");
            assertNotNull(drv.getNumConnectionsByHost());
            assertEquals(2, drv.getNumConnectionsByHost().get("127.0.0.1:27017"));
            Thread.sleep(8100);
            assertEquals(2, drv.getNumConnectionsByHost().get("127.0.0.1:27017"));
            var c1 = drv.getPrimaryConnection(null);
            var c2 = drv.getPrimaryConnection(null);
            var c3 = drv.getPrimaryConnection(null);
            var c4 = drv.getPrimaryConnection(null);
            assertEquals(4, drv.getNumConnectionsByHost().get("127.0.0.1:27017"));
            drv.releaseConnection(c1);
            drv.releaseConnection(c2);
            drv.releaseConnection(c3);
            drv.releaseConnection(c4);
            assertTrue(drv.getNumConnectionsByHost().get("127.0.0.1:27017") > 2);
            Thread.sleep(6100);
            assertEquals(2, drv.getNumConnectionsByHost().get("127.0.0.1:27017"));
        } finally {
            drv.close();
        }
    }

    @Test
    public void dropCmdTest() throws Exception {
        var drv = getDriver();
        drv.connect();
        new Thread(()->{
            try {
                synchronized (sync) {
                    sync.wait();
                }

                DropDatabaseMongoCommand drop = new DropDatabaseMongoCommand(drv.getPrimaryConnection(null));
                drop.setDb("test");
                drop.execute();
                drop.releaseConnection();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        new Thread(()->{
            try {
                synchronized (sync) {
                    sync.wait();
                }

                InsertMongoCommand insert = new InsertMongoCommand(drv.getPrimaryConnection(null));
                int i = 10;
                var om = new ObjectMapperImpl();
                insert.setDocuments(Arrays.asList(om.serialize(new UncachedObject("value_" + (i + amount), i + amount)), om.serialize(new UncachedObject("value_" + i, i))));
                insert.setColl("uncached_object").setDb("morphium_test");

                try {
                    var m = insert.execute();
                } catch (MorphiumDriverException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                insert.releaseConnection();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }).start();
        new Thread(()->{
            try {
                synchronized (sync) {
                    sync.wait();
                }

                var drop = new DropDatabaseMongoCommand(drv.getPrimaryConnection(null));
                drop.setDb("morphium_test");

                try {
                    drop.execute();
                } catch (MorphiumDriverException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                drop.releaseConnection();
                assertNull(drop.getConnection());
                assertTrue(((PooledDriver) drv).getBorrowedConnections().size() <= 1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        synchronized (sync) {
            sync.notifyAll();
        }

        Thread.sleep(1000);
    }
}
