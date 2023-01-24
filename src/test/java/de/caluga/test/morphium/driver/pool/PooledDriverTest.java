package de.caluga.test.morphium.driver.pool;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.ClusterConnectionMode;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.commands.DropDatabaseMongoCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;


public class PooledDriverTest {
    private Logger log = LoggerFactory.getLogger(PooledDriverTest.class);

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
        try (drv) {
            drv.connect();
            //dropping testdb
            var con = drv.getPrimaryConnection(null);
            DropDatabaseMongoCommand cmd = new DropDatabaseMongoCommand(con);
            cmd.setDb("morphium_test");
            cmd.execute();
            con.release();
            //Write
            int amount = 1000;
            for (int i = 0; i < amount; i++) {
                con = drv.getPrimaryConnection(null);
                InsertMongoCommand insert = new InsertMongoCommand(con);
                insert.setDocuments(Arrays.asList(
                        om.serialize(new UncachedObject("value_" + (i + amount), i + amount)),
                        om.serialize(new UncachedObject("value_" + i, i))
                ));
                insert.setColl("uncached_object").setDb("morphium_test");
                var m = insert.execute();
                assertEquals( 2, m.get("n"),"should insert 2 elements");
                //log.info(Utils.toJsonString(m));
                con.release();
            }
            con.release();
            long d = System.currentTimeMillis() - start;
            log.info("Writing took: " + d);
            start = System.currentTimeMillis();
//            log.info("Waiting for reconnects due to idle / max age connections");
//            Thread.sleep(1000); //forcing idle reconnects

            //reading
            var c = drv.getReadConnection(ReadPreference.secondaryPreferred());
            for (int i = 0; i < amount; i++) {

                //log.info("got connection to " + c.getConnectedTo());
                FindCommand fnd = new FindCommand(c).setLimit(10).setColl("uncached_object").setDb("morphium_test")
                        .setFilter(Doc.of("counter", i));
                var ret = fnd.execute();
                assertNotNull(ret);
                assertEquals( ret.size(), 1,"Should find exactly one element");

            }
            c.release();
        }
        long dur = System.currentTimeMillis() - start;
        log.info("PooledDriver took " + dur + "ms");
        drv.close();
    }
    private com.mongodb.WriteConcern toMongoWriteConcern(de.caluga.morphium.driver.WriteConcern w){
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
        o.applyToSocketSettings(socketSettings -> {
            socketSettings.connectTimeout(1000, TimeUnit.MILLISECONDS);
            socketSettings.readTimeout(10000, TimeUnit.MILLISECONDS);
        });

        o.applyToConnectionPoolSettings(connectionPoolSettings -> {
            connectionPoolSettings.maxConnectionIdleTime(500, TimeUnit.MILLISECONDS);
            connectionPoolSettings.maxConnectionLifeTime(1000, TimeUnit.MILLISECONDS);
            connectionPoolSettings.maintenanceFrequency(2000, TimeUnit.MILLISECONDS);
            connectionPoolSettings.maxSize(10);
            connectionPoolSettings.minSize(2);
            connectionPoolSettings.maxWaitTime(10000, TimeUnit.MILLISECONDS);
        });
        o.applyToClusterSettings(clusterSettings -> {
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
        int amount = 1000;
        for (int i = 0; i < amount; i++) {
            col = mongo.getDatabase("morphium_test").getCollection("uncached_object");
            var ret = col.insertMany(Arrays.asList(
                    new Document(om.serialize(new UncachedObject("value_" + (i + amount), i + amount))),
                    new Document(om.serialize(new UncachedObject("value_" + i, i)))));

            assertEquals(2, ret.getInsertedIds().size(),"should insert 2 elements");
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
        drv.setHostSeed("localhost:27017", "localhost:27018", "localhost:27019");
        drv.setMaxConnectionsPerHost(105);
        drv.setMinConnectionsPerHost(2);
        drv.setConnectionTimeout(5000);
        drv.setMaxConnectionLifetime(1000);
        drv.setMaxConnectionIdleTime(500);
        drv.setDefaultReadPreference(ReadPreference.nearest());
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
        int loops = 15;
        int readThreads = 15;
        int primaryThreads = 10;
        var error = new AtomicInteger(0);

        List<Thread> threads = new ArrayList<>();
        var primaryRun = new Runnable() {
            public void run() {

                for (int i = 0; i < loops; i++) {
                    try {
                        var con = drv.getPrimaryConnection(null);
                        Thread.sleep((long) (1000 * Math.random()));
                        con.release();
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
                        Thread.sleep((long) (1000 * Math.random()));
                        con.release();
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
        assertEquals(0, error.get(),"Too man errors");
        Map<MorphiumDriver.DriverStatsKey, Double> driverStats = drv.getDriverStats();
        for (var e : driverStats.entrySet()) {
            log.info(e.getKey() + "  =  " + e.getValue());
        }
        assertEquals(driverStats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_BORROWED), driverStats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_RELEASED), "Release vs. borrow do not match up!");
        assertEquals(driverStats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_OPENED).doubleValue(), driverStats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_CLOSED) + driverStats.get(MorphiumDriver.DriverStatsKey.CONNECTIONS_IN_POOL), 0);
        drv.close();
    }


}
