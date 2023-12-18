package de.caluga.test.morphium.driver.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.List;
import java.util.Map;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.commands.CountMongoCommand;
import de.caluga.morphium.driver.commands.DropDatabaseMongoCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.ShutdownCommand;
import de.caluga.morphium.driver.wire.PooledDriver;


@Disabled
public class FailoverTests {
    private Logger log=LoggerFactory.getLogger(FailoverTests.class);
    @Test
    public void testPrimaryFailover() throws Exception {
        log.info("Not testing failover!!!!");
        if (true)return;
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
                log.error("write failed " + e.getMessage());
                // e.printStackTrace();
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
        long start = System.currentTimeMillis();

        while (true) {
            cnt = 0;

            for (var e : drv.getNumConnectionsByHost().entrySet()) {
                if (e.getValue() > 0) {
                    cnt++;
                }
            }

            if (cnt == 3) {
                log.info("got 3 - retrying...");
                Thread.sleep(5000);

                cnt = 0;

                for (var e : drv.getNumConnectionsByHost().entrySet()) {
                    if (e.getValue() > 0) {
                        cnt++;
                    }
                }
                if (cnt==3) break;
            }

            Thread.sleep(1000);
            log.info("Host-Seed still: " + cnt);
            assertTrue(System.currentTimeMillis() - start < 180000);
        }

        log.info("Waiting for master...");


        log.info("Got all connections...");
        for (int i = 2000; i < 2100; i++) {
            InsertMongoCommand insert = null;

            try {
                insert = new InsertMongoCommand(drv.getPrimaryConnection(null));
                insert.setDocuments(List.of(Map.of("_id", new ObjectId(), "val", i, "str", "Str" + i), Map.of("_id", new ObjectId(), "val", i + 100, "str", "Str" + (i + 100))));
                insert.setDb("test");
                insert.setColl("tst_data");
                log.info("Writing to: " + insert.getConnection().getConnectedTo());
                insert.execute();
                insert.releaseConnection();
                Thread.sleep(100);
            } catch (Exception e) {
                log.error("write failed " + e.getMessage());
                e.printStackTrace();
                errors++;
            }

            if (insert != null) {
                insert.releaseConnection();
            }
        }
        assertTrue(errors < 10);
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

}
