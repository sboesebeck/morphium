package de.caluga.test.morphium.driver.connection;/**
                                                   * Created by stephan on 30.11.15.
                                                   */

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerSettings;
import com.mongodb.internal.connection.MongoCredentialWithCache;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.test.OutputHelper;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * TODO: Add Documentation here
 **/
public class MorphiumDriverSpeedTest {

    private static int countObjs = 25;
    private Logger log = LoggerFactory.getLogger(MorphiumDriverSpeedTest.class);

    @Test
    public void mongoDbVsMorphiumSyncDriver() throws Exception {
        int amount = 3117;
        //warmup
        log.info("Warmup");
        runMongoDriver(10);
        runSingleConnect(10);
        Thread.sleep(100);
        log.info("Starting...");
        long start = System.currentTimeMillis();
        var mongo = runMongoDriver(amount);
        long dur = System.currentTimeMillis() - start;
        mongo.put("total", dur);
        start = System.currentTimeMillis();
        var single = runSingleConnect(amount);
        dur = System.currentTimeMillis() - start;
        single.put("total", dur);
        OutputHelper.figletOutput(log, "Reports:");
        log.info("SingleConnect Driver timings: ");

        for (var e : single.entrySet()) {
            log.info(e.getKey() + ": " + e.getValue());
        }

        log.info("Mongo Driver timings: ");

        for (var e : mongo.entrySet()) {
            log.info(e.getKey() + ": " + e.getValue());
        }

        log.info("Diffs:");

        for (var e : mongo.entrySet()) {
            long diff = e.getValue() - single.get(e.getKey());

            if (diff > 0) {
                log.info(e.getKey() + ": " + diff);
            } else {
                log.error(e.getKey() + ": " + diff);
            }
        }
    }

    public Map<String, Long> runSingleConnect(int amount) throws Exception {
        var m = new ObjectMapperImpl();
        Map<String, Long> ret = new HashMap<>();
        log.info("Using sync connection...");
        SingleMongoConnectDriver con = new SingleMongoConnectDriver();
        con.setHostSeed("localhost:27017");
        con.setUser("test");
        con.setPassword("test");
        con.setAuthDb("admin");
        con.setDefaultBatchSize(100);
        con.connect();
        log.info("Connected");
        ClearCollectionCommand clear = new ClearCollectionCommand(con.getConnection());
        clear.setDb("morphium_test").setColl("uncached_object");
        log.info("Deleted documents:" + clear.doClear());
        List<Map<String, Object>> dat = new ArrayList<>();

        for (int i = 0; i < amount; i++) {
            dat.add(m.serialize(new UncachedObject("str_value " + (i % 5), i)));
        }

        long start = System.currentTimeMillis();
        InsertMongoCommand insert = new InsertMongoCommand(con.getConnection()).setDocuments(dat).setColl("uncached_object").setDb("morphium_test");
        insert.execute();
        long dur = System.currentTimeMillis() - start;
        //log.info(String.format("Storing took %dms", dur));
        ret.put("storing", dur);
        start = System.currentTimeMillis();

        for (int i = 0; i < amount; i++) {
            FindCommand fnd = new FindCommand(con.getConnection());
            fnd.setColl("uncached_object").setDb("morphium_test").setFilter(Doc.of("counter", i));
            var it = fnd.executeIterable(con.getDefaultBatchSize());
            var d = it.next();
            assertEquals(i, d.get("counter"));
            assertEquals(Boolean.FALSE, it.hasNext());
        }

        dur = System.currentTimeMillis() - start;
        log.info(String.format("reading took %dms", dur));
        ret.put("reading", dur);
        con.close();
        return ret;
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
    public Map<String, Long> runMongoDriver(int amount) {
        Map<String, Long> ret = new HashMap<>();
        MongoClientSettings.Builder o = MongoClientSettings.builder();
        o.writeConcern(toMongoWriteConcern(de.caluga.morphium.driver.WriteConcern.getWc(1, true, 10000)));
        //read preference check
        o.retryReads(true);
        o.retryWrites(true);
        o.credential(MongoCredential.createCredential("test", "admin", "test".toCharArray()));
        o.applyToConnectionPoolSettings(connectionPoolSettings->{
            connectionPoolSettings.maxConnectionIdleTime(10000, TimeUnit.MILLISECONDS);
            connectionPoolSettings.maxConnectionLifeTime(60000, TimeUnit.MILLISECONDS);
            connectionPoolSettings.maintenanceFrequency(500, TimeUnit.MILLISECONDS);
            connectionPoolSettings.maxSize(100);
            connectionPoolSettings.minSize(10);
            connectionPoolSettings.maxWaitTime(10000, TimeUnit.MILLISECONDS);
        });
        o.applyToClusterSettings(clusterSettings->{
            clusterSettings.serverSelectionTimeout(10000, TimeUnit.MILLISECONDS);
            clusterSettings.mode(ClusterConnectionMode.SINGLE);
            List<ServerAddress> hosts = new ArrayList<>();
            hosts.add(new ServerAddress("127.0.0.1", 27017));
            clusterSettings.hosts(hosts);
            clusterSettings.serverSelectionTimeout(10000, TimeUnit.MILLISECONDS);
            clusterSettings.localThreshold(10000, TimeUnit.MILLISECONDS);
        });
        var m = new ObjectMapperImpl();
        log.info("Using Mongo Client...");
        MongoClient client = MongoClients.create(o.build());
        var delResult = client.getDatabase("morphium_test").getCollection("uncached_object").deleteMany(new Document());
        log.info("Deleted: " + delResult.getDeletedCount());
        List<Document> data = new ArrayList<>();

        for (int i = 0; i < amount; i++) {
            data.add(new Document(m.serialize(new UncachedObject("str_value " + (i % 5), i))));
        }

        long start = System.currentTimeMillis();
        //create elements
        client.getDatabase("morphium_test").getCollection("uncached_object").insertMany(data);
        long dur = System.currentTimeMillis() - start;
        //        log.info(String.format("Storing took %dms", dur));
        ret.put("storing", dur);
        start = System.currentTimeMillis();

        for (int i = 0; i < amount; i++) {
            //data.add(m.serialize(new UncachedObject("str_value "+(i%5),i)));
            Document query = new Document();
            query.put("counter", i);
            var result = client.getDatabase("morphium_test").getCollection("uncached_object").find().filter(new Document("counter", i));
            MongoCursor<Document> iterator = result.iterator();
            var document = iterator.next();
            assertEquals(i, document.get("counter"));
            assertEquals(Boolean.FALSE, iterator.hasNext());
        }

        dur = System.currentTimeMillis() - start;
        //        log.info(String.format("reading took %dms", dur));
        ret.put("reading", dur);
        client.close();
        return ret;
    }
}
