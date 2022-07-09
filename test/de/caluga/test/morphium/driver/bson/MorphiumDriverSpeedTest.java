package de.caluga.test.morphium.driver.bson;/**
 * Created by stephan on 30.11.15.
 */

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.connection.ClusterConnectionMode;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.ClearCollectionSettings;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.objectmapping.ObjectMapperImpl;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.bson.Document;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TODO: Add Documentation here
 **/
public class MorphiumDriverSpeedTest {

    private static int countObjs = 25;
    private Logger log = LoggerFactory.getLogger(MorphiumDriverSpeedTest.class);

    @Test
    public void mongoDbVsMorphiumSyncDriver() throws Exception {
        MongoClientSettings.Builder o = MongoClientSettings.builder();
        o.writeConcern(de.caluga.morphium.driver.WriteConcern.getWc(1, true, 10000).toMongoWriteConcern());
        //read preference check
        o.retryReads(true);
        o.retryWrites(true);
        o.applyToConnectionPoolSettings(connectionPoolSettings -> {
            connectionPoolSettings.maxConnectionIdleTime(10000, TimeUnit.MILLISECONDS);
            connectionPoolSettings.maxConnectionLifeTime(60000, TimeUnit.MILLISECONDS);
            connectionPoolSettings.maintenanceFrequency(500, TimeUnit.MILLISECONDS);
            connectionPoolSettings.maxSize(100);
            connectionPoolSettings.minSize(10);
            connectionPoolSettings.maxWaitTime(10000, TimeUnit.MILLISECONDS);
        });
        o.applyToClusterSettings(clusterSettings -> {
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
        int amount = 117;
        for (int i = 0; i < amount; i++) {
            data.add(new Document(m.serialize(new UncachedObject("str_value " + (i % 5), i))));
        }
        long start = System.currentTimeMillis();
        //create elements
        client.getDatabase("morphium_test").getCollection("uncached_object").insertMany(data);
        long dur = System.currentTimeMillis() - start;
        log.info(String.format("Storing took %dms", dur));
        start = System.currentTimeMillis();
        for (int i = 0; i < amount; i++) {
            //data.add(m.serialize(new UncachedObject("str_value "+(i%5),i)));
            Document query = new Document();
            query.put("counter", i);
            var result = client.getDatabase("morphium_test").getCollection("uncached_object").find().filter(new Document("counter", i));
            MongoCursor<Document> iterator = result.iterator();
            var document = iterator.next();
            assertThat(iterator.hasNext()).isEqualTo(Boolean.FALSE);
        }
        dur = System.currentTimeMillis() - start;
        log.info(String.format("reading took %dms", dur));

        client.close();
        log.info("Using sync connection...");
        SingleMongoConnectDriver con = new SingleMongoConnectDriver();
        con.setHostSeed("localhost:27017");
        con.setDefaultBatchSize(100);
        con.connect();
        log.info("Connected");

        ClearCollectionSettings clear = new ClearCollectionSettings(con.getConnection());
        clear.setDb("morphium_test").setColl("uncached_object");
        log.info("Deleted documents:" + clear.doClear());

        List<Map<String, Object>> dat = new ArrayList<>();
        for (int i = 0; i < amount; i++) {
            dat.add(m.serialize(new UncachedObject("str_value " + (i % 5), i)));
        }
        start = System.currentTimeMillis();
        InsertMongoCommand insert = new InsertMongoCommand(con.getConnection()).setDocuments(dat).setColl("uncached_object").setDb("morphium_test");
        insert.execute();
        dur = System.currentTimeMillis() - start;
        log.info(String.format("Storing took %dms", dur));
        start = System.currentTimeMillis();
        for (int i = 0; i < amount; i++) {
            FindCommand fnd = new FindCommand(con.getConnection());
            fnd.setColl("uncached_object").setDb("morphium_test").setFilter(Doc.of("counter", i));
            var it = fnd.executeIterable();
            var d = it.next();
            assertThat(it.hasNext()).isEqualTo(Boolean.FALSE);
        }
        dur = System.currentTimeMillis() - start;
        log.info(String.format("reading took %dms", dur));
        con.disconnect();
    }
}
