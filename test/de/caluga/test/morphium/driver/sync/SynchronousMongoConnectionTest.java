package de.caluga.test.morphium.driver.sync;

import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.commands.ClearCollectionSettings;
import de.caluga.morphium.driver.commands.DeleteCmdSettings;
import de.caluga.morphium.driver.commands.FindCmdSettings;
import de.caluga.morphium.driver.commands.UpdateCmdSettings;
import de.caluga.morphium.driver.sync.SynchronousMongoConnection;
import de.caluga.morphium.objectmapping.ObjectMapperImpl;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class SynchronousMongoConnectionTest {
    private final static String coll = "uncached_object";
    private final static String db = "testdb";
    private Logger log = LoggerFactory.getLogger(SynchronousMongoConnectionTest.class);

    @Test
    public void testSyncConnection() throws Exception {
        SynchronousMongoConnection con = new SynchronousMongoConnection();
        con.setHostSeed("localhost:27017");
        con.setSlaveOk(true);

        con.connect();
        log.info("Connected");
        int deleted = con.clearCollection(new ClearCollectionSettings().setColl(coll).setDb(db));
        log.info("Deleted old data: " + deleted);

        ObjectMapperImpl objectMapper = new ObjectMapperImpl();
        for (int i = 0; i < 100; i++) {
            UncachedObject o = new UncachedObject("value", 123 + i);
            con.store(db, coll, Arrays.asList(Doc.of(objectMapper.serialize(o))), null);
        }
        log.info("created test data");
        log.info("running find...");
        FindCmdSettings fnd = new FindCmdSettings().setColl(coll).setDb(db).setBatchSize(100).setFilter(Doc.of("counter", 123));
        List<Doc> res = con.find(fnd);
        assertThat(res.size()).isEqualTo(1);
        assertThat(res.get(0).get("counter")).isEqualTo(123);
        log.info("done.");

        log.info("Updating...");
        UpdateCmdSettings update = new UpdateCmdSettings();
        update.addUpdate(Doc.of("q", Doc.of("_id", res.get(0).get("_id")), "u", Doc.of("$set", Doc.of("counter", 9999))));
        update.setOrdered(false);
        update.setDb(db);
        update.setColl(coll);

        Map<String, Object> updateInfo = con.update(update);
        assertThat(updateInfo.get("nModified")).isEqualTo(1);
        log.info("...done");
        log.info("Re-Reading...");
        fnd.setFilter(Doc.of("_id", res.get(0).get("_id")));
        res = con.find(fnd);
        assertThat(res.size()).isEqualTo(1);
        assertThat(res.get(0).get("counter")).isEqualTo(9999);
        con.close();
    }


    @Test
    public void testUpdateSyncConnection() throws Exception {
        SynchronousMongoConnection con = new SynchronousMongoConnection();
        con.setHostSeed("localhost:27017");
        con.setSlaveOk(true);
        con.connect();
        log.info("Connected");
        int deleted = con.clearCollection(new ClearCollectionSettings().setDb(db).setColl(coll));
        log.info("Deleted old data: " + deleted);

        ObjectMapperImpl objectMapper = new ObjectMapperImpl();
        for (int i = 0; i < 100; i++) {
            UncachedObject o = new UncachedObject("value", 123 + i);
            con.store(db, coll, Arrays.asList(Doc.of(objectMapper.serialize(o))), null);
        }
        log.info("created test data");
        log.info("running find...");
        FindCmdSettings fnd = new FindCmdSettings().setColl(coll).setDb(db).setBatchSize(100).setFilter(Doc.of("counter", 123));
        List<Doc> res = con.find(fnd);
        assertThat(res.size()).isEqualTo(1);
        assertThat(res.get(0).get("counter")).isEqualTo(123);
        log.info("done.");

        log.info("Updating...");
        UpdateCmdSettings update = new UpdateCmdSettings()
                .setDb(db)
                .setColl(coll)
                .addUpdate(Doc.of("q", Doc.of("_id", res.get(0).get("_id")), "u", Doc.of("$set", Doc.of("counter", 9999))));

        Map<String, Object> updateInfo = con.update(update);
        assertThat(updateInfo.get("nModified")).isEqualTo(1);
        log.info("...done");
        log.info("Re-Reading...");
        fnd.setFilter(Doc.of("_id", res.get(0).get("_id")));
        res = con.find(fnd);
        assertThat(res.size()).isEqualTo(1);
        assertThat(res.get(0).get("counter")).isEqualTo(9999);
        con.close();
    }


    @Test
    public void testWatch() throws Exception {
        SynchronousMongoConnection con = new SynchronousMongoConnection();
        con.setHostSeed("localhost:27017");
        con.setSlaveOk(true);
        con.setDefaultBatchSize(5);

        con.connect();
        log.info("Connected");
        ObjectMapperImpl objectMapper = new ObjectMapperImpl();

        UncachedObject o = new UncachedObject("value", 123);
        con.store(db, coll, Arrays.asList(Doc.of(objectMapper.serialize(o))), null);

        long start = System.currentTimeMillis();
        new Thread() {
            public void run() {
                try {
                    log.info("Thread is connecting...");
                    SynchronousMongoConnection con = new SynchronousMongoConnection();
                    con.setHostSeed("localhost:27017");
                    con.setSlaveOk(true);
                    con.setDefaultBatchSize(5);

                    con.connect();
                    log.info("Thread connected");
                    ObjectMapperImpl objectMapper = new ObjectMapperImpl();
                    log.info("Thread is waiting...");
                    Thread.sleep(3500);
                    UncachedObject o = new UncachedObject("value", 123);
                    UncachedObject o2 = new UncachedObject("value2", 124);
                    con.store(db, coll, Arrays.asList(Doc.of(objectMapper.serialize(o)), Doc.of(objectMapper.serialize(o2))), null);
                    log.info("stored data after " + (System.currentTimeMillis() - start) + "ms");
                    con.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }.start();
        con.watch(db, coll, 5000, false, null, 20, new DriverTailableIterationCallback() {
            private int counter = 0;

            @Override
            public void incomingData(Map<String, Object> data, long dur) {
                log.info("got data after " + (System.currentTimeMillis() - start) + "ms");
                log.info(Utils.toJsonString(data));
                counter++;
            }

            @Override
            public boolean isContinued() {
                return counter < 2;
            }
        });

        log.info("Watch endet");

        con.close();
    }
}
