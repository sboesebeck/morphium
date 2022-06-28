package de.caluga.test.morphium.driver.sync;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.sync.SingleMongoConnection;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.morphium.objectmapping.ObjectMapperImpl;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SingleMongoConnectionTest {
    private final static String coll = "uncached_object";
    private final static String db = "testdb";
    private Logger log = LoggerFactory.getLogger(SingleMongoConnectionTest.class);

    @Test
    public void testSyncConnection() throws Exception {
        SingleMongoConnection con = getSynchronousMongoConnection();
        log.info("Connected");
        int deleted = (int) new ClearCollectionSettings(con).setColl(coll).setDb(db).doClear();
        log.info("Deleted old data: " + deleted);

        ObjectMapperImpl objectMapper = new ObjectMapperImpl();
        for (int i = 0; i < 100; i++) {
            UncachedObject o = new UncachedObject("value", 123 + i);
            StoreMongoCommand cmd = new StoreMongoCommand(con)
                    .setDb(db).setColl(coll).setDocs(Arrays.asList(Doc.of(objectMapper.serialize(o))));
            cmd.execute();
        }
        log.info("created test data");
        log.info("running find...");
        FindCommand fnd = new FindCommand(con).setColl(coll).setDb(db).setBatchSize(10).setFilter(Doc.of("counter", 123));
        List<Map<String, Object>> res = fnd.execute();
        assertThat(res.size()).isEqualTo(1);
        assertThat(res.get(0).get("counter")).isEqualTo(123);
        log.info("done.");

        log.info("Updating...");
        UpdateMongoCommand update = new UpdateMongoCommand(con);
        update.addUpdate(Doc.of("q", Doc.of("_id", res.get(0).get("_id")), "u", Doc.of("$set", Doc.of("counter", 9999))));
        update.setOrdered(false);
        update.setDb(db);
        update.setColl(coll);

        Map<String, Object> updateInfo = update.execute();
        assertThat(updateInfo.get("nModified")).isEqualTo(1);
        log.info("...done");
        log.info("Re-Reading...");
        fnd.setFilter(Doc.of("_id", res.get(0).get("_id")));
        res = fnd.execute();
        assertThat(res.size()).isEqualTo(1);
        assertThat(res.get(0).get("counter")).isEqualTo(9999);
        con.disconnect();
    }

    @Test
    public void testUpdateSyncConnection() throws Exception {
        SingleMongoConnection con = getSynchronousMongoConnection();
        new ClearCollectionSettings(con).setDb(db).setColl(coll).execute();
        //log.info("Deleted old data: " + deleted);

        ObjectMapperImpl objectMapper = new ObjectMapperImpl();
        for (int i = 0; i < 100; i++) {
            UncachedObject o = new UncachedObject("value", 123 + i);
            StoreMongoCommand cmd = new StoreMongoCommand(con)
                    .setDb(db).setColl(coll).setDocs(Arrays.asList(Doc.of(objectMapper.serialize(o))));
            cmd.execute();
        }
        log.info("created test data");
        log.info("running find...");
        FindCommand fnd = new FindCommand(con).setColl(coll).setDb(db).setBatchSize(100).setFilter(Doc.of("counter", 123));
        List<Map<String, Object>> res = fnd.execute();
        log.info("got result");
        assertThat(res.size()).isEqualTo(1);
        assertThat(res.get(0).get("counter")).isEqualTo(123);
        log.info("done.");

        log.info("Updating...");
        UpdateMongoCommand update = new UpdateMongoCommand(con)
                .setDb(db)
                .setColl(coll)
                .addUpdate(Doc.of("q", Doc.of("_id", res.get(0).get("_id")), "u", Doc.of("$set", Doc.of("counter", 9999))));

        Map<String, Object> updateInfo = update.execute();
        assertThat(updateInfo.get("nModified")).isEqualTo(1);
        log.info("...done");
        log.info("Re-Reading...");
        fnd.setFilter(Doc.of("_id", res.get(0).get("_id")));
        res = fnd.execute();
        assertThat(res.size()).isEqualTo(1);
        assertThat(res.get(0).get("counter")).isEqualTo(9999);
        con.disconnect();
    }


    @Test
    public void testWatch() throws Exception {
        SingleMongoConnection con = getSynchronousMongoConnection();
        ObjectMapperImpl objectMapper = new ObjectMapperImpl();

        UncachedObject o = new UncachedObject("value", 123);
        StoreMongoCommand cmd = new StoreMongoCommand(con)
                .setDb(db).setColl(coll).setDocs(Arrays.asList(Doc.of(objectMapper.serialize(o))));
        cmd.execute();

        long start = System.currentTimeMillis();
        new Thread() {
            public void run() {
                try {
                    log.info("Thread is connecting...");
                    SingleMongoConnection con = new SingleMongoConnection();
                    con.setHostSeed("localhost:27017");

                    con.setDefaultBatchSize(5);

                    con.connect();
                    log.info("Thread connected");
                    ObjectMapperImpl objectMapper = new ObjectMapperImpl();
                    log.info("Thread is waiting...");
                    Thread.sleep(3500);
                    UncachedObject o = new UncachedObject("value", 123);
                    UncachedObject o2 = new UncachedObject("value2", 124);
                    StoreMongoCommand cmd = new StoreMongoCommand(con)
                            .setDb(db).setColl(coll).setDocs(Arrays.asList(Doc.of(objectMapper.serialize(o)), Doc.of(objectMapper.serialize(o2))));
                    cmd.execute();
                    log.info("stored data after " + (System.currentTimeMillis() - start) + "ms");
                    con.disconnect();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }.start();
        con.watch(new WatchSettings(con).setMaxWaitTime(10000).setCb(new DriverTailableIterationCallback() {
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
        }).setBatchSize(1).setColl(coll).setDb(db).setMaxWaitTime(1000));

        log.info("Watch endet");

        con.disconnect();
    }

    private SingleMongoConnection getSynchronousMongoConnection() throws MorphiumDriverException {
        SingleMongoConnection con = new SingleMongoConnection();
        con.setHostSeed("localhost:27017");
        con.setDefaultBatchSize(5);
        con.setMaxWaitTime(100000);
        con.setConnectionTimeout(1000);
        con.connect();
        log.info("Connected");
        return con;
    }


    @Test
    public void testMapReduce() throws Exception {
        SingleMongoConnection con = getSynchronousMongoConnection();
        Object deleted = new ClearCollectionSettings(con).setDb(db).setColl(coll).execute().get("n");
        log.info("Deleted old data: " + deleted);
        List<Map<String, Object>> testList = new ArrayList<>();
        MorphiumObjectMapper om = new ObjectMapperImpl();
        for (int i = 0; i < 100; i++) {
            testList.add(Doc.of(om.serialize(new UncachedObject("strValue" + i, (int) (i * i / (i + 1))))));
        }
        StoreMongoCommand cmd = new StoreMongoCommand(con)
                .setDb(db).setColl(coll).setDocs(testList);
        cmd.execute();

        MapReduceCommand settings = new MapReduceCommand(con)
                .setColl(coll)
                .setDb(db)
                .setOutConfig(Doc.of("inline", 1))
                .setMap("function(){ if (this.counter%2==0) emit(this.counter,1); }")
                .setReduce("function(key,values){ return values; }")
                .setFinalize("function(key, reducedValue){ return reducedValue;}")
                .setSort(Doc.of("counter", 1))
                .setScope(Doc.of("myVar", 1));
        List<Map<String, Object>> res = settings.execute();
        res.sort((o1, o2) -> ((Comparable) o1.get("_id")).compareTo(o2.get("_id")));
        log.info("Got results");
        assertThat(res.size()).isEqualTo(50);
        assertThat(res.get(0).containsKey("_id")).isTrue();
        assertThat(res.get(0).containsKey("value"));
        assertThat(((List) res.get(14).get("value")).get(0)).isEqualTo(1.0);
        con.disconnect();
    }


    @Test
    public void testRunCommand() throws Exception {
        SingleMongoConnection con = getSynchronousMongoConnection();
        Object deleted = new ClearCollectionSettings(con).setDb(db).setColl(coll).execute().get("n");
        log.info("Deleted old data: " + deleted);
        List<Map<String, Object>> testList = new ArrayList<>();
        MorphiumObjectMapper om = new ObjectMapperImpl();
        for (int i = 0; i < 100; i++) {
            testList.add(Doc.of(om.serialize(new UncachedObject("strValue" + i, (int) (i * i / (i + 1))))));
        }
        StoreMongoCommand cmd = new StoreMongoCommand(con)
                .setDb(db).setColl(coll).setDocs(testList);
        cmd.execute();

        var result = con.runCommand(db, Doc.of("hello", 1)).next();
        assertThat(result != null).isTrue();
        assertThat(result.get("primary")).isEqualTo(result.get("me"));
        assertThat(result.get("secondary")).isEqualTo(false);
        con.disconnect();
    }


    @Test
    public void iteratorTest() throws Exception {
        SingleMongoConnection con = getSynchronousMongoConnection();
        Object deleted = new ClearCollectionSettings(con).setDb(db).setColl(coll).execute().get("n");
        log.info("Deleted old data: " + deleted);
        List<Map<String, Object>> testList = new ArrayList<>();
        MorphiumObjectMapper om = new ObjectMapperImpl();
        for (int i = 0; i < 1000; i++) {
            testList.add(Doc.of(om.serialize(new UncachedObject("strValue" + i, (int) (i * i / (i + 1))))));
        }
        StoreMongoCommand cmd = new StoreMongoCommand(con)
                .setDb(db).setColl(coll).setDocs(testList);
        cmd.execute();

        FindCommand fnd = new FindCommand(con).setDb(db).setColl(coll).setBatchSize(17);
        var crs = con.runCommand(db, fnd.asMap());
        int cnt = 0;
        while (crs.hasNext()) {
            cnt++;
            var obj = crs.next();
            assertThat(obj).isNotNull();
            assertThat(obj).isNotEmpty();
            assertThat(obj.get("_id")).isNotNull();
        }
        assertThat(cnt).isEqualTo(1000);
        //GEtAll
        crs = con.runCommand(db, fnd.asMap());
        List<Map<String, Object>> lst = crs.getAll();
        assertThat(lst).isNotNull();
        assertThat(lst.size()).isEqualTo(1000);
        assertThat(lst.get(0).get("_id")).isNotNull();

        con.disconnect();
    }

    @Test
    public void pipeliningCheck() throws Exception {
//        SynchronousMongoConnection con = getSynchronousMongoConnection();
//        int deleted = con.clearCollection(new ClearCollectionSettings().setDb(db).setColl(coll));
//        log.info("Deleted old data: " + deleted);
//        List<Map<String, Object>> testList = new ArrayList<>();
//        MorphiumObjectMapper om = new ObjectMapperImpl();
//        for (int i = 0; i < 10000; i++) {
//            testList.add(Doc.of(om.serialize(new UncachedObject("strValue" + i, (int) (i * i / (i + 1))))));
//        }
//        StoreMongoCommand cmd = new StoreMongoCommand()
//                .setDb(db).setColl(coll).setDocs(testList);
//        con.store(cmd);
//        long start = System.currentTimeMillis();
//        int id = con.sendCommand(db, cmd.asMap("errorMsg"));
//        log.info((System.currentTimeMillis() - start) + ": sent " + id);
//        id = con.sendCommand(db, new FindCommand().setDb(db).setColl(coll).setFilter(Doc.of("$where", "for (let i=0;i<1000000;i++){ var s=s+i;}; return true;")).asMap("find"));
//        log.info((System.currentTimeMillis() - start) + ": sent " + id);
//        id = con.sendCommand("admin", Doc.of("hello", true));
//        log.info((System.currentTimeMillis() - start) + ": sent " + id);
//
//        var reply = con.getReplyFor();
//
//        log.info((System.currentTimeMillis() - start) + ": Reply for " + reply.getResponseTo());
//        reply = con.getNextReply();
//        log.info((System.currentTimeMillis() - start) + ": Reply for " + reply.getResponseTo());
//        reply = con.getNextReply();
//        log.info((System.currentTimeMillis() - start) + ": Reply for " + reply.getResponseTo());
    }
}
