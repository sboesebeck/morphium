package de.caluga.test.morphium.driver.sync;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.sync.SynchronousMongoConnection;
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
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

public class SynchronousMongoConnectionTest {
    private final static String coll = "uncached_object";
    private final static String db = "testdb";
    private Logger log = LoggerFactory.getLogger(SynchronousMongoConnectionTest.class);

    @Test
    public void testSyncConnection() throws Exception {
        SynchronousMongoConnection con = getSynchronousMongoConnection();
        log.info("Connected");
        int deleted = con.clearCollection(new ClearCollectionSettings().setColl(coll).setDb(db));
        log.info("Deleted old data: " + deleted);

        ObjectMapperImpl objectMapper = new ObjectMapperImpl();
        for (int i = 0; i < 100; i++) {
            UncachedObject o = new UncachedObject("value", 123 + i);
            StoreCmdSettings cmd = new StoreCmdSettings()
                    .setDb(db).setColl(coll).setDocs(Arrays.asList(Doc.of(objectMapper.serialize(o))));
            con.store(cmd);
        }
        log.info("created test data");
        log.info("running find...");
        FindCmdSettings fnd = new FindCmdSettings().setColl(coll).setDb(db).setBatchSize(100).setFilter(Doc.of("counter", 123));
        List<Map<String, Object>> res = con.find(fnd);
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
        con.disconnect();
    }


//
//    public void pseudoCode(){
//        FindCommand fnd=FindCommand.builder(driver).setQuery().setDB().set...();
//        List(Map<String,Object>) fnd.exec();
//        //Builder
//        static Vector<FindCommand> commands=new Vector<FindCommand>();
//        runner={
//                while (commands.size()<10){
//                    commands.add(new FindCommand(drvier));
//                }
//        };
//        scheduler.schedule(runner,10sek);
//
//
//        builder{
//            return commands.remove(0);
//        }
//        purge(){
//            clear();
//            commands.add(this);
//        }
//        fnd.purge();
//
//    }


    @Test
    public void testUpdateSyncConnection() throws Exception {
        SynchronousMongoConnection con = getSynchronousMongoConnection();
        int deleted = con.clearCollection(new ClearCollectionSettings().setDb(db).setColl(coll));
        log.info("Deleted old data: " + deleted);

        ObjectMapperImpl objectMapper = new ObjectMapperImpl();
        for (int i = 0; i < 100; i++) {
            UncachedObject o = new UncachedObject("value", 123 + i);
            StoreCmdSettings cmd = new StoreCmdSettings()
                    .setDb(db).setColl(coll).setDocs(Arrays.asList(Doc.of(objectMapper.serialize(o))));
            con.store(cmd);
        }
        log.info("created test data");
        log.info("running find...");
        FindCmdSettings fnd = new FindCmdSettings().setColl(coll).setDb(db).setBatchSize(100).setFilter(Doc.of("counter", 123));
        List<Map<String, Object>> res = con.find(fnd);
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
        con.disconnect();
    }


    @Test
    public void testWatch() throws Exception {
        SynchronousMongoConnection con = getSynchronousMongoConnection();
        ObjectMapperImpl objectMapper = new ObjectMapperImpl();

        UncachedObject o = new UncachedObject("value", 123);
        StoreCmdSettings cmd = new StoreCmdSettings()
                .setDb(db).setColl(coll).setDocs(Arrays.asList(Doc.of(objectMapper.serialize(o))));
        con.store(cmd);

        long start = System.currentTimeMillis();
        new Thread() {
            public void run() {
                try {
                    log.info("Thread is connecting...");
                    SynchronousMongoConnection con = new SynchronousMongoConnection();
                    con.setHostSeed("localhost:27017");

                    con.setDefaultBatchSize(5);

                    con.connect();
                    log.info("Thread connected");
                    ObjectMapperImpl objectMapper = new ObjectMapperImpl();
                    log.info("Thread is waiting...");
                    Thread.sleep(3500);
                    UncachedObject o = new UncachedObject("value", 123);
                    UncachedObject o2 = new UncachedObject("value2", 124);
                    StoreCmdSettings cmd = new StoreCmdSettings()
                            .setDb(db).setColl(coll).setDocs(Arrays.asList(Doc.of(objectMapper.serialize(o)), Doc.of(objectMapper.serialize(o2))));
                    con.store(cmd);
                    log.info("stored data after " + (System.currentTimeMillis() - start) + "ms");
                    con.disconnect();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }.start();
        con.watch(new WatchCmdSettings().setCb(new DriverTailableIterationCallback() {
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

    private SynchronousMongoConnection getSynchronousMongoConnection() {
        SynchronousMongoConnection con = new SynchronousMongoConnection();
        con.setHostSeed("localhost:27017");
        con.setDefaultBatchSize(5);
        con.connect();
        log.info("Connected");
        return con;
    }


    @Test
    public void testMapReduce() throws Exception {
        SynchronousMongoConnection con = getSynchronousMongoConnection();
        int deleted = con.clearCollection(new ClearCollectionSettings().setDb(db).setColl(coll));
        log.info("Deleted old data: " + deleted);
        List<Map<String, Object>> testList = new ArrayList<>();
        MorphiumObjectMapper om = new ObjectMapperImpl();
        for (int i = 0; i < 100; i++) {
            testList.add(Doc.of(om.serialize(new UncachedObject("strValue" + i, (int) (i * i / (i + 1))))));
        }
        StoreCmdSettings cmd = new StoreCmdSettings()
                .setDb(db).setColl(coll).setDocs(testList);
        con.store(cmd);

        MapReduceSettings settings = new MapReduceSettings()
                .setColl(coll)
                .setDb(db)
                .setOutConfig(Doc.of("inline", 1))
                .setMap("function(){ if (this.counter%2==0) emit(this.counter,1); }")
                .setReduce("function(key,values){ return values; }")
                .setFinalize("function(key, reducedValue){ return reducedValue;}")
                .setSort(Doc.of("counter", 1))
                .setScope(Doc.of("myVar", 1));
        List<Map<String, Object>> res = con.mapReduce(settings);
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
        SynchronousMongoConnection con = getSynchronousMongoConnection();
        int deleted = con.clearCollection(new ClearCollectionSettings().setDb(db).setColl(coll));
        log.info("Deleted old data: " + deleted);
        List<Map<String, Object>> testList = new ArrayList<>();
        MorphiumObjectMapper om = new ObjectMapperImpl();
        for (int i = 0; i < 100; i++) {
            testList.add(Doc.of(om.serialize(new UncachedObject("strValue" + i, (int) (i * i / (i + 1))))));
        }
        StoreCmdSettings cmd = new StoreCmdSettings()
                .setDb(db).setColl(coll).setDocs(testList);
        con.store(cmd);

        var result = con.runCommand(db, null, Doc.of("hello", 1)).next();
        assertThat(result != null).isTrue();
        assertThat(result.get("primary")).isEqualTo(result.get("me"));
        assertThat(result.get("secondary")).isEqualTo(false);
        con.disconnect();
    }


    @Test
    public void iteratorTest() throws Exception {
        SynchronousMongoConnection con = getSynchronousMongoConnection();
        int deleted = con.clearCollection(new ClearCollectionSettings().setDb(db).setColl(coll));
        log.info("Deleted old data: " + deleted);
        List<Map<String, Object>> testList = new ArrayList<>();
        MorphiumObjectMapper om = new ObjectMapperImpl();
        for (int i = 0; i < 1000; i++) {
            testList.add(Doc.of(om.serialize(new UncachedObject("strValue" + i, (int) (i * i / (i + 1))))));
        }
        StoreCmdSettings cmd = new StoreCmdSettings()
                .setDb(db).setColl(coll).setDocs(testList);
        con.store(cmd);

        FindCmdSettings fnd = new FindCmdSettings().setDb(db).setColl(coll).setBatchSize(17);
        var crs = con.runCommand(db, coll, fnd.asMap("find"));
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
        crs = con.runCommand(db, coll, fnd.asMap("find"));
        List<Map<String, Object>> lst = crs.getAll();
        assertThat(lst).isNotNull();
        assertThat(lst.size()).isEqualTo(1000);
        assertThat(lst.get(0).get("_id")).isNotNull();

        con.disconnect();
    }

    @Test
    public void pipeliningCheck() throws Exception {
        SynchronousMongoConnection con = getSynchronousMongoConnection();
        int deleted = con.clearCollection(new ClearCollectionSettings().setDb(db).setColl(coll));
        log.info("Deleted old data: " + deleted);
        List<Map<String, Object>> testList = new ArrayList<>();
        MorphiumObjectMapper om = new ObjectMapperImpl();
        for (int i = 0; i < 10000; i++) {
            testList.add(Doc.of(om.serialize(new UncachedObject("strValue" + i, (int) (i * i / (i + 1))))));
        }
        StoreCmdSettings cmd = new StoreCmdSettings()
                .setDb(db).setColl(coll).setDocs(testList);
        con.store(cmd);
        long start = System.currentTimeMillis();
        int id = con.sendCommand(db, cmd.asMap("errorMsg"));
        log.info((System.currentTimeMillis() - start) + ": sent " + id);
        id = con.sendCommand(db, new FindCmdSettings().setDb(db).setColl(coll).setFilter(Doc.of("$where", "for (let i=0;i<1000000;i++){ var s=s+i;}; return true;")).asMap("find"));
        log.info((System.currentTimeMillis() - start) + ": sent " + id);
        id = con.sendCommand("admin", Doc.of("hello", true));
        log.info((System.currentTimeMillis() - start) + ": sent " + id);

        var reply = con.getNextReply();

        log.info((System.currentTimeMillis() - start) + ": Reply for " + reply.getResponseTo());
        reply = con.getNextReply();
        log.info((System.currentTimeMillis() - start) + ": Reply for " + reply.getResponseTo());
        reply = con.getNextReply();
        log.info((System.currentTimeMillis() - start) + ": Reply for " + reply.getResponseTo());
    }
}
