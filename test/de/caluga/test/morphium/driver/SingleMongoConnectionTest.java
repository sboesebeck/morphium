package de.caluga.test.morphium.driver;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class SingleMongoConnectionTest extends ConnectionTestBase {

    private Logger log = LoggerFactory.getLogger(SingleMongoConnectionTest.class);

    @Test(expected = MorphiumDriverException.class)
    public void failTest() throws Exception{
        try (var con = getConnection()) {

            ShutdownCommand cmd = new ShutdownCommand(con);
            cmd.setForce(true);
            cmd.setTimeoutSecs(2);
            var msg = con.sendCommand(cmd.asMap());
            log.info("Sent Shutdown command...");
            Thread.sleep(3000);
            //server should be down now
            log.info("Should be down now. Sending hello...");

            HelloCommand c = new HelloCommand(con).setHelloOk(true);
            while (true) {
                log.info("Sending message...");
                msg = con.sendCommand(c.asMap());
                Thread.sleep(500);
            }

        }
    }

    @Test
    public void testSyncConnection() throws Exception {
        var con = getConnection();
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
        con.close();
    }


    @Test
    public void testUpdateSyncConnection() throws Exception {
        var con = getConnection();
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
        con.close();
    }


    @Test
    public void testWatchDb() throws Exception {
        var con = getConnection();
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
                    SingleMongoConnectDriver con = new SingleMongoConnectDriver();
                    con.setHostSeed("localhost:27017");

                    con.setDefaultBatchSize(5);

                    con.connect();
                    log.info("Thread connected");
                    ObjectMapperImpl objectMapper = new ObjectMapperImpl();
                    log.info("Thread is waiting...");
                    Thread.sleep(3500);
                    UncachedObject o = new UncachedObject("value", 123);
                    UncachedObject o2 = new UncachedObject("value2", 124);
                    StoreMongoCommand cmd = new StoreMongoCommand(con.getConnection())
                            .setDb(db).setColl(coll).setDocs(Arrays.asList(Doc.of(objectMapper.serialize(o)), Doc.of(objectMapper.serialize(o2))));
                    cmd.execute();
                    cmd.setColl("test2");
                    cmd.execute();
                    log.info("stored data after " + (System.currentTimeMillis() - start) + "ms");
                    con.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }.start();
        con.watch(new WatchCommand(con).setMaxTimeMS(10000).setCb(new DriverTailableIterationCallback() {
            private int counter = 0;

            @Override
            public void incomingData(Map<String, Object> data, long dur) {
                log.info("got data after " + (System.currentTimeMillis() - start) + "ms");
                log.info(Utils.toJsonString(data));
                counter++;
            }

            @Override
            public boolean isContinued() {
                return counter < 4;
            }
        }).setBatchSize(1).setColl(null).setDb(db).setMaxTimeMS(1000));

        log.info("Watch ended");

        con.close();
    }


    @Test
    public void testWatchColl() throws Exception {
        var con = getConnection();
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
                    SingleMongoConnectDriver con = new SingleMongoConnectDriver();
                    con.setHostSeed("localhost:27017");

                    con.setDefaultBatchSize(5);

                    con.connect();
                    log.info("Thread connected");
                    ObjectMapperImpl objectMapper = new ObjectMapperImpl();
                    log.info("Thread is waiting...");
                    Thread.sleep(3500);
                    UncachedObject o = new UncachedObject("value", 123);
                    UncachedObject o2 = new UncachedObject("value2", 124);
                    StoreMongoCommand cmd = new StoreMongoCommand(con.getConnection())
                            .setDb(db).setColl(coll).setDocs(Arrays.asList(Doc.of(objectMapper.serialize(o)), Doc.of(objectMapper.serialize(o2))));
                    cmd.execute();
                    cmd.setColl("test2");
                    cmd.execute();
                    log.info("stored data after " + (System.currentTimeMillis() - start) + "ms");
                    con.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }.start();
        final AtomicInteger counter=new AtomicInteger(0);
        final long waitStart=System.currentTimeMillis();
        con.watch(new WatchCommand(con).setMaxTimeMS(10000).setCb(new DriverTailableIterationCallback() {


            @Override
            public void incomingData(Map<String, Object> data, long dur) {
                log.info("got data after " + (System.currentTimeMillis() - start) + "ms");
                log.info(Utils.toJsonString(data));
                counter.incrementAndGet();
            }

            @Override
            public boolean isContinued() {
                return System.currentTimeMillis()-waitStart < 5000;
            }
        }).setBatchSize(1).setColl(coll).setDb(db).setMaxTimeMS(1000));

        log.info("Watch ended");
        assertThat(counter.get()).isLessThanOrEqualTo(2);
        con.close();
    }



    @Test
    public void testMapReduce() throws Exception {
        var con = getConnection();
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
        con.close();
    }


    @Test
    public void testRunCommand() throws Exception {
        var con = getConnection();
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

        var msg = con.sendCommand(Doc.of("hello", 1,"$db","local"));
        var result=con.readSingleAnswer(msg);
        assertThat(result != null).isTrue();
        assertThat(result.get("primary")).isEqualTo(result.get("me"));
        assertThat(result.get("secondary")).isEqualTo(false);
        con.close();
    }


    @Test
    public void iteratorTest() throws Exception {
        var con = getConnection();
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
        var msg = con.sendCommand(fnd.asMap());
        var crs=con.getAnswerFor(msg);
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
        msg=con.sendCommand(fnd.asMap());
        crs = con.getAnswerFor(msg);
        List<Map<String, Object>> lst = crs.getAll();
        assertThat(lst).isNotNull();
        assertThat(lst.size()).isEqualTo(1000);
        assertThat(lst.get(0).get("_id")).isNotNull();

        con.close();
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
