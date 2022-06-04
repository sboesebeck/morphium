package de.caluga.test.morphium.driver.bson;/**
 * Created by stephan on 30.11.15.
 */

import de.caluga.morphium.*;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.objectmapping.ObjectMapperImpl;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Level;

/**
 * TODO: Add Documentation here
 **/
public class MorphiumDriverSpeedTest {

    private static int countObjs = 25;
    private Logger log = LoggerFactory.getLogger(MorphiumDriverSpeedTest.class);

    @BeforeClass
    public static void setup() {
        System.setProperty("morphium.log.level", "4");
        System.setProperty("morphium.log.synced", "true");
        System.setProperty("morphium.log.file", "-");
        java.util.logging.Logger l = java.util.logging.Logger.getGlobal();
        l.setLevel(Level.SEVERE);
    }


    //    @Test
    //    public void speedCompareMultithread() throws Exception {
    //        Morphium m = null;
    //        MorphiumConfig cfg = new MorphiumConfig("morphium_test", 100, 1000, 1000);
    //        //        cfg.addHostToSeed("192.168.44.209:30001");
    //        //        cfg.addHostToSeed("192.168.44.209:30002");
    //        cfg.addHostToSeed("localhost:27017");
    //        cfg.addHostToSeed("localhost:27018");
    //        cfg.setReplicasetMonitoring(true);
    //        cfg.setDriverClass(MetaDriver.class.getName());
    //        cfg.setMaxWaitTime(30000);
    //        cfg.setMinConnectionsPerHost(1);
    //        cfg.setMaxConnections(100);
    //        cfg.setLogLevelForClass(MetaDriver.class, 5);
    //        m = new Morphium(cfg);
    //        //        m.getDriver().connect();
    //        //        Thread.sleep(10000);
    //        log.info("Testing multithreadded with Metadriver:");
    //        multithreadTest(m);
    //        m.close();
    //        //wait for threads to finish...
    //        Thread.sleep(5000);
    //
    //        cfg = new MorphiumConfig("morphium_test", 100, 1000, 1000);
    //        cfg.addHostToSeed("localhost");
    //        cfg.setReplicasetMonitoring(false);
    //        cfg.setDriverClass(SingleConnectThreaddedDriver.class.getName());
    //        cfg.setMinConnectionsPerHost(1);
    //        cfg.setMaxConnections(100);
    //        cfg.setMaxWaitTime(3000);
    //        m = new Morphium(cfg);
    //        //        m.getDriver().connect();
    //        //        Thread.sleep(10000);
    //        log.info("Testing multithreadded with SingeConnect driver:");
    //        multithreadTest(m);
    //        m.close();
    //
    //        cfg = new MorphiumConfig("morphium_test", 100, 1000, 1000);
    //        cfg.addHostToSeed("localhost");
    //        cfg.setReplicasetMonitoring(false);
    //        cfg.setDriverClass(SingleConnectDirectDriver.class.getName());
    //        cfg.setMinConnectionsPerHost(1);
    //        cfg.setMaxConnections(100);
    //        cfg.setMaxWaitTime(3000);
    //        m = new Morphium(cfg);
    //        //        m.getDriver().connect();
    //        //        Thread.sleep(10000);
    //        log.info("Testing multithreadded with SingeConnectDirect driver:");
    //        multithreadTest(m);
    //        m.close();
    //
    //        cfg = new MorphiumConfig("morphium_test", 100, 1000, 1000);
    //        cfg.addHostToSeed("localhost");
    //        cfg.setReplicasetMonitoring(false);
    //        cfg.setMinConnectionsPerHost(1);
    //        cfg.setMaxConnections(100);
    //        m = new Morphium(cfg);
    //
    //        log.info("Testing multithreadded with mongodb driver:");
    //        multithreadTest(m);
    //        m.close();
    //
    //        cfg = new MorphiumConfig("morphium_test", 100, 1000, 1000);
    //        cfg.setDriverClass(InMemoryDriver.class.getName());
    //        cfg.addHostToSeed("localhost");
    //        cfg.setReplicasetMonitoring(false);
    //        m = new Morphium(cfg);
    //        log.info("Testing with inMemory driver:");
    //        multithreadTest(m);
    //        m.close();
    //
    //    }

//
//    @Test
//    public void profilingTest() throws Exception {
//        Morphium m = null;
//        MorphiumConfig cfg = null;
//
//        cfg = new MorphiumConfig("morphium_test", 100, 1000, 1000);
//        cfg.setMaxConnections(5);
//        cfg.addHostToSeed("localhost");
//        cfg.setReplicasetMonitoring(false);
//        cfg.setDriverClass(MetaDriver.class.getName());
//        cfg.setMaxWaitTime(3000);
//        cfg.setMaxConnectionIdleTime(10000);
//        cfg.setMaxConnectionLifeTime(50000);
//        m = new Morphium(cfg);
//        //        m.getDriver().connect();
//        //        Thread.sleep(10000);
//        log.info("+++++++++++   ++++++    Profiling Metadriver:");
//        while (true) {
//            long tm = doTest(m);
//            if (tm < 10) break;
//        }
//        m.close();
//    }

    @Test
    public void speedCompare() throws Exception {
//        Morphium m = null;
//        MorphiumConfig cfg = null;
//
//        cfg = new MorphiumConfig("morphium_test", 100, 1000, 1000);
//        cfg.setMaxConnections(5);
//        cfg.addHostToSeed("localhost");
//        cfg.setReplicasetMonitoring(false);
//        cfg.setDriverClass(MetaDriver.class.getName());
//        cfg.setMaxWaitTime(3000);
//        cfg.setCursorBatchSize(10000);
//        m = new Morphium(cfg);
//        //        m.getDriver().connect();
//        //        Thread.sleep(10000);
//        log.info("+++++++++++   ++++++    Testing with Metadriver:");
//        log.info("+++++++++++   ++++++    Testing with Metadriver:");
//        log.info("+++++++++++   ++++++    Testing with Metadriver:");
//        long tm = doTest(m); //ignoring first run
//        tm = doTest(m);
//        tm += doTest(m);
//        tm += doTest(m);
//        log.info("===============> Testing with Metadriver: Average " + (tm / 3));
//        m.close();
//
//        cfg = new MorphiumConfig("morphium_test", 100, 1000, 1000);
//        cfg.addHostToSeed("localhost");
//        cfg.setReplicasetMonitoring(false);
//        cfg.setDriverClass(SingleConnectThreaddedDriver.class.getName());
//        cfg.setMaxWaitTime(3000);
//        m = new Morphium(cfg);
//        //        m.getDriver().connect();
//        //        Thread.sleep(10000);
//        log.info("+++++++++++   ++++++    Testing with SingeConnect driver:");
//        log.info("+++++++++++   ++++++    Testing with SingeConnect driver:");
//        log.info("+++++++++++   ++++++    Testing with SingeConnect driver:");
//        tm = doTest(m); //ignoring first run
//        tm = doTest(m);
//        tm += doTest(m);
//        tm += doTest(m);
//        log.info("===============> Testing with SingeConnect driver Average: " + (tm / 3));
//        m.close();
//
//
//        cfg = new MorphiumConfig("morphium_test", 100, 1000, 1000);
//        cfg.addHostToSeed("localhost");
//        cfg.setReplicasetMonitoring(false);
//        cfg.setDriverClass(SingleConnectDirectDriver.class.getName());
//        cfg.setMaxWaitTime(3000);
//        m = new Morphium(cfg);
//        //        m.getDriver().connect();
//        //        Thread.sleep(10000);
//        log.info("+++++++++++   ++++++    Testing with SingeConnectDirect driver:");
//        log.info("+++++++++++   ++++++    Testing with SingeConnectDirect driver:");
//        log.info("+++++++++++   ++++++    Testing with SingeConnectDirect driver:");
//        tm = doTest(m); //ignoring first run
//        tm = doTest(m);
//        tm += doTest(m);
//        tm += doTest(m);
//        log.info("===============> Testing with SingeConnectDirect driver Average: " + (tm / 3));
//        m.close();
//
//
//        log.info("+++++++++++   ++++++    Testing with mongodb driver:");
//        log.info("+++++++++++   ++++++    Testing with mongodb driver:");
//        log.info("+++++++++++   ++++++    Testing with mongodb driver:");
//        cfg = new MorphiumConfig("morphium_test", 100, 1000, 1000);
//        cfg.addHostToSeed("localhost");
//        cfg.setReplicasetMonitoring(false);
//        m = new Morphium(cfg);
//        tm = doTest(m); //ignoring first run
//        tm = doTest(m);
//        tm += doTest(m);
//        tm += doTest(m);
//        log.info("===============> Testing with mongodb driver Average: " + (tm / 3));
    }


    public void multithreadTest(Morphium m) throws Exception {
        long startTotal = System.currentTimeMillis();
        long start = System.currentTimeMillis();
        m.dropCollection(UncachedObject.class);
        List<Thread> threads = new Vector<>();
        for (int i = 0; i < 50; i++) {
            final int t = i;
            Thread thr = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < 1000; j++) {
                        UncachedObject uc = new UncachedObject();
                        uc.setCounter(t * 100 + j);
                        uc.setStrValue("By thread " + t);
                        m.store(uc);
                    }
                    //                    log.info("Thread " + t + " finished!");
                }
            };
            threads.add(thr);
            thr.start();
        }
        //        for (Thread t : threads) t.join();
        //        long dur = System.currentTimeMillis() - start;
        //        log.info("Storing took " + dur);

        start = System.currentTimeMillis();

        for (int i = 0; i < 50; i++) {
            final int t = i;
            Thread thr = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < 1000; j++) {
                        Query<UncachedObject> q = m.createQueryFor(UncachedObject.class);
                        q.f("counter").eq(t * 100 + j);
                        UncachedObject uc = q.get();
                    }
                    //                    log.info("Thread " + t + " finished!");
                }
            };
            threads.add(thr);
            thr.start();
        }
        for (Thread t : threads) t.join();
        //        dur = System.currentTimeMillis() - start;
        //        log.info("Reading took " + dur);

        long dur = System.currentTimeMillis() - startTotal;
        log.info("Overall dur " + dur);


    }

    public long doTest(Morphium m) throws Exception {
        m.dropCollection(UncachedObject.class);
        Thread.sleep(100);
        long totalTime = System.currentTimeMillis();
        long start = System.currentTimeMillis();
        List<UncachedObject> lst = new ArrayList<>();
        for (int i = 0; i < countObjs; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i);
            uc.setStrValue("V:" + i);
            lst.add(uc);
        }
        m.storeList(lst);
        long dur = System.currentTimeMillis() - start;
        log.info("Write Took:  " + dur);

        //waiting for persitence
        long c = 0;
        start = System.currentTimeMillis();
        while (c < countObjs) {
            c = m.createQueryFor(UncachedObject.class).countAll();
            dur = System.currentTimeMillis() - start;
        }
        log.info("persisted after:  " + dur);

        start = System.currentTimeMillis();
        c = m.createQueryFor(UncachedObject.class).countAll();
        dur = System.currentTimeMillis() - start;
        log.info("count took:  " + dur);
        //        log.info("Counter=="+c);

        start = System.currentTimeMillis();
        lst = m.createQueryFor(UncachedObject.class).asList();
        dur = System.currentTimeMillis() - start;

        log.info("Read took:   " + dur);

        start = System.currentTimeMillis();
        m.delete(m.createQueryFor(UncachedObject.class).f("counter").eq(100));
        dur = System.currentTimeMillis() - start;
        log.info("Delete took: " + dur);

        start = System.currentTimeMillis();
        long count = m.createQueryFor(UncachedObject.class).countAll();
        dur = System.currentTimeMillis() - start;
        log.info("count took:  " + dur);
        assert (count >= countObjs - 1) : "Count wrong: " + count + " vs " + countObjs;
        start = System.currentTimeMillis();
        count = m.createQueryFor(UncachedObject.class).countAll();
        dur = System.currentTimeMillis() - start;
        log.info("count took:  " + dur);
        totalTime = System.currentTimeMillis() - totalTime;
        log.info(" ====> In Total " + totalTime + "ms\n");
        return totalTime;
    }

    @Test
    public void crudTest() throws Exception {
//        SingleConnectThreaddedDriver drv = new SingleConnectThreaddedDriver();
//        drv.setHostSeed("localhost:27017");
//        drv.connect();
//
//        drv.drop("morphium_test", "tst", null);
//        long start = System.currentTimeMillis();
//        MorphiumObjectMapper objectMapper = new ObjectMapperImpl();
//        List<Map<String, Object>> lst = new ArrayList<>();
//        for (int i = 0; i < countObjs; i++) {
//            UncachedObject uc = new UncachedObject();
//            uc.setCounter(i);
//            uc.setValue("V:" + i);
//            lst.add(objectMapper.serialize(uc));
//        }
//        drv.insert("morphium_test", "tst", lst, null);
//
//        //        Thread.sleep(1000);
//
//        lst = drv.find("morphium_test", "tst", new HashMap<>(), null, null, 0, 0, 1000, null, null, null);
//        log.info("List: " + lst.size());
//        assert (lst.size() == countObjs);
//
//        drv.delete("morphium_test", "tst", UtilsMap.of("counter", 100), false, null, null);
//        Thread.sleep(1000);
//        lst = drv.find("morphium_test", "tst", new HashMap<>(), null, null, 0, 0, 1000, null, null, null);
//        assert (lst.size() != countObjs) : "Size is still " + lst.size();
//        assert (lst.size() == countObjs - 1) : "Size is not correct " + lst.size();
//
//        long c = drv.count("morphium_test", "tst", new HashMap<>(), null, null);
//        assert (c == countObjs - 1) : "Count wrong: " + c;
//
//
//        lst = drv.find("morphium_test", "tst", UtilsMap.of("counter", UtilsMap.of("$lt", 10)), null, null, 0, 0, 1000, null, null, null);
//        assert (lst.size() == 10);
//
//        c = drv.count("morphium_test", "tst", UtilsMap.of("counter", UtilsMap.of("$lt", 10)), null, null);
//        assert (c == 10) : "Counter is " + c;
//
//        long dur = System.currentTimeMillis() - start;
//        log.info("Duratuion: " + dur);
    }

    @Test
    public void crudTestMongod() throws Exception {
//        Driver drv = new Driver();
//        drv.setHostSeed("localhost:27017");
//        drv.connect();
//
//        drv.drop("morphium_test", "tst", null);
//
//        long start = System.currentTimeMillis();
//        List<Map<String, Object>> lst = new ArrayList<>();
//        for (int i = 0; i < countObjs; i++) {
//            UncachedObject uc = new UncachedObject();
//            uc.setCounter(i);
//            uc.setValue("V:" + i);
//            lst.add(new ObjectMapperImpl().serialize(uc));
//        }
//        drv.insert("morphium_test", "tst", lst, null);
//
//        //        Thread.sleep(1000);
//
//        lst = drv.find("morphium_test", "tst", new HashMap<>(), null, null, 0, 0, 1000, null, null);
//        log.info("List: " + lst.size());
//        assert (lst.size() == countObjs);
//
//        drv.delete("morphium_test", "tst", UtilsMap.of("counter", 100), false, null);
//        //        Thread.sleep(1000);
//        lst = drv.find("morphium_test", "tst", new HashMap<>(), null, null, 0, 0, 1000, null, null);
//        assert (lst.size() != countObjs) : "Size is still " + lst.size();
//        assert (lst.size() == countObjs - 1) : "Size is not correct " + lst.size();
//
//        long c = drv.count("morphium_test", "tst", new HashMap<>(), null);
//        assert (c == countObjs - 1) : "Count wrong: " + c;
//
//
//        lst = drv.find("morphium_test", "tst", UtilsMap.of("counter", UtilsMap.of("$lt", 10)), null, null, 0, 0, 1000, null, null);
//        assert (lst.size() == 10);
//
//        c = drv.count("morphium_test", "tst", UtilsMap.of("counter", UtilsMap.of("$lt", 10)), null);
//        assert (c == 10) : "Counter is " + c;
//        long dur = System.currentTimeMillis() - start;
//        log.info("Duration: " + dur);
    }


    @Test
    public void deleteTest() throws Exception {
//        SingleConnectThreaddedDriver drv = new SingleConnectThreaddedDriver();
//        drv.setHostSeed("localhost:27017");
//        drv.connect();
//        drv.delete("morphium_test", "tst", UtilsMap.of("counter", 100), false, null, null);
//        Thread.sleep(1000);
    }

    @Test
    public void reverseEngineer() throws Exception {
        String s = "00000000:  C4 00 00 00 1F 00 00 00 00 00 00 00 D4 07 00 00     .---.-------..--\n" +
                "00000010:  00 00 00 00 6D 6F 72 70 68 69 75 6D 5F 74 65 73     ----morphium_tes\n" +
                "00000020:  74 2E 24 63 6D 64 00 00 00 00 00 FF FF FF FF 95     t..cmd-----.....\n" +
                "00000030:  00 00 00 02 64 65 6C 65 74 65 00 10 00 00 00 75     ---.delete-.---u\n" +
                "00000040:  6E 63 61 63 68 65 64 5F 6F 62 6A 65 63 74 00 08     ncached_object-.\n" +
                "00000050:  6F 72 64 65 72 65 64 00 01 03 77 72 69 74 65 43     ordered-..writeC\n" +
                "00000060:  6F 6E 63 65 72 6E 00 26 00 00 00 10 77 00 01 00     oncern-.---.w-.-\n" +
                "00000070:  00 00 10 77 74 69 6D 65 6F 75 74 00 E8 03 00 00     --.wtimeout-..--\n" +
                "00000080:  08 66 73 79 6E 63 00 00 08 6A 00 00 00 04 64 65     .fsync--.j---.de\n" +
                "00000090:  6C 65 74 65 73 00 2D 00 00 00 03 30 00 25 00 00     letes-.---..-.--\n" +
                "000000A0:  00 03 71 00 12 00 00 00 10 63 6F 75 6E 74 65 72     -.q-.---.counter\n" +
                "000000B0:  00 64 00 00 00 00 10 6C 69 6D 69 74 00 00 00 00     -d----.limit----\n" +
                "000000C0:  00 00 00 00";

        BufferedReader br = new BufferedReader(new StringReader(s));
        String l = null;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while ((l = br.readLine()) != null) {
            int len = l.length();
            String d = l.substring(11, len < 58 ? l.length() - 1 : 58);
            //            d=d.replaceAll(" ","");
            log.info("got " + d);
            for (String n : d.split(" ")) {
                String c1 = n.substring(0, 0);
                String c2 = n.substring(1, 1);
                int v1 = 0;
                int v2 = 0;
                for (int i = 0; i < Utils.hexChars.length; i++) {
                    if (Utils.hexChars[i].equals(c1)) {
                        v1 = i;
                    }
                    if (Utils.hexChars[i].equals(c2)) {
                        v2 = i;
                    }
                }
                byte b = (byte) ((byte) (v1 << 4) | (byte) v2);
                out.write(b);
            }
        }

        log.info("Got buffer: " + Utils.getHex(out.toByteArray()));

//        OpQuery op = new OpQuery();
//        op.setColl("$cmd");
//        op.setDb("morphium_test");
//        op.setReqId(1);
//        op.setLimit(-1);
//
//        Map<String, Object> o = new LinkedHashMap<>();
//        o.put("delete", "uncached_object");
//        o.put("ordered", false);
//        Map<String, Object> wc = new LinkedHashMap<>();
//        wc.put("w", 1);
//        wc.put("wtimeout", 100);
//        wc.put("fsync", false);
//        wc.put("j", false);
//        o.put("writeConcern", wc);
//
//        Map<String, Object> q = new LinkedHashMap<>();
//        q.put("q", UtilsMap.of("counter", 100));
//        q.put("limit", 0);
//        List<Map<String, Object>> del = new ArrayList<>();
//        del.add(q);
//
//        o.put("deletes", del);
//        op.setDoc(o);
//        log.info("mine: " + Utils.getHex(op.bytes()));

    }

    @Test
    public void check() throws Exception {
        AnnotationAndReflectionHelper hlp = new AnnotationAndReflectionHelper(true);
        UncachedObject obj = new UncachedObject();
        MorphiumId id = new MorphiumId();
        for (int i = 0; i < 10000; i++) hlp.getIdField(obj).set(obj, id);
    }

    @Test
    public void mongotest() throws Exception {
//        Morphium m = new Morphium("localhost", "morphium_test");
//        Query q = m.createQueryFor(UncachedObject.class);
//        q.f("counter").eq(100);
//        BulkRequestContext op = m.getDriver().createBulkContext(m, "morphium_test", "tst", false, null);
//        List<Map<String, Object>> lst = new ArrayList<>();
//        ObjectMapperImpl objectMapper = new ObjectMapperImpl();
//        for (int i = 0; i < countObjs; i++) {
//            UncachedObject uc = new UncachedObject();
//            uc.setCounter(i);
//            uc.setStrValue("V:" + i);
//
//            lst.add(objectMapper.serialize(uc));
//        }
//        op.addInsertBulkRequest(lst);
//        op.execute();
//        Thread.sleep(1000);
    }
}
