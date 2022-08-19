package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.commands.DropDatabaseMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

//import de.caluga.morphium.driver.inmem.InMemoryDriver;
//import de.caluga.morphium.driver.meta.MetaDriver;
//import de.caluga.morphium.driver.singleconnect.SingleConnectDirectDriver;
//import de.caluga.morphium.driver.singleconnect.SingleConnectThreaddedDriver;


/**
 * User: Stpehan Bösebeck
 * Date: 26.02.12
 * Time: 16:17
 * <p/>
 */
public class MultiDriverTestBase {

    public static AtomicInteger number = new AtomicInteger(0);
    protected static Logger log = LoggerFactory.getLogger(MultiDriverTestBase.class);
    private static Properties props;

    public MultiDriverTestBase() {

    }

    public static synchronized Properties getProps() {
        if (props == null) {
            props = new Properties();
        }
        File f = getFile();
        if (f.exists()) {
            try {
                props.load(new FileReader(f));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return props;
    }

    private static File getFile() {
        return new File(System.getProperty("user.home") + "/.morphiumtest.cfg");
    }

    @org.junit.BeforeClass
    public static synchronized void setUpClass() {
        System.gc();
    }

    @org.junit.AfterClass
    public static void tearDownClass() {
        // LoggerFactory.getLogger(MorphiumTestBase.class).info("NOT Shutting down - might be reused!");
        //        morphium.close();
    }

//
//    public static synchronized List<Morphium> getMorphiums() {
//        if (morphiums == null) {
//            morphiums = new ArrayList<>();
//            morphiums.add(morphiumMongodb);
//            morphiums.add(morphiumInMemeory);
//            morphiums.add(morphiumSingleConnect);
//            morphiums.add(morphiumSingleConnectThreadded);
//            morphiums.add(morphiumMeta);
//        }
//        return morphiums;
//    }

    private static void storeProps() {
        File f = getFile();
        try {
            getProps().store(new FileWriter(f), "created by morphium test");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean stringWordCompare(String m1, String m2) {
        if (m1 == null && m2 == null) return true;
        if (m1 == null || m2 == null) return false;
        m1 = m1.replaceAll(" ", "");
        m2 = m2.replaceAll(" ", "");
        if (m1.length() != m2.length()) return false;
        String[] wrd = m1.split("[ \\{\\},\\.\\(\\)\\[\\]]");
        for (String w : wrd) {
            if (!m2.contains(w)) return false;
        }
        return true;
    }

    public static Stream<Arguments> getMorphiumInstances() {
        //Diferent Drivers
//        MorphiumConfig pooled = MorphiumConfig.fromProperties(getProps());
//        pooled.setDriverName(PooledDriver.driverName);
//        pooled.setDatabase("morphium_test_" + number.incrementAndGet());
//        log.info("Running test with DB morphium_test_" + number.get() + " for " + pooled.getDriverName());
//
//        MorphiumConfig singleConnection = MorphiumConfig.fromProperties(getProps());
//        singleConnection.setDriverName(SingleMongoConnectDriver.driverName);
//        singleConnection.setDatabase("morphium_test_" + number.incrementAndGet());
//        log.info("Running test with DB morphium_test_" + number.get() + " for " + singleConnection.getDriverName());

//
//        MorphiumConfig mongoDriver = MorphiumConfig.fromProperties(getProps());
//        mongoDriver.setDriverName(MongoDriver.driverName);
//        mongoDriver.setDatabase("morphium_test_" + number.incrementAndGet());
//        log.info("Running test with DB morphium_test_" + number.get() + " for " + mongoDriver.getDriverName());
//
//
        MorphiumConfig inMemDriver = MorphiumConfig.fromProperties(getProps());
        inMemDriver.setDriverName(InMemoryDriver.driverName);
        inMemDriver.setReplicasetMonitoring(false);
        inMemDriver.setDatabase("morphium_test_" + number.incrementAndGet());

        log.info("Running test with DB morphium_test_" + number.get() + " for " + inMemDriver.getDriverName());


        //dropping all existing test-dbs
//        for (String db : singleConMorphium.listDatabases()) {
//            if (db.startsWith("morphium")) {
//                log.info("Dropping db " + db);
//                try {
//                    DropDatabaseMongoCommand cmd = new DropDatabaseMongoCommand(singleConMorphium.getDriver().getPrimaryConnection(null));
//                    cmd.setDb(db);
//                    cmd.setComment("Delete for testing");
//
//                    cmd.execute();
//                    cmd.getConnection().release();
//                } catch (MorphiumDriverException e) {
//                }
//
//            }
//        }

//        Morphium pooledMorphium = new Morphium(pooled);
//        Morphium singleConMorphium = new Morphium(singleConnection);
        var inMem = new Morphium(inMemDriver);
        ((InMemoryDriver) inMem.getDriver()).setExpireCheck(1000); //speed up expiry check
        return Stream.of(//Arguments.of(pooledMorphium),
                //Arguments.of(singleConMorphium),
//                Arguments.of(new Morphium(mongoDriver)),
                Arguments.of(inMem)
        );
    }

    private void init() {
        log.info("in init!");


        Properties p = getProps();
        if (p.getProperty("database") == null) {
            MorphiumConfig cfg;
            //creating default config
            cfg = new MorphiumConfig("morphium_test", 2055, 50000, 5000);
            cfg.addHostToSeed("localhost", 27017);
            cfg.addHostToSeed("localhost", 27018);
            cfg.addHostToSeed("localhost", 27019);
            cfg.setWriteCacheTimeout(1000);
            cfg.setConnectionTimeout(2000);
            cfg.setRetryReads(false);
            cfg.setRetryWrites(false);
            cfg.setReadTimeout(1000);
            cfg.setMaxWaitTime(2000);
            cfg.setMaxConnectionLifeTime(60000);
            cfg.setMaxConnectionIdleTime(30000);
            cfg.setMaxConnections(100);
            cfg.setMinConnections(1);
            cfg.setMaximumRetriesBufferedWriter(1000);
            cfg.setMaximumRetriesWriter(1000);
            cfg.setMaximumRetriesAsyncWriter(1000);
            cfg.setRetryWaitTimeAsyncWriter(1000);
            cfg.setRetryWaitTimeWriter(1000);
            cfg.setRetryWaitTimeBufferedWriter(1000);
            cfg.setHeartbeatFrequency(500);

            cfg.setGlobalCacheValidTime(1000);
            cfg.setHousekeepingTimeout(500);
            cfg.setThreadPoolMessagingCoreSize(50);
            cfg.setThreadPoolMessagingMaxSize(1500);
            cfg.setThreadPoolMessagingKeepAliveTime(10000);

            cfg.setGlobalFsync(false);
            cfg.setGlobalJ(false);
            cfg.setGlobalW(1);

            cfg.setCheckForNew(true);


            //            cfg.setMongoAdminUser("adm");
            //            cfg.setMongoAdminPwd("adm");
            ////
            //            cfg.setMongoLogin("tst");
            //            cfg.setMongoPassword("tst");
            //            cfg.setMongoLogin("morphium");
            //            cfg.setMongoPassword("tst");

            //necessary for Replicaset Status to work
            //            cfg.setMongoAdminUser("admin");
            //            cfg.setMongoAdminPwd("admin");

            cfg.setDefaultReadPreference(ReadPreference.nearest());
            p.putAll(cfg.asProperties());
            p.put("failovertest", "false");
            cfg.setThreadConnectionMultiplier(2);
            storeProps();
        }
        log.info("created test-settings file");
    }
//
//    public void createUncachedObjectsInMemory(int amount) {
//        createUncachedObjects(morphiumInMemeory, amount);
//    }

    @org.junit.After
    public void tearDown() throws InterruptedException {

    }

    public boolean waitForAsyncOperationsToStart(Morphium morphium, long maxWaitMs) {
        long start = System.currentTimeMillis();
        while (morphium.getWriteBufferCount() == 0) {
            Thread.yield();
            if (System.currentTimeMillis() - start > maxWaitMs) {
                return false;
            }
        }
        return true;
    }

    public void createUncachedObjects(Morphium morphium, int amount) {
        List<UncachedObject> lst = new ArrayList<>();
        for (int i = 0; i < amount; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i + 1);
            uc.setStrValue("v");
            lst.add(uc);
            if (i % 1000 == 999) {
                morphium.insert(lst);
                lst.clear();
            }
        }
        morphium.storeList(lst);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        while (q.countAll() < amount) {
            log.info("Waiting for data to be stored..." + q.countAll() + "/" + amount);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
    }

    public void createCachedObjects(Morphium morphium, int amount) {
        List<CachedObject> lst = new ArrayList<>();
        for (int i = 0; i < amount; i++) {
            CachedObject uc = new CachedObject();
            uc.setCounter(i + 1);
            uc.setValue("v");
            lst.add(uc);
        }
        morphium.storeList(lst);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @org.junit.Before
    public void setUp() {
        init();
    }

    public void logStats(Morphium m) {
        Map<String, Double> stats = m.getStatistics();
        log.info("Statistics: ");
        for (Map.Entry<String, Double> e : stats.entrySet()) {
            log.info(e.getKey() + " - " + e.getValue());
        }
    }

    public void waitForWrites(Morphium morphium) {
        int count = 0;
        while (morphium.getWriteBufferCount() > 0) {
            count++;
            if (count % 100 == 0) {
                log.info("still " + morphium.getWriteBufferCount() + " writers active (" + morphium.getBufferedWriterBufferCount() + " + " + morphium.getWriterBufferCount() + ")");
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
            }
        }
        //waiting for it to be persisted
        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
        }
    }

    public long waitForCondidtionToBecomeTrue(long maxDuration, String failMessage, Condition tst) {
        long start = System.currentTimeMillis();
        while (!tst.test()) {
            if (System.currentTimeMillis() - start > maxDuration) {
                throw new AssertionError(failMessage);
            }
            Thread.yield();
        }
        return System.currentTimeMillis() - start;
    }

    public interface Condition {
        boolean test();
    }

}
