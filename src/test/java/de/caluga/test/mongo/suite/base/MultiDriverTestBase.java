package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.MorphiumConfig.CappedCheck;
import de.caluga.morphium.MorphiumConfig.IndexCheck;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.commands.DropDatabaseMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.encryption.AESEncryptionProvider;
import de.caluga.morphium.encryption.DefaultEncryptionKeyProvider;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

// import de.caluga.morphium.driver.inmem.InMemoryDriver;
// import de.caluga.morphium.driver.meta.MetaDriver;
// import de.caluga.morphium.driver.singleconnect.SingleConnectDirectDriver;
// import de.caluga.morphium.driver.singleconnect.SingleConnectThreaddedDriver;

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
        var configDir = new File(System.getProperty("user.home") + "/.config/");

        if (!configDir.exists()) {
            configDir.mkdirs();
        }

        return new File(System.getProperty("user.home") + "/.config/morphiumtest.cfg");
    }

    @BeforeAll
    public static synchronized void setUpClass() {
        System.gc();
    }

    @AfterAll
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
        if (m1 == null && m2 == null) {
            return true;
        }

        if (m1 == null || m2 == null) {
            return false;
        }

        m1 = m1.replaceAll(" ", "");
        m2 = m2.replaceAll(" ", "");

        if (m1.length() != m2.length()) {
            return false;
        }

        String[] wrd = m1.split("[ \\{\\},\\.\\(\\)\\[\\]]");

        for (String w : wrd) {
            if (!m2.contains(w)) {
                return false;
            }
        }

        return true;
    }

    public static Stream<Arguments> getMorphiumInstancesPooledOnly() {
        return getMorphiumAllInstances(false, false, true);
    }

    public static Stream<Arguments> getMorphiumInstancesSingleOnly() {
        return getMorphiumAllInstances(true, false, false);
    }

    public static Stream<Arguments> getMorphiumInstancesNoSingle() {
        return getMorphiumAllInstances(false, true, true);
    }

    public static Stream<Arguments> getMorphiumInstancesInMemOnly() {
        return getMorphiumAllInstances(false, true, false);
    }

    public static Stream<Arguments> getMorphiumInstancesNoInMem() {
        return getMorphiumAllInstances(true, false, true);
    }

    public static Stream<Arguments> getMorphiumInstances() {
        return getMorphiumAllInstances(true, true, true);
    }

    public static Stream<Arguments> getInMemInstanceOnly() {
        return getMorphiumAllInstances(false, true, false);
    }

    public static Stream<Arguments> getMorphiumAllInstances(boolean includeSingle, boolean includeInMem, boolean includePooled) {
        init();
        List<Arguments> morphiums = new ArrayList<>();
        var enc = new AESEncryptionProvider();
        enc.setEncryptionKey("1234567890abcdef".getBytes());
        enc.setDecryptionKey("1234567890abcdef".getBytes());
        var p = MorphiumTestBase.getProps();
        String pwd = (String)p.get("mongoPassword");
        String login = (String)p.get("mongoLogin");
        String adb = (String)p.get("mongoAuthDb");
        String password = null;
        String user = null;
        String authDb = null;

        if (adb != null && !adb.isBlank() && !adb.isEmpty()) {
            password = Base64.getEncoder().encodeToString(enc.encrypt(pwd.getBytes(StandardCharsets.UTF_8)));
            user = Base64.getEncoder().encodeToString(enc.encrypt(login.getBytes(StandardCharsets.UTF_8)));
            authDb = Base64.getEncoder().encodeToString(enc.encrypt(adb.getBytes(StandardCharsets.UTF_8)));
        }

        // var password = Base64.getEncoder().encodeToString(enc.encrypt("test".getBytes(StandardCharsets.UTF_8)));
        // var user = Base64.getEncoder().encodeToString(enc.encrypt("test".getBytes(StandardCharsets.UTF_8)));
        // var authDb = Base64.getEncoder().encodeToString(enc.encrypt("admin".getBytes(StandardCharsets.UTF_8)));

        //Diferent Drivers
        if (includePooled) {
            MorphiumConfig pooled = MorphiumConfig.fromProperties(getProps());
            pooled.setCredentialsEncrypted(true);
            pooled.setCredentialsEncryptionKey("1234567890abcdef");
            pooled.setCredentialsDecryptionKey("1234567890abcdef");

            if (authDb != null) {
                pooled.setMongoAuthDb(authDb);
                pooled.setMongoPassword(password);
                pooled.setMongoLogin(user);
            }

            pooled.setDriverName(PooledDriver.driverName);
            pooled.setDatabase("morphium_test_" + number.incrementAndGet());
            pooled.setIndexCheck(IndexCheck.CREATE_ON_STARTUP);
            pooled.setCappedCheck(CappedCheck.CREATE_ON_STARTUP);
            log.info("Running test with DB morphium_test_" + number.get() + " for " + pooled.getDriverName());
            Morphium pooledMorphium = new Morphium(pooled);
            morphiums.add(Arguments.of(pooledMorphium));
        }

        if (includeSingle) {
            MorphiumConfig singleConnection = MorphiumConfig.fromProperties(getProps());
            singleConnection.setDriverName(SingleMongoConnectDriver.driverName);
            singleConnection.setCredentialsEncrypted(true);
            singleConnection.setCredentialsEncryptionKey("1234567890abcdef");
            singleConnection.setCredentialsDecryptionKey("1234567890abcdef");

            if (authDb != null) {
                singleConnection.setMongoAuthDb(authDb);
                singleConnection.setMongoPassword(password);
                singleConnection.setMongoLogin(user);
            }

            singleConnection.setDatabase("morphium_test_" + number.incrementAndGet());
            singleConnection.setIndexCheck(IndexCheck.CREATE_ON_STARTUP);
            singleConnection.setCappedCheck(CappedCheck.CREATE_ON_STARTUP);
            log.info("Running test with DB morphium_test_" + number.get() + " for " + singleConnection.getDriverName());
            Morphium singleConMorphium = new Morphium(singleConnection);
            morphiums.add(Arguments.of(singleConMorphium));
        }

        //
        //        MorphiumConfig mongoDriver = MorphiumConfig.fromProperties(getProps());
        //        mongoDriver.setDriverName(MongoDriver.driverName);
        //        mongoDriver.setDatabase("morphium_test_" + number.incrementAndGet());
        //        log.info("Running test with DB morphium_test_" + number.get() + " for " + mongoDriver.getDriverName());
        //
        //
        if (includeInMem) {
            MorphiumConfig inMemDriver = MorphiumConfig.fromProperties(getProps());
            inMemDriver.setDriverName(InMemoryDriver.driverName);
            inMemDriver.setReplicasetMonitoring(false);
            inMemDriver.setMongoAuthDb(null);
            inMemDriver.setMongoLogin(null);
            inMemDriver.setMongoPassword(null);
            inMemDriver.setDatabase("morphium_test_" + number.incrementAndGet());
            inMemDriver.setCappedCheck(CappedCheck.CREATE_ON_STARTUP);
            inMemDriver.setIndexCheck(IndexCheck.CREATE_ON_STARTUP);
            var inMem = new Morphium(inMemDriver);
            ((InMemoryDriver) inMem.getDriver()).setExpireCheck(1000); //speed up expiry check
            morphiums.add(Arguments.of(inMem));
            log.info("Running test with DB morphium_test_" + number.get() + " for " + inMemDriver.getDriverName());
        }

        //dropping all existing test-dbs
        if (morphiums.get(0).get()[0] instanceof InMemoryDriver) {
            log.info("Not erasing DBs - inMem");
        } else {
            Morphium m = (Morphium) morphiums.get(0).get()[0];

            if (m == null) return morphiums.stream();

            if (m.listDatabases() == null) return morphiums.stream();

            for (String db : m.listDatabases()) {
                if (db.startsWith("morphium")) {
                    log.info(m.getDriver().getName() + ": Dropping db " + db);

                    try {
                        DropDatabaseMongoCommand cmd = new DropDatabaseMongoCommand(m.getDriver().getPrimaryConnection(null));
                        cmd.setDb(db);
                        cmd.setComment("Delete for testing");
                        cmd.execute();
                        cmd.releaseConnection();
                    } catch (MorphiumDriverException e) {
                        log.error(m.getDriver().getName() + " Dropping failed", e);
                    }
                }
            }
        }

        return morphiums.stream();
    }

    private static void init() {
        log.info("in init!");
        Properties p = getProps();

        if (p.getProperty("database") == null) {
            MorphiumConfig cfg;
            //creating default config
            cfg = new MorphiumConfig("morphium_test", 2055, 50000, 5000);
            cfg.addHostToSeed("localhost", 27017);
            cfg.addHostToSeed("localhost", 27018);
            cfg.addHostToSeed("localhost", 27019);
            cfg.setDriverName(PooledDriver.driverName);
            cfg.setMongoAuthDb("admin");
            cfg.setCredentialsEncrypted(true);
            cfg.setMongoLogin("test");
            cfg.setMongoPassword("test");
            cfg.setValueEncryptionProviderClass(AESEncryptionProvider.class);
            cfg.setEncryptionKeyProviderClass(DefaultEncryptionKeyProvider.class);
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
            //            cfg.setIndexCheck(MorphiumConfig.IndexCheck.CREATE_ON_STARTUP);
            //            cfg.setCappedCheck(MorphiumConfig.CappedCheck.CREATE_ON_STARTUP);
            cfg.setIndexCheck(MorphiumConfig.IndexCheck.CREATE_ON_WRITE_NEW_COL);
            cfg.setCappedCheck(MorphiumConfig.CappedCheck.CREATE_ON_WRITE_NEW_COL);
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
            log.info("created test-settings file");
        }
    }

    //
    //    public void createUncachedObjectsInMemory(int amount) {
    //        createUncachedObjects(morphiumInMemeory, amount);
    //    }

    @AfterEach
    public void tearDown() {
    }

    public boolean waitForAsyncOperationsToStart(Morphium morphium, long maxWaitMs) {
        long start = System.currentTimeMillis();

        while (morphium.getWriteBufferCount() == 0) {
            Thread.yield();

            if (System.currentTimeMillis() - start > maxWaitMs) {
                log.error("Timeout reached, " + maxWaitMs + " but buffer is still " + morphium.getWriteBufferCount());
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

    public void logStats(Morphium m) {
        Map<String, Double> stats = m.getStatistics();
        log.info("Statistics: ");

        for (Map.Entry<String, Double> e : stats.entrySet()) {
            log.info(e.getKey() + " - " + e.getValue());
        }
    }

}
