package de.caluga.test.mongo.suite.base;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.config.CollectionCheckSettings.CappedCheck;
import de.caluga.morphium.config.CollectionCheckSettings.IndexCheck;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.DropDatabaseMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.test.support.TestConfig;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;

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

    public MultiDriverTestBase() {
    }

    // Local file-based config removed; using centralized TestConfig.

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

    // No-op: no property storage in tests anymore.

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
        return getMorphiumAllInstances(false, true, true);
    }

    public static Stream<Arguments> getInMemInstanceOnly() {
        return getMorphiumAllInstances(false, true, false);
    }

    public static Stream<Arguments> getMorphiumAllInstances(boolean includeSingle, boolean includeInMem, boolean includePooled) {
        init();
        List<Arguments> morphiums = new ArrayList<>();
        MorphiumConfig base = TestConfig.load();

        boolean externalEnabled = isExternalEnabled();
        var allowed = getAllowedDrivers(externalEnabled);

        // var password = Base64.getEncoder().encodeToString(enc.encrypt("test".getBytes(StandardCharsets.UTF_8)));
        // var user = Base64.getEncoder().encodeToString(enc.encrypt("test".getBytes(StandardCharsets.UTF_8)));
        // var authDb = Base64.getEncoder().encodeToString(enc.encrypt("admin".getBytes(StandardCharsets.UTF_8)));

        //Diferent Drivers
        if (includePooled && allowed.contains(PooledDriver.driverName)) {
            MorphiumConfig pooled = MorphiumConfig.fromProperties(base.asProperties());
            pooled.driverSettings().setDriverName(PooledDriver.driverName);
            pooled.connectionSettings().setDatabase("morphium_test_" + number.incrementAndGet());
            pooled.collectionCheckSettings().setIndexCheck(IndexCheck.CREATE_ON_STARTUP)
                  .setCappedCheck(CappedCheck.CREATE_ON_STARTUP);
            log.info("Running test with DB morphium_test_" + number.get() + " for " + pooled.driverSettings().getDriverName());
            morphiums.add(Arguments.of(new Morphium(pooled)));
        }

        if (includeSingle && allowed.contains(SingleMongoConnectDriver.driverName)) {
            MorphiumConfig single = MorphiumConfig.fromProperties(base.asProperties());
            single.driverSettings().setDriverName(SingleMongoConnectDriver.driverName);
            single.connectionSettings().setDatabase("morphium_test_" + number.incrementAndGet());
            single.collectionCheckSettings().setIndexCheck(IndexCheck.CREATE_ON_STARTUP)
                  .setCappedCheck(CappedCheck.CREATE_ON_STARTUP);
            log.info("Running test with DB morphium_test_" + number.get() + " for " + single.driverSettings().getDriverName());
            morphiums.add(Arguments.of(new Morphium(single)));
        }

        //
        //        MorphiumConfig mongoDriver = MorphiumConfig.fromProperties(getProps());
        //        mongoDriver.setDriverName(MongoDriver.driverName);
        //        mongoDriver.setDatabase("morphium_test_" + number.incrementAndGet());
        //        log.info("Running test with DB morphium_test_" + number.get() + " for " + mongoDriver.getDriverName());
        //
        //
        if (includeInMem && allowed.contains(InMemoryDriver.driverName)) {
            MorphiumConfig inMemCfg = MorphiumConfig.fromProperties(base.asProperties());
            inMemCfg.driverSettings().setDriverName(InMemoryDriver.driverName);
            inMemCfg.authSettings().setMongoAuthDb(null).setMongoLogin(null).setMongoPassword(null);
            inMemCfg.connectionSettings().setDatabase("morphium_test_" + number.incrementAndGet());
            inMemCfg.collectionCheckSettings().setCappedCheck(CappedCheck.CREATE_ON_STARTUP)
                     .setIndexCheck(IndexCheck.CREATE_ON_STARTUP);
            Morphium inMem = new Morphium(inMemCfg);
            ((InMemoryDriver) inMem.getDriver()).setExpireCheck(500);
            morphiums.add(Arguments.of(inMem));
            log.info("Running test with DB morphium_test_" + number.get() + " for " + inMemCfg.driverSettings().getDriverName());
        }

        //dropping all existing test-dbs
        if (((Morphium) morphiums.get(0).get()[0]).getDriver() instanceof InMemoryDriver) {
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

    private static boolean isExternalEnabled() {
        // Explicit flag from Maven/runner
        if (Boolean.getBoolean("morphium.tests.external")) return true;
        String envExt = System.getenv("MORPHIUM_EXTERNAL");
        if (envExt != null && (envExt.equalsIgnoreCase("true") || envExt.equals("1") || envExt.equalsIgnoreCase("yes"))) return true;

        // Presence of a MongoDB URI implies external
        String uri = System.getProperty("morphium.uri");
        if (uri == null) uri = System.getenv("MONGODB_URI");
        if (uri == null) uri = System.getenv("MORPHIUM_URI");
        return uri != null && !uri.isBlank();
    }

    private static java.util.Set<String> getAllowedDrivers(boolean externalEnabled) {
        var allowed = new java.util.LinkedHashSet<String>();
        String drvProp = System.getProperty("morphium.driver");
        if (drvProp == null) drvProp = System.getenv("MORPHIUM_DRIVER");

        if (!externalEnabled) {
            // Only allow inmem regardless of property
            allowed.add(InMemoryDriver.driverName);
            return allowed;
        }

        if (drvProp == null || drvProp.isBlank()) {
            allowed.add(PooledDriver.driverName);
            allowed.add(SingleMongoConnectDriver.driverName);
            allowed.add(InMemoryDriver.driverName);
            return allowed;
        }

        for (String token : drvProp.split(",")) {
            var t = token.trim().toLowerCase(java.util.Locale.ROOT);
            switch (t) {
                case "pooled": case "pooleddriver":
                    allowed.add(PooledDriver.driverName); break;
                case "single": case "singleconnect": case "singlemongoconnectdriver":
                    allowed.add(SingleMongoConnectDriver.driverName); break;
                case "inmem": case "inmemory": case "inmemorydriver":
                    allowed.add(InMemoryDriver.driverName); break;
                case "all":
                    allowed.add(PooledDriver.driverName);
                    allowed.add(SingleMongoConnectDriver.driverName);
                    allowed.add(InMemoryDriver.driverName);
                    break;
                default:
                    allowed.add(token.trim());
            }
        }

        if (allowed.isEmpty()) {
            allowed.add(InMemoryDriver.driverName);
        }

        return allowed;
    }

    private static void init() {
        log.info("in init!");
        // No init needed; configuration is provided centrally via TestConfig.
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
