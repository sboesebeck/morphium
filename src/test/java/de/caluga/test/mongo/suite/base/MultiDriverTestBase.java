package de.caluga.test.mongo.suite.base;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.provider.Arguments;
import java.util.concurrent.TimeUnit;
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
 * <p>
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES) // Global timeout for all tests to prevent hangs
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

        // IMPORTANT: tests can run in parallel slots via `runtests.sh --parallel N`.
        // Each slot sets `-Dmorphium.database=morphium_test_<slotId>` and we must respect that here.
        // Otherwise different JVMs will all use the same DB names (morphium_test_1, morphium_test_2, ...)
        // and will drop/overwrite each other's data, causing flakiness and timeouts.
        String baseDbPrefix = System.getProperty("morphium.database");
        if (baseDbPrefix == null || baseDbPrefix.isBlank()) {
            baseDbPrefix = base.connectionSettings().getDatabase();
        }
        if (baseDbPrefix == null || baseDbPrefix.isBlank()) {
            baseDbPrefix = "morphium_test";
        }

        boolean externalEnabled = isExternalEnabled();
        var allowed = getAllowedDrivers(externalEnabled);

        // var password = Base64.getEncoder().encodeToString(enc.encrypt("test".getBytes(StandardCharsets.UTF_8)));
        // var user = Base64.getEncoder().encodeToString(enc.encrypt("test".getBytes(StandardCharsets.UTF_8)));
        // var authDb = Base64.getEncoder().encodeToString(enc.encrypt("admin".getBytes(StandardCharsets.UTF_8)));

        //Diferent Drivers
        if (includePooled && allowed.contains(PooledDriver.driverName)) {
            MorphiumConfig pooled = MorphiumConfig.fromProperties(base.asProperties());
            pooled.driverSettings().setDriverName(PooledDriver.driverName);
            pooled.connectionSettings().setDatabase(baseDbPrefix + "_" + number.incrementAndGet());
            pooled.collectionCheckSettings().setIndexCheck(IndexCheck.CREATE_ON_STARTUP)
                  .setCappedCheck(CappedCheck.CREATE_ON_STARTUP);
            log.info("Running test with DB " + pooled.connectionSettings().getDatabase() + " for " + pooled.driverSettings().getDriverName());
            morphiums.add(Arguments.of(new Morphium(pooled)));
        }

        if (includeSingle && allowed.contains(SingleMongoConnectDriver.driverName)) {
            MorphiumConfig single = MorphiumConfig.fromProperties(base.asProperties());
            single.driverSettings().setDriverName(SingleMongoConnectDriver.driverName);
            single.connectionSettings().setDatabase(baseDbPrefix + "_" + number.incrementAndGet());
            single.collectionCheckSettings().setIndexCheck(IndexCheck.CREATE_ON_STARTUP)
                  .setCappedCheck(CappedCheck.CREATE_ON_STARTUP);
            log.info("Running test with DB " + single.connectionSettings().getDatabase() + " for " + single.driverSettings().getDriverName());
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
            inMemCfg.connectionSettings().setDatabase(baseDbPrefix + "_" + number.incrementAndGet());
            inMemCfg.collectionCheckSettings().setCappedCheck(CappedCheck.CREATE_ON_STARTUP)
                    .setIndexCheck(IndexCheck.CREATE_ON_STARTUP);
            Morphium inMem = new Morphium(inMemCfg);
            ((InMemoryDriver) inMem.getDriver()).setExpireCheck(500);
            morphiums.add(Arguments.of(inMem));
            log.info("Running test with DB " + inMemCfg.connectionSettings().getDatabase() + " for " + inMemCfg.driverSettings().getDriverName());
        }

        //dropping all existing test-dbs
        if (morphiums.isEmpty()) {
            log.info("No morphium instances created for requested driver configuration");
            return morphiums.stream();
        }

        // Clean databases for ALL morphium instances, not just the first one.
        // This is critical because PooledDriver→MorphiumServer and InMemoryDriver
        // are completely separate storage systems.
        for (Arguments arg : morphiums) {
            Morphium m = (Morphium) arg.get()[0];
            if (m == null) continue;

            // IMPORTANT: Only drop THIS instance's own database, NOT all databases matching the prefix!
            // Dropping all prefix-matching databases causes race conditions when tests run in parallel,
            // because one test's cleanup will delete another test's data while it's still running.
            String ownDb = m.getConfig().connectionSettings().getDatabase();
            if (ownDb != null && !ownDb.isBlank()) {
                try {
                    log.info("{}: Dropping own database '{}' to ensure clean state", m.getDriver().getName(), ownDb);
                    DropDatabaseMongoCommand cmd = new DropDatabaseMongoCommand(m.getDriver().getPrimaryConnection(null));
                    cmd.setDb(ownDb);
                    cmd.setComment("Clean own database before test");
                    cmd.execute();
                    cmd.releaseConnection();

                    // Wait for drop to complete
                    int maxWait = m.getDriver() instanceof InMemoryDriver ? 1000 : 10_000;
                    long start = System.currentTimeMillis();
                    while (System.currentTimeMillis() - start < maxWait) {
                        try {
                            var dbs = m.listDatabases();
                            if (dbs == null || !dbs.contains(ownDb)) {
                                log.info("{}: Database '{}' confirmed dropped after {}ms",
                                         m.getDriver().getName(), ownDb, System.currentTimeMillis() - start);
                                break;
                            }
                            Thread.sleep(50);
                        } catch (Exception ignore) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.debug("{}: Error dropping own database (may not exist): {}",
                              m.getDriver().getName(), e.getMessage());
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
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        long existingCount = q.countAll();
        if (existingCount > 0) {
            log.warn("createUncachedObjects: Collection already has {} objects before insert! DB={}, driver={}, driverHash={}",
                     existingCount, morphium.getConfig().connectionSettings().getDatabase(),
                     morphium.getDriver().getName(), System.identityHashCode(morphium.getDriver()));
            // Try to drop the collection to ensure clean state
            log.warn("Attempting to drop collection to ensure clean state...");
            try {
                morphium.dropCollection(UncachedObject.class);
                Thread.sleep(200);
                long afterDrop = q.countAll();
                log.warn("After explicit collection drop: {} objects remain", afterDrop);
            } catch (Exception e) {
                log.error("Failed to drop collection", e);
            }
        }

        List<UncachedObject> lst = new ArrayList<>();

        for (int i = 0; i < amount; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i);
            uc.setStrValue("v");
            lst.add(uc);

            if (i % 1000 == 999) {
                morphium.insert(lst);
                lst.clear();
            }
        }

        morphium.storeList(lst);

        long targetCount = amount;
        TestUtils.waitForConditionToBecomeTrue(targetCount * 100, (dur, e)->log.error("Could not store"), ()->q.countAll() >= targetCount, (dur)->log.info("Waiting for data to be stored...{}/{}", q.countAll(), targetCount));
        log.info("createUncachedObjects complete: created {} objects, total now {}", amount, q.countAll());
    }

    public void createCachedObjects(Morphium morphium, int amount) {
        List<CachedObject> lst = new ArrayList<>();

        for (int i = 0; i < amount; i++) {
            CachedObject uc = new CachedObject();
            uc.setCounter(i);
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
