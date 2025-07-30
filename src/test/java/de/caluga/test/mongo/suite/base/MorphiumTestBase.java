package de.caluga.test.mongo.suite.base;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import de.caluga.morphium.messaging.StdMessaging;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.ShutdownListener;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.commands.ListCollectionsCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.query.Query;
import de.caluga.test.OutputHelper;
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
public class MorphiumTestBase {

    public static Morphium morphium;
    private static Properties props;
    private static File configDir;
    protected Logger log;
    public static AtomicInteger number = new AtomicInteger(0);

    public MorphiumTestBase() {
        log = LoggerFactory.getLogger(getClass().getName());
    }

    public static synchronized Properties getProps() {
        if (props == null) {
            props = new Properties();
            File f = getFile();

            if (f.exists()) {
                try {
                    props.load(new FileReader(f));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return props;
    }

    private static File getFile() {
        configDir = new File(System.getProperty("user.home") + "/.config/");

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

    private void init() {
        log.info("in init!");

        if (morphium == null) {
            MorphiumConfig cfg;
            Properties p = getProps();

            if (p.getProperty("database") != null) {
                cfg = MorphiumConfig.fromProperties(p);
                cfg.setMaxConnections(300);
                cfg.setThreadConnectionMultiplier(2);
            } else {
                //creating default config
                cfg = new MorphiumConfig("morphium_test", 2055, 50000, 5000);
                cfg.addHostToSeed("localhost", 27017);
                cfg.addHostToSeed("localhost", 27018);
                cfg.addHostToSeed("localhost", 27019);
                cfg.setMongoAuthDb("admin");
                cfg.setMongoPassword("test");
                cfg.setMongoLogin("test");
                cfg.setCompressionType(MorphiumConfig.CompressionType.SNAPPY);
                cfg.setCredentialsEncrypted(false);
                cfg.setWriteCacheTimeout(1000);
                cfg.setConnectionTimeout(2000);
                cfg.setRetryReads(false);
                cfg.setRetryWrites(false);
                cfg.setReadTimeout(1000);
                cfg.setMaxWaitTime(10000);
                cfg.setMaxConnectionLifeTime(60000);
                cfg.setMaxConnectionIdleTime(30000);
                cfg.setMaxConnections(300);
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

            morphium = new Morphium(cfg);
            log.info("Morphium instanciated");
        }

        // int num = number.incrementAndGet();
        OutputHelper.figletOutput(log, "---------");
        // OutputHelper.figletOutput(log, "Test#: " + num);
        // try {
        //     if (!morphium.getConfig().isAtlas()) {
        //         log.info("Dropping database: " + morphium.getConfig().getDatabase());
        //         DropDatabaseMongoCommand settings = new DropDatabaseMongoCommand(morphium.getDriver().getPrimaryConnection(null));
        //         settings.setComment("Dropping from morphiumg test base");
        //         settings.setDb(morphium.getConfig().getDatabase());
        //         settings.execute();
        //         settings.releaseConnection();
        //     }
        // } catch (MorphiumDriverException e) {
        //     e.printStackTrace();
        // }
        log.info("Init complete");
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (morphium == null) {
            return;
        }

        logStats(morphium);
        morphium.getCache().resetCache();
        morphium.resetStatistics();
        //looking for registered shutdownListeners
        List<ShutdownListener> toRemove = new ArrayList<>();

        for (ShutdownListener l : morphium.getShutDownListeners()) {
            if (l instanceof StdMessaging) {
                try {
                    ((StdMessaging) l).terminate();
                } catch (Exception e) {
                    log.error("could not terminate messaging!!!");
                }

                long start = System.currentTimeMillis();
                var id = ((StdMessaging) l).getSenderId();
                log.info("Terminating still running messaging..." + id);

                while (((StdMessaging) l).isRunning()) {
                    log.info("Waiting for messaging to finish");

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        //swallow
                    }

                    if (System.currentTimeMillis() - start > 5000) {
                        throw new RuntimeException("Could not kill Messaging: " + id);
                    }
                }

                toRemove.add(l);
                morphium.dropCollection(Msg.class, ((StdMessaging) l).getCollectionName(), null);
            } else if (l instanceof ChangeStreamMonitor) {
                try {
                    log.info("Changestream Monitor still running");
                    ((ChangeStreamMonitor) l).terminate();

                    while (((ChangeStreamMonitor) l).isRunning()) {
                        log.info("Waiting for changestreamMonitor to finish");
                        Thread.sleep(100);
                    }

                    Field f = l.getClass().getDeclaredField("listeners");
                    f.setAccessible(true);
                    ((Collection) f.get(l)).clear();
                    toRemove.add(l);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        for (ShutdownListener t : toRemove) {
            morphium.removeShutdownListener(t);
        }

        MongoConnection con = null;
        ListCollectionsCommand cmd = null;

        try {
            boolean retry = true;

            while (retry) {
                con = morphium.getDriver().getPrimaryConnection(null);
                cmd = new ListCollectionsCommand(con).setDb(morphium.getDatabase());
                var lst = cmd.execute();
                cmd.releaseConnection();

                for (var collMap : lst) {
                    String coll = (String) collMap.get("name");
                    log.info("Dropping collection " + coll);

                    try {
                        morphium.clearCollection(UncachedObject.class, coll); //faking it
                    } catch (Exception e) {
                        //e.printStackTrace();
                    }

                    morphium.dropCollection(UncachedObject.class, coll, null); //faking it a bit ;-)
                }

                long start = System.currentTimeMillis();
                retry = false;
                boolean collectionsExist = true;

                while (collectionsExist) {
                    Thread.sleep(100);
                    con = morphium.getDriver().getPrimaryConnection(null);
                    cmd = new ListCollectionsCommand(con).setDb(morphium.getDatabase());
                    lst = cmd.execute();
                    cmd.releaseConnection();

                    for (var k : lst) {
                        log.info("Collections still there..." + k.get("name"));
                    }

                    if (System.currentTimeMillis() - start > 1500) {
                        retry = true;
                        break;
                    }

                    if (lst.size() == 0) {
                        collectionsExist = false;
                    }
                }
            }
        } catch (Exception e) {
            log.info("Could not clean up: ", e);
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }

        Thread.sleep(150);
    }

    public boolean waitForAsyncOperationsToStart(long maxWait) {
        return waitForAsyncOperationsToStart(morphium, maxWait);
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

    //
    //    public void createUncachedObjectsInMemory(int amount) {
    //        createUncachedObjects(morphiumInMemeory, amount);
    //    }

    public void createUncachedObjects(int amount) {
        createUncachedObjects(morphium, amount);
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

    public void createCachedObjects(int amount) {
        createCachedObjects(morphium, amount);
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

    @BeforeEach
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

    public void waitForWrites() {
        TestUtils.waitForWrites(morphium, log);
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

}
