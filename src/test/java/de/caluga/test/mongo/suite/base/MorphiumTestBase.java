package de.caluga.test.mongo.suite.base;

// (removed local file config I/O)
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import de.caluga.morphium.messaging.MorphiumMessaging;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.ShutdownListener;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
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
 * <p>
 */
public class MorphiumTestBase {

    public static List<String> messagingsToTest = List.of("MultiCollectionMessaging", "StandardMessaging" );
    public static Morphium morphium;
    protected Logger log;
    public static AtomicInteger number = new AtomicInteger(0);

    public MorphiumTestBase() {
        log = LoggerFactory.getLogger(getClass().getName());
    }

    // Local file-based test configuration removed; using centralized TestConfig instead.

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

    private void init(TestInfo info) {
        log.info("in init!");

        if (morphium == null) {
            MorphiumConfig cfg = de.caluga.test.support.TestConfig.load();
            // Per-test unique database name for fast teardown, enforce 63-char limit
            String cls = sanitize(info.getTestClass().map(Class::getSimpleName).orElse("UnknownClass"));
            String mth = sanitize(info.getDisplayName());
            String db = buildDbName(cls, mth, number.incrementAndGet());
            cfg.connectionSettings().setDatabase(db);
            morphium = new Morphium(cfg);
            log.info("Morphium instantiated with database: {} (instance: {})", db, System.identityHashCode(morphium));
        }

        // int num = number.incrementAndGet();
        if (Boolean.getBoolean("morphium.tests.verbose")) {
            OutputHelper.figletOutput(log, "---------");
        }
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
        log.info("Shutting down...");
        if (morphium == null) {
            return;
        }

        logStats(morphium);
        morphium.getCache().resetCache();
        morphium.resetStatistics();
        //looking for registered shutdownListeners
        List<ShutdownListener> toRemove = new ArrayList<>();

        for (ShutdownListener l : morphium.getShutDownListeners()) {
            if (l instanceof MorphiumMessaging) {
                try {
                    ((MorphiumMessaging) l).terminate();
                } catch (Exception e) {
                    log.error("could not terminate messaging!!!");
                }

                long start = System.currentTimeMillis();
                var id = ((MorphiumMessaging) l).getSenderId();
                log.info("Terminating still running messaging..." + id);

                while (((MorphiumMessaging) l).isRunning()) {
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
                morphium.dropCollection(Msg.class, ((MorphiumMessaging) l).getCollectionName(), null);
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

        // Drop whole database for this test and close the instance
        try {
            var drop = new de.caluga.morphium.driver.commands.DropDatabaseMongoCommand(morphium.getDriver().getPrimaryConnection(null));
            drop.setDb(morphium.getDatabase());
            drop.setComment("Dropping from MorphiumTestBase teardown");
            drop.execute();
            drop.releaseConnection();
        } catch (Exception e) {
            log.info("Could not drop DB '" + morphium.getDatabase() + "': ", e);
        }

        try { morphium.close(); } catch (Exception ignore) {}
        morphium = null;
        Thread.sleep(50);
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
            uc.setCounter(i);
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

    @BeforeEach
    public void setUp(TestInfo info) {
        init(info);
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

    private static String sanitize(String s) {
        return s.replaceAll("[^A-Za-z0-9_]+", "_");
    }

    private static String buildDbName(String cls, String mth, int counter) {
        final int MAX = 63;
        String suffix = "_" + counter;
        String base = "morphium_test_" + cls + "_" + mth;
        // Trim if needed to fit 63 chars
        int allowed = MAX - suffix.length();
        if (allowed < 1) {
            // Fallback minimal name
            return ("mt_" + counter).substring(0, Math.min(MAX, ("mt_" + counter).length()));
        }
        String stem = base.length() > allowed ? base.substring(0, allowed) : base;
        // Ensure no trailing illegal characters (already sanitized to underscores and alnum)
        return stem + suffix;
    }

}
