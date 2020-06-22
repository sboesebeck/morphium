package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.ShutdownListener;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.replicaset.OplogMonitor;
import de.caluga.morphium.writer.BufferedMorphiumWriterImpl;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.TestEntityNameProvider;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

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
public class MorphiumTestBase {

    public static Morphium morphium;
    private static Properties props;
    protected Logger log;

    public MorphiumTestBase() {
        log = LoggerFactory.getLogger(getClass().getName());
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

    }

    @org.junit.AfterClass
    public static void tearDownClass() {
        LoggerFactory.getLogger(MorphiumTestBase.class).info("NOT Shutting down - might be reused!");
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
        if (morphium == null) {
            MorphiumConfig cfg;
            Properties p = getProps();
            if (p.getProperty("database") != null) {
                cfg = MorphiumConfig.fromProperties(p);
                cfg.setMaxConnections(100);
                cfg.setBlockingThreadsMultiplier(2);
            } else {
                //creating default config
                cfg = new MorphiumConfig("morphium_test", 2055, 50000, 5000);
                cfg.addHostToSeed("localhost", 27017);
                //                cfg.addHostToSeed("localhost", 27018);
                //                cfg.addHostToSeed("localhost", 27019);
                cfg.setWriteCacheTimeout(1000);
                cfg.setConnectionTimeout(2000);
                cfg.setMaxWaitTime(2000);
                cfg.setMaxAutoReconnectTime(500);
                cfg.setMaxConnectionLifeTime(60000);
                cfg.setMaxConnectionIdleTime(30000);
                cfg.setMaxConnections(100);
                cfg.setMinConnectionsPerHost(1);
                cfg.setAutoreconnect(true);
                cfg.setMaximumRetriesBufferedWriter(1000);
                cfg.setMaximumRetriesWriter(1000);
                cfg.setMaximumRetriesAsyncWriter(1000);
                cfg.setRetryWaitTimeAsyncWriter(1000);
                cfg.setRetryWaitTimeWriter(1000);
                cfg.setRetryWaitTimeBufferedWriter(1000);
                cfg.setSocketTimeout(0);
                cfg.setSocketKeepAlive(true);
                cfg.setHeartbeatConnectTimeout(1000);
                cfg.setHeartbeatSocketTimeout(1000);
                cfg.setHeartbeatFrequency(500);
                cfg.setMinHearbeatFrequency(1000);

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

                cfg.setMaxAutoReconnectTime(5000);
                cfg.setDefaultReadPreference(ReadPreference.nearest());
                p.putAll(cfg.asProperties());
                p.put("failovertest", "false");
                cfg.setBlockingThreadsMultiplier(2);
                storeProps();
            }

            morphium = new Morphium(cfg);

        }
    }

    public boolean waitForAsyncOperationToStart(int maxWaits) {
        int cnt = 0;
        while (morphium.getWriteBufferCount() == 0) {
            Thread.yield();
            if (cnt++ > maxWaits) {
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
            uc.setValue("v");
            lst.add(uc);
            if (i % 1000 == 999) {
                morphium.storeList(lst);
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

    @org.junit.After
    public void tearDown() {
        if (morphium == null) return;
        logStats(morphium);
        try {
            Field f = morphium.getClass().getDeclaredField("shutDownListeners");
            f.setAccessible(true);
            List<ShutdownListener> listeners = (List<ShutdownListener>) f.get(morphium);
            for (ShutdownListener l : listeners) {
                if (l instanceof Messaging) {
                    Messaging msg = (Messaging) l;
                    if (msg.isRunning()) {
                        msg.terminate();
                        log.info("Terminating still running messaging..." + msg.getSenderId());
                        while (msg.isRunning()) {
                            log.info("Waiting for messaging to finish");
                            log.info("Messaging " + msg.getSenderId() + " still running");
                            Thread.sleep(100);
                        }
                        log.info("Messaging terminated");
                    }
                } else if (l instanceof OplogMonitor) {

                    OplogMonitor om = (OplogMonitor) l;
                    if (om.isRunning()) {
                        om.stop();
                        while (om.isRunning()) {
                            log.info("Waiting for oplogmonitor to finish");
                            Thread.sleep(100);
                        }
                        f = l.getClass().getDeclaredField("listeners");
                        f.setAccessible(true);
                        ((Collection) f.get(l)).clear();
                    }
                } else if (l instanceof ChangeStreamMonitor) {

                    log.info("Changestream Monitor still running");
                    ChangeStreamMonitor csm = (ChangeStreamMonitor) l;
                    if (csm.isRunning()) {
                        csm.terminate();
                        while (csm.isRunning()) {
                            log.info("Waiting for changestreamMonitor to finish");
                            Thread.sleep(100);
                        }
                        log.info("ChangeStreamMonitor terminated!");
                        f = l.getClass().getDeclaredField("listeners");
                        f.setAccessible(true);
                        ((Collection) f.get(l)).clear();
                    }
                } else if (l instanceof BufferedMorphiumWriterImpl) {
                    BufferedMorphiumWriterImpl buf = (BufferedMorphiumWriterImpl) l;
                    buf.close();
                }
            }
        } catch (Exception e) {
            log.error("Could not shutdown properly!", e);
        }
        try {
            log.info("---------------------------------------- Re-connecting to mongo");
            int num = TestEntityNameProvider.number.incrementAndGet();
            log.info("------ > Number: " + num);

            log.info("resetting DB...");
//            List<String> lst = morphium.getDriver().getCollectionNames(morphium.getConfig().getDatabase());
//            for (String col : lst) {
//                log.info("Dropping " + col);
//                morphium.getDriver().drop(morphium.getConfig().getDatabase(), col, morphium.getWriteConcernForClass(UncachedObject.class));
//            }
            boolean ok = false;

            while (!ok) {
                try {
                    morphium.getDriver().drop(morphium.getConfig().getDatabase(), WriteConcern.getWc(1, true, false, 1000));
                    ok = true;
                } catch (MorphiumDriverException e) {
                    Thread.sleep(200);
                    e.printStackTrace();
                }
            }

            morphium.getCache().resetCache();
            morphium.reset();
            morphium = null;
            Thread.sleep(200);
        } catch (Exception e) {
            log.error("Error during preparation!");
            e.printStackTrace();
        }

    }

    public void waitForWrites() {
        waitForWrites(morphium);
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

    public boolean stringWordCompare(String m1, String m2) {
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

}
