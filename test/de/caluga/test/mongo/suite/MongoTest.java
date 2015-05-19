package de.caluga.test.mongo.suite;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Logger;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.query.MorphiumIteratorImpl;
import de.caluga.morphium.query.Query;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * User: Stpehan Bösebeck
 * Date: 26.02.12
 * Time: 16:17
 * <p/>
 */
public class MongoTest {
    protected Logger log;
    private static Properties props;

    public MongoTest() {
        log = new Logger(getClass().getName());
        log.setLevel(5);
        log.setSynced(true);
    }

    public static Properties getProps() {
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
    public static void setUpClass() throws Exception {
        System.setProperty("morphium.log.level", "5");
        System.setProperty("morphium.log.synced", "true");
        System.setProperty("morphium.log.file", "-");
        if (!MorphiumSingleton.isConfigured()) {
            MorphiumConfig cfg = null;
            Properties p = getProps();
            if (p.getProperty("database") != null) {
                cfg = MorphiumConfig.fromProperties(p);
                cfg.setMaxConnections(100);
                cfg.setBlockingThreadsMultiplier(100);
            } else {
                //creating default config
                cfg = new MorphiumConfig("morphium_test", 2055, 50000, 5000);
                cfg.addHost("localhost", 27017);
                cfg.addHost("localhost", 27018);
                cfg.addHost("localhost", 27019);
                cfg.setWriteCacheTimeout(100);
                cfg.setConnectionTimeout(1000);
                cfg.setMaxWaitTime(10000);
                cfg.setMaxConnections(2000);
                cfg.setMinConnectionsPerHost(1);
                cfg.setMaxConnectionIdleTime(500);
                cfg.setAutoreconnect(true);
                cfg.setMaximumRetriesBufferedWriter(1000);
                cfg.setMaximumRetriesWriter(1000);
                cfg.setMaximumRetriesAsyncWriter(1000);
                cfg.setRetryWaitTimeAsyncWriter(1000);
                cfg.setRetryWaitTimeWriter(1000);
                cfg.setRetryWaitTimeBufferedWriter(1000);
                cfg.setGlobalFsync(false);
                cfg.setGlobalJ(false);
                cfg.setGlobalW(1);



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
                cfg.setDefaultReadPreference(ReadPreferenceLevel.NEAREST);
                p.putAll(cfg.asProperties());
                p.put("failovertest", "false");
                cfg.setBlockingThreadsMultiplier(100);
                storeProps();
            }
            cfg.setGlobalLogLevel(3);
            cfg.setGlobalLogFile("-");
            cfg.setGlobalLogSynced(true);

            cfg.setLogLevelForClass(AnnotationAndReflectionHelper.class, 4);
            cfg.setLogLevelForClass(MorphiumIteratorImpl.class, 3);
            cfg.setLogLevelForPrefix("de.caluga.test", 5);
            cfg.setLogSyncedForPrefix("de.caluga.test", true);
            MorphiumSingleton.setConfig(cfg);
            MorphiumSingleton.get();

//            MorphiumSingleton.get().addListener(new MorphiumStorageAdapter() {
//                @Override
//                public void preStore(Morphium m, Object r, boolean isNew) {
//                    if (m.getARHelper().isBufferedWrite(r.getClass())) {
//                        new Logger(MongoTest.class).info("Buffered store of "+r.getClass());
//                    }
//                }
//
//                @Override
//                public void preRemove(Morphium m, Object r) {
//                     if (m.getARHelper().isBufferedWrite(r.getClass())) {
//                        new Logger(MongoTest.class).info("Buffered remove of "+r.getClass());
//                    }
//                }
//
//                @Override
//                public void preRemove(Morphium m, Query q) {
//                    if (m.getARHelper().isBufferedWrite(q.getType())) {
//                        new Logger(MongoTest.class).info("Buffered remove of "+q.getType(),new Exception());
//                    }
//                }
//
//                @Override
//                public void preDrop(Morphium m, Class cls) {
//                    if (m.getARHelper().isBufferedWrite(cls)) {//                        new Logger(MongoTest.class).info("Buffered drop of "+cls);
//                    }
//                }
//
//                @Override
//                public void preUpdate(Morphium m, Class cls, Enum updateType) {
//                    if (m.getARHelper().isBufferedWrite(cls)) {
//                        new Logger(MongoTest.class).info("Buffered update of "+cls);
//                    }
//                }
//            });
        }
    }

    private static void storeProps() {
        File f = getFile();
        try {
            getProps().store(new FileWriter(f), "created by morphium test");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @org.junit.AfterClass
    public static void tearDownClass() throws Exception {
        new Logger(MongoTest.class).info("NOT Shutting down - might be reused!");
//        MorphiumSingleton.get().close();
    }

    public boolean waitForAsyncOperationToStart(int maxWaits) {
        int cnt = 0;
        while (MorphiumSingleton.get().getWriteBufferCount() == 0) {
            Thread.yield();
            if (cnt++ > maxWaits) return false;
        }
        return true;
    }

    public void createUncachedObjects(int amount) {
        List<UncachedObject> lst = new ArrayList<UncachedObject>();
        for (int i = 0; i < amount; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i + 1);
            uc.setValue("v");
            lst.add(uc);
            if (i % 1000 == 999) {
                MorphiumSingleton.get().storeList(lst);
                lst.clear();
            }
        }
        MorphiumSingleton.get().storeList(lst);
    }

    public void createCachedObjects(int amount) {
        List<CachedObject> lst = new ArrayList<CachedObject>();
        for (int i = 0; i < amount; i++) {
            CachedObject uc = new CachedObject();
            uc.setCounter(i + 1);
            uc.setValue("v");
            lst.add(uc);
        }
        MorphiumSingleton.get().storeList(lst);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @org.junit.Before
    public void setUp() throws Exception {

        try {
            log.info("Preparing collections...");
            AsyncOperationCallback cb = new AsyncOperationCallback() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query q, long duration, List result, Object entity, Object... param) {

                }

                @Override
                public void onOperationError(AsyncOperationType type, Query q, long duration, String error, Throwable t, Object entity, Object... param) {
                    log.error("Error: " + error, t);
                }
            };
            MorphiumSingleton.get().dropCollection(UncachedObject.class, cb);
            MorphiumSingleton.get().dropCollection(CachedObject.class, cb);
            MorphiumSingleton.get().dropCollection(ComplexObject.class, cb);
            MorphiumSingleton.get().dropCollection(EnumEntity.class, cb);
            MorphiumSingleton.get().dropCollection(Msg.class, cb);
            MorphiumSingleton.get().dropCollection(Person.class, cb);
//            MorphiumSingleton.get().ensureIndex(UncachedObject.class, "counter", "value");
//            MorphiumSingleton.get().ensureIndex(CachedObject.class, "counter", "value");
            waitForAsyncOperationToStart(1000);
            waitForWrites();
//            Thread.sleep(1000);
            int count = 0;
            while (count < 10) {
                count++;
                Map<String, Double> stats = MorphiumSingleton.get().getStatistics();
                Double ent = stats.get("X-Entries for: de.caluga.test.mongo.suite.CachedObject");
                if (ent == null || ent == 0) {
                    break;
                }
                Thread.sleep(2000);
            }

            assert (count < 10) : "Cache not cleared? count is " + count;

            log.info("Preparation finished");
        } catch (Exception e) {
            log.fatal("Error during preparation!");
            e.printStackTrace();
        }

    }

    @org.junit.After
    public void tearDown() throws Exception {
        log.info("Cleaning up...");
        MorphiumSingleton.get().dropCollection(UncachedObject.class);
        MorphiumSingleton.get().dropCollection(CachedObject.class);
        MorphiumSingleton.get().dropCollection(Msg.class);
        waitForWrites();
        log.info("done...");
    }

    public void waitForWrites() {
        int count = 0;
        while (MorphiumSingleton.get().getWriteBufferCount() > 0) {
            count++;
            if (count % 100 == 0)
                log.info("still " + MorphiumSingleton.get().getWriteBufferCount() + " writers active (" + MorphiumSingleton.get().getBufferedWriterBufferCount() + " + " + MorphiumSingleton.get().getWriterBufferCount() + ")");
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
            }
        }
        //waiting for it to be persisted
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }
    }
}
