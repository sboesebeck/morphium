package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.messaging.Msg;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * User: Stpehan Bösebeck
 * Date: 26.02.12
 * Time: 16:17
 * <p/>
 */
public class MongoTest {
    protected Logger log;

    public MongoTest() {
        log = Logger.getLogger(getClass().getName());
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
    }


    @org.junit.Before
    public void setUp() throws Exception {

        try {
            log.info("Preparing collections...");
            MorphiumSingleton.get().clearCollection(UncachedObject.class);
            MorphiumSingleton.get().clearCollection(CachedObject.class);
            MorphiumSingleton.get().clearCollection(ComplexObject.class);
            MorphiumSingleton.get().clearCollection(EnumEntity.class);
            MorphiumSingleton.get().clearCollection(Msg.class);
            MorphiumSingleton.get().ensureIndex(UncachedObject.class, "counter", "value");
            MorphiumSingleton.get().ensureIndex(CachedObject.class, "counter", "value");
//            Thread.sleep(1000);
            Map<String, Double> stats = MorphiumSingleton.get().getStatistics();
            Double ent = stats.get("X-Entries for: de.caluga.test.mongo.suite.CachedObject");
            assert (ent == null || ent == 0) : "Still cache entries???";
            ent = stats.get("X-Entries for: de.caluga.test.mongo.suite.UncachedObject");
            assert (ent == null || ent == 0) : "Uncached Object cached?";

            waitForWrites();
            log.info("Preparation finished");
        } catch (Exception e) {
            log.fatal("Error during preparation!");
            e.printStackTrace();
        }

    }

    @org.junit.After
    public void tearDown() throws Exception {
        log.info("Cleaning up...");
        MorphiumSingleton.get().clearCollection(UncachedObject.class);
        MorphiumSingleton.get().clearCollection(CachedObject.class);
        MorphiumSingleton.get().clearCollection(Msg.class);
        waitForWrites();
        log.info("done...");
    }


    @org.junit.BeforeClass
    public static void setUpClass() throws Exception {
        if (!MorphiumSingleton.isConfigured()) {
            MorphiumConfig cfg = new MorphiumConfig("morphium_test", 55, 50000, 5000, "morphium-log4j-test.xml");
//            cfg.setTimeoutBugWorkAroundEnabled(true);


            cfg.addAddress("localhost", 27017);
            cfg.addAddress("localhost", 27018);
            cfg.addAddress("localhost", 27019);
//            cfg.addAddress("mongo1", 27017);
//            cfg.addAddress("mongo2", 27017);
//            cfg.addAddress("mongo3", 27017);
            cfg.setWriteCacheTimeout(100);
            cfg.setConnectionTimeout(10000);
            cfg.setMaxWaitTime(1000);
            cfg.setAutoreconnect(true);
            cfg.setMaximumRetriesBufferedWriter(1000);
            cfg.setMaximumRetriesWriter(1000);
            cfg.setMaximumRetriesAsyncWriter(1000);
            cfg.setRetryWaitTimeAsyncWriter(1000);
            cfg.setRetryWaitTimeWriter(1000);
            cfg.setRetryWaitTimeBufferedWriter(1000);
//            cfg.setMongoLogin("morphium");
//            cfg.setMongoPassword("tst");

            //necessary for Replicaset Status to work
//            cfg.setMongoAdminUser("admin");
//            cfg.setMongoAdminPwd("admin");

            cfg.setMaxAutoReconnectTime(5000);
            cfg.setDefaultReadPreference(ReadPreferenceLevel.NEAREST);
            MorphiumSingleton.setConfig(cfg);
            MorphiumSingleton.get();

//            MorphiumSingleton.get().addListener(new MorphiumStorageAdapter() {
//                @Override
//                public void preStore(Morphium m, Object r, boolean isNew) {
//                    if (m.getARHelper().isBufferedWrite(r.getClass())) {
//                        Logger.getLogger(MongoTest.class).info("Buffered store of "+r.getClass());
//                    }
//                }
//
//                @Override
//                public void preDelete(Morphium m, Object r) {
//                     if (m.getARHelper().isBufferedWrite(r.getClass())) {
//                        Logger.getLogger(MongoTest.class).info("Buffered delete of "+r.getClass());
//                    }
//                }
//
//                @Override
//                public void preRemove(Morphium m, Query q) {
//                    if (m.getARHelper().isBufferedWrite(q.getType())) {
//                        Logger.getLogger(MongoTest.class).info("Buffered remove of "+q.getType(),new Exception());
//                    }
//                }
//
//                @Override
//                public void preDrop(Morphium m, Class cls) {
//                    if (m.getARHelper().isBufferedWrite(cls)) {//                        Logger.getLogger(MongoTest.class).info("Buffered drop of "+cls);
//                    }
//                }
//
//                @Override
//                public void preUpdate(Morphium m, Class cls, Enum updateType) {
//                    if (m.getARHelper().isBufferedWrite(cls)) {
//                        Logger.getLogger(MongoTest.class).info("Buffered update of "+cls);
//                    }
//                }
//            });
        }
    }

    @org.junit.AfterClass
    public static void tearDownClass() throws Exception {
        System.out.println("NOT Shutting down - might be reused!");
//        MorphiumSingleton.get().close();
    }

    public void waitForWrites() {
        int count = 0;
        while (MorphiumSingleton.get().getWriteBufferCount() > 0) {
            count++;
            if (count % 100 == 0)
                log.info("still " + MorphiumSingleton.get().getWriteBufferCount() + " writers active (" + MorphiumSingleton.get().getBufferedWriterBufferCount() + " + " + MorphiumSingleton.get().getWriterBufferCount() + ")");
            try {
                Thread.sleep(10);
            } catch (InterruptedException ex) {
                Logger.getLogger(BasicFunctionalityTest.class).fatal(ex);
            }
        }
        //waiting for it to be persisted
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }
    }
}
