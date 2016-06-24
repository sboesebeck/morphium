package de.caluga.test.mongo.suite;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.meta.MetaDriver;
import de.caluga.morphium.driver.singleconnect.SingleConnectDirectDriver;
import de.caluga.morphium.driver.singleconnect.SingleConnectThreaddedDriver;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.query.PrefetchingMorphiumIterator;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.Person;
import de.caluga.test.mongo.suite.data.UncachedObject;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;


/**
 * User: Stpehan Bösebeck
 * Date: 26.02.12
 * Time: 16:17
 * <p/>
 */
public class MongoTest {

    public static Morphium morphium;
    public static Morphium morphiumInMemeory;
    public static Morphium morphiumSingleConnect;
    public static Morphium morphiumSingleConnectThreadded;
    public static Morphium morphiumMeta;
    public static Morphium morphiumMongodb;
    private static List<Morphium> morphiums;
    private static Properties props;
    protected Logger log;


    public MongoTest() {
        log = new Logger(getClass().getName());
        log.setLevel(5);
        log.setSynced(true);
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
    public static synchronized void setUpClass() throws Exception {
        //        System.setProperty("morphium.log.level", "4");
        //        System.setProperty("morphium.log.synced", "true");
        //        System.setProperty("morphium.log.file", "-");
        java.util.logging.Logger l = java.util.logging.Logger.getGlobal();
        l.setLevel(Level.SEVERE);
        //        l.addHandler(new Handler() {
        //            @Override
        //            public void publish(LogRecord record) {
        //                Logger l=new Logger(record.getLoggerName());
        //                if (record.getLevel().equals(Level.ALL)) {
        //                    l.debug(record.getMessage(),record.getThrown());
        //                } else if (record.getLevel().equals(Level.FINE)) {
        //                    l.warn(record.getMessage(),record.getThrown());
        //                } else if (record.getLevel().equals(Level.FINER)) {
        //                    l.info(record.getMessage(),record.getThrown());
        //                } else if (record.getLevel().equals(Level.FINEST)) {
        //                    l.debug(record.getMessage(),record.getThrown());
        //                } else if (record.getLevel().equals(Level.CONFIG)) {
        //                    l.debug(record.getMessage(),record.getThrown());
        //                } else if (record.getLevel().equals(Level.INFO)) {
        //                    l.info(record.getMessage(),record.getThrown());
        //                } else if (record.getLevel().equals(Level.WARNING)) {
        //                    l.warn(record.getMessage(),record.getThrown());
        //                } else if (record.getLevel().equals(Level.SEVERE)) {
        //                    l.fatal(record.getMessage(),record.getThrown());
        //                } else  {
        //                    l.info(record.getMessage(),record.getThrown());
        //
        //                }
        //            }
        //
        //            @Override
        //            public void flush() {
        //
        //            }
        //
        //            @Override
        //            public void close() throws SecurityException {
        //
        //            }
        //        });
        l = java.util.logging.Logger.getLogger("connection");
        l.setLevel(java.util.logging.Level.OFF);
        if (morphium == null) {
            MorphiumConfig cfg = null;
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
                cfg.setDefaultReadPreference(ReadPreference.nearest());
                p.putAll(cfg.asProperties());
                p.put("failovertest", "false");
                cfg.setBlockingThreadsMultiplier(2);
                storeProps();
            }
            cfg.setDefaultReadPreference(ReadPreference.secondaryPreferred());
            cfg.setDefaultReadPreferenceType("SECONDARY_PREFERRED");
            //Setting up logging
            cfg.setGlobalLogLevel(4);
            cfg.setGlobalLogFile("-");
            cfg.setGlobalLogSynced(true);

            cfg.setLogLevelForClass(AnnotationAndReflectionHelper.class, 4);
            cfg.setLogLevelForClass(PrefetchingMorphiumIterator.class, 3);
            cfg.setLogLevelForPrefix("de.caluga.test", 5);
            cfg.setLogSyncedForPrefix("de.caluga.test", true);
            cfg.setLogLevelForClass(SingleConnectThreaddedDriver.class, 5);
            //            cfg.setLogLevelForPrefix("de.caluga.morphium.driver", 3);
            //            cfg.setLogLevelForPrefix(MetaDriver.class.getName(), 5);

            //InMemoryTest
            //            cfg.setDriverClass(InMemoryDriver.class.getName());
            //MetaDriverTEst
            //            cfg.setDriverClass(MetaDriver.class.getName());
            //            cfg.setDriverClass(SingleConnectDirectDriver.class.getName());
            //            cfg.setDriverClass(InMemoryDriver.class.getName());
            cfg.setReplicasetMonitoring(true);


            morphium = new Morphium(cfg);

            morphiumMongodb = morphium;
            MorphiumConfig cfgtmp = MorphiumConfig.createFromJson(cfg.toString());
            cfgtmp.setDriverClass(MetaDriver.class.getName());
            morphiumMeta = new Morphium(cfgtmp);


            cfgtmp = MorphiumConfig.createFromJson(cfg.toString());
            cfgtmp.setDriverClass(InMemoryDriver.class.getName());
            cfgtmp.setReplicasetMonitoring(false);
            morphiumInMemeory = new Morphium(cfgtmp);

            cfgtmp = MorphiumConfig.createFromJson(cfg.toString());
            cfgtmp.setDriverClass(SingleConnectDirectDriver.class.getName());
            morphiumSingleConnect = new Morphium(cfgtmp);

            cfgtmp = MorphiumConfig.createFromJson(cfg.toString());
            cfgtmp.setDriverClass(SingleConnectThreaddedDriver.class.getName());
            morphiumSingleConnectThreadded = new Morphium(cfgtmp);


            //            morphium.addListener(new MorphiumStorageAdapter() {
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


    public static synchronized List<Morphium> getMorphiums() {
        if (morphiums == null) {
            morphiums = new ArrayList<>();
            morphiums.add(morphiumMongodb);
            morphiums.add(morphiumInMemeory);
            morphiums.add(morphiumSingleConnect);
            morphiums.add(morphiumSingleConnectThreadded);
            morphiums.add(morphiumMeta);
        }
        return morphiums;
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
        //        morphium.close();
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
        while (q.countAll() != amount) {
            log.info("Waiting for data to be stored..." + q.countAll());
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
            morphium.dropCollection(UncachedObject.class, cb);
            morphium.dropCollection(CachedObject.class, cb);
            morphium.dropCollection(ComplexObject.class, cb);
            morphium.dropCollection(EnumEntity.class, cb);
            morphium.dropCollection(Msg.class, cb);
            morphium.dropCollection(Person.class, cb);
            //            morphium.ensureIndex(UncachedObject.class, "counter", "value");
            //            morphium.ensureIndex(CachedObject.class, "counter", "value");
            waitForAsyncOperationToStart(1000);
            waitForWrites();
            //            Thread.sleep(1000);
            int count = 0;
            while (count < 10) {
                count++;
                Map<String, Double> stats = morphium.getStatistics();
                Double ent = stats.get("X-Entries for: de.caluga.test.mongo.suite.data.CachedObject");
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
        morphium.dropCollection(UncachedObject.class);
        morphium.dropCollection(CachedObject.class);
        morphium.dropCollection(Msg.class);
        waitForWrites();
        log.info("done...");
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
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }
    }

    public void logSeparator(String s) {
        log.getOutput().println("\n\n************************************************************");
        log.getOutput().println("***    " + s);
        log.getOutput().println("************************************************************\n");
    }
}
