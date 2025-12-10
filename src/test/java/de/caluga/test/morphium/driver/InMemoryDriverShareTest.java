package de.caluga.test.morphium.driver;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.config.MessagingSettings;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MultiCollectionMessaging;
import de.caluga.morphium.messaging.SingleCollectionMessaging;
import de.caluga.morphium.objectmapping.AtomicIntegerMapper;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.test.mongo.suite.data.UncachedObject;
import de.caluga.test.mongo.suite.inmem.MorphiumInMemTestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Tag("inmemory")
public class InMemoryDriverShareTest extends MorphiumInMemTestBase {



    // @Test
    // public void performanceComparisionTest() throws Exception {

    //     MorphiumConfig cfg1 = new MorphiumConfig();
    //     cfg1.driverSettings().setDriverName(InMemoryDriver.driverName);
    //     cfg1.clusterSettings().addHostToSeed("mem");
    //     cfg1.connectionSettings().setDatabase("db1");
    //     Morphium inMem = new Morphium(cfg1);

    //     runPerformanceTest(inMem, 1000, SingleCollectionMessaging.NAME);
    //     runPerformanceTest(inMem, 1000, MultiCollectionMessaging.NAME);

    //     inMem.close();


    //     MorphiumConfig cfgPooled = new MorphiumConfig();
    //     cfgPooled.driverSettings().setDriverName(PooledDriver.driverName);
    //     cfgPooled.clusterSettings().addHostToSeed("mongo1.fritz.box");
    //     cfgPooled.clusterSettings().addHostToSeed("mongo2.fritz.box");
    //     cfgPooled.clusterSettings().addHostToSeed("mongo3.fritz.box");
    //     Morphium pooled = new Morphium(cfgPooled);

    //     runPerformanceTest(pooled, 1000, SingleCollectionMessaging.NAME);
    //     runPerformanceTest(pooled, 1000, MultiCollectionMessaging.NAME);
    //     pooled.close();

    // }


    // public void runPerformanceTest(Morphium m, int num, String impl) throws Exception {
    //     log.info("Running test with ---- {}", m.getDriver().getName());

    //     AtomicInteger count = new AtomicInteger();
    //     var msgcfg = (MessagingSettings)m.getConfig().messagingSettings().copy();
    //     msgcfg.setMessagingImplementation(impl);
    //     MorphiumMessaging sender = m.createMessaging(msgcfg);
    //     sender.start();
    //     m.clearCollection(Msg.class, sender.getCollectionName("test"));
    //     Thread.sleep(1000);

    //     MorphiumMessaging rec = m.createMessaging(msgcfg);
    //     rec.start();
    //     rec.addListenerForTopic("test", (me, msg)->{
    //         count.incrementAndGet();
    //         return null;
    //     });

    //     long start = System.currentTimeMillis();
    //     for (int i = 0; i < num; i++) {
    //         sender.sendMessage(new Msg("test", "test", "test" + i));
    //     }

    //     while (count.get() != num) {
    //         Thread.yield();
    //     }
    //     long dur = System.currentTimeMillis() - start;
    //     log.info("============>>>>>>>>> It took {}ms for {} messages and {} Messaging driver {}", dur, num, impl, m.getDriver().getName());
    //     rec.terminate();
    //     sender.terminate();
    // }

    @Test
    public void testSeparateInMemoryDrivers() throws Exception {
        InMemoryDriver drv1 = new InMemoryDriver();
        drv1.connect();

        InMemoryDriver drv2 = new InMemoryDriver();
        drv2.connect();

        String db = "testdb";
        String coll = "testcoll";

        // Insert data into the first driver
        InsertMongoCommand insert = new InsertMongoCommand(drv1).setDb(db).setColl(coll);
        insert.setDocuments(Arrays.asList(Doc.of("_id", new MorphiumId(), "value", "test value")));
        insert.execute();


        // Attempt to read the data from the second driver
        FindCommand find = new FindCommand(drv2).setDb(db).setColl(coll).setFilter(Doc.of());
        List<Map<String, Object>> result = find.execute();

        // Assert that the data is not found, proving the drivers are separate
        assertTrue(result.isEmpty(), "In-memory drivers should be separate by default and not share data.");

        drv1.close();
        drv2.close();
    }

    @Test
    public void testSeparateMorphiumInstancesDifferentDBs() throws InterruptedException {

        MorphiumConfig cfg1 = new MorphiumConfig();
        cfg1.driverSettings().setDriverName("InMemDriver");
        cfg1.clusterSettings().addHostToSeed("mem");
        cfg1.connectionSettings().setDatabase("db1");


        MorphiumConfig cfg2 = new MorphiumConfig();
        cfg2.driverSettings().setDriverName("InMemDriver");
        cfg2.clusterSettings().addHostToSeed("mem");
        cfg2.connectionSettings().setDatabase("db2");

        try (Morphium m1 = new Morphium(cfg1);
            Morphium m2 = new Morphium(cfg2)) {

            assertNotSame(m1.getDriver(), m2.getDriver(), "Morphium with same DB should not share in-memory driver instance by default.");

            m1.store(new UncachedObject("value", 10));
            Thread.sleep(1000);
            assertEquals(0, m2.createQueryFor(UncachedObject.class).countAll());
            assertNull(m2.createQueryFor(UncachedObject.class).get());
        }
    }
    @Test
    public void testSeparateMorphiumInstancesSameDBWithSharing() throws InterruptedException {
        String dbName = "shareddb";

        MorphiumConfig cfg1 = new MorphiumConfig();
        cfg1.driverSettings().setDriverName("InMemDriver");
        cfg1.driverSettings().setInMemorySharedDatabases(true);
        cfg1.clusterSettings().addHostToSeed("mem");
        cfg1.connectionSettings().setDatabase(dbName);


        MorphiumConfig cfg2 = new MorphiumConfig();
        cfg2.driverSettings().setDriverName("InMemDriver");
        cfg2.driverSettings().setInMemorySharedDatabases(true);
        cfg2.clusterSettings().addHostToSeed("mem");
        cfg2.connectionSettings().setDatabase(dbName);

        try (Morphium m1 = new Morphium(cfg1);
            Morphium m2 = new Morphium(cfg2)) {

            assertSame(m1.getDriver(), m2.getDriver(), "Morphium with same DB and sharing enabled should share in-memory driver instance.");

            m1.store(new UncachedObject("value", 10));
            Thread.sleep(1000);
            assertEquals(1, m2.createQueryFor(UncachedObject.class).countAll());
            assertNotNull(m2.createQueryFor(UncachedObject.class).get());
        }
    }

    @Test
    public void testSeparateMorphiumInstancesSameDBWithoutSharing() throws InterruptedException {
        String dbName = "shareddb_no_sharing";

        MorphiumConfig cfg1 = new MorphiumConfig();
        cfg1.driverSettings().setDriverName("InMemDriver");
        // inMemorySharedDatabases is false by default
        cfg1.clusterSettings().addHostToSeed("mem");
        cfg1.connectionSettings().setDatabase(dbName);


        MorphiumConfig cfg2 = new MorphiumConfig();
        cfg2.driverSettings().setDriverName("InMemDriver");
        // inMemorySharedDatabases is false by default
        cfg2.clusterSettings().addHostToSeed("mem");
        cfg2.connectionSettings().setDatabase(dbName);

        try (Morphium m1 = new Morphium(cfg1);
            Morphium m2 = new Morphium(cfg2)) {

            assertNotSame(m1.getDriver(), m2.getDriver(), "Morphium with same DB but sharing disabled should NOT share in-memory driver instance.");

            m1.store(new UncachedObject("value", 10));
            Thread.sleep(1000);
            assertEquals(0, m2.createQueryFor(UncachedObject.class).countAll(), "Data should not be visible in separate driver instance");
            assertNull(m2.createQueryFor(UncachedObject.class).get(), "Data should not be visible in separate driver instance");
        }
    }
}
