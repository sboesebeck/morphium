package de.caluga.test.mongo.suite.base;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.caluga.test.mongo.suite.data.*;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.StatisticKeys;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.objectmapping.MorphiumTypeMapper;
import de.caluga.morphium.query.Query;

/**
 * @author stephan
 */
@SuppressWarnings({"AssertWithSideEffects", "Duplicates", "unchecked"})
public class BasicFunctionalityTest extends MultiDriverTestBase {
    public static final int NO_OBJECTS = 100;
    private static final Logger log = LoggerFactory.getLogger(BasicFunctionalityTest.class);

    public BasicFunctionalityTest() {
    }

    // @Test
    // public void shardTest() throws Exception {
    //     MorphiumConfig cfg=new MorphiumConfig();
    //     cfg.setHostSeed("hercules1.dev.genios.de:27017");
    //     cfg.setDatabase("hercules");
    //     Morphium m=new Morphium(cfg);
    //
    //     log.info("connected");
    // }
    //
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testAdditionalData(Morphium morphium) throws Exception {
        try (morphium) {
            log.info("start: " + morphium.getDriver().getName());

            for (int i = 0; i < NO_OBJECTS; i++) {
                AdditionalDataEntity add = new AdditionalDataEntity();
                add.setCounter(i);
                add.setStrValue("str" + i);
                morphium.store(add);

                if (i % 10 == 0) {
                    log.info("Stored..." + i);
                }
            }

            log.info("Stored...");
            var q = morphium.createQueryFor(AdditionalDataEntity.class).f("counter").eq(10);
            q.set("additional.value", "was set");
            Thread.sleep(100);
            var ae = q.get();
            assertNotNull(ae.getAdditionals());
            assertNotNull(ae.getAdditionals().get("additional"));
            assertEquals("was set", ((Map) ae.getAdditionals().get("additional")).get("value"));
            morphium.set(ae, "str_value", (Object) null, false, null);
            assertNull(ae.getStrValue());
            morphium.set(ae, "add.value", "the value");
            assertNotNull(ae.getAdditionals().get("add"));
            log.info("finish");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testConnectionFail(Morphium morphium) {
        if (morphium.getDriver() instanceof InMemoryDriver) {
            return;
        }

        try (morphium) {
            //trying to connect with the sepcified driver to non existent mongo
            MorphiumConfig cfg = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
            cfg.setCredentialsEncrypted(false);
            cfg.setHostSeed("localhost:12312"); //does not exist, but should be easy to find
            Morphium m = new Morphium(cfg);
            log.info("No exception!");
            assertTrue(false, "Exception not thrown!");
        } catch (Exception e) {
            log.info("Got exception!", e);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void idQueryTest(Morphium m) {
        try (m) {
            createUncachedObjects(m, 10);
            var lst = m.createQueryFor(UncachedObject.class).limit(3).idList();
            long cnt = m.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.morphiumId).nin(lst).countAll();
            assertEquals(7, cnt);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void inTest(Morphium m) {
        try (m) {
            createUncachedObjects(m, 10);
            long cnt = m.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).in(Arrays.asList(1, 5, 6, 102)).countAll();
            assertEquals(3, cnt);
            cnt = m.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).nin(Arrays.asList(1, 2, 102)).countAll();
            assertEquals(8, cnt);
            var lst = m.createQueryFor(UncachedObject.class).limit(3).idList();
            var q = m.createQueryFor(UncachedObject.class).f("_id").nin(lst);
            var q1 = q.q().f("counter").eq(2);
            var q2 = q.q().f("counter").eq(9);
            q.or(q1, q2);
            assertEquals(1, q.countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void existTest(Morphium m) {
        try (m) {
            createUncachedObjects(m, 10);
            long cnt = m.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.boolData).exists().countAll();
            assertEquals(0, cnt);
            cnt = m.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(4).countAll();
            assertEquals(1, cnt);
            cnt = m.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.boolData).notExists().countAll();
            assertEquals(10, cnt);
            cnt = m.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.boolData).notExists().f(UncachedObject.Fields.counter).eq(2).countAll();
            assertEquals(1, cnt);
            //list
            ListContainer lc = new ListContainer();
            lc.addString("one");
            lc.addString("one");
            lc.addString("one");
            m.store(lc);
            cnt = m.createQueryFor(ListContainer.class).f(ListContainer.Fields.stringList).exists().countAll();
            assertEquals(1, cnt);
            cnt = m.createQueryFor(ListContainer.class).f("string_list.0").exists().countAll();
            assertEquals(1, cnt);
            cnt = m.createQueryFor(ListContainer.class).f("string_list.1").exists().countAll();
            assertEquals(1, cnt);
            cnt = m.createQueryFor(ListContainer.class).f("string_list.5").exists().countAll();
            assertEquals(0, cnt);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void subObjectQueryTest(Morphium morphium) {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            Query<ComplexObject> q = morphium.createQueryFor(ComplexObject.class);
            q = q.f("embed.testValueLong").eq(null).f("entityEmbeded.binaryData").eq(null);
            String queryString = q.toQueryObject().toString();
            log.info(queryString);
            assert(queryString.contains("embed.test_value_long") && queryString.contains("entityEmbeded.binary_data"));
            q = q.f("embed.test_value_long").eq(null).f("entity_embeded.binary_data").eq(null);
            queryString = q.toQueryObject().toString();
            log.info(queryString);
            assert(queryString.contains("embed.test_value_long") && queryString.contains("entityEmbeded.binary_data"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void subObjectQueryTestUnknownField(Morphium morphium) {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            Query<ComplexObject> q = morphium.createQueryFor(ComplexObject.class);
            q = q.f("embed.testValueLong").eq(null).f("entityEmbeded.binaryData.non_existent").eq(null);
            String queryString = q.toQueryObject().toString();
            log.info(queryString);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void sortTest(Morphium morphium) throws Exception {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            for (int i = 1; i <= NO_OBJECTS; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("Uncached " + i % 2);
                morphium.store(o);
            }

            Thread.sleep(500);
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f(UncachedObject.Fields.counter).gt(0).sort("-counter", "strValue");
            List<UncachedObject> lst = q.asList();
            assert(!lst.get(0).getStrValue().equals(lst.get(1).getStrValue()));
            q = q.q().f("counter").gt(0).sort("str_value", "-counter");
            List<UncachedObject> lst2 = q.asList();
            assert(lst2.get(0).getStrValue().equals(lst2.get(1).getStrValue()));
            log.info("Sorted");
            q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("counter").gt(0).limit(5).sort("-counter");
            int st = q.asList().size();
            q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("counter").gt(0).sort("-counter").limit(5);
            assert(st == q.asList().size()) : "List length differ?";
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void arrayOfPrimitivesTest(Morphium morphium) {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            UncachedObject o = new UncachedObject();
            int[] binaryData = new int[100];

            for (int i = 0; i < binaryData.length; i++) {
                binaryData[i] = i;
            }

            o.setIntData(binaryData);
            long[] longData = new long[100];

            for (int i = 0; i < longData.length; i++) {
                longData[i] = i;
            }

            o.setLongData(longData);
            float[] floatData = new float[100];

            for (int i = 0; i < floatData.length; i++) {
                floatData[i] = (float)(i / 100.0);
            }

            o.setFloatData(floatData);
            double[] doubleData = new double[100];

            for (int i = 0; i < doubleData.length; i++) {
                doubleData[i] = (float)(i / 100.0);
            }

            o.setDoubleData(doubleData);
            boolean[] bd = new boolean[100];

            for (int i = 0; i < bd.length; i++) {
                bd[i] = i % 2 == 0;
            }

            o.setBoolData(bd);
            morphium.store(o);
            morphium.reread(o);

            for (int i = 0; i < o.getIntData().length; i++) {
                assert(o.getIntData()[i] == binaryData[i]);
            }

            for (int i = 0; i < o.getLongData().length; i++) {
                assert(o.getLongData()[i] == longData[i]);
            }

            for (int i = 0; i < o.getFloatData().length; i++) {
                assert(o.getFloatData()[i] == floatData[i]);
            }

            for (int i = 0; i < o.getDoubleData().length; i++) {
                assert(o.getDoubleData()[i] == doubleData[i]);
            }

            for (int i = 0; i < o.getBoolData().length; i++) {
                assert(o.getBoolData()[i] == bd[i]);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void updateBinaryDataTest(Morphium morphium) {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            UncachedObject o = new UncachedObject();
            byte[] binaryData = new byte[100];

            for (int i = 0; i < binaryData.length; i++) {
                binaryData[i] = (byte) i;
            }

            o.setBinaryData(binaryData);
            morphium.store(o);
            waitForAsyncOperationsToStart(morphium, 3000);
            TestUtils.waitForWrites(morphium, log);
            morphium.reread(o);

            for (int i = 0; i < binaryData.length; i++) {
                binaryData[i] = (byte)(i + 2);
            }

            o.setBinaryData(binaryData);
            morphium.store(o);
            waitForAsyncOperationsToStart(morphium, 3000);
            TestUtils.waitForWrites(morphium, log);

            for (int i = 0; i < o.getBinaryData().length; i++) {
                assert(o.getBinaryData()[i] == binaryData[i]);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void binaryDataTest(Morphium morphium) {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            UncachedObject o = new UncachedObject();
            byte[] binaryData = new byte[100];

            for (int i = 0; i < binaryData.length; i++) {
                binaryData[i] = (byte) i;
            }

            o.setBinaryData(binaryData);
            morphium.store(o);
            waitForAsyncOperationsToStart(morphium, 3000);
            TestUtils.waitForWrites(morphium, log);
            morphium.reread(o);

            for (int i = 0; i < o.getBinaryData().length; i++) {
                assert(o.getBinaryData()[i] == binaryData[i]);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void queryTest(Morphium morphium) {
        try (morphium) {
            String tstName = new Object() {
            }
            .getClass().getEnclosingMethod().getName();
            log.info("Running test " + tstName + " with " + morphium.getDriver().getName());
            createUncachedObjects(morphium, 100);
            var q = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(15).f(UncachedObject.Fields.strValue).eq("nothing");
            assertEquals(0, q.countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void idTest(Morphium morphium) throws Exception {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            log.info("Storing Uncached objects...");
            // long start = System.currentTimeMillis();
            UncachedObject last = null;

            for (int i = 1; i <= NO_OBJECTS; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("Uncached " + i);
                morphium.store(o);
                last = o;
            }

            assertNotNull(last.getMorphiumId(), "ID null?!?!?");
            UncachedObject uc = null;
            long s = System.currentTimeMillis();

            while (uc == null) {
                Thread.sleep(100);
                uc = morphium.findById(UncachedObject.class, last.getMorphiumId());
                assert(System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
            }

            assertNotNull(uc, "Not found?!?");
            assert(uc.getCounter() == last.getCounter()) : "Different Object? " + uc.getCounter() + " != " + last.getCounter();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void orTest(Morphium morphium) {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            log.info("Storing Uncached objects...");
            // long start = System.currentTimeMillis();

            for (int i = 1; i <= NO_OBJECTS; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("Uncached " + i);
                morphium.store(o);
            }

            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q.or(q.q().f("counter").lt(10), q.q().f("strValue").eq("Uncached 50"));
            log.info("Query string: " + q.toQueryObject().toString());
            List<UncachedObject> lst = q.asList();

            for (UncachedObject o : lst) {
                assert(o.getCounter() < 10 || o.getStrValue().equals("Uncached 50")) : "Value did not match: " + o;
                log.info(o.toString());
            }

            log.info("1st test passed");

            for (int i = 1; i < 120; i++) {
                //Storing some additional test content:
                UncachedObject uc = new UncachedObject();
                uc.setCounter(i);
                uc.setStrValue("Complex Query Test " + i);
                morphium.store(uc);
            }

            assert(morphium.getStatistics().get("X-Entries for: resultCache|de.caluga.test.mongo.suite.data.UncachedObject") == null) : "Cached Uncached Object?!?!?!";
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void uncachedSingeTest(Morphium morphium) throws Exception {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            log.info("Storing Uncached objects...");
            long start = System.currentTimeMillis();

            for (int i = 1; i <= NO_OBJECTS; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("Uncached " + i);
                morphium.store(o);
            }

            long dur = System.currentTimeMillis() - start;
            log.info("Storing single took " + dur + " ms");
            //        assert (dur < NO_OBJECTS * 5) : "Storing took way too long";
            Thread.sleep(500);
            log.info("Searching for objects");
            checkUncached(morphium);
            assert(morphium.getStatistics().get("X-Entries for: resultCache|de.caluga.test.mongo.suite.data.UncachedObject") == null) : "Cached Uncached Object?!?!?!";
        }
    }

    @Test
    public void testAnnotationCache() {
        Morphium morphium = (Morphium) getMorphiumInstances().findFirst().get().get()[0];
        Entity e = morphium.getARHelper().getAnnotationFromHierarchy(EmbeddedObject.class, Entity.class);
        assert(e == null);
        Embedded em = morphium.getARHelper().getAnnotationFromHierarchy(EmbeddedObject.class, Embedded.class);
        assertNotNull(em);;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void uncachedListTest(Morphium morphium) throws Exception {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            log.info("Preparing a list...");
            long start = System.currentTimeMillis();
            List<UncachedObject> lst = new ArrayList<>();

            for (int i = 1; i <= NO_OBJECTS; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("Uncached " + i);
                lst.add(o);
            }

            morphium.storeList(lst);
            long dur = System.currentTimeMillis() - start;
            log.info("Storing a list  took " + dur + " ms");
            Thread.sleep(1000);
            checkUncached(morphium);
            assert(morphium.getStatistics().get("X-Entries for: resultCache|de.caluga.test.mongo.suite.data.UncachedObject") == null) : "Cached Uncached Object?!?!?!";
        }
    }

    private void checkUncached(Morphium morphium) {
        long start;
        long dur;
        start = System.currentTimeMillis();

        for (int i = 1; i <= NO_OBJECTS; i++) {
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q.f("counter").eq(i);
            List<UncachedObject> l = q.asList();
            assert(l != null && !l.isEmpty()) : "Nothing found!?!?!?!? Value: " + i;
            UncachedObject fnd = l.get(0);
            assertNotNull(fnd, "Error finding element with id " + i);
            assert(fnd.getCounter() == i) : "Counter not equal: " + fnd.getCounter() + " vs. " + i;
            assert(fnd.getStrValue().equals("Uncached " + i)) : "value not equal: " + fnd.getCounter() + "(" + fnd.getStrValue() + ") vs. " + i;
        }

        dur = System.currentTimeMillis() - start;
        log.info("Searching  took " + dur + " ms");
    }

    private void randomCheck(Morphium morphium) {
        log.info("Random access to cached objects");
        long start;
        long dur;
        start = System.currentTimeMillis();

        for (int idx = 1; idx <= NO_OBJECTS * 3.5; idx++) {
            int i = (int)(Math.random() * (double) NO_OBJECTS);

            if (i == 0) {
                i = 1;
            }

            Query<CachedObject> q = morphium.createQueryFor(CachedObject.class);
            q.f("counter").eq(i);
            List<CachedObject> l = q.asList();
            assertNotNull(l);
            assertFalse(l.isEmpty());
            CachedObject fnd = l.get(0);
            assertNotNull(fnd, "Error finding element with id " + i);
            assert(fnd.getCounter() == i) : "Counter not equal: " + fnd.getCounter() + " vs. " + i;
            assert(fnd.getValue().equals("Cached " + i)) : "value not equal: " + fnd.getCounter() + " vs. " + i;
        }

        dur = System.currentTimeMillis() - start;
        log.info("Searching  took " + dur + " ms");
        log.info("Cache Hits Percentage: " + morphium.getStatistics().get(StatisticKeys.CHITSPERC.name()) + "%");
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void cachedWritingTest(Morphium m) throws Exception {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + m.getDriver().getName());

        try (m) {
            log.info("Starting background writing test - single objects");
            long start = System.currentTimeMillis();

            for (int i = 1; i <= NO_OBJECTS; i++) {
                CachedObject o = new CachedObject();
                o.setCounter(i);
                o.setValue("Cached " + i);
                m.store(o);
            }

            long dur = System.currentTimeMillis() - start;
            log.info("Storing (in Cache) single took " + dur + " ms");
            //        wiatForAsyncOpToStart(1000);
            //       TestUtils.waitForWrites(morphium,log);
            var q = m.createQueryFor(CachedObject.class);

            while (q.countAll() < NO_OBJECTS) {
                m.clearCachefor(CachedObject.class);
                assertThat(System.currentTimeMillis() - start < 5000);
            }

            dur = System.currentTimeMillis() - start;
            log.info("Storing took " + dur + " ms overall");
            randomCheck(m);
            Map<String, Double> statistics = m.getStatistics();
            Double uc = statistics.get("X-Entries resultCache|for: de.caluga.test.mongo.suite.data.UncachedObject");
            assert(uc == null || uc == 0) : "Cached Uncached Object?!?!?!";
            assert(statistics.get("X-Entries for: resultCache|de.caluga.test.mongo.suite.data.CachedObject") > 0) : "No Cached Object cached?!?!?!";
        }
    }

    @ParameterizedTest()
    @MethodSource("getMorphiumInstances")
    public void checkListWriting(Morphium morphium) {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            List<CachedObject> lst = new ArrayList<>();

            try {
                morphium.store(lst);
                morphium.storeBuffered(lst);
            } catch (Exception e) {
                log.info("Got exception, good!");
                return;
            }

            //noinspection ConstantConditions
            assert(false) : "Exception missing!";
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void checkToStringUniqueness(Morphium morphium) {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            Query<CachedObject> q = morphium.createQueryFor(CachedObject.class);
            q = q.f("value").eq("Test").f("counter").gt(5);
            String t = q.toString();
            log.info("Tostring: " + q);
            q = morphium.createQueryFor(CachedObject.class);
            q = q.f("counter").gt(5).f("value").eq("Test");
            String s = q.toString();

            if (!s.equals(t)) {
                log.warn("Warning: order is important s=" + s + " and t=" + t);
            }

            q = morphium.createQueryFor(CachedObject.class);
            q = q.f("counter").gt(5).sort("counter", "-value");
            t = q.toString();
            q = morphium.createQueryFor(CachedObject.class);
            q = q.f(CachedObject.Fields.counter).gt(5);
            s = q.toString();
            assert(!t.equals(s)) : "Values should not be equal: s=" + s + " t=" + t;
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void mixedListWritingTest(Morphium morphium) throws Exception {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            List<Object> tst = new ArrayList<>();
            int cached = 0;
            int uncached = 0;

            for (int i = 0; i < NO_OBJECTS; i++) {
                if (Math.random() < 0.5) {
                    cached++;
                    CachedObject c = new CachedObject();
                    c.setValue("List Test!");
                    c.setCounter(11111);
                    tst.add(c);
                } else {
                    uncached++;
                    UncachedObject uc = new UncachedObject();
                    uc.setStrValue("List Test uc");
                    uc.setCounter(22222);
                    tst.add(uc);
                }
            }

            log.info("Writing " + cached + " Cached and " + uncached + " uncached objects!");
            morphium.storeList(tst);
            long start = System.currentTimeMillis();

            while (true) {
                Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class);
                Query<CachedObject> q = morphium.createQueryFor(CachedObject.class);

                if (uncached == qu.countAll() && cached == q.countAll()) {
                    break;
                }

                assertThat(System.currentTimeMillis() - start > 5000);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void listOfIdsTest(Morphium morphium) {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(ListOfIdsContainer.class);
            TestUtils.waitForConditionToBecomeTrue(1500, "Collection not dropped", ()->!morphium.exists(ListOfIdsContainer.class));
            List<MorphiumId> lst = new ArrayList<>();

            for (int i = 0; i < 10; i++) {
                lst.add(new MorphiumId());
            }

            Map<String, MorphiumId> map = new HashMap<>();

            for (int i = 0; i < 10; i++) {
                map.put("" + i, new MorphiumId());
            }

            ListOfIdsContainer c = new ListOfIdsContainer();
            c.value = "a value";
            c.others = lst;
            c.idMap = map;
            c.simpleId = new MorphiumId();
            morphium.store(c);
            Query<ListOfIdsContainer> q = morphium.createQueryFor(ListOfIdsContainer.class);
            ListOfIdsContainer cnt = q.get();
            assertEquals(c.id, cnt.id);
            assertEquals(c.value, cnt.value);

            for (int i = 0; i < 10; i++) {
                assertEquals(c.others.get(i), cnt.others.get(i));
                assertEquals(c.idMap.get("" + i), cnt.idMap.get("" + i));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void insertTest(Morphium morphium) throws Exception {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(UncachedObject.class);
            TestUtils.waitForConditionToBecomeTrue(1500, "Collection not dropped", ()->!morphium.exists(UncachedObject.class));
            UncachedObject uc = new UncachedObject();
            uc.setCounter(1);
            uc.setStrValue("A value");
            log.info("Storing new value - no problem");
            morphium.insert(uc);
            assertNotNull(uc.getMorphiumId());
            Thread.sleep(200);
            assertNotNull(morphium.findById(UncachedObject.class, uc.getMorphiumId()));
            log.info("Inserting again - exception expected");
            boolean ex = false;

            try {
                morphium.insert(uc);
            } catch (Exception e) {
                log.info("Got exception as expected " + e.getMessage());
                ex = true;
            }

            assertTrue(ex);
            uc = new UncachedObject();
            uc.setStrValue("2");
            uc.setMorphiumId(new MorphiumId());
            uc.setCounter(3);
            morphium.insert(uc);
            Thread.sleep(200);
            assertNotNull(morphium.findById(UncachedObject.class, uc.getMorphiumId()));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void insertListTest(Morphium morphium) throws Exception {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(UncachedObject.class);
            TestUtils.waitForConditionToBecomeTrue(1500, "Collection not dropped", ()->!morphium.exists(UncachedObject.class));
            List<UncachedObject> lst = new ArrayList<>();

            for (int i = 0; i < 100; i++) {
                UncachedObject uc = new UncachedObject();
                uc.setCounter(i);
                uc.setStrValue("" + i);
                lst.add(uc);
            }

            morphium.insert(lst);
            TestUtils.waitForConditionToBecomeTrue(5000, "Did not write?", ()->TestUtils.countUC(morphium) == 100);
            List<UncachedObject> lst2 = new ArrayList<>();

            for (int i = 0; i < 10; i++) {
                UncachedObject uc = new UncachedObject();
                uc.setCounter(i);
                uc.setStrValue("" + i);
                lst2.add(uc);
            }

            lst2.add(lst.get(0));
            Thread.sleep(100);
            boolean ex = false;

            try {
                morphium.insert(lst2);
            } catch (Throwable e) {
                log.info("Exception expected!");
                ex = true;
            }

            assertTrue(ex);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void customMapperObjectIdTest(Morphium morphium) {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            MorphiumTypeMapper<ObjectIdTest> mapper = new MorphiumTypeMapper<ObjectIdTest>() {
                @Override
                public Object marshall(ObjectIdTest o) {
                    Map serialized = new HashMap();
                    serialized.put("value", o.value);
                    serialized.put("_id", o.id);
                    return serialized;
                }
                @Override
                public ObjectIdTest unmarshall(Object d) {
                    Map obj = ((Map) d);
                    ObjectIdTest o = new ObjectIdTest();
                    o.id = new ObjectId(obj.get("_id").toString());
                    o.value = (String)(obj.get("value"));
                    return o;
                }
            };
            morphium.getMapper().registerCustomMapperFor(ObjectIdTest.class, mapper);
            ObjectIdTest t = new ObjectIdTest();
            t.value = "test1";
            t.id = new ObjectId();
            morphium.store(t);
            morphium.reread(t);
            t = new ObjectIdTest();
            t.value = "test2";
            t.id = new ObjectId();
            morphium.store(t);
            List<ObjectIdTest> lst = morphium.createQueryFor(ObjectIdTest.class).asList();

            for (ObjectIdTest tst : lst) {
                log.info("T: " + tst.value + " id: " + tst.id.toHexString());
            }

            morphium.getMapper().deregisterCustomMapperFor(ObjectIdTest.class);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void objectIdIdstest(Morphium morphium) {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            ObjectIdTest o = new ObjectIdTest();
            o.value = "test1";
            morphium.store(o);
            assertNotNull(o.id);
            assertTrue(o.id instanceof ObjectId);
            morphium.reread(o);
            assertNotNull(o.id);
            assertTrue(o.id instanceof ObjectId);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testFindOneAndDelete(Morphium morphium) throws Exception {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            UncachedObject uc = new UncachedObject("value", 123);
            morphium.store(uc);
            UncachedObject ret = morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).findOneAndDelete();
            assertEquals(ret.getStrValue(), "value");
            Thread.sleep(100);
            assertEquals(TestUtils.countUC(morphium), 0);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testFindOneAndUpdate(Morphium morphium) throws Exception {
        String tstName = new Object() {
        }
        .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            UncachedObject uc = new UncachedObject("value", 123);
            morphium.store(uc);
            Thread.sleep(150);
            UncachedObject ret = morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).findOneAndUpdate(UtilsMap.of("$set", UtilsMap.of("counter", 42)));
            assertEquals("value", ret.getStrValue());
            assertEquals(1, TestUtils.countUC(morphium));
            morphium.reread(uc);
            assertEquals(42, uc.getCounter());
        }
    }

    @Entity
    public static class StringIdTestEntity {
        @Id
        public String id;
        public String value;

        public enum Fields {
            value, id
        }
    }

    @Entity
    public static class ObjectIdTest {
        @Id
        public ObjectId id;
        public String value;

        public enum Fields {
            value, id
        }
    }

    @Entity
    public static class ListOfIdsContainer {
        @Id
        public MorphiumId id;
        public List<MorphiumId> others;
        public Map<String, MorphiumId> idMap;
        public MorphiumId simpleId;
        public String value;

        public enum Fields {
            idMap, others, simpleId, value, id
        }
    }

}
