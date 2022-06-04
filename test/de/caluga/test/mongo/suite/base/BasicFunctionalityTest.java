/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.*;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.objectmapping.MorphiumTypeMapper;
import de.caluga.morphium.objectmapping.ObjectMapperImpl;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

/**
 * @author stephan
 */
@SuppressWarnings({"AssertWithSideEffects", "Duplicates", "unchecked"})
public class BasicFunctionalityTest extends MorphiumTestBase {
    public static final int NO_OBJECTS = 100;
    private static final Logger log = LoggerFactory.getLogger(BasicFunctionalityTest.class);

    public BasicFunctionalityTest() {
    }

    @Test
    //@Ignore("setWarnOnNoEntitySerialization(true) ist not possible in this static environment")
    public void mapSerializationTest() {
        ObjectMapperImpl OM = (ObjectMapperImpl) morphium.getMapper();
        morphium.getConfig().setWarnOnNoEntitySerialization(true);
        Map<String, Object> map = OM.serialize(new ObjectMapperImplTest.Simple());
        log.info("Got map");
        assert (map.get("test").toString().startsWith("test"));

        ObjectMapperImplTest.Simple s = OM.deserialize(ObjectMapperImplTest.Simple.class, map);
        log.info("Got simple");

        Map<String, Object> m = new HashMap<>();
        m.put("test", "testvalue");
        m.put("simple", s);

        map = OM.serializeMap(m, null);
        assert (map.get("test").equals("testvalue"));

        List<ObjectMapperImplTest.Simple> lst = new ArrayList<>();
        lst.add(new ObjectMapperImplTest.Simple());
        lst.add(new ObjectMapperImplTest.Simple());
        lst.add(new ObjectMapperImplTest.Simple());

        List serializedList = OM.serializeIterable(lst, null, null);
        assert (serializedList.size() == 3);

        List<ObjectMapperImplTest.Simple> deserializedList = OM.deserializeList(serializedList);
        log.info("Deserialized");
        morphium.getConfig().setWarnOnNoEntitySerialization(false);

    }


    @Test
    public void readPreferenceTest() {
        ReadPreferenceLevel.NEAREST.setPref(ReadPreference.nearest());
        assert (ReadPreferenceLevel.NEAREST.getPref().getType().equals(ReadPreference.nearest().getType()));
    }

    @Test
    public void subObjectQueryTest() {
        Query<ComplexObject> q = morphium.createQueryFor(ComplexObject.class);

        q = q.f("embed.testValueLong").eq(null).f("entityEmbeded.binaryData").eq(null);
        String queryString = q.toQueryObject().toString();
        log.info(queryString);
        assert (queryString.contains("embed.test_value_long") && queryString.contains("entityEmbeded.binary_data"));
        q = q.f("embed.test_value_long").eq(null).f("entity_embeded.binary_data").eq(null);
        queryString = q.toQueryObject().toString();
        log.info(queryString);
        assert (queryString.contains("embed.test_value_long") && queryString.contains("entityEmbeded.binary_data"));
    }

    @Test
    public void subObjectQueryTestUnknownField() {
        Query<ComplexObject> q = morphium.createQueryFor(ComplexObject.class);

        q = q.f("embed.testValueLong").eq(null).f("entityEmbeded.binaryData.non_existent").eq(null);
        String queryString = q.toQueryObject().toString();
        log.info(queryString);
    }

    @Test
    public void getDatabaseListTest() {
        List<String> dbs = morphium.listDatabases();
        assert (dbs != null);
        assert (dbs.size() != 0);
        for (String s : dbs) {
            log.info("Got DB: " + s);
        }
    }


    @Test
    public void reconnectDatabaseTest() throws Exception {
        List<String> dbs = morphium.listDatabases();
        assert (dbs != null);
        assert (dbs.size() != 0);
        for (String s : dbs) {
            log.info("Got DB: " + s);
            morphium.reconnectToDb(s);
            log.info("Logged in...");
            Thread.sleep(100);
        }

        morphium.reconnectToDb("morphium_test");
    }

    @Test
    public void listCollections() {
        UncachedObject u = new UncachedObject("test", 1);
        morphium.store(u);

        List<String> cols = morphium.listCollections();
        assert (cols != null);
    }

    @Test
    public void sortTest() throws Exception {
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
        assert (!lst.get(0).getStrValue().equals(lst.get(1).getStrValue()));

        q = q.q().f("counter").gt(0).sort("str_value", "-counter");
        List<UncachedObject> lst2 = q.asList();
        assert (lst2.get(0).getStrValue().equals(lst2.get(1).getStrValue()));
        log.info("Sorted");

        q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").gt(0).limit(5).sort("-counter");
        int st = q.asList().size();
        q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").gt(0).sort("-counter").limit(5);
        assert (st == q.asList().size()) : "List length differ?";

    }

    @Test
    public void arrayOfPrimitivesTest() {
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
            floatData[i] = (float) (i / 100.0);
        }
        o.setFloatData(floatData);

        double[] doubleData = new double[100];
        for (int i = 0; i < doubleData.length; i++) {
            doubleData[i] = (float) (i / 100.0);
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
            assert (o.getIntData()[i] == binaryData[i]);
        }

        for (int i = 0; i < o.getLongData().length; i++) {
            assert (o.getLongData()[i] == longData[i]);
        }
        for (int i = 0; i < o.getFloatData().length; i++) {
            assert (o.getFloatData()[i] == floatData[i]);
        }

        for (int i = 0; i < o.getDoubleData().length; i++) {
            assert (o.getDoubleData()[i] == doubleData[i]);
        }

        for (int i = 0; i < o.getBoolData().length; i++) {
            assert (o.getBoolData()[i] == bd[i]);
        }


    }

    @Test
    public void updateBinaryDataTest() {
        UncachedObject o = new UncachedObject();
        byte[] binaryData = new byte[100];
        for (int i = 0; i < binaryData.length; i++) {
            binaryData[i] = (byte) i;
        }
        o.setBinaryData(binaryData);
        morphium.store(o);


        waitForAsyncOperationToStart(1000000);
        waitForWrites();
        morphium.reread(o);
        for (int i = 0; i < binaryData.length; i++) {
            binaryData[i] = (byte) (i + 2);
        }
        o.setBinaryData(binaryData);
        morphium.store(o);
        waitForAsyncOperationToStart(1000000);
        waitForWrites();

        for (int i = 0; i < o.getBinaryData().length; i++) {
            assert (o.getBinaryData()[i] == binaryData[i]);
        }
    }

    @Test
    public void binaryDataTest() {
        UncachedObject o = new UncachedObject();
        byte[] binaryData = new byte[100];
        for (int i = 0; i < binaryData.length; i++) {
            binaryData[i] = (byte) i;
        }
        o.setBinaryData(binaryData);
        morphium.store(o);

        waitForAsyncOperationToStart(1000000);
        waitForWrites();
        morphium.reread(o);
        for (int i = 0; i < o.getBinaryData().length; i++) {
            assert (o.getBinaryData()[i] == binaryData[i]);
        }
    }

    @Test
    public void whereTest() {
        for (int i = 1; i <= NO_OBJECTS; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setStrValue("Uncached " + i);
            morphium.store(o);
        }
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.where("this.counter<10").f("counter").gt(5);
        log.info(q.toQueryObject().toString());

        List<UncachedObject> lst = q.asList();
        for (UncachedObject o : lst) {
            assert (o.getCounter() < 10 && o.getCounter() > 5) : "Counter is wrong: " + o.getCounter();
        }

        assert (morphium.getStatistics().get("X-Entries for: idCache|de.caluga.test.mongo.suite.data.UncachedObject") == null) : "Cached Uncached Object?!?!?!";


    }

    @Test
    public void existsTest() throws Exception {
        for (int i = 1; i <= 10; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setStrValue("Uncached " + i);
            morphium.store(o);
        }
        long s = System.currentTimeMillis();
        while (morphium.createQueryFor(UncachedObject.class).countAll() < 10) {
            Thread.sleep(100);
            assert (System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
        }
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").exists().f("str_value").eq("Uncached 1");
        long c = q.countAll();
        s = System.currentTimeMillis();
        while (c != 1) {
            c = q.countAll();
            Thread.sleep(100);
            assert (System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
        }
        assert (c == 1) : "Count wrong: " + c;

        UncachedObject o = q.get();
        s = System.currentTimeMillis();
        while (o == null) {
            Thread.sleep(100);
            assert (System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
            o = q.get();
        }
        assert (o.getCounter() == 1);
    }

    @Test
    public void notExistsTest() {
        for (int i = 1; i <= 10; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setStrValue("Uncached " + i);
            morphium.store(o);
        }
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").notExists().f("str_value").eq("Uncached 1");
        long c = q.countAll();
        assert (c == 0);
    }


    @Test
    public void idTest() throws Exception {
        log.info("Storing Uncached objects...");

        long start = System.currentTimeMillis();

        UncachedObject last = null;
        for (int i = 1; i <= NO_OBJECTS; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setStrValue("Uncached " + i);
            morphium.store(o);
            last = o;
        }

        assert (last.getMorphiumId() != null) : "ID null?!?!?";
        UncachedObject uc = null;
        long s = System.currentTimeMillis();
        while (uc == null) {
            Thread.sleep(100);
            uc = morphium.findById(UncachedObject.class, last.getMorphiumId());
            assert (System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
        }
        assert (uc != null) : "Not found?!?";
        assert (uc.getCounter() == last.getCounter()) : "Different Object? " + uc.getCounter() + " != " + last.getCounter();

    }

    //    @Test
    //    public void currentOpTest() throws Exception{
    //        new Thread() {
    //            public void run() {
    //                createUncachedObjects(1000);
    //            }
    //        }.start();
    //        List<Map<String, Object>> lst = morphium.getDriver().find("local", "$cmd.sys.inprog", new HashMap<String, Object>(), null, null, 0, 1000, 1000, null, null);
    //        log.info("got: "+lst.size());
    //    }

    @Test
    public void orTest() {
        log.info("Storing Uncached objects...");

        long start = System.currentTimeMillis();

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
            assert (o.getCounter() < 10 || o.getStrValue().equals("Uncached 50")) : "Value did not match: " + o;
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

        assert (morphium.getStatistics().get("X-Entries for: resultCache|de.caluga.test.mongo.suite.data.UncachedObject") == null) : "Cached Uncached Object?!?!?!";

    }


    @Test
    public void uncachedSingeTest() throws Exception {
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

        checkUncached();
        assert (morphium.getStatistics().get("X-Entries for: resultCache|de.caluga.test.mongo.suite.data.UncachedObject") == null) : "Cached Uncached Object?!?!?!";

    }

    @Test
    public void testAnnotationCache() {
        Entity e = morphium.getARHelper().getAnnotationFromHierarchy(EmbeddedObject.class, Entity.class);
        assert (e == null);
        Embedded em = morphium.getARHelper().getAnnotationFromHierarchy(EmbeddedObject.class, Embedded.class);
        assert (em != null);
    }

    @Test
    public void uncachedListTest() throws Exception {
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
        checkUncached();
        assert (morphium.getStatistics().get("X-Entries for: resultCache|de.caluga.test.mongo.suite.data.UncachedObject") == null) : "Cached Uncached Object?!?!?!";

    }

    private void checkUncached() {
        long start;
        long dur;
        start = System.currentTimeMillis();
        for (int i = 1; i <= NO_OBJECTS; i++) {
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q.f("counter").eq(i);
            List<UncachedObject> l = q.asList();
            assert (l != null && !l.isEmpty()) : "Nothing found!?!?!?!? Value: " + i;
            UncachedObject fnd = l.get(0);
            assert (fnd != null) : "Error finding element with id " + i;
            assert (fnd.getCounter() == i) : "Counter not equal: " + fnd.getCounter() + " vs. " + i;
            assert (fnd.getStrValue().equals("Uncached " + i)) : "value not equal: " + fnd.getCounter() + "(" + fnd.getStrValue() + ") vs. " + i;
        }
        dur = System.currentTimeMillis() - start;
        log.info("Searching  took " + dur + " ms");
    }

    private void randomCheck() {
        log.info("Random access to cached objects");
        long start;
        long dur;
        start = System.currentTimeMillis();
        for (int idx = 1; idx <= NO_OBJECTS * 3.5; idx++) {
            int i = (int) (Math.random() * (double) NO_OBJECTS);
            if (i == 0) {
                i = 1;
            }
            Query<CachedObject> q = morphium.createQueryFor(CachedObject.class);
            q.f("counter").eq(i);
            List<CachedObject> l = q.asList();
            assert (l != null && !l.isEmpty()) : "Nothing found!?!?!?!? " + i;
            CachedObject fnd = l.get(0);
            assert (fnd != null) : "Error finding element with id " + i;
            assert (fnd.getCounter() == i) : "Counter not equal: " + fnd.getCounter() + " vs. " + i;
            assert (fnd.getValue().equals("Cached " + i)) : "value not equal: " + fnd.getCounter() + " vs. " + i;
        }
        dur = System.currentTimeMillis() - start;
        log.info("Searching  took " + dur + " ms");
        log.info("Cache Hits Percentage: " + morphium.getStatistics().get(StatisticKeys.CHITSPERC.name()) + "%");
    }


    @Test
    public void cachedWritingTest() throws Exception {
        log.info("Starting background writing test - single objects");
        long start = System.currentTimeMillis();
        for (int i = 1; i <= NO_OBJECTS; i++) {
            CachedObject o = new CachedObject();
            o.setCounter(i);
            o.setValue("Cached " + i);
            morphium.store(o);
        }

        long dur = System.currentTimeMillis() - start;
        log.info("Storing (in Cache) single took " + dur + " ms");
        waitForAsyncOperationToStart(100000);
        waitForWrites();
        dur = System.currentTimeMillis() - start;
        log.info("Storing took " + dur + " ms overall");
        Thread.sleep(500);
        randomCheck();
        Map<String, Double> statistics = morphium.getStatistics();
        Double uc = statistics.get("X-Entries resultCache|for: de.caluga.test.mongo.suite.data.UncachedObject");
        assert (uc == null || uc == 0) : "Cached Uncached Object?!?!?!";
        assert (statistics.get("X-Entries for: resultCache|de.caluga.test.mongo.suite.data.CachedObject") > 0) : "No Cached Object cached?!?!?!";

    }


    @Test
    public void checkListWriting() {
        List<CachedObject> lst = new ArrayList<>();
        try {
            morphium.store(lst);
            morphium.storeBuffered(lst);
        } catch (Exception e) {
            log.info("Got exception, good!");
            return;
        }
        //noinspection ConstantConditions
        assert (false) : "Exception missing!";
    }

    @Test
    public void checkToStringUniqueness() {
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
        assert (!t.equals(s)) : "Values should not be equal: s=" + s + " t=" + t;
    }

    @Test
    public void mixedListWritingTest() {
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
        waitForAsyncOperationToStart(1000000);
        waitForWrites();
        //Still waiting - storing lists is not shown in number of write buffer entries
        //        try {
        //            Thread.sleep(2000);
        //        } catch (InterruptedException e) {
        //            throw new RuntimeException(e);
        //        }
        Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class);
        assert (qu.countAll() == uncached) : "Difference in object count for cached objects. Wrote " + uncached + " found: " + qu.countAll();
        Query<CachedObject> q = morphium.createQueryFor(CachedObject.class);
        assert (q.countAll() == cached) : "Difference in object count for cached objects. Wrote " + cached + " found: " + q.countAll();

    }


    @Test
    public void arHelperTest() {
        AnnotationAndReflectionHelper annotationHelper = morphium.getARHelper();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 500000; i++)
            annotationHelper.isAnnotationPresentInHierarchy(UncachedObject.class, Entity.class);
        long dur = System.currentTimeMillis() - start;
        log.info("present duration: " + dur);

        start = System.currentTimeMillis();
        for (int i = 0; i < 500000; i++)
            annotationHelper.isAnnotationPresentInHierarchy(UncachedObject.class, Entity.class);
        dur = System.currentTimeMillis() - start;
        log.info("present duration: " + dur);

        start = System.currentTimeMillis();
        for (int i = 0; i < 500000; i++)
            annotationHelper.getFields(UncachedObject.class, Id.class);
        dur = System.currentTimeMillis() - start;
        log.info("fields an duration: " + dur);
        start = System.currentTimeMillis();
        for (int i = 0; i < 500000; i++)
            annotationHelper.getFields(UncachedObject.class, Id.class);
        dur = System.currentTimeMillis() - start;
        log.info("fields an duration: " + dur);

        start = System.currentTimeMillis();
        for (int i = 0; i < 500000; i++)
            annotationHelper.getFields(UncachedObject.class);
        dur = System.currentTimeMillis() - start;
        log.info("fields duration: " + dur);

        start = System.currentTimeMillis();
        for (int i = 0; i < 500000; i++)
            annotationHelper.getFields(UncachedObject.class);
        dur = System.currentTimeMillis() - start;
        log.info("fields duration: " + dur);

        start = System.currentTimeMillis();
        for (int i = 0; i < 500000; i++)
            annotationHelper.getAnnotationFromHierarchy(UncachedObject.class, Entity.class);
        dur = System.currentTimeMillis() - start;
        log.info("Hierarchy duration: " + dur);

        start = System.currentTimeMillis();
        for (int i = 0; i < 500000; i++)
            annotationHelper.getAnnotationFromHierarchy(UncachedObject.class, Entity.class);
        dur = System.currentTimeMillis() - start;
        log.info("Hierarchy duration: " + dur);


        start = System.currentTimeMillis();
        for (int i = 0; i < 50000; i++) {
            List<String> lst = annotationHelper.getFields(UncachedObject.class);
            for (String f : lst) {
                Field fld = annotationHelper.getField(UncachedObject.class, f);
                fld.isAnnotationPresent(Id.class);
            }
        }
        dur = System.currentTimeMillis() - start;
        log.info("fields / getField duration: " + dur);

        start = System.currentTimeMillis();
        for (int i = 0; i < 50000; i++) {
            List<String> lst = annotationHelper.getFields(UncachedObject.class);
            for (String f : lst) {
                Field fld = annotationHelper.getField(UncachedObject.class, f);
                fld.isAnnotationPresent(Id.class);
            }
        }
        dur = System.currentTimeMillis() - start;
        log.info("fields / getField duration: " + dur);


    }

    @Test
    public void listOfIdsTest() {
        morphium.dropCollection(ListOfIdsContainer.class);
        List<MorphiumId> lst = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            lst.add(new MorphiumId());
        }

        Map<String, MorphiumId> map = new HashMap<>();
        for (int i = 0; i < 10; i++) map.put("" + i, new MorphiumId());
        ListOfIdsContainer c = new ListOfIdsContainer();
        c.value = "a value";
        c.others = lst;
        c.idMap = map;
        c.simpleId = new MorphiumId();


        morphium.store(c);

        Query<ListOfIdsContainer> q = morphium.createQueryFor(ListOfIdsContainer.class);
        ListOfIdsContainer cnt = q.get();

        assert (c.id.equals(cnt.id));
        assert (c.value.equals(cnt.value));

        for (int i = 0; i < 10; i++) {
            assert (c.others.get(i).equals(cnt.others.get(i)));
            assert (c.idMap.get("" + i).equals(cnt.idMap.get("" + i)));
        }

    }

    @Test
    public void insertTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        UncachedObject uc = new UncachedObject();
        uc.setCounter(1);
        uc.setStrValue("A value");
        log.info("Storing new value - no problem");
        morphium.insert(uc);
        assert (uc.getMorphiumId() != null);
        Thread.sleep(200);
        assert (morphium.findById(UncachedObject.class, uc.getMorphiumId()) != null);

        log.info("Inserting again - exception expected");
        boolean ex = false;
        try {
            morphium.insert(uc);
        } catch (Exception e) {
            log.info("Got exception as expected " + e.getMessage());
            ex = true;
        }
        assert (ex);
        uc = new UncachedObject();
        uc.setStrValue("2");
        uc.setMorphiumId(new MorphiumId());
        uc.setCounter(3);
        morphium.insert(uc);
        Thread.sleep(200);
        assert (morphium.findById(UncachedObject.class, uc.getMorphiumId()) != null);

    }


    @Test
    public void insertListTest() throws Exception {

        morphium.dropCollection(UncachedObject.class);
        Thread.sleep(100);
        List<UncachedObject> lst = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i);
            uc.setStrValue("" + i);
            lst.add(uc);
        }
        morphium.insert(lst);
        Thread.sleep(1000);
        long c = morphium.createQueryFor(UncachedObject.class).countAll();
        System.err.println("Found " + c);
        assert (c == 100);
        List<UncachedObject> lst2 = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i);
            uc.setStrValue("" + i);
            lst2.add(uc);
        }
        lst2.add(lst.get(0));
        Thread.sleep(1000);
        boolean ex = false;
        try {
            morphium.insert(lst);
        } catch (Throwable e) {
            log.info("Exception expected!");
            ex = true;
        }
        assert (ex);

    }

    @Test
    public void marshallListOfIdsTest() {
        ListOfIdsContainer c = new ListOfIdsContainer();
        c.others = new ArrayList<>();
        c.others.add(new MorphiumId());
        c.others.add(new MorphiumId());
        c.others.add(new MorphiumId());
        c.others.add(new MorphiumId());
        c.simpleId = new MorphiumId();

        c.idMap = new HashMap<>();
        c.idMap.put("1", new MorphiumId());

        Map<String, Object> marshall = morphium.getMapper().serialize(c);
        assert (marshall.get("simple_id") instanceof ObjectId);
        assert (((Map) marshall.get("id_map")).get("1") instanceof ObjectId);
        for (Object i : (List) marshall.get("others")) {
            assert (i instanceof ObjectId);
        }

        ///

        c = morphium.getMapper().deserialize(ListOfIdsContainer.class, marshall);
        //noinspection ConstantConditions
        assert (c.idMap != null && c.idMap.get("1") != null && c.idMap.get("1") instanceof MorphiumId);
        //noinspection ConstantConditions
        assert (c.others.size() == 4 && c.others.get(0) instanceof MorphiumId);
        assert (c.simpleId != null);
    }

    @Test
    public void customMapperObjectIdTest() {
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
                o.value = (String) (obj.get("value"));
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

    @Test
    public void objectIdIdstest() {

        ObjectIdTest o = new ObjectIdTest();
        o.value = "test1";
        morphium.store(o);

        assert (o.id != null);
        assert (o.id instanceof ObjectId);

        morphium.reread(o);
        assert (o.id != null);
        assert (o.id instanceof ObjectId);

    }


    @Test
    public void testExistst() throws Exception {
        morphium.store(new UncachedObject("value", 123));
        //assert (morphium.getDriver().exists(morphium.getConfig().getDatabase()));
    }

    @Test
    public void testFindOneAndDelete() throws Exception {
        UncachedObject uc = new UncachedObject("value", 123);
        morphium.store(uc);
        UncachedObject ret = morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).findOneAndDelete();
        assert (ret.getStrValue().equals("value"));
        Thread.sleep(100);
        assert (morphium.createQueryFor(UncachedObject.class).countAll() == 0);

    }

    @Test
    public void testFindOneAndUpdate() throws Exception {
        UncachedObject uc = new UncachedObject("value", 123);
        morphium.store(uc);
        Thread.sleep(150);
        UncachedObject ret = morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).findOneAndUpdate(UtilsMap.of("$set", UtilsMap.of("counter", 42)));
        assert (ret.getStrValue().equals("value"));
        assert (morphium.createQueryFor(UncachedObject.class).countAll() == 1);
        morphium.reread(uc);
        assert (uc.getCounter() == 42);

    }


    @Test
    public void shardingReplacementTest() throws Exception {
//        try {
//            Map<String, Object> state = morphium.getDriver().runCommand("admin", UtilsMap.of("listShards", 1));
//        } catch (MorphiumDriverException e) {
//            log.info("Not sharded, it seems");
//            return;
//        }
//
//        try {
//            Map<String, Object> state = morphium.getDriver().runCommand("admin", UtilsMap.of("shardCollection", (Object) ("morphium_test." + morphium.getMapper().getCollectionName(UncachedObject.class)),
//                    "key", UtilsMap.of("_id", "hashed")));
//
//
//        } catch (MorphiumDriverException e) {
//            log.error("Sharding is enabled, but morphium_test sharding is not it seems");
//        }


        createUncachedObjects(10000);
        UncachedObject uc = new UncachedObject("toReplace", 1234);
        morphium.store(uc, morphium.getMapper().getCollectionName(UncachedObject.class), null);
        Thread.sleep(100);
        uc.setStrValue("again");
        morphium.store(uc, morphium.getMapper().getCollectionName(UncachedObject.class), null);

        morphium.reread(uc, morphium.getMapper().getCollectionName(UncachedObject.class));
        assert (uc.getStrValue().equals("again"));

        uc = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(42).get();
        uc.setStrValue("another value");
        morphium.store(uc, morphium.getMapper().getCollectionName(UncachedObject.class), null);
        Thread.sleep(100);
        morphium.reread(uc, morphium.getMapper().getCollectionName(UncachedObject.class));
        assert (uc.getStrValue().equals("another value"));

    }


    @Test
    public void shardingStringIdReplacementTest() throws Exception {
//        try {
//            Map<String, Object> state = morphium.getDriver().runCommand("admin", UtilsMap.of("listShards", 1));
//        } catch (MorphiumDriverException e) {
//            log.info("Not sharded, it seems");
//            return;
//        }
//
//
//        try {
//            Map<String, Object> state = morphium.getDriver().runCommand("admin", UtilsMap.of("shardCollection", (Object) ("morphium_test." + morphium.getMapper().getCollectionName(StringIdTestEntity.class)),
//                    "key", UtilsMap.of("_id", "hashed")));
//
//        } catch (MorphiumDriverException e) {
//            log.error("Sharding is enabled, but morphium_test sharding is not it seems");
//        }
        StringIdTestEntity uc = new StringIdTestEntity();
        uc.value = "test123e";
        morphium.store(uc, morphium.getMapper().getCollectionName(StringIdTestEntity.class), null);
        Thread.sleep(100);
        uc.value = "again";
        morphium.store(uc, morphium.getMapper().getCollectionName(StringIdTestEntity.class), null);

        morphium.reread(uc, morphium.getMapper().getCollectionName(StringIdTestEntity.class));
        assert (uc.value.equals("again"));


        uc = new StringIdTestEntity();
        uc.value = "test123";
        morphium.store(uc, morphium.getMapper().getCollectionName(StringIdTestEntity.class), null);
        uc = morphium.createQueryFor(StringIdTestEntity.class).f(StringIdTestEntity.Fields.value).eq("test123").get();
        uc.value = "another value";
        morphium.store(uc, morphium.getMapper().getCollectionName(StringIdTestEntity.class), null);
        Thread.sleep(100);
        morphium.reread(uc, morphium.getMapper().getCollectionName(StringIdTestEntity.class));
        assert (uc.value.equals("another value"));

    }

    
    @Entity
    public static class StringIdTestEntity {
        @Id
        public String id;
        public String value;

        public enum Fields {value, id}
    }

    @Entity
    public static class ObjectIdTest {
        @Id
        public ObjectId id;
        public String value;

        public enum Fields {value, id}
    }


    @Entity
    public static class ListOfIdsContainer {
        @Id
        public MorphiumId id;
        public List<MorphiumId> others;
        public Map<String, MorphiumId> idMap;
        public MorphiumId simpleId;
        public String value;


        public enum Fields {idMap, others, simpleId, value, id}
    }

}
