/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.test.mongo.suite;

import de.caluga.morphium.Logger;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.StatisticKeys;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.query.Query;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author stephan
 */
public class BasicFunctionalityTest extends MongoTest {
    public static final int NO_OBJECTS = 100;
    private final static Logger log = new Logger(BasicFunctionalityTest.class);

    public BasicFunctionalityTest() {
    }

    @Test
    public void subObjectQueryTest() throws Exception {
        Query<ComplexObject> q = MorphiumSingleton.get().createQueryFor(ComplexObject.class);

        q = q.f("embed.testValueLong").eq(null).f("entityEmbeded.binaryData").eq(null);
        String queryString = q.toQueryObject().toString();
        log.info(queryString);
        assert (queryString.contains("embed.test_value_long") && queryString.contains("entityEmbeded.binary_data"));
        q = q.f("embed.test_value_long").eq(null).f("entity_embeded.binary_data").eq(null);
        queryString = q.toQueryObject().toString();
        log.info(queryString);
        assert (queryString.contains("embed.test_value_long") && queryString.contains("entityEmbeded.binary_data"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void subObjectQueryTestUnknownField() throws Exception {
        Query<ComplexObject> q = MorphiumSingleton.get().createQueryFor(ComplexObject.class);

        q = q.f("embed.testValueLong").eq(null).f("entityEmbeded.binaryData.non_existent").eq(null);
        String queryString = q.toQueryObject().toString();
        log.info(queryString);
    }

    @Test
    public void sortTest() throws Exception {
        for (int i = 1; i <= NO_OBJECTS; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i % 2);
            MorphiumSingleton.get().store(o);
        }
        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q = q.f("counter").gt(0).sort("-counter", "value");
        List<UncachedObject> lst = q.asList();
        assert (!lst.get(0).getValue().equals(lst.get(1).getValue()));

        q = q.q().f("counter").gt(0).sort("value", "-counter");
        List<UncachedObject> lst2 = q.asList();
        assert (lst2.get(0).getValue().equals(lst2.get(1).getValue()));
        log.info("Sorted");

        q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q = q.f("counter").gt(0).limit(5).sort("-counter");
        int st = q.asList().size();
        q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q = q.f("counter").gt(0).sort("-counter").limit(5);
        assert (st == q.asList().size()) : "List length differ?";

    }

    @Test
    public void arrayOfPrimitivesTest() throws Exception {
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

        MorphiumSingleton.get().store(o);


        MorphiumSingleton.get().reread(o);

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
    public void binaryDataTest() throws Exception {
        UncachedObject o = new UncachedObject();
        byte[] binaryData = new byte[100];
        for (int i = 0; i < binaryData.length; i++) {
            binaryData[i] = (byte) i;
        }
        o.setBinaryData(binaryData);
        MorphiumSingleton.get().store(o);

        waitForAsyncOperationToStart(1000000);
        waitForWrites();
        MorphiumSingleton.get().reread(o);
        for (int i = 0; i < o.getBinaryData().length; i++) {
            assert (o.getBinaryData()[i] == binaryData[i]);
        }
    }

    @Test
    public void whereTest() throws Exception {

        for (int i = 1; i <= NO_OBJECTS; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            MorphiumSingleton.get().store(o);
        }
        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q.where("this.counter<10").f("counter").gt(5);
        log.info(q.toQueryObject().toString());

        List<UncachedObject> lst = q.asList();
        for (UncachedObject o : lst) {
            assert (o.getCounter() < 10 && o.getCounter() > 5) : "Counter is wrong: " + o.getCounter();
        }

        assert (MorphiumSingleton.get().getStatistics().get("X-Entries for: de.caluga.test.mongo.suite.UncachedObject") == null) : "Cached Uncached Object?!?!?!";


    }

    @Test
    public void existsTest() throws Exception {
        for (int i = 1; i <= 10; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            MorphiumSingleton.get().store(o);
        }
        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q = q.f("counter").exists().f("value").eq("Uncached 1");
        long c = q.countAll();
        assert (c == 1);

        UncachedObject o = q.get();
        assert (o.getCounter() == 1);
    }

    @Test
    public void notExistsTest() throws Exception {
        for (int i = 1; i <= 10; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            MorphiumSingleton.get().store(o);
        }
        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q = q.f("counter").notExists().f("value").eq("Uncached 1");
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
            o.setValue("Uncached " + i);
            MorphiumSingleton.get().store(o);
            last = o;
        }

        assert (last.getMongoId() != null) : "ID null?!?!?";

        UncachedObject uc = MorphiumSingleton.get().findById(UncachedObject.class, last.getMongoId());
        assert (uc != null) : "Not found?!?";
        assert (uc.getCounter() == last.getCounter()) : "Different Object? " + uc.getCounter() + " != " + last.getCounter();

    }

    @Test
    public void orTest() {
        log.info("Storing Uncached objects...");

        long start = System.currentTimeMillis();

        for (int i = 1; i <= NO_OBJECTS; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            MorphiumSingleton.get().store(o);
        }

        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q.or(q.q().f("counter").lt(10), q.q().f("value").eq("Uncached 50"));
        log.info("Query string: " + q.toQueryObject().toString());
        List<UncachedObject> lst = q.asList();
        for (UncachedObject o : lst) {
            assert (o.getCounter() < 10 || o.getValue().equals("Uncached 50")) : "Value did not match: " + o.toString();
            log.info(o.toString());
        }
        log.info("1st test passed");
        for (int i = 1; i < 120; i++) {
            //Storing some additional test content:
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i);
            uc.setValue("Complex Query Test " + i);
            MorphiumSingleton.get().store(uc);
        }

        assert (MorphiumSingleton.get().getStatistics().get("X-Entries for: de.caluga.test.mongo.suite.UncachedObject") == null) : "Cached Uncached Object?!?!?!";

    }


    @Test
    public void uncachedSingeTest() {
        log.info("Storing Uncached objects...");

        long start = System.currentTimeMillis();

        for (int i = 1; i <= NO_OBJECTS; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            MorphiumSingleton.get().store(o);
        }
        long dur = System.currentTimeMillis() - start;
        log.info("Storing single took " + dur + " ms");
//        assert (dur < NO_OBJECTS * 5) : "Storing took way too long";

        log.info("Searching for objects");

        checkUncached();
        assert (MorphiumSingleton.get().getStatistics().get("X-Entries for: de.caluga.test.mongo.suite.UncachedObject") == null) : "Cached Uncached Object?!?!?!";

    }

    @Test
    public void testAnnotationCache() throws Exception {
        Entity e = MorphiumSingleton.get().getARHelper().getAnnotationFromHierarchy(EmbeddedObject.class, Entity.class);
        assert (e == null);
        Embedded em = MorphiumSingleton.get().getARHelper().getAnnotationFromHierarchy(EmbeddedObject.class, Embedded.class);
        assert (em != null);
    }
    @Test
    public void uncachedListTest() {
        MorphiumSingleton.get().clearCollection(UncachedObject.class);
        log.info("Preparing a list...");

        long start = System.currentTimeMillis();
        List<UncachedObject> lst = new ArrayList<UncachedObject>();
        for (int i = 1; i <= NO_OBJECTS; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i);
            o.setValue("Uncached " + i);
            lst.add(o);
        }
        MorphiumSingleton.get().storeList(lst);
        long dur = System.currentTimeMillis() - start;
        log.info("Storing a list  took " + dur + " ms");
        checkUncached();
        assert (MorphiumSingleton.get().getStatistics().get("X-Entries for: de.caluga.test.mongo.suite.UncachedObject") == null) : "Cached Uncached Object?!?!?!";

    }

    private void checkUncached() {
        long start;
        long dur;
        start = System.currentTimeMillis();
        for (int i = 1; i <= NO_OBJECTS; i++) {
            Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
            q.f("counter").eq(i);
            List<UncachedObject> l = q.asList();
            assert (l != null && l.size() > 0) : "Nothing found!?!?!?!? Value: " + i;
            UncachedObject fnd = l.get(0);
            assert (fnd != null) : "Error finding element with id " + i;
            assert (fnd.getCounter() == i) : "Counter not equal: " + fnd.getCounter() + " vs. " + i;
            assert (fnd.getValue().equals("Uncached " + i)) : "value not equal: " + fnd.getCounter() + "(" + fnd.getValue() + ") vs. " + i;
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
            if (i == 0) i = 1;
            Query<CachedObject> q = MorphiumSingleton.get().createQueryFor(CachedObject.class);
            q.f("counter").eq(i);
            List<CachedObject> l = q.asList();
            assert (l != null && l.size() > 0) : "Nothing found!?!?!?!? " + i;
            CachedObject fnd = l.get(0);
            assert (fnd != null) : "Error finding element with id " + i;
            assert (fnd.getCounter() == i) : "Counter not equal: " + fnd.getCounter() + " vs. " + i;
            assert (fnd.getValue().equals("Cached " + i)) : "value not equal: " + fnd.getCounter() + " vs. " + i;
        }
        dur = System.currentTimeMillis() - start;
        log.info("Searching  took " + dur + " ms");
        log.info("Cache Hits Percentage: " + MorphiumSingleton.get().getStatistics().get(StatisticKeys.CHITSPERC.name()) + "%");
    }


    @Test
    public void cachedWritingTest() {
        log.info("Starting background writing test - single objects");
        long start = System.currentTimeMillis();
        for (int i = 1; i <= NO_OBJECTS; i++) {
            CachedObject o = new CachedObject();
            o.setCounter(i);
            o.setValue("Cached " + i);
            MorphiumSingleton.get().store(o);
        }

        long dur = System.currentTimeMillis() - start;
        log.info("Storing (in Cache) single took " + dur + " ms");
        waitForAsyncOperationToStart(1000000);
        waitForWrites();
        dur = System.currentTimeMillis() - start;
        log.info("Storing took " + dur + " ms overall");
        randomCheck();
        Map<String, Double> statistics = MorphiumSingleton.get().getStatistics();
        Double uc = statistics.get("X-Entries for: de.caluga.test.mongo.suite.UncachedObject");
        assert (uc == null || uc == 0) : "Cached Uncached Object?!?!?!";
        assert (statistics.get("X-Entries for: de.caluga.test.mongo.suite.CachedObject") > 0) : "No Cached Object cached?!?!?!";

    }


    @Test
    public void checkListWriting() {
        List<CachedObject> lst = new ArrayList<CachedObject>();
        try {
            MorphiumSingleton.get().store(lst);
            MorphiumSingleton.get().storeBuffered(lst);
        } catch (Exception e) {
            log.info("Got exception, good!");
            return;
        }
        assert (false) : "Exception missing!";
    }

    @Test
    public void checkToStringUniqueness() {
        Query<CachedObject> q = MorphiumSingleton.get().createQueryFor(CachedObject.class);
        q = q.f("value").eq("Test").f("counter").gt(5);
        String t = q.toString();
        log.info("Tostring: " + q.toString());
        q = MorphiumSingleton.get().createQueryFor(CachedObject.class);
        q = q.f("counter").gt(5).f("value").eq("Test");
        String s = q.toString();
        if (!s.equals(t)) {
            log.warn("Warning: order is important s=" + s + " and t=" + t);
        }

        q = MorphiumSingleton.get().createQueryFor(CachedObject.class);
        q = q.f("counter").gt(5).sort("counter", "-value");
        t = q.toString();
        q = MorphiumSingleton.get().createQueryFor(CachedObject.class);
        q = q.f("counter").gt(5);
        s = q.toString();
        assert (!t.equals(s)) : "Values should not be equal: s=" + s + " t=" + t;
    }

    @Test
    public void mixedListWritingTest() {
        List<Object> tst = new ArrayList<Object>();
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
                uc.setValue("List Test uc");
                uc.setCounter(22222);
                tst.add(uc);
            }
        }
        log.info("Writing " + cached + " Cached and " + uncached + " uncached objects!");

        MorphiumSingleton.get().storeList(tst);
        waitForAsyncOperationToStart(1000000);
        waitForWrites();
        //Still waiting - storing lists is not shown in number of write buffer entries
//        try {
//            Thread.sleep(2000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
        Query<UncachedObject> qu = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        assert (qu.countAll() == uncached) : "Difference in object count for cached objects. Wrote " + uncached + " found: " + qu.countAll();
        Query<CachedObject> q = MorphiumSingleton.get().createQueryFor(CachedObject.class);
        assert (q.countAll() == cached) : "Difference in object count for cached objects. Wrote " + cached + " found: " + q.countAll();

    }


}
