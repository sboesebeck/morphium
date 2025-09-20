package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.StatisticKeys;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Reference;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.LazyLoadingObject;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * User: Stephan Bösebeck
 * Date: 29.05.12
 * Time: 00:02
 * <p/>
 */
@SuppressWarnings("AssertWithSideEffects")
@Tag("core")
public class LazyLoadingTest extends MorphiumTestBase {

    private final boolean wouldDeref = false;
    private final boolean didDeref = false;

    @Test
    public void deRefTest() throws Exception {
        morphium.clearCollection(LazyLoadingObject.class);
        LazyLoadingObject lz = new LazyLoadingObject();
        UncachedObject o = new UncachedObject();
        o.setCounter(15);
        o.setStrValue("A uncached value");
        morphium.store(o);

        CachedObject co = new CachedObject();
        co.setCounter(22);
        co.setValue("A cached Value");
        morphium.store(co);

        TestUtils.waitForWrites(morphium, log);

        lz.setName("Lazy");
        lz.setLazyCached(co);
        lz.setLazyUncached(o);
        morphium.store(lz);

        TestUtils.waitForWrites(morphium, log);
        Query<LazyLoadingObject> q = morphium.createQueryFor(LazyLoadingObject.class);
        q = q.f("name").eq("Lazy");
        LazyLoadingObject lzRead = q.get();
        Object id = morphium.getId(lzRead);
        assertNotNull(id);
        ;
        assert (lzRead.getLazyUncached().getCounter() == 15);
        assert (lzRead.getLazyUncached().getStrValue().equals("A uncached value"));
        co = lzRead.getLazyCached();
        Thread.sleep(1000);
        id = morphium.getId(co);
        assert (co.getCounter() == 22) : "Counter wrong.." + co.getCounter();
        assertNotNull(id);
        ;

    }

    @Test
    public void lazyLoadingTest() {
        Query<LazyLoadingObject> q = morphium.createQueryFor(LazyLoadingObject.class);
        //clean
        morphium.delete(q);


        LazyLoadingObject lz = new LazyLoadingObject();
        UncachedObject o = new UncachedObject();
        o.setCounter(15);
        o.setStrValue("A uncached value");
        morphium.store(o);

        CachedObject co = new CachedObject();
        co.setCounter(22);
        co.setValue("A cached Value");
        morphium.store(co);

        TestUtils.waitForWrites(morphium, log);

        lz.setName("Lazy");
        lz.setLazyCached(co);
        lz.setLazyUncached(o);

        List<UncachedObject> lst = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setStrValue("Part of list");
            uc.setCounter(i * 5 + 7);
            lst.add(uc);
        }
        lz.setLazyLst(lst);

        morphium.store(lz);

        TestUtils.waitForWrites(morphium, log);

        //Test for lazy loading


        q = q.f("name").eq("Lazy");
        LazyLoadingObject lzRead = q.get();

        assertNotNull(lzRead, "Not found????");
        log.info("LZRead: " + lzRead.getClass().getName());
        assert (!lzRead.getClass().getName().contains("$EnhancerByCGLIB$")) : "Lazy loader in Root-Object?";
        Double rd = morphium.getStatistics().get(StatisticKeys.READS.name());
        if (rd == null) {
            rd = 0.0;
        }
        //Field f=morphium.getConfig().getMapper().getField(LazyLoadingObject.class,"lazy_uncached");

        int cnt = lzRead.getLazyUncached().getCounter();
        log.info("uncached: " + lzRead.getLazyUncached().getClass().getName());
        assert (lzRead.getLazyUncached().getClass().getName().contains("$EnhancerByCGLIB$")) : "Not lazy loader?";

        assert (cnt == o.getCounter()) : "Counter not equal";
        double rd2 = morphium.getStatistics().get(StatisticKeys.READS.name());
        assert (rd2 > rd) : "No read?";

        if (morphium.getDriver().getName().equals(InMemoryDriver.driverName)) {
            log.info("Cannot check for caching, inMemoryDriver enabled");
        } else {
            rd = morphium.getStatistics().get(StatisticKeys.READS.name());
            double crd = morphium.getStatistics().get(StatisticKeys.CACHE_ENTRIES.name());
            cnt = lzRead.getLazyCached().getCounter();
            assert (cnt == co.getCounter()) : "Counter (cached) not equal";
            rd2 = morphium.getStatistics().get(StatisticKeys.READS.name());
            assert (rd2 > rd) : "No read?";
            log.info("Cache Entries:" + morphium.getStatistics().get(StatisticKeys.CACHE_ENTRIES.name()));
            assertTrue (morphium.getStatistics().get(StatisticKeys.CACHE_ENTRIES.name()) > crd, "not cached");
        }

        assert (lzRead.getLazyLst().size() == lz.getLazyLst().size()) : "List sizes differ?!?!";
        for (UncachedObject uc : lzRead.getLazyLst()) {
            assert (uc.getClass().getName().contains("$EnhancerByCGLIB$")) : "Lazy list not lazy?";

        }


    }


    @Test
    @org.junit.jupiter.api.Disabled("Performance tests don't provide meaningful assertions for test coverage")
    public void lazyLoadingPerformanceTest() {
        Query<LazyLoadingObject> q = morphium.createQueryFor(LazyLoadingObject.class);
        //clean
        morphium.delete(q);

        log.info("Creating lots of lazyobjects");
        int numberOfObjects = 20;
        for (int i = 0; i < numberOfObjects; i++) {
            LazyLoadingObject lz = new LazyLoadingObject();
            UncachedObject o = new UncachedObject();
            o.setCounter(i * 2 + 50);
            o.setStrValue("A uncached value " + i);
            morphium.store(o);

            CachedObject co = new CachedObject();
            co.setCounter(i + numberOfObjects);
            co.setValue("A cached Value " + i);
            morphium.store(co);

            TestUtils.waitForWrites(morphium, log);

            lz.setName("Lazy " + i);
            lz.setLazyCached(co);
            lz.setLazyUncached(o);
            log.info("Storing...");
            morphium.store(lz);
            log.info("Stored object " + i + "/" + 20);

        }
        TestUtils.waitForWrites(morphium, log);
        log.info("done - now creating not lazy referenced objects");
        for (int i = 0; i < numberOfObjects; i++) {
            ComplexObject co = new ComplexObject();
            co.setEinText("Txt " + i);
            UncachedObject o = new UncachedObject();
            o.setCounter(i * 2 + 50);
            o.setStrValue("A uncached value " + i);
            morphium.store(o);
            co.setRef(o);

            CachedObject cmo = new CachedObject();
            cmo.setCounter(i + numberOfObjects);
            cmo.setValue("A cached Value " + i);
            morphium.store(co);

            TestUtils.waitForWrites(morphium, log);

            co.setcRef(cmo);

            morphium.store(co);
        }
        log.info("done");

        log.info("Reading in the not-lazy objects");
        long start = System.currentTimeMillis();
        morphium.readAll(ComplexObject.class);
        long dur = System.currentTimeMillis() - start;
        log.info("Reading all took: " + dur + "ms ");

        log.info("now reading in the lazy objects");
        start = System.currentTimeMillis();
        List<LazyLoadingObject> lzlst = morphium.readAll(LazyLoadingObject.class);
        dur = System.currentTimeMillis() - start;
        log.info("Reading all lazy took: " + dur + "ms (" + lzlst.size() + " objects)");

        log.info("Reading them single...");
        start = System.currentTimeMillis();

        for (int i = 0; i < numberOfObjects; i++) {
            Query<ComplexObject> coq = morphium.createQueryFor(ComplexObject.class);
            coq = coq.f("einText").eq("Txt " + i);
            coq.get(); //should only be one!!!
        }
        dur = System.currentTimeMillis() - start;
        log.info("Reading single un-lazy took " + dur + " ms");

        log.info("Reading lazy objects single...");
        start = System.currentTimeMillis();
        //Store them to prefent finalizer() to be called causing the lazy loading to take place
        List<LazyLoadingObject> storage = new ArrayList<>();
        for (int i = 0; i < numberOfObjects; i++) {
            Query<LazyLoadingObject> coq = morphium.createQueryFor(LazyLoadingObject.class);
            coq = coq.f("name").eq("Lazy " + i);
            //            storage.add(coq.get()); //should only be one!!!
            coq.get();
        }
        dur = System.currentTimeMillis() - start;
        log.info("Reading single lazy took " + dur + " ms");
    }


    @Test
    public void testLazyRef() throws Exception {
        Morphium m = morphium;
        m.clearCollection(SimpleEntity.class);

        SimpleEntity s1 = new SimpleEntity(1);
        SimpleEntity s2 = new SimpleEntity(2);
        SimpleEntity s3 = new SimpleEntity(3);

        m.store(s1);
        m.store(s3);


        s2.ref = s1;
        s2.lazyRef = s3;
        m.store(s2);
        Thread.sleep(200);

        SimpleEntity s1Fetched = m.createQueryFor(SimpleEntity.class).f("value").eq(1).get();
        assert (s1Fetched.value == 1);
        SimpleEntity s2Fetched = m.createQueryFor(SimpleEntity.class).f("value").eq(2).get();
        assert (s2Fetched.value == 2);
        SimpleEntity s3Fetched = m.createQueryFor(SimpleEntity.class).f("value").eq(3).get();
        assert (s3Fetched.value == 3);
        assert (s2Fetched.getRef().getValue() == 1);
        System.out.println(s2Fetched.lazyRef.value);
        assert (s2Fetched.getLazyRef().getValue() == 3);

    }

    @Entity
    public static class SimpleEntity {

        protected int value;
        @Id
        MorphiumId id;
        @Reference
        SimpleEntity ref;
        @Reference(lazyLoading = true)
        SimpleEntity lazyRef;

        public SimpleEntity(int value) {
            this.value = value;
        }

        public SimpleEntity() {
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public SimpleEntity getLazyRef() {
            return lazyRef;
        }

        public void setLazyRef(SimpleEntity lazyRef) {
            this.lazyRef = lazyRef;
        }

        public SimpleEntity getRef() {
            return ref;
        }

        public void setRef(SimpleEntity ref) {
            this.ref = ref;
        }

        public MorphiumId getId() {
            return id;
        }

        public void setId(MorphiumId id) {
            this.id = id;
        }
    }

}
