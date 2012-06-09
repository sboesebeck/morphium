package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.Query;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 29.05.12
 * Time: 00:02
 * <p/>
 * TODO: Add documentation here
 */
public class LazyLoadingTest extends MongoTest {

    @Test
    public void lazyLoadingTest() throws Exception {
        Query<LazyLoadingObject> q = MorphiumSingleton.get().createQueryFor(LazyLoadingObject.class);
        //clean
        MorphiumSingleton.get().delete(q);


        LazyLoadingObject lz = new LazyLoadingObject();
        UncachedObject o = new UncachedObject();
        o.setCounter(15);
        o.setValue("A uncached value");
        MorphiumSingleton.get().store(o);

        CachedObject co = new CachedObject();
        co.setCounter(22);
        co.setValue("A cached Value");
        MorphiumSingleton.get().store(co);

        waitForWrites();

        lz.setName("Lazy");
        lz.setLazyCached(co);
        lz.setLazyUncached(o);

        List<UncachedObject> lst = new ArrayList<UncachedObject>();
        for (int i = 0; i < 10; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setValue("Part of list");
            uc.setCounter(i * 5 + 7);
            lst.add(uc);
        }
        lz.setLazyLst(lst);

        MorphiumSingleton.get().store(lz);

        waitForWrites();

        //Test for lazy loading


        q = q.f("name").eq("Lazy");
        LazyLoadingObject lzRead = q.get();

        assert (lzRead != null) : "Not found????";
        log.info("LZRead: " + lzRead.getClass().getName());
        assert (!lzRead.getClass().getName().contains("$EnhancerByCGLIB$")) : "Lazy loader in Root-Object?";
        Double rd = MorphiumSingleton.get().getStatistics().get(Morphium.StatisticKeys.READS.name());
        if (rd == null) rd = 0.0;
        //Field f=MorphiumSingleton.get().getConfig().getMapper().getField(LazyLoadingObject.class,"lazy_uncached");

        int cnt = lzRead.getLazyUncached().getCounter();
        log.info("uncached: " + lzRead.getLazyUncached().getClass().getName());
        assert (lzRead.getLazyUncached().getClass().getName().contains("$EnhancerByCGLIB$")) : "Not lazy loader?";

        assert (cnt == o.getCounter()) : "Counter not equal";
        double rd2 = MorphiumSingleton.get().getStatistics().get(Morphium.StatisticKeys.READS.name());
        assert (rd2 > rd) : "No read?";

        rd = MorphiumSingleton.get().getStatistics().get(Morphium.StatisticKeys.READS.name());
        double crd = MorphiumSingleton.get().getStatistics().get(Morphium.StatisticKeys.CACHE_ENTRIES.name());
        cnt = lzRead.getLazyCached().getCounter();
        assert (cnt == co.getCounter()) : "Counter (cached) not equal";
        rd2 = MorphiumSingleton.get().getStatistics().get(Morphium.StatisticKeys.READS.name());
        assert (MorphiumSingleton.get().getStatistics().get(Morphium.StatisticKeys.CACHE_ENTRIES.name()) > crd) : "Not cached?";
        assert (rd2 > rd) : "No read?";
        log.info("Cache Entries:" + MorphiumSingleton.get().getStatistics().get(Morphium.StatisticKeys.CACHE_ENTRIES.name()));

        assert (lzRead.getLazyLst().size() == lz.getLazyLst().size()) : "List sizes differ?!?!";
        for (UncachedObject uc : lzRead.getLazyLst()) {
            assert (uc.getClass().getName().contains("$EnhancerByCGLIB$")) : "Lazy list not lazy?";

        }


    }


    @Test
    public void lazyLoadingPerformanceTest() throws Exception {
        Query<LazyLoadingObject> q = MorphiumSingleton.get().createQueryFor(LazyLoadingObject.class);
        //clean
        MorphiumSingleton.get().delete(q);

        log.info("Creating lots of lazyobjects");
        int numberOfObjects = 200;
        for (int i = 0; i < numberOfObjects; i++) {
            LazyLoadingObject lz = new LazyLoadingObject();
            UncachedObject o = new UncachedObject();
            o.setCounter(i * 2 + 50);
            o.setValue("A uncached value " + i);
            MorphiumSingleton.get().store(o);

            CachedObject co = new CachedObject();
            co.setCounter(i + numberOfObjects);
            co.setValue("A cached Value " + i);
            MorphiumSingleton.get().store(co);

            waitForWrites();

            lz.setName("Lazy " + i);
            lz.setLazyCached(co);
            lz.setLazyUncached(o);
            MorphiumSingleton.get().store(lz);


        }
        waitForWrites();
        log.info("done - now creating not lazy referenced objects");
        for (int i = 0; i < numberOfObjects; i++) {
            ComplexObject co = new ComplexObject();
            co.setEinText("Txt " + i);
            UncachedObject o = new UncachedObject();
            o.setCounter(i * 2 + 50);
            o.setValue("A uncached value " + i);
            MorphiumSingleton.get().store(o);
            co.setRef(o);

            CachedObject cmo = new CachedObject();
            cmo.setCounter(i + numberOfObjects);
            cmo.setValue("A cached Value " + i);
            MorphiumSingleton.get().store(co);

            waitForWrites();

            co.setcRef(cmo);

            MorphiumSingleton.get().store(co);
        }
        log.info("done");

        log.info("Reading in the not-lazy objects");
        long start = System.currentTimeMillis();
        MorphiumSingleton.get().readAll(ComplexObject.class);
        long dur = System.currentTimeMillis() - start;
        log.info("Reading all took: " + dur + "ms ");

        log.info("now reading in the lazy objects");
        start = System.currentTimeMillis();
        List<LazyLoadingObject> lzlst = MorphiumSingleton.get().readAll(LazyLoadingObject.class);
        dur = System.currentTimeMillis() - start;
        log.info("Reading all lazy took: " + dur + "ms (" + lzlst.size() + " objects)");

        log.info("Reading them single...");
        start = System.currentTimeMillis();

        for (int i = 0; i < numberOfObjects; i++) {
            Query<ComplexObject> coq = MorphiumSingleton.get().createQueryFor(ComplexObject.class);
            coq = coq.f("einText").eq("Txt " + i);
            coq.get(); //should only be one!!!
        }
        dur = System.currentTimeMillis() - start;
        log.info("Reading single un-lazy took " + dur + " ms");

        log.info("Reading lazy objects single...");
        start = System.currentTimeMillis();
        //Store them to prefent finalizer() to be called causing the lazy loading to take place
        List<LazyLoadingObject> storage = new ArrayList<LazyLoadingObject>();
        for (int i = 0; i < numberOfObjects; i++) {
            Query<LazyLoadingObject> coq = MorphiumSingleton.get().createQueryFor(LazyLoadingObject.class);
            coq = coq.f("name").eq("Lazy " + i);
//            storage.add(coq.get()); //should only be one!!!
            coq.get();
        }
        dur = System.currentTimeMillis() - start;
        log.info("Reading single lazy took " + dur + " ms");
    }
}
