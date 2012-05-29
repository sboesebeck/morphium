package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.Query;
import de.caluga.morphium.StatisticKeys;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * User: Stephan Bösebeck
 * Date: 29.05.12
 * Time: 00:02
 * <p/>
 * TODO: Add documentation here
 */
public class LazyLoadingTest extends MongoTest{

    @Test
    public void lazyLoadingTest() throws Exception {
        Query<LazyLoadingObject> q=MorphiumSingleton.get().createQueryFor(LazyLoadingObject.class);
        //clean
        MorphiumSingleton.get().delete(q);


        LazyLoadingObject lz=new LazyLoadingObject();
        UncachedObject o=new UncachedObject();
        o.setCounter(15);
        o.setValue("A uncached value");
        MorphiumSingleton.get().store(o);

        CachedObject co=new CachedObject();
        co.setCounter(22);
        co.setValue("A cached Value");
        MorphiumSingleton.get().store(co);

        waitForWrites();

        lz.setName("Lazy");
        lz.setLazyCached(co);
        lz.setLazyUncached(o);
        MorphiumSingleton.get().store(lz);

        waitForWrites();

        //Test for lazy loading


        q=q.f("name").eq("Lazy");
        LazyLoadingObject lzRead=q.get();

        assert(lzRead!=null):"Not found????";
        log.info("LZRead: "+lzRead.getClass().getName());
        //Field f=MorphiumSingleton.get().getConfig().getMapper().getField(LazyLoadingObject.class,"lazy_uncached");

        log.info("uncached: " + lzRead.getLazyUncached().getClass().getName());
        int cnt = lzRead.getLazyUncached().getCounter();
        assert(cnt==o.getCounter()):"Counter not equal";

        cnt=lzRead.getLazyCached().getCounter();
        assert(cnt==co.getCounter()):"Counter (cached) not equal";

        log.info("Cache Entries:" +MorphiumSingleton.get().getStatistics().get(StatisticKeys.CACHE_ENTRIES));

    }
}
