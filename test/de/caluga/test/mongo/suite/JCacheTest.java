package de.caluga.test.mongo.suite;

import de.caluga.morphium.cache.MorphiumCache;
import de.caluga.morphium.cache.MorphiumCacheJCacheImpl;
import de.caluga.morphium.cache.jcache.CacheManagerImpl;
import de.caluga.test.mongo.suite.data.CachedObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 17.05.18
 * Time: 08:18
 * <p>
 * TODO: Add documentation here
 */
public class JCacheTest extends MongoTest {
    private Logger log = LoggerFactory.getLogger(JCacheTest.class);

    @Test
    public void getProviderTest() throws Exception {
        MorphiumCache cache = morphium.getCache();
        morphium.getConfig().setCache(new MorphiumCacheJCacheImpl());
        //keep original setting
        CacheManager def = cache.getCacheManager();

        List<CacheManager> lst = new ArrayList<>();

        //JCacheCachingProvider provider=new JCacheCachingProvider();
        CachingProvider provider = Caching.getCachingProvider();
        lst.add(provider.getCacheManager());
        lst.add(new CacheManagerImpl(morphium.getConfig().asProperties()));

        for (CacheManager m : lst) {
            long start = System.currentTimeMillis();
            log.info("Testing cache " + m.getClass().getName());
            cache.setCacheManager(m);
            cacheTest(cache);
            long dur = System.currentTimeMillis() - start;
            log.info("    duration: " + dur + "\n\n");
        }
        cache.setCacheManager(def);
        morphium.getConfig().setCache(cache);

    }

    private void cacheTest(MorphiumCache cache) throws Exception {
        morphium.dropCollection(CachedObject.class);
        morphium.resetStatistics();

        for (int i = 0; i < 100; i++) {
            CachedObject o = new CachedObject();
            o.setValue("test " + i);
            o.setCounter(i);
            morphium.store(o);
        }
        Thread.sleep(5000);

        morphium.createQueryFor(CachedObject.class).f("counter").eq(12).get();
        morphium.createQueryFor(CachedObject.class).f("counter").eq(12).get();
        morphium.createQueryFor(CachedObject.class).f("counter").eq(12).get();
        morphium.createQueryFor(CachedObject.class).f("counter").eq(55).get();
        morphium.createQueryFor(CachedObject.class).f("counter").eq(13).get();
        morphium.createQueryFor(CachedObject.class).f("counter").eq(34).get();

        for (int i = 0; i < 1000; i++) {
            morphium.createQueryFor(CachedObject.class).f("counter").eq(12).get();
        }
        Map<String, Integer> sizes = cache.getSizes();
        for (String k : sizes.keySet()) {
            log.info("Key " + k + " size: " + sizes.get(k));
            assert (sizes.get(k) > 0);
        }
        Map<String, Double> stats = morphium.getStatistics();
        for (String k : stats.keySet()) {
            log.info(k + ": " + stats.get(k));
        }
    }
}
