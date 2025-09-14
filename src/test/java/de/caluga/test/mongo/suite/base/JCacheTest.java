package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.cache.MorphiumCache;
import de.caluga.morphium.cache.MorphiumCacheJCacheImpl;
import de.caluga.morphium.cache.jcache.CacheEntry;
import de.caluga.morphium.cache.jcache.CacheManagerImpl;
import de.caluga.morphium.cache.jcache.CachingProviderImpl;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.ehcache.jcache.JCacheCachingProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.CacheManager;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 17.05.18
 * Time: 08:18
 * <p>
 * TODO: Add documentation here
 */
@Tag("core")
@Tag("cache")
public class JCacheTest extends MorphiumTestBase {
    private final Logger log = LoggerFactory.getLogger(JCacheTest.class);

    @Test
    public void getProviderTest() throws Exception {
        MorphiumCache cache = morphium.getCache();
        morphium.getConfig().setCache(new MorphiumCacheJCacheImpl());
        //keep original setting
        CacheManager def = cache.getCacheManager();
        morphium.getCache().setCacheManager(def);
        morphium.getCache().removeEntryFromCache(String.class, "123");
        List<UncachedObject> l = new ArrayList<>();
        l.add(new UncachedObject("val", 123));
        l.get(0).setMorphiumId(new MorphiumId());
        morphium.getCache().addToCache("test", UncachedObject.class, l);


        List<CacheManager> lst = new ArrayList<>();

        JCacheCachingProvider provider = new JCacheCachingProvider();
//        CachingProvider provider = Caching.getCachingProvider();
        lst.add(provider.getCacheManager());
        CacheManagerImpl e = new CacheManagerImpl(morphium.getConfig().asProperties());
        e.getCachingProvider();
        e.getCacheNames();
        e.getClassLoader();
        e.getURI();
        e.getUri();
        e.getProperties();
        e.getCaches();
        e.createCache("Testcache", null);
        e.enableManagement("Testcache", true);
        e.enableStatistics("Testcache", true);
        e.destroyCache("Testcache");
        e.unwrap(e.getClass());

        assert (!e.isClosed());

        lst.add(e);


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
        e.setUri(e.getUri());
        e.setProperties(e.getProperties());
        e.setCachingProvider(new CachingProviderImpl());
        e.setClassLoader(e.getClassLoader());
        e.getCachingProvider().getCacheManager();
        e.getCachingProvider().getDefaultClassLoader();
        e.getCachingProvider().getDefaultURI();
        e.getCachingProvider().getDefaultProperties();
        e.getCache("test").getAll(new HashSet<>());
        e.getCache("test").removeAll(new HashSet<>());
        e.getCache("test").removeAll();
        e.getCache("test").getAndPut(new CacheEntry<String>("test", 1), new CacheEntry<String>("test2", 2));
        e.getCache("test").getAndReplace(new CacheEntry<String>("test", 1), new CacheEntry<String>("test2", 2));
        e.getCache("test").getAndRemove(new CacheEntry<String>("test", 1));
        e.getCache("test").replace(new CacheEntry<String>("test", 1), new CacheEntry<String>("test2", 2));
        e.getCache("test").replace("123", new CacheEntry<String>("test", 1), new CacheEntry<String>("test2", 2));
        e.getCache("test").get("123");
        e.getCache("test").put("123", new CacheEntry<String>("test", 1));
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
