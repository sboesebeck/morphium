package de.caluga.test.mongo.suite;

import de.caluga.morphium.cache.MorphiumCache;
import de.caluga.test.mongo.suite.data.CachedObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
//        CacheManager def = cache.getCacheManager();
//        cache.setCacheManager(new CacheManagerImpl(morphium.getConfig().asProperties()));

        for (int i = 0; i < 100; i++) {
            CachedObject o = new CachedObject();
            o.setValue("test " + i);
            o.setCounter(i);
            morphium.store(o);
        }
        Thread.sleep(3000);
        morphium.createQueryFor(CachedObject.class).f("counter").eq(12).get();
        morphium.createQueryFor(CachedObject.class).f("counter").eq(12).get();
        morphium.createQueryFor(CachedObject.class).f("counter").eq(12).get();
        morphium.createQueryFor(CachedObject.class).f("counter").eq(55).get();
        morphium.createQueryFor(CachedObject.class).f("counter").eq(13).get();
        morphium.createQueryFor(CachedObject.class).f("counter").eq(34).get();

        Map<String, Integer> sizes = cache.getSizes();
        for (String k : sizes.keySet()) {
            log.info("Key " + k + " size: " + sizes.get(k));
            assert (sizes.get(k) > 0);
        }
//        cache.setCacheManager(def);
    }
}
