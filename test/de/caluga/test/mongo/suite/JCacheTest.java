package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.cache.CacheObject;
import de.caluga.morphium.cache.jcache.CacheEntry;
import de.caluga.morphium.cache.jcache.CachingProviderImpl;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.CachedObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 17.05.18
 * Time: 08:18
 * <p>
 * TODO: Add documentation here
 */
public class JCacheTest {
    private Logger log = LoggerFactory.getLogger(JCacheTest.class);

    @Test
    public void getProviderTest() throws Exception {
        if (System.getProperty(Caching.JAVAX_CACHE_CACHING_PROVIDER) == null) {
            System.setProperty(Caching.JAVAX_CACHE_CACHING_PROVIDER, CachingProviderImpl.class.getName());
        }
        CachingProvider cp = Caching.getCachingProvider();
        assert (cp != null);
        URI c = new URI(CachedObject.class.getName());
        log.info("URI: " + c.toString());

        MorphiumConfig cfg = new MorphiumConfig();
        CacheManager mgr = cp.getCacheManager(c, this.getClass().getClassLoader(), cfg.asProperties());
        Cache<MorphiumId, CacheEntry<CachedObject>> idCache = mgr.getCache("idcache");
        Cache<String, CacheEntry<List<MorphiumId>>> results = mgr.getCache("results");

        CachedObject cachedObject = new CachedObject();
        cachedObject.setId(new MorphiumId());
        CacheObject<CachedObject> obj = new CacheObject<>(cachedObject);

        List<MorphiumId> idlst = new ArrayList<>();
        idlst.add(cachedObject.getId());
        results.put("search key", new CacheEntry<>(idlst));
        idCache.put(cachedObject.getId(), new CacheEntry<>(cachedObject));
        assert (results.get("search key") != null);
        assert (idCache.get(cachedObject.getId()).getResult().equals(cachedObject));
        assert (results.get("search key").getCreated() != 0);

    }
}
