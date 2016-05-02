package de.caluga.test.mongo.suite.unit;

import de.caluga.morphium.Utils;
import de.caluga.morphium.cache.*;
import de.caluga.morphium.driver.bson.MorphiumId;
import de.caluga.test.mongo.suite.data.CachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 28.11.15
 * Time: 14:31
 * <p>
 * TODO: Add documentation here
 */
public class MorphiumCacheTest {

    @Test
    public void testAddCacheListener() throws Exception {
        MorphiumCacheImpl imp = new MorphiumCacheImpl();
        CacheListener cl = new CacheListener() {
            @Override
            public <T> CacheObject<T> wouldAddToCache(CacheObject<T> toCache) {
                return null;
            }

            @Override
            public <T> boolean wouldClearCache(Class<T> affectedEntityType) {
                return false;
            }

            @Override
            public <T> boolean wouldRemoveEntryFromCache(Class cls, Object id, Object entity) {
                return false;
            }
        };
        imp.addCacheListener(cl);
        assert (imp.isListenerRegistered(cl));
        imp.removeCacheListener(cl);
        assert (!imp.isListenerRegistered(cl));
    }


    @Test
    public void testAddToCache() throws Exception {
        MorphiumCacheImpl imp = new MorphiumCacheImpl();
        List<CachedObject> lst = new ArrayList<>();
        CachedObject o = new CachedObject();
        MorphiumId id = new MorphiumId();
        o.setId(id);
        o.setCounter(999);
        lst.add(o);
        imp.addToCache("key", CachedObject.class, lst);
        assert (imp.isCached(CachedObject.class, "key"));

        lst = imp.getFromCache(CachedObject.class, "key");
        assert (lst.size() == 1);
        assert (imp.getFromIDCache(CachedObject.class, id).getCounter() == 999);
        assert (imp.getFromIDCache(CachedObject.class, id).getId().equals(id));
        imp.removeEntryFromCache(CachedObject.class, id);

        assert (!imp.isCached(CachedObject.class, "key"));

        assert (imp.getFromIDCache(CachedObject.class, id) == null);

    }

    @Test
    public void testClearCacheIfNecessary() throws Exception {
        MorphiumCacheImpl imp = new MorphiumCacheImpl();
        imp.clearCacheIfNecessary(CachedObject.class);
    }


    @Test
    public void testGetCache() throws Exception {
        MorphiumCacheImpl imp = new MorphiumCacheImpl();
        assert (imp.getCache() != null);
        assert (imp.getCache() instanceof Map);
    }

    @Test
    public void testGetIdCache() throws Exception {
        MorphiumCache imp = new MorphiumCacheImpl();
        assert (imp.getIdCache() != null);
        assert (imp.getIdCache() instanceof Map);
        assert (imp.getIdCache().isEmpty());
    }

    @Test
    public void testGetCacheKey() throws Exception {
        MorphiumCache imp = new MorphiumCacheImpl();
        Map<String, Object> q = Utils.getMap("counter", 999);
        Map<String, Integer> sort = Utils.getIntMap("counter", -1);

        String k = imp.getCacheKey(q, sort, "uncached_object", 123, 321);
        assert (k.equals("{counter=999} c:uncached_object l:321 s:123 sort: counter:-1"));
    }


    @Test
    public void testClearCachefor() throws Exception {
        MorphiumCache imp = new MorphiumCacheImpl();
        imp.addToCache("key", CachedObject.class, new ArrayList<>());
        assert (imp.isCached(CachedObject.class, "key"));
        imp.clearCachefor(CachedObject.class);
        assert (!imp.isCached(CachedObject.class, "key"));
        assert (imp.getFromCache(CachedObject.class, "key") == null);
    }

    @Test
    public void testResetCache() throws Exception {
        MorphiumCache imp = new MorphiumCacheImpl();
        imp.addToCache("key", CachedObject.class, new ArrayList<>());
        assert (imp.isCached(CachedObject.class, "key"));
        imp.resetCache();
        assert (!imp.isCached(CachedObject.class, "key"));
        assert (imp.getFromCache(CachedObject.class, "key") == null);
    }

    @Test
    public void testSetCache() throws Exception {
        MorphiumCache imp = new MorphiumCacheImpl();
        Map<Class<?>, Map<String, CacheElement>> o = new HashMap<>();
        imp.setCache(o);
        imp.addToCache("key", CachedObject.class, new ArrayList<>());
        assert (o.size() != 0);
    }


}