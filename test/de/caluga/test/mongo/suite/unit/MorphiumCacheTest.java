package de.caluga.test.mongo.suite.unit;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Utils;
import de.caluga.morphium.cache.*;
import de.caluga.morphium.driver.bson.MorphiumId;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.*;

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
    public void testGetIdCacheAdding() throws Exception {
        MorphiumCache imp = new MorphiumCacheImpl();
        imp.setAnnotationAndReflectionHelper(new AnnotationAndReflectionHelper(true));
        Map<String, Object> q = Utils.getMap("counter", 999);
        Map<String, Integer> sort = Utils.getIntMap("counter", -1);
        String k = imp.getCacheKey(UncachedObject.class, q, sort, null, "uncached_object", 123, 321);
        UncachedObject uc = new UncachedObject();
        uc.setMorphiumId(new MorphiumId());
        uc.setCounter(123);
        imp.addToCache(k, UncachedObject.class, Arrays.asList(uc));
        assert (imp.getIdCache().size() != 0);
        assert (imp.getCache().size() != 0);

        imp.getIdCache().clear();
        imp.getCache().clear();

        k = imp.getCacheKey(UncachedObject.class, q, sort, Utils.getMap("counter", 0), "uncached_object", 123, 321);
        imp.addToCache(k, UncachedObject.class, Arrays.asList(uc));
        assert (imp.getIdCache().size() == 0);
        assert (imp.getCache().size() != 0);
    }

    @Test
    public void testGetCacheKey() throws Exception {
        MorphiumCache imp = new MorphiumCacheImpl();
        Map<String, Object> q = Utils.getMap("counter", 999);
        Map<String, Integer> sort = Utils.getIntMap("counter", -1);

        String k = imp.getCacheKey(UncachedObject.class, q, sort, null, "uncached_object", 123, 321);
        assert (k.equals("{counter=999} c:uncached_object l:321 s:123 sort:{ counter:-1}"));
    }

    @Test
    public void testGetCacheKeyProjection() throws Exception {
        MorphiumCache imp = new MorphiumCacheImpl();
        Map<String, Object> q = Utils.getMap("counter", 999);
        Map<String, Integer> sort = Utils.getIntMap("counter", -1);

        String k = imp.getCacheKey(UncachedObject.class, q, sort, Utils.getMap("value", -1), "uncached_object", 123, 321);
        assert (k.equals("{counter=999} c:uncached_object l:321 s:123 sort:{ counter:-1} project:{ value:-1}"));
        AnnotationAndReflectionHelper ar = new AnnotationAndReflectionHelper(true);
        List<Field> lst = ar.getAllFields(UncachedObject.class);
        Map<String, Object> projection = new HashMap<>();
        for (Field f : lst) {
            projection.put(f.getName(), 1);
        }

        k = imp.getCacheKey(UncachedObject.class, q, sort, projection, "uncached_object", 123, 321);
        assert (k.equals("{counter=999} c:uncached_object l:321 s:123 sort:{ counter:-1} project:{ dval:1 morphiumId:1 binaryData:1 longData:1 boolData:1 doubleData:1 floatData:1 counter:1 value:1 intData:1}"));
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