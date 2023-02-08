package de.caluga.morphium.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.cache.CacheManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Utils;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.cache.jcache.CacheEntry;
import de.caluga.morphium.query.Query;

/**
 * User: Stephan Bösebeck
 * Date: 07.03.13
 * Time: 10:56
 * <p/>
 * The Cache implementation for morphium.
 */
public class MorphiumCacheImpl implements MorphiumCache {
    private final List<CacheListener> cacheListeners;
    private final Logger logger = LoggerFactory.getLogger(MorphiumCacheImpl.class);
    private final CacheHousekeeper cacheHousekeeper;
    private Map<Class<?>, Map<String, CacheEntry>> cache;
    private final Map<Class<?>, Map<Object, Object>> idCache;
    private AnnotationAndReflectionHelper annotationHelper = new AnnotationAndReflectionHelper(false); // only used to
    // get id's and
    // annotations,
    // camalcase
    // conversion
    // never happens

    public MorphiumCacheImpl() {
        cache = new ConcurrentHashMap<>();
        idCache = new ConcurrentHashMap<>();
        cacheListeners = Collections.synchronizedList(new ArrayList<>());
        cacheHousekeeper = new CacheHousekeeper(this);
        cacheHousekeeper.start();
    }

    @Override
    public void setGlobalCacheTimeout(int tm) {
        cacheHousekeeper.setGlobalValidCacheTime(tm);
    }

    @Override
    public void setHouskeepingIntervalPause(int p) {
        cacheHousekeeper.setHouskeepingPause(p);
    }

    @Override
    public void setAnnotationAndReflectionHelper(AnnotationAndReflectionHelper hlp) {
        annotationHelper = hlp;
        cacheHousekeeper.setAnnotationHelper(hlp);
    }

    @Override
    public void addCacheListener(CacheListener cl) {
        cacheListeners.add(cl);
    }

    @Override
    public boolean isListenerRegistered(CacheListener cl) {
        return cacheListeners.contains(cl);
    }

    @Override
    public void removeCacheListener(CacheListener cl) {
        cacheListeners.remove(cl);
    }

    /**
     * adds some list of objects to the cache manually...
     * is being used internally, and should be used with care
     *
     * @param k    - Key, usually the mongodb query string - should be created by
     *             createCacheKey
     * @param type - class type
     * @param ret  - list of results
     * @param <T>  - Type of record
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> void addToCache(String k, Class<? extends T> type, List<T> ret) {
        if (k == null) {
            return;
        }

        CacheEntry<T> co = new CacheEntry(ret, k);
        co.setKey(k);
        co.setType(type);

        for (CacheListener cl : cacheListeners) {
            co = cl.wouldAddToCache(k, co, getFromCache(type, k) != null);

            if (co == null) {
                return;
            }
        }

        if (!k.endsWith("idlist") && !k.contains("project:")) {
            // copy from idCache
            Map<Class<?>, Map<Object, Object>> idCacheClone = idCache; // getIdCache();

            for (T record : ret) {
                idCacheClone.putIfAbsent(type, new ConcurrentHashMap<>());
                idCacheClone.get(type).put(annotationHelper.getId(record), record);
            }

            // setIdCache(idCacheClone);
        }

        CacheEntry<T> e = new CacheEntry(ret, k);
        e.setLru(System.currentTimeMillis());
        // Map<Class<?>, Map<String, CacheEntry>> cl = (Map<Class<?>, Map<String,
        // CacheEntry>>) (((HashMap) cache).clone());
        cache.putIfAbsent(type, new ConcurrentHashMap<>());
        cache.get(type).put(k, e);
        // atomar execution of this operand - no synchronization needed
        // cache = cl;
    }

    @Override
    public void clearCacheIfNecessary(Class cls) {
        Cache c = annotationHelper.getAnnotationFromHierarchy(cls, Cache.class); // cls.getAnnotation(Cache.class);

        if (c != null) {
            if (c.clearOnWrite()) {
                clearCachefor(cls);
            }
        }
    }

    @SuppressWarnings("CommentedOutCode")
    @Override
    public boolean isCached(Class<?> type, String k) {
        // Cache c = annotationHelper.getAnnotationFromHierarchy(type, Cache.class);
        // ///type.getAnnotation(Cache.class);
        // if (c != null) {
        // if (!c.readCache()) return false;
        // } else {
        // return false;
        // }
        Map<Class<?>, Map<String, CacheEntry>> snapshotCache = cache;

        try {
            return snapshotCache.get(type) != null && snapshotCache.get(type).get(k) != null
                && snapshotCache.get(type).get(k).getResult() != null;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * return object by from cache. Cache key usually is the string-representation
     * of the search
     * query.toQueryObject()
     *
     * @param type - type
     * @param k    - cache key
     * @param <T>  - type param
     * @return resulting list
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> getFromCache(Class<? extends T> type, String k) {
        Map<Class<?>, Map<String, CacheEntry>> snapshotCache = cache;

        if (snapshotCache.get(type) == null || snapshotCache.get(type).get(k) == null) {
            return null;
        }

        try {
            final CacheEntry CacheEntry = snapshotCache.get(type).get(k);
            CacheEntry.setLru(System.currentTimeMillis());
            return (List<T>) CacheEntry.getResult();
        } catch (Exception e) {
            // can happen, when cache is cleared in thw wron moment
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    public Map<Class<?>, Map<String, CacheEntry>> getCache() {
        return cache; // (Map<Class<?>, Map<String, CacheEntry>>) (((ConcurrentHashMap)
        // cache).clone());
    }

    public void setCache(Map<Class<?>, Map<String, CacheEntry>> cache) {
        this.cache = cache;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getFromIDCache(Class<? extends T> type, Object id) {
        if (idCache.get(type) != null && id!=null) {
            return (T) idCache.get(type).get(id);
        }

        return null;
    }

    @Override
    public void setValidCacheTime(Class type, int time) {
        cacheHousekeeper.setValidCacheTime(type, time);
    }

    @Override
    public void setDefaultCacheTime(Class type) {
        cacheHousekeeper.setDefaultValidCacheTime(type);
    }

    @SuppressWarnings("StringBufferMayBeStringBuilder")
    @Override
    public String getCacheKey(Class type, Map<String, Object> qo, Map<String, Integer> sort,
        Map<String, Object> projection, String collection, int skip, int limit) {
        return Utils.getCacheKey(type, qo, sort, projection, collection, skip, limit, annotationHelper);
    }

    /**
     * create unique cache key for queries, also honoring skip & limit and sorting
     *
     * @param q the query
     * @return the resulting cache key
     */
    @Override
    public String getCacheKey(Query q) {
        // noinspection unchecked,unchecked
        return getCacheKey(q.getType(), q.toQueryObject(), q.getSort(), q.getFieldListForQuery(), q.getCollectionName(),
                q.getSkip(), q.getLimit());
    }

    @Override
    public void clearCachefor(Class<?> cls) {
        for (CacheListener cl : cacheListeners) {
            if (!cl.wouldClearCache(cls)) {
                logger.info("Not clearing cache due to veto of cache listener " + cl.getClass().getName());
                return;
            }
        }

        if (cache.get(cls) != null) {
            cache.get(cls).clear();
        }

        if (idCache.get(cls) != null) {
            idCache.get(cls).clear();
        }

        // clearCacheFor(cls);
    }

    @Override
    public Set<Class<?>> getCachedTypes() {
        return idCache.keySet();
    }

    @Override
    public void resetCache() {
        setCache(new ConcurrentHashMap<>());
    }

    @Override
    public void close() {
        cacheHousekeeper.end();
        resetCache();
    }

    @Override
    public void removeEntryFromIdCache(Class cls, Object id) {
        if (idCache.get(cls) != null) {
            idCache.get(cls).remove(id);
        }
    }

    @SuppressWarnings({ "unchecked", "CommentedOutCode" })
    @Override
    public void removeEntryFromCache(Class cls, Object id) {
        // Map<Class<?>, Map<String, CacheEntry>> c = getCache();
        // Map<Class<?>, Map<Object, Object>> idc = getIdCache();
        if (idCache.get(cls) != null && idCache.get(cls).get(id) != null) {
            for (CacheListener cl : cacheListeners) {
                CacheEntry e = new CacheEntry(idCache.get(cls).get(id), id);

                if (!cl.wouldRemoveEntryFromCache(id, e, false)) {
                    logger.info("Not removing from cache due to veto from CacheListener " + cl.getClass().getName());
                    return;
                }
            }
        }

        idCache.get(cls).remove(id);
        ArrayList<String> toRemove = new ArrayList<>();

        for (String key : cache.get(cls).keySet()) {
            if (cache.get(cls).get(key) != null) {
                for (Object el : (List) cache.get(cls).get(key).getResult()) {
                    Object lid = annotationHelper.getId(el);

                    if (lid == null) {
                        logger.error("Null id in CACHE?");
                        toRemove.add(key);
                    }

                    if (lid != null && lid.equals(id)) {
                        toRemove.add(key);
                    }
                }
            } else {
                logger.error("Null element in CACHE?");
            }
        }

        for (String k : toRemove) {
            cache.get(cls).remove(k);
        }

        // setCache(c);
        // setIdCache(idc);
    }

    @Override
    public Map<String, Integer> getSizes() {
        Map<String, Integer> ret = new HashMap<>();

        for (Class type : cache.keySet()) {
            idCache.putIfAbsent(type, new ConcurrentHashMap<>());
            cache.putIfAbsent(type, new ConcurrentHashMap<>());
            ret.put("resultCache|" + type.getName(), cache.get(type).size());
            ret.put("idCache|" + type.getName(), idCache.get(type).size());
        }

        return ret;
    }

    @Override
    public void setCacheManager(CacheManager cacheManager) {
    }

    @Override
    public CacheManager getCacheManager() {
        return null;
    }
}
