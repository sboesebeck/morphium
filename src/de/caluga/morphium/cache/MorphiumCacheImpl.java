package de.caluga.morphium.cache;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Logger;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.query.Query;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * User: Stephan Bösebeck
 * Date: 07.03.13
 * Time: 10:56
 * <p/>
 * TODO: Add documentation here
 */
public class MorphiumCacheImpl implements MorphiumCache {
    private Map<Class<?>, Map<String, CacheElement>> cache;
    private Map<Class<?>, Map<Object, Object>> idCache;
    private AnnotationAndReflectionHelper annotationHelper = new AnnotationAndReflectionHelper(false); //only used to get id's and annotations, camalcase conversion never happens

    private List<CacheListener> cacheListeners;

    private Logger logger = new Logger(MorphiumCacheImpl.class);

    public MorphiumCacheImpl() {
        cache = new HashMap<Class<?>, Map<String, CacheElement>>();
        idCache = new HashMap<>();
        cacheListeners = new CopyOnWriteArrayList<CacheListener>();
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
     * @param k    - Key, usually the mongodb query string - should be created by createCacheKey
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
        CacheObject<T> co = new CacheObject<T>();
        co.setKey(k);
        co.setType(type);
        co.setResult(ret);
        for (CacheListener cl : cacheListeners) {
            co = cl.wouldAddToCache(co);
            if (co == null) {
                return;
            }
        }
        if (!k.endsWith("idlist")) {
            //copy from idCache
            Map<Class<?>, Map<Object, Object>> idCacheClone = cloneIdCache();
            for (T record : ret) {
                if (idCacheClone.get(type) == null) {
                    idCacheClone.put(type, new Hashtable<Object, Object>());
                }
                idCacheClone.get(type).put(annotationHelper.getId(record), record);
            }
            setIdCache(idCacheClone);
        }

        CacheElement<T> e = new CacheElement<T>(ret);
        e.setLru(System.currentTimeMillis());
        Map<Class<?>, Map<String, CacheElement>> cl = (Map<Class<?>, Map<String, CacheElement>>) new HashMap<>(cache);
        if (cl.get(type) == null) {
            cl.put(type, new HashMap<String, CacheElement>());
        }
        cl.get(type).put(k, e);

        //atomar execution of this operand - no synchronization needed
        cache = cl;

    }




    @Override
    public void clearCacheIfNecessary(Class cls) {
        Cache c = annotationHelper.getAnnotationFromHierarchy(cls, Cache.class); //cls.getAnnotation(Cache.class);
        if (c != null) {
            if (c.clearOnWrite()) {
                clearCachefor(cls);
            }
        }
    }

    @Override
    public boolean isCached(Class<?> type, String k) {
        Cache c = annotationHelper.getAnnotationFromHierarchy(type, Cache.class); ///type.getAnnotation(Cache.class);
        if (c != null) {
            if (!c.readCache()) return false;
        } else {
            return false;
        }
        Map<Class<?>, Map<String, CacheElement>> snapshotCache = cache;

        try {
            return snapshotCache.get(type) != null && snapshotCache.get(type).get(k) != null && snapshotCache.get(type).get(k).getFound() != null;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * return object by from cache. Cache key usually is the string-representation of the search
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
        Map<Class<?>, Map<String, CacheElement>> snapshotCache = cache;
        if (snapshotCache.get(type) == null || snapshotCache.get(type).get(k) == null) return null;
        try {
            final CacheElement cacheElement = snapshotCache.get(type).get(k);
            cacheElement.setLru(System.currentTimeMillis());
            return cacheElement.getFound();
        } catch (Exception e) {
            //can happen, when cache is cleared in thw wron moment
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<Class<?>, Map<String, CacheElement>> cloneCache() {
        return (Map<Class<?>, Map<String, CacheElement>>) new HashMap<>(cache);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<Class<?>, Map<Object, Object>> cloneIdCache() {
        return (Map<Class<?>, Map<Object, Object>>) new HashMap<>(idCache);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getFromIDCache(Class<? extends T> type, Object id) {
        if (idCache.get(type) != null) {
            return (T) idCache.get(type).get(id);
        }
        return null;
    }


    @SuppressWarnings("StringBufferMayBeStringBuilder")
    @Override
    public String getCacheKey(DBObject qo, Map<String, Integer> sort, String collection, int skip, int limit) {
        StringBuilder b = new StringBuilder();
        b.append(qo.toString());
        b.append(" c:" + collection);
        b.append(" l:");
        b.append(limit);
        b.append(" s:");
        b.append(skip);
        if (sort != null) {
            b.append(" sort:");
            b.append(new BasicDBObject(sort).toString());
        }
        return b.toString();
    }

    /**
     * create unique cache key for queries, also honoring skip & limit and sorting
     *
     * @param q the query
     * @return the resulting cache key
     */
    @Override
    public String getCacheKey(Query q) {
        return getCacheKey(q.toQueryObject(), q.getSort(), q.getCollectionName(), q.getSkip(), q.getLimit());
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
        //clearCacheFor(cls);
    }

    @Override
    public void resetCache() {
        setCache(new HashMap<Class<?>, Map<String, CacheElement>>());
    }

    @Override
    public void setCache(Map<Class<?>, Map<String, CacheElement>> cache) {
        this.cache = cache;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void removeEntryFromCache(Class cls, Object id) {
        Map<Class<?>, Map<String, CacheElement>> c = cloneCache();
        Map<Class<?>, Map<Object, Object>> idc = cloneIdCache();
        if (idc.get(cls) != null && idc.get(cls).get(id) != null) {
            for (CacheListener cl : cacheListeners) {
                if (!cl.wouldRemoveEntryFromCache(cls, id, idc.get(cls).get(id))) {
                    logger.info("Not removing from cache due to veto from CacheListener " + cl.getClass().getName());
                    return;
                }
            }
        }
        idc.get(cls).remove(id);

        ArrayList<String> toRemove = new ArrayList<String>();
        for (String key : c.get(cls).keySet()) {

            if (c.get(cls).get(key) != null) {
                for (Object el : c.get(cls).get(key).getFound()) {
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
            c.get(cls).remove(k);
        }
        setCache(c);
        setIdCache(idc);
    }

    @Override
    public void setIdCache(Map<Class<?>, Map<Object, Object>> c) {
        idCache = c;
    }


}
