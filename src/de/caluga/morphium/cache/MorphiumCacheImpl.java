package de.caluga.morphium.cache;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.query.Query;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 07.03.13
 * Time: 10:56
 * <p/>
 * TODO: Add documentation here
 */
public class MorphiumCacheImpl implements MorphiumCache {
    private Hashtable<Class<?>, Hashtable<String, CacheElement>> cache;
    private Hashtable<Class<?>, Hashtable<ObjectId, Object>> idCache;
    private AnnotationAndReflectionHelper annotationHelper = new AnnotationAndReflectionHelper();

    private Logger logger = Logger.getLogger(MorphiumCacheImpl.class);

    public MorphiumCacheImpl() {
        cache = new Hashtable<Class<?>, Hashtable<String, CacheElement>>();
        idCache = new Hashtable<Class<?>, Hashtable<ObjectId, Object>>();
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
    public <T> void addToCache(String k, Class<?> type, List<T> ret) {
        if (k == null) {
            return;
        }
        if (!k.endsWith("idlist")) {
            //copy from idCache
            Hashtable<Class<?>, Hashtable<ObjectId, Object>> idCacheClone = cloneIdCache();
            for (T record : ret) {
                if (idCacheClone.get(type) == null) {
                    idCacheClone.put(type, new Hashtable<ObjectId, Object>());
                }
                idCacheClone.get(type).put(annotationHelper.getId(record), record);
            }
            setIdCache(idCacheClone);
        }

        CacheElement<T> e = new CacheElement<T>(ret);
        e.setLru(System.currentTimeMillis());
        Hashtable<Class<?>, Hashtable<String, CacheElement>> cl = (Hashtable<Class<?>, Hashtable<String, CacheElement>>) cache.clone();
        if (cl.get(type) == null) {
            cl.put(type, new Hashtable<String, CacheElement>());
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
        return cache.get(type) != null && cache.get(type).get(k) != null && cache.get(type).get(k).getFound() != null;
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
        if (cache.get(type) == null || cache.get(type).get(k) == null) return null;
        try {
            final CacheElement cacheElement = cache.get(type).get(k);
            cacheElement.setLru(System.currentTimeMillis());
            return cacheElement.getFound();
        } catch (Exception e) {
            //can happen, when cache is cleared in thw wron moment
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Hashtable<Class<?>, Hashtable<String, CacheElement>> cloneCache() {
        return (Hashtable<Class<?>, Hashtable<String, CacheElement>>) cache.clone();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Hashtable<Class<?>, Hashtable<ObjectId, Object>> cloneIdCache() {
        return (Hashtable<Class<?>, Hashtable<ObjectId, Object>>) idCache.clone();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getFromIDCache(Class<? extends T> type, ObjectId id) {
        if (idCache.get(type) != null) {
            return (T) idCache.get(type).get(id);
        }
        return null;
    }


    @SuppressWarnings("StringBufferMayBeStringBuilder")
    @Override
    public String getCacheKey(DBObject qo, Map<String, Integer> sort, String collection, int skip, int limit) {
        StringBuffer b = new StringBuffer();
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
        setCache(new Hashtable<Class<?>, Hashtable<String, CacheElement>>());
    }

    @Override
    public void setCache(Hashtable<Class<?>, Hashtable<String, CacheElement>> cache) {
        this.cache = cache;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void removeEntryFromCache(Class cls, ObjectId id) {
        Hashtable<Class<?>, Hashtable<String, CacheElement>> c = cloneCache();
        Hashtable<Class<?>, Hashtable<ObjectId, Object>> idc = cloneIdCache();
        idc.get(cls).remove(id);

        ArrayList<String> toRemove = new ArrayList<String>();
        for (String key : c.get(cls).keySet()) {

            for (Object el : c.get(cls).get(key).getFound()) {
                ObjectId lid = annotationHelper.getId(el);
                if (lid == null) {
                    logger.error("Null id in CACHE?");
                    toRemove.add(key);
                }
                if (lid != null && lid.equals(id)) {
                    toRemove.add(key);
                }
            }
        }
        for (String k : toRemove) {
            c.get(cls).remove(k);
        }
        setCache(c);
        setIdCache(idc);
    }

    @Override
    public void setIdCache(Hashtable<Class<?>, Hashtable<ObjectId, Object>> c) {
        idCache = c;
    }


}
