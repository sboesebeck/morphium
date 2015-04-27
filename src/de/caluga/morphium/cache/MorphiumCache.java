package de.caluga.morphium.cache;

import com.mongodb.DBObject;
import de.caluga.morphium.query.Query;

import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 07.03.13
 * Time: 09:57
 * <p/>
 * TODO: Add documentation here
 */
public interface MorphiumCache {
    public <T> void addToCache(String k, Class<? extends T> type, List<T> ret);

    public String getCacheKey(DBObject qo, Map<String, Integer> sort, String collection, int skip, int limit);

    public <T> List<T> getFromCache(Class<? extends T> type, String k);

    public Map<Class<?>, Map<String, CacheElement>> cloneCache();

    public Map<Class<?>, Map<Object, Object>> cloneIdCache();

    public void clearCachefor(Class<?> cls);

    public void setCache(Map<Class<?>, Map<String, CacheElement>> cache);

    public void resetCache();

    public void removeEntryFromCache(Class cls, Object id);

    public void setIdCache(Map<Class<?>, Map<Object, Object>> c);

    public <T> T getFromIDCache(Class<? extends T> type, Object id);

    public String getCacheKey(Query q);

    public boolean isCached(Class<?> type, String k);

    public void clearCacheIfNecessary(Class cls);

    public void addCacheListener(CacheListener cl);

    public void removeCacheListener(CacheListener cl);

    public boolean isListenerRegistered(CacheListener cl);
}
