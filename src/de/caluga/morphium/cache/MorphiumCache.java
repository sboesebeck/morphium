package de.caluga.morphium.cache;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.query.Query;

import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 07.03.13
 * Time: 09:57
 * <p/>
 * Cache interface . you can set your own cache implementation to morphium if it implements this interface
 */
public interface MorphiumCache {
    <T> void addToCache(String k, Class<? extends T> type, List<T> ret);

    String getCacheKey(Map<String, Object> qo, Map<String, Integer> sort, String collection, int skip, int limit);

    <T> List<T> getFromCache(Class<? extends T> type, String k);

    Map<Class<?>, Map<String, CacheElement>> getCache();

    Map<Class<?>, Map<Object, Object>> getIdCache();

    void clearCachefor(Class<?> cls);

    void setCache(Map<Class<?>, Map<String, CacheElement>> cache);

    void resetCache();

    void removeEntryFromCache(Class cls, Object id);

    void setIdCache(Map<Class<?>, Map<Object, Object>> c);

    <T> T getFromIDCache(Class<? extends T> type, Object id);

    String getCacheKey(Query q);

    boolean isCached(Class<?> type, String k);

    void clearCacheIfNecessary(Class cls);

    void addCacheListener(CacheListener cl);

    void removeCacheListener(CacheListener cl);

    boolean isListenerRegistered(CacheListener cl);

    void setGlobalCacheTimeout(int tm);

    void setAnnotationAndReflectionHelper(AnnotationAndReflectionHelper hlp);

    void setHouskeepingIntervalPause(int p);

    void setValidCacheTime(Class type, int time);

    void setDefaultCacheTime(Class type);
}
