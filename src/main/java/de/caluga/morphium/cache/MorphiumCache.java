package de.caluga.morphium.cache;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.query.Query;

import javax.cache.CacheManager;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * User: Stephan Bösebeck
 * Date: 07.03.13
 * Time: 09:57
 * <p>
 * Cache interface . you can set your own cache implementation to morphium if it implements this interface
 */
public interface MorphiumCache {
    <T> void addToCache(String k, Class<? extends T> type, List<T> ret);

    String getCacheKey(Class type, Map<String, Object> qo, Map<String, Integer> sort, Map<String, Object> project, String collection, int skip, int limit);

    <T> List<T> getFromCache(Class<? extends T> type, String k);

    void clearCachefor(Class<?> cls);

    void resetCache();

    void close();

    void removeEntryFromIdCache(Class cls, Object id);

    void removeEntryFromCache(Class cls, Object id);

    <T> T getFromIDCache(Class<? extends T> type, Object id);

    Set<Class<?>> getCachedTypes();

    String getCacheKey(Query q);

    boolean isCached(Class<?> type, String k);

    void clearCacheIfNecessary(Class cls);

    void addCacheListener(CacheListener cl);

    void removeCacheListener(CacheListener cl);

    boolean isListenerRegistered(CacheListener cl);

    void setGlobalCacheTimeout(int tm);

    void setAnnotationAndReflectionHelper(AnnotationAndReflectionHelper hlp);

    void setHouskeepingIntervalPause(int p);

    /**
     * override the settings given in @Cache annotation with this value
     * this is useful to change cache behaviour during runtime
     *
     * @param type
     * @param time
     */
    void setValidCacheTime(Class type, int time);

    /**
     * reset cache time settings to default, if settings were cahnged using setValidCacheTime
     * @param type
     */
    void setDefaultCacheTime(Class type);

    Map<String, Integer> getSizes();

    void setCacheManager(CacheManager cacheManager);

    CacheManager getCacheManager();

}
