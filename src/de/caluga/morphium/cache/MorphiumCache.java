package de.caluga.morphium.cache;

import com.mongodb.DBObject;
import de.caluga.morphium.query.Query;

import java.util.Hashtable;
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
    public <T> void addToCache(String k, Class<?> type, List<T> ret);

    public String getCacheKey(DBObject qo, Map<String, Integer> sort, String collection, int skip, int limit);

    public <T> List<T> getFromCache(Class<? extends T> type, String k);

    public Hashtable<Class<?>, Hashtable<String, CacheElement>> cloneCache();

    public Hashtable<Class<?>, Hashtable<Object, Object>> cloneIdCache();

    public void clearCachefor(Class<?> cls);

    public void setCache(Hashtable<Class<?>, Hashtable<String, CacheElement>> cache);

    public void resetCache();

    public void removeEntryFromCache(Class cls, Object id);

    public void setIdCache(Hashtable<Class<?>, Hashtable<Object, Object>> c);

    public <T> T getFromIDCache(Class<? extends T> type, Object id);

    String getCacheKey(Query q);

    boolean isCached(Class<?> type, String k);

    void clearCacheIfNecessary(Class cls);
}
