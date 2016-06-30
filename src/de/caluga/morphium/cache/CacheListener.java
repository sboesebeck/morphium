package de.caluga.morphium.cache;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 15.04.14
 * Time: 09:55
 * To change this template use File | Settings | File Templates.
 */
public interface CacheListener {
    /**
     * ability to alter cached entries or avoid caching overall
     *
     * @param toCache - datastructure containing cache key and result
     * @param <T>     - the type
     * @return null, if not to cache
     */
    <T> CacheObject<T> wouldAddToCache(CacheObject<T> toCache);

    <T> boolean wouldClearCache(Class<T> affectedEntityType);

    <T> boolean wouldRemoveEntryFromCache(Class cls, Object id, Object entity);

    enum Operation {
        @SuppressWarnings("unused")delete, @SuppressWarnings("unused")store, @SuppressWarnings("unused")update,
    }
}
