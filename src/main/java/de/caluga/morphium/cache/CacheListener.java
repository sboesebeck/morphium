package de.caluga.morphium.cache;

import de.caluga.morphium.cache.jcache.CacheEntry;

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
     * @return false, if not to cache
     */
    <T> CacheEntry<T> wouldAddToCache(Object k, CacheEntry<T> toCache, boolean updated);

    <T> boolean wouldClearCache(Class<T> affectedEntityType);

    <T> boolean wouldRemoveEntryFromCache(Object key, CacheEntry<T> toRemove, boolean expired);

}
