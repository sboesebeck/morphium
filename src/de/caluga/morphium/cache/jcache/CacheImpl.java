package de.caluga.morphium.cache.jcache;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Stephan Bösebeck
 * Date: 17.05.18
 * Time: 08:38
 * <p>
 * simple copy before update implemenation of a in-Memory Cache
 */
public class CacheImpl<K, CE> implements Cache<K, CacheEntry<CE>> {
    private Map<K, CacheEntry<CE>> theCache = new ConcurrentHashMap<>();

    private String name = "";

    @Override
    public CacheEntry<CE> get(K key) {
        return theCache.get(key);
    }

    @Override
    public Map<K, CacheEntry<CE>> getAll(Set<? extends K> keys) {
        Map<K, CacheEntry<CE>> ret = new HashMap<>();
        for (K k : keys) {
            ret.put(k, theCache.get(k));
        }
        return ret;
    }

    @Override
    public boolean containsKey(K key) {
        return theCache.containsKey(key);
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        throw new RuntimeException("not implemented yet,sorry");
    }

    @Override
    public void put(K key, CacheEntry<CE> value) {
        theCache.put(key, value);
    }

    @Override
    public CacheEntry<CE> getAndPut(K key, CacheEntry<CE> value) {
        CacheEntry<CE> ret = theCache.get(key);
        theCache.put(key, value);
        return ret;
    }

    @Override
    public void putAll(Map<? extends K, ? extends CacheEntry<CE>> map) {
        theCache.putAll(map);
    }

    @Override
    public boolean putIfAbsent(K key, CacheEntry<CE> value) {
        if (!theCache.containsKey(key)) {
            theCache.put(key, value);
            return true;
        } else {
            return false;
        }

    }

    @Override
    public boolean remove(K key) {
        if (!theCache.containsKey(key)) return false;
        theCache.remove(key);
        return true;
    }

    @Override
    public boolean remove(K key, CacheEntry<CE> oldValue) {
        if (!theCache.containsKey(key) || !theCache.get(key).equals(oldValue)) {
            return false;
        }
        theCache.remove(key, oldValue);
        return true;

    }

    @Override
    public CacheEntry<CE> getAndRemove(K key) {
        CacheEntry<CE> ret = theCache.get(key);
        theCache.remove(key);
        return ret;
    }

    @Override
    public boolean replace(K key, CacheEntry<CE> oldValue, CacheEntry<CE> newValue) {
        if (!theCache.containsKey(key)) return false;
        if (!theCache.get(key).equals(oldValue)) return false;
        theCache.put(key, newValue);
        return true;
    }

    @Override
    public boolean replace(K key, CacheEntry<CE> value) {
        if (!theCache.containsKey(key)) return false;
        theCache.put(key, value);
        return true;
    }

    @Override
    public CacheEntry<CE> getAndReplace(K key, CacheEntry<CE> value) {
        if (theCache.containsKey(key)) {
            CacheEntry<CE> oldValue = theCache.get(key);
            theCache.put(key, value);
            return oldValue;
        } else {
            return null;
        }
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        for (K k : keys) theCache.remove(k);
    }

    @Override
    public void removeAll() {
        theCache.clear();

    }

    @Override
    public void clear() {
        theCache.clear();
    }

    @Override
    public <C extends Configuration<K, CacheEntry<CE>>> C getConfiguration(Class<C> clazz) {
        return null;
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, CacheEntry<CE>, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        return null;
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, CacheEntry<CE>, T> entryProcessor, Object... arguments) {
        return null;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public CacheManager getCacheManager() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return null;
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, CacheEntry<CE>> cacheEntryListenerConfiguration) {

    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, CacheEntry<CE>> cacheEntryListenerConfiguration) {

    }

    @Override
    public Iterator<Entry<K, CacheEntry<CE>>> iterator() {

        return new Iterator<Entry<K, CacheEntry<CE>>>() {
            Iterator<Map.Entry<K, CacheEntry<CE>>> it = theCache.entrySet().iterator();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Entry<K, CacheEntry<CE>> next() {
                return (Entry<K, CacheEntry<CE>>) it.next();
            }
        };
    }

    public int size() {
        return theCache.size();
    }
}
