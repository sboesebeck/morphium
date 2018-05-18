package de.caluga.morphium.cache.jcache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.event.*;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.*;
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
    private Map<CacheEntryEventFilter<? super K, ? super CacheEntry<CE>>, CacheEntryListener<K, CacheEntry<CE>>> listeners = new ConcurrentHashMap<>();
    private CacheManager cacheManager;
    private String name = "";
    private Logger log = LoggerFactory.getLogger(CacheImpl.class);

    public void setCacheManager(CacheManager cm) {
        cacheManager = cacheManager;
    }

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
        if (!fireEvent(new CEvent(this, EventType.CREATED, key, value, null))) {
            log.info("Not adding element to cache " + key.toString());

        } else {
            theCache.put(key, value);
        }
    }

    @Override
    public CacheEntry<CE> getAndPut(K key, CacheEntry<CE> value) {
        CacheEntry<CE> ret = theCache.get(key);

        EventType evtType = EventType.UPDATED;
        if (ret == null) {
            evtType = EventType.CREATED;
        }
        if (!fireEvent(new CEvent(this, evtType, key, value, ret))) {
            log.info("not updateing element " + key);
            return ret;
        }
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
            fireEvent(new CEvent(this, EventType.CREATED, key, value, null));
            return true;
        } else {
            return false;
        }

    }

    @Override
    public boolean remove(K key) {
        if (!theCache.containsKey(key)) return false;
        if (!fireEvent(new CEvent(this, EventType.REMOVED, key, null, null))) {
            log.info("aborting cache operation");
            return false;
        }
        ;
        theCache.remove(key);
        return true;
    }

    @Override
    public boolean remove(K key, CacheEntry<CE> oldValue) {
        if (!theCache.containsKey(key) || !theCache.get(key).equals(oldValue)) {
            return false;
        }
        if (!fireEvent(new CEvent(this, EventType.REMOVED, key, oldValue, null))) {
            log.info("aborting cache operation");
            return false;
        }
        theCache.remove(key, oldValue);
        return true;

    }

    @Override
    public CacheEntry<CE> getAndRemove(K key) {
        CacheEntry<CE> ret = theCache.get(key);
        if (!fireEvent(new CEvent(this, EventType.REMOVED, key, null, null))) {
            log.info("aborting cache operation");
            return null;
        }
        theCache.remove(key);
        return ret;
    }

    @Override
    public boolean replace(K key, CacheEntry<CE> oldValue, CacheEntry<CE> newValue) {
        if (!theCache.containsKey(key)) return false;
        if (!theCache.get(key).equals(oldValue)) return false;
        if (!fireEvent(new CEvent(this, EventType.UPDATED, key, newValue, oldValue))) {
            log.info("aborting cache operation");
            return false;
        }
        theCache.put(key, newValue);
        return true;
    }

    @Override
    public boolean replace(K key, CacheEntry<CE> value) {
        if (!theCache.containsKey(key)) return false;
        if (!fireEvent(new CEvent(this, EventType.UPDATED, key, value, null))) {
            log.info("aborting cache operation");
            return false;
        }
        theCache.put(key, value);
        return true;
    }

    @Override
    public CacheEntry<CE> getAndReplace(K key, CacheEntry<CE> value) {
        if (theCache.containsKey(key)) {
            CacheEntry<CE> oldValue = theCache.get(key);

            if (!fireEvent(new CEvent(this, EventType.UPDATED, key, value, oldValue))) {
                log.info("aborting cache operation");
                return oldValue;
            }
            theCache.put(key, value);
            return oldValue;
        } else {
            return null;
        }
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        for (K k : keys) {
            if (!fireEvent(new CEvent(this, EventType.REMOVED, k, null, null))) {
                log.info("aborting cache operation");
                continue;
            }
            theCache.remove(k);
        }
    }

    @Override
    public void removeAll() {
        if (!fireEvent(new CEvent(this, EventType.REMOVED, null, null, null))) {
            log.info("aborting cache clear operation");
            return;
        }
        theCache.clear();

    }

    @Override
    public void clear() {
        if (!fireEvent(new CEvent(this, EventType.REMOVED, null, null, null))) {
            log.info("aborting cache clear operation");
            return;
        }
        theCache.clear();
    }

    @Override
    public <C extends Configuration<K, CacheEntry<CE>>> C getConfiguration(Class<C> clazz) {
        return null;
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, CacheEntry<CE>, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        throw new RuntimeException("not implemented yet, sorry");
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, CacheEntry<CE>, T> entryProcessor, Object... arguments) {
        throw new RuntimeException("not implemented yet, sorry");
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
        return cacheManager;
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
        return clazz.cast(this);
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, CacheEntry<CE>> cacheEntryListenerConfiguration) {
        CacheEntryEventFilter<? super K, ? super CacheEntry<CE>> ef = cacheEntryListenerConfiguration.getCacheEntryEventFilterFactory().create();
        listeners.put(ef, (CacheEntryListener<K, CacheEntry<CE>>) cacheEntryListenerConfiguration.getCacheEntryListenerFactory().create());
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, CacheEntry<CE>> cacheEntryListenerConfiguration) {
        CacheEntryEventFilter<? super K, ? super CacheEntry<CE>> ef = cacheEntryListenerConfiguration.getCacheEntryEventFilterFactory().create();
        listeners.remove(ef, cacheEntryListenerConfiguration.getCacheEntryListenerFactory().create());
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
                final Map.Entry<K, CacheEntry<CE>> entry = it.next();
                Entry<K, CacheEntry<CE>> ret = new Entry<K, CacheEntry<CE>>() {
                    @Override
                    public K getKey() {
                        return entry.getKey();
                    }

                    @Override
                    public CacheEntry<CE> getValue() {
                        return entry.getValue();
                    }

                    @Override
                    public <T> T unwrap(Class<T> clazz) {
                        return clazz.cast(this);
                    }
                };
                return ret;
            }
        };
    }

    public int size() {
        return theCache.size();
    }

    public void expire(Object k) {
        theCache.remove(k);
    }


    public class CEvent extends CacheEntryEvent<K, CacheEntry<CE>> {
        private CacheEntry<CE> value;
        private CacheEntry<CE> oldValue;
        private K key;

        public CEvent(Cache source, EventType t, K k, CacheEntry<CE> v, CacheEntry<CE> ov) {
            super(source, t);
            value = v;
            oldValue = ov;
            key = k;
        }

        @Override
        public CacheEntry<CE> getValue() {
            return value;
        }

        @Override
        public CacheEntry<CE> getOldValue() {
            return oldValue;
        }

        @Override
        public boolean isOldValueAvailable() {
            return oldValue != null;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public <T> T unwrap(Class<T> clazz) {
            return clazz.cast(this);
        }
    }

    private boolean fireEvent(CacheEntryEvent<K, CacheEntry<CE>> evt) {
        EventType type = evt.getEventType();
        boolean ret = true;
        for (CacheEntryEventFilter<? super K, ? super CacheEntry<CE>> k : listeners.keySet()) {
            try {
                if (k.evaluate(evt)) {
                    switch (type) {
                        case CREATED:
                            if (listeners.get(k) instanceof CacheEntryCreatedListener) {
                                CacheEntryCreatedListener<K, CacheEntry<CE>> l = (CacheEntryCreatedListener<K, CacheEntry<CE>>) listeners.get(k);
                                final ArrayList<CacheEntryEvent<? extends K, ? extends CacheEntry<CE>>> lst = new ArrayList<>();
                                lst.add(evt);
                                l.onCreated(lst::listIterator);
                            }
                            break;
                        case EXPIRED:
                            if (listeners.get(k) instanceof CacheEntryExpiredListener) {
                                CacheEntryExpiredListener<K, CacheEntry<CE>> l = (CacheEntryExpiredListener<K, CacheEntry<CE>>) listeners.get(k);
                                final ArrayList<CacheEntryEvent<? extends K, ? extends CacheEntry<CE>>> lst = new ArrayList<>();
                                lst.add(evt);
                                l.onExpired(lst::listIterator);
                            }
                            break;
                        case REMOVED:
                            if (listeners.get(k) instanceof CacheEntryRemovedListener) {
                                CacheEntryRemovedListener<K, CacheEntry<CE>> l = (CacheEntryRemovedListener<K, CacheEntry<CE>>) listeners.get(k);
                                final ArrayList<CacheEntryEvent<? extends K, ? extends CacheEntry<CE>>> lst = new ArrayList<>();
                                lst.add(evt);
                                l.onRemoved(lst::listIterator);
                            }
                            break;
                        case UPDATED:
                            if (listeners.get(k) instanceof CacheEntryUpdatedListener) {
                                CacheEntryUpdatedListener<K, CacheEntry<CE>> l = (CacheEntryUpdatedListener<K, CacheEntry<CE>>) listeners.get(k);
                                final ArrayList<CacheEntryEvent<? extends K, ? extends CacheEntry<CE>>> lst = new ArrayList<>();
                                lst.add(evt);
                                l.onUpdated(lst::listIterator);
                            }
                            break;
                    }
                }
            } catch (CacheEventVetoException cev) {
                ret = false;
            }
        }
        return ret;
    }
}

