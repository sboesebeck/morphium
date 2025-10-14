package de.caluga.morphium.cache;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.Utils;
import de.caluga.morphium.cache.jcache.CacheEntry;
import de.caluga.morphium.cache.jcache.CachingProviderImpl;
import de.caluga.morphium.cache.jcache.HouseKeepingHelper;
import de.caluga.morphium.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.*;
import javax.cache.expiry.EternalExpiryPolicy;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * User: Stephan Bösebeck
 * Date: 18.05.18
 * Time: 14:52
 * <p>
 */
public class MorphiumCacheJCacheImpl implements MorphiumCache, CacheEntryExpiredListener<Object, CacheEntry>, CacheEntryCreatedListener<Object, CacheEntry>, CacheEntryRemovedListener<Object, CacheEntry>, CacheEntryUpdatedListener<Object, CacheEntry> {
    public final static String RESULT_CACHE_NAME = "resultCache";
    public final static String ID_CACHE_NAME = "idCache";

    private CacheManager cacheManager;

    private final Map < Class<?>, Cache> idCaches = new HashMap<>();
    private final Map < Class<?>, Cache> resultCaches = new HashMap<>();

    private AnnotationAndReflectionHelper anHelper = new AnnotationAndReflectionHelper(false);

    private final List<CacheListener> cacheListeners = new Vector<>();

    private final Logger log = LoggerFactory.getLogger(MorphiumCacheJCacheImpl.class);

    private final ScheduledThreadPoolExecutor housekeeping = new ScheduledThreadPoolExecutor(1);
    private final HouseKeepingHelper hkHelper = new HouseKeepingHelper();
    private final Runnable hkTask;
    @SuppressWarnings("FieldMayBeFinal")
    private boolean cacheListenerRegistered = false;

    public MorphiumCacheJCacheImpl() {
        MorphiumConfig cfg = new MorphiumConfig();

        hkHelper.setGlobalValidCacheTime(cfg.getGlobalCacheValidTime());
        hkHelper.setAnnotationHelper(new AnnotationAndReflectionHelper(false));
        hkTask = () -> {
            for (String k : getCacheManager().getCacheNames()) {
                hkHelper.housekeep(getCacheManager().getCache(k), cacheListeners);
            }

        };
        housekeeping.scheduleWithFixedDelay(hkTask, 1000, 5000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void setCacheManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
        resultCaches.clear();
        idCaches.clear();
    }

    @Override
    public CacheManager getCacheManager() {
        if (cacheManager == null) {
            try {
                cacheManager = Caching.getCachingProvider().getCacheManager();
            } catch (Exception e) {
                log.info("using default cache Manager - " + e.getMessage());
                cacheManager = new CachingProviderImpl().getCacheManager();
            }
        }
        return cacheManager;
    }

    @Override
    public <T> void addToCache(String k, Class <? extends T> type, List<T> ret) {

        Cache idCache = getIdCache(type);
        Cache resultCache = getResultCache(type);
        @SuppressWarnings("unchecked") CacheEntry v = new CacheEntry(ret, k);
        for (CacheListener cl : cacheListeners) {
            //noinspection unchecked
            v = cl.wouldAddToCache(k, v, false);
            if (v == null) {
                log.warn("Not adding null entry to cache - veto by listener");
            }
        }
        for (T obj : ret) {
            Object id = anHelper.getId(obj);
            //noinspection unchecked
            if (!idCache.containsKey(id)) {
                //noinspection unchecked,unchecked
                idCache.put(id, new CacheEntry(obj, id));
            }
        }
        //noinspection unchecked
        resultCache.put(k, v);
    }

    @Override
    public Map<String, Integer> getSizes() {
        Map<String, Integer> ret = new HashMap<>();

        for (String n : getCacheManager().getCacheNames()) {
            Cache c = getCacheManager().getCache(n);
            try {
                //GetSize works with MorphiumCache and EHCache
                Method m = c.getClass().getMethod("getSize");
                ret.put(n, (Integer) m.invoke(c));
            } catch (Exception e) {
                Iterator iterator = c.iterator();
                int size = 0;
                while (iterator.hasNext()) {
                    iterator.next();
                    size++;
                }

                ret.put(n, size);
            }
        }
        return ret;
    }

    @Override
    public String getCacheKey(Class type, Map<String, Object> qo, Map<String, Integer> sort, Map<String, Object> projection, String collection, int skip, int limit) {
        return Utils.getCacheKey(type, qo, sort, projection, collection, skip, limit, anHelper);
    }

    @Override
    public <T> List<T> getFromCache(Class <? extends T > type, String k) {
        Cache<Object, CacheEntry<List<T>>> resultCache = getResultCache(type);
        synchronized (this) {
            if (resultCache.get(k) != null) {
                return resultCache.get(k).getResult();
            } else {
                return null;
            }
        }
    }

    @Override
    public Set<Class<?>> getCachedTypes() {
        return idCaches.keySet();
    }

    @SuppressWarnings("CommentedOutCode")
    private <T> Cache<Object, CacheEntry<T>> getIdCache(Class <? extends T> type) {
        if (idCaches.containsKey(type)) {
            //noinspection unchecked
            return (Cache<Object, CacheEntry<T>>) idCaches.get(type);
        }
        Cache<Object, CacheEntry<T>> cache;
        log.info("Creating new cache for " + type.getName());
        MutableConfiguration config =
                        new MutableConfiguration<>()
        .setTypes(Object.class, Object.class)
        .setExpiryPolicyFactory(EternalExpiryPolicy.factoryOf())
        .setStoreByValue(false)
        .setStatisticsEnabled(false);

        try {
            //noinspection unchecked
            cache = getCacheManager().createCache(ID_CACHE_NAME + "|" + type.getName(), config);
        } catch (Exception e) {
            //maybe already there
            cache = getCacheManager().getCache(ID_CACHE_NAME + "|" + type.getName());
        }

//        if (!cacheListenerRegistered) {
//            try {
//                cache.registerCacheEntryListener(new MutableCacheEntryListenerConfiguration<Object, CacheEntry<T>>(
//                        FactoryBuilder.factoryOf(getClass().getName()), null, false, false));
//            } catch (Exception e) {
//            }
//            cacheListenerRegistered=true;
//        }
        idCaches.put(type, cache);
        return cache;
    }

    @SuppressWarnings("CommentedOutCode")
    private <T> Cache<Object, CacheEntry<List<T>>> getResultCache(Class <? extends T> type) {
        if (resultCaches.containsKey(type)) {
            //noinspection unchecked
            return resultCaches.get(type);
        }
        Cache<Object, CacheEntry<List<T>>> cache;
        log.info("Creating new cache for " + type.getName());
        MutableConfiguration config =
                        new MutableConfiguration<>()
        .setTypes(Object.class, Object.class)
        .setStoreByValue(false)
        .setExpiryPolicyFactory(EternalExpiryPolicy.factoryOf())
        .setStatisticsEnabled(false);
        try {
            //noinspection unchecked
            cache = getCacheManager().createCache(RESULT_CACHE_NAME + "|" + type.getName(), config);
        } catch (Exception e) {
            //maybe already there
            cache = getCacheManager().getCache(RESULT_CACHE_NAME + "|" + type.getName());
        }
//        if (!cacheListenerRegistered) {
//            try {
//                cache.registerCacheEntryListener(new MutableCacheEntryListenerConfiguration<Object, CacheEntry<List<T>>>(
//                        FactoryBuilder.factoryOf(getClass().getName()), null, false, false));
//            } catch (Exception e) {
//
//            }
//            cacheListenerRegistered=true;
//        }
        resultCaches.put(type, cache);
        return cache;
    }

    @Override
    public void clearCachefor(Class<?> cls) {
        for (CacheListener cl : cacheListeners) {
            if (!(cl.wouldClearCache(cls))) {
                log.warn("Veto from listener for clearing cache for " + cls.getName());
                return;
            }
        }
        getIdCache(cls).clear();
        getResultCache(cls).clear();
    }

    @Override
    public void resetCache() {
        getCacheManager().close();
    }

    @Override
    public void close() {
        resetCache();
    }

    @Override
    public void removeEntryFromIdCache(Class cls, Object id) {
        for (CacheListener cl : cacheListeners) {
            //noinspection unchecked,unchecked
            if (!(cl.wouldRemoveEntryFromCache(id, (CacheEntry) getIdCache(cls).get(id), false))) {
                log.info("Veto from listener for ID " + id);
            }
        }
        //noinspection unchecked
        getIdCache(cls).remove(id);
    }

    @Override
    public void removeEntryFromCache(Class cls, Object id) {
        @SuppressWarnings("unchecked") Object obj = getIdCache(cls).get(id);
        if (obj != null) {
            //noinspection unchecked
            getIdCache(cls).remove(id);
        }
        Set<String> toRemove = new HashSet<>();
        //noinspection unchecked
        for (Cache.Entry<String, CacheEntry> entry : (Iterable<Cache.Entry<String, CacheEntry >> ) getResultCache(cls)) {
            for (Object el : (List) entry.getValue().getResult()) {
                Object lid = anHelper.getId(el);
                for (CacheListener cl : cacheListeners) {
                    //noinspection unchecked
                    if (!(cl.wouldRemoveEntryFromCache(entry.getKey(), entry.getValue(), false))) {
                        log.warn("Veto from listener for ID " + id);
                    }
                }
                if (lid == null) {
                    log.error("Null id in CACHE?");
                    toRemove.add(entry.getKey());
                }
                if (lid != null && lid.equals(id)) {
                    toRemove.add(entry.getKey());
                }
            }
        }

        //noinspection unchecked
        getResultCache(cls).removeAll(toRemove);

    }

    @Override
    public <T> T getFromIDCache(Class <? extends T > type, Object id) {
        Cache<Object, CacheEntry<T>> c = getIdCache(type);
        if (!c.containsKey(id)) return null;
        return c.get(id).getResult();
    }

    @Override
    public String getCacheKey(Query q) {
        //noinspection unchecked,unchecked,unchecked
        return getCacheKey(q.getType(), q.toQueryObject(), q.getSort(), q.getFieldListForQuery(), q.getCollectionName(), q.getSkip(), q.getLimit());
    }

    @Override
    public boolean isCached(Class<?> type, String k) {
        return getResultCache(type).containsKey(k);
    }

    @Override
    public void clearCacheIfNecessary(Class cls) {
        de.caluga.morphium.annotations.caching.Cache c = anHelper.getAnnotationFromHierarchy(cls, de.caluga.morphium.annotations.caching.Cache.class); //cls.getAnnotation(Cache.class);
        if (c != null) {
            if (c.clearOnWrite()) {
                for (CacheListener cl : cacheListeners) {
                    //noinspection unchecked
                    if (!(cl.wouldClearCache(cls))) {
                        log.warn("Veto from listener for clearing cache for " + cls.getName());
                    }
                }
                clearCachefor(cls);
            }
        }
    }

    @Override
    public void addCacheListener(CacheListener cl) {
        cacheListeners.add(cl);
    }

    @Override
    public void removeCacheListener(CacheListener cl) {
        cacheListeners.remove(cl);
    }

    @Override
    public boolean isListenerRegistered(CacheListener cl) {
        return cacheListeners.contains(cl);
    }

    @Override
    public void setGlobalCacheTimeout(int tm) {
        hkHelper.setGlobalValidCacheTime(tm);
    }

    @Override
    public void setAnnotationAndReflectionHelper(AnnotationAndReflectionHelper hlp) {
        anHelper = hlp;
    }

    @Override
    public void setHouskeepingIntervalPause(int p) {
        housekeeping.remove(hkTask);
        housekeeping.scheduleAtFixedRate(hkTask, p, p, TimeUnit.MILLISECONDS);
    }

    @Override
    public void setValidCacheTime(Class type, int time) {
        hkHelper.setValidCacheTime(type, time);
    }

    @Override
    public void setDefaultCacheTime(Class type) {
        hkHelper.setDefaultValidCacheTime(type);
    }


    @Override
    public void onCreated(Iterable iterable) throws CacheEntryListenerException {
        for (CacheListener cl : cacheListeners) {
            //noinspection unchecked
            iterable.forEach(o -> {
                @SuppressWarnings("unchecked") CacheEntryEvent<Object, CacheEntry> evt = (CacheEntryEvent) o;
                //noinspection unchecked
                cl.wouldAddToCache(evt.getKey(), evt.getValue(), false);
            });

        }
    }

    @Override
    public void onExpired(Iterable iterable) throws CacheEntryListenerException {
        for (CacheListener cl : cacheListeners) {
            //noinspection unchecked
            iterable.forEach(o -> {
                @SuppressWarnings("unchecked") CacheEntryEvent<Object, CacheEntry> evt = (CacheEntryEvent) o;
                //noinspection unchecked
                cl.wouldRemoveEntryFromCache(evt.getKey(), evt.getValue(), true);
            });

        }
    }

    @Override
    public void onRemoved(Iterable iterable) throws CacheEntryListenerException {
        for (CacheListener cl : cacheListeners) {
            //noinspection unchecked
            iterable.forEach(o -> {
                @SuppressWarnings("unchecked") CacheEntryEvent<Object, CacheEntry> evt = (CacheEntryEvent) o;
                if (evt.getKey() == null) {
                    //clear / removeall
                    try {
                        cl.wouldClearCache(Class.forName(evt.getSource().getCacheManager().getURI().toString()));
                    } catch (ClassNotFoundException e) {
                        //TODO: Implement Handling
                        throw new CacheEntryListenerException("Could not get type", e);
                    }
                } else {
                    //noinspection unchecked
                    cl.wouldRemoveEntryFromCache(evt.getKey(), evt.getValue(), false);
                }
            });

        }
    }

    @Override
    public void onUpdated(Iterable iterable) throws CacheEntryListenerException {
        for (CacheListener cl : cacheListeners) {
            //noinspection unchecked
            iterable.forEach(o -> {
                @SuppressWarnings("unchecked") CacheEntryEvent<Object, CacheEntry> evt = (CacheEntryEvent) o;
                //noinspection unchecked
                cl.wouldAddToCache(evt.getKey(), evt.getValue(), true);
            });

        }
    }

}
