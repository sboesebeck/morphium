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
 * TODO: Add documentation here
 */
public class MorphiumCacheJCacheImpl implements MorphiumCache, CacheEntryExpiredListener, CacheEntryCreatedListener, CacheEntryRemovedListener, CacheEntryUpdatedListener {
    public final static String RESULT_CACHE_NAME = "resultCache";
    public final static String ID_CACHE_NAME = "idCache";

    private CacheManager cacheManager;
    private MorphiumConfig cfg;

    private Map<Class, Cache> idCaches = new HashMap<>();
    private Map<Class, Cache> resultCaches = new HashMap<>();

    private AnnotationAndReflectionHelper anHelper = new AnnotationAndReflectionHelper(false);

    private List<CacheListener> cacheListeners = new Vector<>();

    private Logger log = LoggerFactory.getLogger(MorphiumCacheJCacheImpl.class);

    private ScheduledThreadPoolExecutor housekeeping = new ScheduledThreadPoolExecutor(1);
    private HouseKeepingHelper hkHelper = new HouseKeepingHelper();
    private final Runnable hkTask;

    public MorphiumCacheJCacheImpl() {
        cfg = new MorphiumConfig();

        hkHelper.setGlobalValidCacheTime(cfg.getGlobalCacheValidTime());
        hkHelper.setAnnotationHelper(new AnnotationAndReflectionHelper(false));
        hkTask = () -> {
            Iterator<String> it = getCacheManager().getCacheNames().iterator();
            while (it.hasNext()) {
                String k = it.next();
                hkHelper.housekeep(getCacheManager().getCache(k));
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
    public <T> void addToCache(String k, Class<? extends T> type, List<T> ret) {
        Cache idCache = getIdCache(type);
        Cache resultCache = getResultCache(type);
        for (T obj : ret) {
            Object id = anHelper.getId(obj);
            if (!idCache.containsKey(id)) {
                idCache.put(id, new CacheEntry(obj, id));
            }
        }
        resultCache.put(k, new CacheEntry(ret, k));
    }

    @Override
    public Map<String, Integer> getSizes() {
        Map<String, Integer> ret = new HashMap<>();

        Iterator<String> it = getCacheManager().getCacheNames().iterator();
        while (it.hasNext()) {
            String n = it.next();
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
    public <T> List<T> getFromCache(Class<? extends T> type, String k) {
        Cache<Object, CacheEntry<List<T>>> resultCache = getResultCache(type);
        if (resultCache.get(k) != null) {
            return resultCache.get(k).getResult();
        } else {
            return null;
        }
    }

    private <T> Cache<Object, CacheEntry<T>> getIdCache(Class<? extends T> type) {
        if (idCaches.containsKey(type)) {
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
            cache = getCacheManager().createCache(ID_CACHE_NAME + "|" + type.getName(), config);
        } catch (Exception e) {
            //maybe already there
            cache = getCacheManager().getCache(ID_CACHE_NAME + "|" + type.getName());
        }
        idCaches.put(type, cache);
        return cache;
    }

    private <T> Cache<Object, CacheEntry<List<T>>> getResultCache(Class<? extends T> type) {
        if (resultCaches.containsKey(type)) {
            return resultCaches.get(type);
        }
        Cache<Object, CacheEntry<List<T>>> cache = null;
        log.info("Creating new cache for " + type.getName());
        MutableConfiguration config =
                new MutableConfiguration<>()
                        .setTypes(Object.class, Object.class)
                        .setStoreByValue(false)
                        .setExpiryPolicyFactory(EternalExpiryPolicy.factoryOf())
                        .setStatisticsEnabled(false);
        try {
            cache = getCacheManager().createCache(RESULT_CACHE_NAME + "|" + type.getName(), config);
        } catch (Exception e) {
            //maybe already there
            cache = getCacheManager().getCache(RESULT_CACHE_NAME + "|" + type.getName());
        }
        resultCaches.put(type, cache);
        return cache;
    }

    @Override
    public void clearCachefor(Class<?> cls) {
        getIdCache(cls).clear();
        getResultCache(cls).clear();
    }

    @Override
    public void resetCache() {
        getCacheManager().close();
    }

    @Override
    public void removeEntryFromIdCache(Class cls, Object id) {
        getIdCache(cls).remove(id);
    }

    @Override
    public void removeEntryFromCache(Class cls, Object id) {
        Object obj = getIdCache(cls).get(id);
        if (obj != null) {
            getIdCache(cls).remove(id);
        }
        Set<String> toRemove = new HashSet<>();
        Iterator<Cache.Entry<String, CacheEntry>> it = getResultCache(cls).iterator();
        while (it.hasNext()) {
            Cache.Entry<String, CacheEntry> entry = it.next();
            for (Object el : (List) entry.getValue().getResult()) {
                Object lid = anHelper.getId(el);
                if (lid == null) {
                    log.error("Null id in CACHE?");
                    toRemove.add(entry.getKey());
                }
                if (lid != null && lid.equals(id)) {
                    toRemove.add(entry.getKey());
                }
            }
        }
        getResultCache(cls).removeAll(toRemove);
    }

    @Override
    public <T> T getFromIDCache(Class<? extends T> type, Object id) {
        Cache<Object, CacheEntry<T>> c = getIdCache(type);
        if (!c.containsKey(id)) return null;
        return c.get(id).getResult();
    }

    @Override
    public String getCacheKey(Query q) {
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
            iterable.forEach(o -> {
                CacheEntryEvent<Object, CacheObject> evt = (CacheEntryEvent) o;
                cl.wouldAddToCache(evt.getKey(), evt.getValue(), false);
            });

        }
    }

    @Override
    public void onExpired(Iterable iterable) throws CacheEntryListenerException {
        for (CacheListener cl : cacheListeners) {
            iterable.forEach(o -> {
                CacheEntryEvent<Object, CacheObject> evt = (CacheEntryEvent) o;
                cl.wouldRemoveEntryFromCache(evt.getKey(), evt.getValue(), true);
            });

        }
    }

    @Override
    public void onRemoved(Iterable iterable) throws CacheEntryListenerException {
        for (CacheListener cl : cacheListeners) {
            iterable.forEach(o -> {
                CacheEntryEvent<Object, CacheObject> evt = (CacheEntryEvent) o;
                if (evt.getKey() == null) {
                    //clear / removeall
                    try {
                        cl.wouldClearCache(Class.forName(evt.getSource().getCacheManager().getURI().toString()));
                    } catch (ClassNotFoundException e) {
                        //TODO: Implement Handling
                        throw new CacheEntryListenerException("Could not get type", e);
                    }
                } else {
                    cl.wouldRemoveEntryFromCache(evt.getKey(), evt.getValue(), false);
                }
            });

        }
    }

    @Override
    public void onUpdated(Iterable iterable) throws CacheEntryListenerException {
        for (CacheListener cl : cacheListeners) {
            iterable.forEach(o -> {
                CacheEntryEvent<Object, CacheObject> evt = (CacheEntryEvent) o;
                cl.wouldAddToCache(evt.getKey(), evt.getValue(), true);
            });

        }
    }
}
