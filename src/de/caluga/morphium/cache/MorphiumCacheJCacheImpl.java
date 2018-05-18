package de.caluga.morphium.cache;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.Utils;
import de.caluga.morphium.cache.jcache.CacheEntry;
import de.caluga.morphium.cache.jcache.CacheImpl;
import de.caluga.morphium.cache.jcache.CachingProviderImpl;
import de.caluga.morphium.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.event.*;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

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

    private CachingProvider cp;
    private MorphiumConfig cfg;

    private AnnotationAndReflectionHelper anHelper = new AnnotationAndReflectionHelper(false);

    private List<CacheListener> cacheListeners = new Vector<>();

    private Logger log = LoggerFactory.getLogger(MorphiumCacheJCacheImpl.class);

    public MorphiumCacheJCacheImpl() {
        if (System.getProperty(Caching.JAVAX_CACHE_CACHING_PROVIDER) == null) {
            System.setProperty(Caching.JAVAX_CACHE_CACHING_PROVIDER, CachingProviderImpl.class.getName());
        }
        cfg = new MorphiumConfig();
        cp = Caching.getCachingProvider();
    }

    @Override
    public <T> void addToCache(String k, Class<? extends T> type, List<T> ret) {
        CacheManager mgr = getCacheManager(type);
        Cache idCache = mgr.getCache(ID_CACHE_NAME);
        Cache resultCache = mgr.getCache(RESULT_CACHE_NAME);
        List idLst = new ArrayList();
        for (T obj : ret) {
            Object id = anHelper.getId(obj);
            idCache.put(id, new CacheEntry(obj, id));
            idLst.add(id);
        }
        resultCache.put(k, new CacheEntry(idLst, k));
    }

    private <T> CacheManager getCacheManager(Class<? extends T> type) {
        try {
            return cp.getCacheManager(new URI(type.getName()), this.getClass().getClassLoader(), cfg.asProperties());
        } catch (URISyntaxException e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Integer> getSizes() {
        Map<String, Integer> ret = new HashMap<>();
        for (CacheManager cm : ((CachingProviderImpl) cp).getCacheManagers()) {
            CacheImpl<String, List> c = (CacheImpl) cm.getCache(RESULT_CACHE_NAME);
            ret.put(cm.getURI().toString() + ":" + RESULT_CACHE_NAME, c.size());
            c = (CacheImpl) cm.getCache(ID_CACHE_NAME);
            ret.put(cm.getURI().toString() + ":" + ID_CACHE_NAME, c.size());

        }
        return ret;
    }

    @Override
    public String getCacheKey(Class type, Map<String, Object> qo, Map<String, Integer> sort, Map<String, Object> projection, String collection, int skip, int limit) {
        return Utils.getCacheKey(type, qo, sort, projection, collection, skip, limit, anHelper);
    }

    @Override
    public <T> List<T> getFromCache(Class<? extends T> type, String k) {
        CacheManager mgr = getCacheManager(type);
        Cache<Object, CacheEntry<T>> idCache = mgr.getCache(ID_CACHE_NAME);
        Cache<Object, CacheEntry<List<Object>>> resultCache = mgr.getCache(RESULT_CACHE_NAME);
        List<T> result = new ArrayList<>();

        CacheEntry<List<Object>> res = resultCache.get(k);
        if (res != null) {
            for (Object id : res.getResult()) {
                if (!idCache.containsKey(id)) {
                    //not found in id-cache - need to read?
                    return null;
                }
                result.add(idCache.get(id).getResult());
            }
        }
        return result;
    }

    @Override
    public Map<Class<?>, Map<Object, Object>> getIdCache() {
        return null;
    }

    @Override
    public void setIdCache(Map<Class<?>, Map<Object, Object>> c) {

    }

    @Override
    public void clearCachefor(Class<?> cls) {
        CacheManager mgr = getCacheManager(cls);
        mgr.getCache(RESULT_CACHE_NAME).clear();
        mgr.getCache(ID_CACHE_NAME).clear();
    }

    @Override
    public void resetCache() {
        cp.close();
        cp = Caching.getCachingProvider();

    }

    @Override
    public void removeEntryFromCache(Class cls, Object id) {
        CacheManager mgr = getCacheManager(cls);
        //run trhouh all results to find this ID
        //remove those entries
        //remove id from idcache
        //TODO mfg.getCache("id")
    }

    @Override
    public <T> T getFromIDCache(Class<? extends T> type, Object id) {
        CacheManager mgr = getCacheManager(type);
        Cache<Object, CacheEntry<T>> c = mgr.getCache(ID_CACHE_NAME);
        if (!c.containsKey(id)) return null;
        return c.get(id).getResult();
    }

    @Override
    public String getCacheKey(Query q) {
        return getCacheKey(q.getType(), q.toQueryObject(), q.getSort(), q.getFieldListForQuery(), q.getCollectionName(), q.getSkip(), q.getLimit());
    }

    @Override
    public boolean isCached(Class<?> type, String k) {
        return getCacheManager(type).getCache(RESULT_CACHE_NAME).containsKey(k);
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

    }

    @Override
    public void setAnnotationAndReflectionHelper(AnnotationAndReflectionHelper hlp) {
        anHelper = hlp;
    }

    @Override
    public void setHouskeepingIntervalPause(int p) {

    }

    @Override
    public void setValidCacheTime(Class type, int time) {

    }

    @Override
    public void setDefaultCacheTime(Class type) {

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
