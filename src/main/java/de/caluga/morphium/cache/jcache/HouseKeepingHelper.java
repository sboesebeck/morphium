/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.cache.jcache;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.annotations.caching.Cache.ClearStrategy;
import de.caluga.morphium.cache.CacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("UnusedDeclaration")
public class HouseKeepingHelper {

    @SuppressWarnings({"CanBeFinal", "FieldMayBeFinal"})
    private Map<Class<?>, Integer> validTimeForClass = new ConcurrentHashMap<>();
    private int gcTimeout = 5000;
    @SuppressWarnings({"CanBeFinal", "FieldMayBeFinal"})
    private Logger log = LoggerFactory.getLogger(HouseKeepingHelper.class);
    private AnnotationAndReflectionHelper annotationHelper;

    @SuppressWarnings("unchecked")
    public HouseKeepingHelper() {
    }

    public void setGlobalValidCacheTime(int gc) {
        gcTimeout = gc;
    }

    public void setAnnotationHelper(AnnotationAndReflectionHelper hlp) {
        annotationHelper = hlp;
    }

    public void setValidCacheTime(Class<?> cls, int timeout) {
        validTimeForClass.put(cls, timeout);
    }

    public void setDefaultValidCacheTime(Class cls) {
        validTimeForClass.remove(cls);
    }

    public Integer getValidCacheTime(Class<?> cls) {
        return validTimeForClass.get(cls);
    }


    @SuppressWarnings("CommentedOutCode")
    public void housekeep(javax.cache.Cache cache, List<CacheListener> cacheListeners) {
        try {
            Class clz = AnnotationAndReflectionHelper.classForName(cache.getName().substring(cache.getName().indexOf("|") + 1));
            List<Object> toDelete = new ArrayList<>();
            int maxEntries = -1;
            Cache cacheSettings = getAnnotationHelper().getAnnotationFromHierarchy(clz, Cache.class);//clz.getAnnotation(Cache.class);
            ClearStrategy strategy = null;
            int time = gcTimeout;
            HashMap<Long, List<Object>> lruTime = new HashMap<>();
            HashMap<Long, List<Object>> fifoTime = new HashMap<>();

            if (validTimeForClass.get(clz) != null) {
                time = validTimeForClass.get(clz);
            }

            int del = 0;

            if (cacheSettings != null) {
                if (cacheSettings.timeout() != -1) {
                    time = cacheSettings.timeout();
                }
                maxEntries = cacheSettings.maxEntries();
                strategy = cacheSettings.strategy();
                validTimeForClass.putIfAbsent(clz, time);
            }
            @SuppressWarnings("unchecked") Iterator<javax.cache.Cache.Entry<String, CacheEntry>> it = cache.iterator();
            int size = 0;
            while (it.hasNext()) {
                size++;
                javax.cache.Cache.Entry<String, CacheEntry> es = it.next();
                CacheEntry ch = es.getValue();


                if (ch == null || ch.getResult() == null || System.currentTimeMillis() - ch.getCreated() > time) {
                    toDelete.add(Objects.requireNonNull(ch).getKey());
                    del++;
                } else {
                    lruTime.putIfAbsent(ch.getLru(), new ArrayList<>());
                    lruTime.get(ch.getLru()).add(ch.getKey());
                    long fifo = System.currentTimeMillis() - ch.getCreated();
                    fifoTime.putIfAbsent(fifo, new ArrayList<>());
                    fifoTime.get(fifo).add(ch.getKey());
                }
            }
            //                    cache.put(clz, ch);

            if (maxEntries > 0 && size - del > maxEntries) {
                Long[] array;
                int idx;
                switch (strategy) {
                    case LRU:
                        array = lruTime.keySet().toArray(new Long[0]);
                        Arrays.sort(array);
                        idx = 0;
                        while (size - del > maxEntries) {
                            if (lruTime.get(array[idx]) != null && !lruTime.get(array[idx]).isEmpty()) {
                                toDelete.add(lruTime.get(array[idx]).get(0));
                                lruTime.get(array[idx]).remove(0);
                                del++;
                                if (lruTime.get(array[idx]).isEmpty()) {
                                    idx++;
                                }

                            }
                        }
                        break;
                    case FIFO:
                        array = fifoTime.keySet().toArray(new Long[0]);
                        Arrays.sort(array);
                        idx = 0;
                        while (size - del > maxEntries) {
                            if (fifoTime.get(array[array.length - 1 - idx]) != null && !fifoTime.get(array[array.length - 1 - idx]).isEmpty()) {
                                toDelete.add(fifoTime.get(array[array.length - 1 - idx]).get(0));
                                fifoTime.get(array[array.length - 1 - idx]).remove(0);
                                del++;
                                if (fifoTime.get(array[array.length - 1 - idx]).isEmpty()) {
                                    idx++;
                                }
                            }
                        }
                        break;
                    case RANDOM:
                        array = fifoTime.keySet().toArray(new Long[0]);
                        List<Long> lst = Arrays.asList(array);
                        Collections.shuffle(lst);
                        array = lst.toArray(new Long[0]);
                        idx = 0;
                        while (size - del > maxEntries) {
                            if (lruTime.get(array[idx]) != null && !lruTime.get(array[idx]).isEmpty()) {
                                toDelete.add(lruTime.get(array[idx]).get(0));
                                del++;
                                if (lruTime.get(array[idx]).isEmpty()) {
                                    idx++;
                                }
                            }
                        }
                        break;
                }


            }
            //                morphium.getCache().setCache(cache);
            //                morphium.getCache().setIdCache(idCacheClone);
            for (Object k : toDelete) {
                for (CacheListener cl : cacheListeners) {
                    try {
                        @SuppressWarnings("unchecked")
                        CacheEntry<?> entry = (CacheEntry<?>) cache.get(k);
                        cl.wouldRemoveEntryFromCache(k, entry, true);
                    } catch (Exception e) {
                        //ignore errors...
                    }
                }
                @SuppressWarnings("unchecked")
                javax.cache.Cache<Object, Object> typedCache = (javax.cache.Cache<Object, Object>) cache;
                typedCache.remove(k);
            }


        } catch (Throwable e) {
            log.warn("Error:" + e.getMessage(), e);
        }
    }

    private AnnotationAndReflectionHelper getAnnotationHelper() {
        if (annotationHelper == null) {
            annotationHelper = new AnnotationAndReflectionHelper(false); //only used for annotations, name conversions do not happen
        }
        return annotationHelper;
    }

}
