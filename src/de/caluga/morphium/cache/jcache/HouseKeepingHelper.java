/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.cache.jcache;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.annotations.caching.Cache.ClearStrategy;
import de.caluga.morphium.cache.CacheObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("UnusedDeclaration")
public class HouseKeepingHelper {

    private int housekeepingIntervalPause = 500;
    private Map<Class<?>, Integer> validTimeForClass = new ConcurrentHashMap<>();
    ;
    private int gcTimeout = 5000;
    private Logger log = LoggerFactory.getLogger(HouseKeepingHelper.class);
    private AnnotationAndReflectionHelper annotationHelper;

    @SuppressWarnings("unchecked")
    public HouseKeepingHelper() {
    }

    public void setGlobalValidCacheTime(int gc) {
        gcTimeout = gc;
    }

    public void setHouskeepingPause(int p) {
        housekeepingIntervalPause = p;
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


    public void housekeep(CacheImpl cache, Class clz) {
        try {

            Map<Class, List<String>> toDelete = new HashMap<>();
            int maxEntries = -1;
            Cache cacheSettings = getAnnotationHelper().getAnnotationFromHierarchy(clz, Cache.class);//clz.getAnnotation(Cache.class);
            ClearStrategy strategy = null;
            int time = gcTimeout;
            HashMap<Long, List<String>> lruTime = new HashMap<>();
            HashMap<Long, List<String>> fifoTime = new HashMap<>();

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
            Iterator<javax.cache.Cache.Entry<String, CacheObject>> it = cache.iterator();
            while (it.hasNext()) {
                javax.cache.Cache.Entry<String, CacheObject> es = it.next();
                CacheObject ch = es.getValue();


                if (ch == null || ch.getResult() == null || System.currentTimeMillis() - ch.getCreated() > time) {
                    toDelete.putIfAbsent(clz, new ArrayList<>());
                    toDelete.get(clz).add(ch.getKey());
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
            if (maxEntries > 0 && cache.size() - del > maxEntries) {
                Long[] array;
                int idx;
                switch (strategy) {
                    case LRU:
                        array = lruTime.keySet().toArray(new Long[0]);
                        Arrays.sort(array);
                        idx = 0;
                        while (cache.size() - del > maxEntries) {
                            if (lruTime.get(array[idx]) != null && !lruTime.get(array[idx]).isEmpty()) {
                                toDelete.putIfAbsent(clz, new ArrayList<>());
                                toDelete.get(clz).add(lruTime.get(array[idx]).get(0));
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
                        while (cache.size() - del > maxEntries) {
                            if (fifoTime.get(array[array.length - 1 - idx]) != null && !fifoTime.get(array[array.length - 1 - idx]).isEmpty()) {
                                toDelete.putIfAbsent(clz, new ArrayList<>());
                                toDelete.get(clz).add(fifoTime.get(array[array.length - 1 - idx]).get(0));
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
                        while (cache.size() - del > maxEntries) {
                            if (lruTime.get(array[idx]) != null && !lruTime.get(array[idx]).isEmpty()) {
                                toDelete.putIfAbsent(clz, new ArrayList<>());
                                toDelete.get(clz).add(lruTime.get(array[idx]).get(0));
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
