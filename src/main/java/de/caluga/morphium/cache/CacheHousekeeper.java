/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.cache;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.ShutdownListener;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.annotations.caching.Cache.ClearStrategy;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.cache.jcache.CacheEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings({"UnusedDeclaration", "BusyWait"})
public class CacheHousekeeper extends Thread implements ShutdownListener {

    private int housekeepingIntervalPause = 500;
    @SuppressWarnings({"CanBeFinal", "FieldMayBeFinal"})
    private Map<Class<?>, Integer> validTimeForClass;
    private int gcTimeout = 5000;
    private boolean running = true;
    @SuppressWarnings({"CanBeFinal", "FieldMayBeFinal"})
    private Logger log = LoggerFactory.getLogger(CacheHousekeeper.class);
    private AnnotationAndReflectionHelper annotationHelper;
    @SuppressWarnings({"CanBeFinal", "FieldMayBeFinal"})
    private MorphiumCache morphiumCache;

    @SuppressWarnings({"unchecked", "CommentedOutCode"})
    public CacheHousekeeper(MorphiumCache m) {
        validTimeForClass = new ConcurrentHashMap<>();
        setDaemon(true);
        morphiumCache = m;

        //Last use Configuration manager to read out cache configurations from Mongo!
        //        Map<String, String> l = m.getConfig().getConfigManager().getMapSetting(MONGODBLAYER_CACHE);
        //        if (l != null) {
        //            for (String k : l.keySet()) {
        //                String v = l.get(k);
        //                if (k.endsWith("_max_entries")) {
        //                    continue;
        //                }
        //                if (k.endsWith("_clear_strategy")) {
        //                    continue;
        //                }
        //                try {
        //                    Class<? extends Object> clz = Class.forName(k);
        //                    Integer tm = Integer.parseInt(v);
        //                    validTimeForClass.put(clz, tm);
        //                } catch (Exception e) {
        //                    log.error("Error", e);
        //                }
        //            }
        //        } else {
        //            ConfigElement e = new ConfigElement();
        //            e.setMapValue(new Hashtable<String, String>());
        //            e.setName(MONGODBLAYER_CACHE);
        ////			morphium.getConfig().getConfigManager().addSetting("mongodblayer.cache", new Hashtable<String,String>());
        //            morphium.getConfig().getConfigManager().storeSetting(e);
        //        }
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

    public void end() {
        running = false;

    }

    @SuppressWarnings({"unchecked", "ConstantConditions", "CommentedOutCode"})
    @Override
    public void run() {
        while (running) {
            if (annotationHelper == null) {
                //noinspection EmptyCatchBlock
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                continue;
            }
            try {
                Map<Class, List<String>> toDelete = new HashMap<>();
                Map<Class<?>, Map<String, CacheEntry>> cache = ((MorphiumCacheImpl) morphiumCache).getCache();
                for (Map.Entry<Class<?>, Map<String, CacheEntry>> es : cache.entrySet()) {
                    Class<?> clz = es.getKey();
                    Map<String, CacheEntry> ch = es.getValue();


                    int maxEntries = -1;
                    Cache cacheSettings = annotationHelper.getAnnotationFromHierarchy(clz, Cache.class);//clz.getAnnotation(Cache.class);
                    NoCache noCache = annotationHelper.getAnnotationFromHierarchy(clz, NoCache.class);// clz.getAnnotation(NoCache.class);
                    int time = gcTimeout;
                    HashMap<Long, List<String>> lruTime = new HashMap<>();
                    HashMap<Long, List<String>> fifoTime = new HashMap<>();
                    ClearStrategy strategy = null;
                    if (noCache == null) {
                        if (cacheSettings != null) {
                            if (cacheSettings.timeout() != -1) {
                                time = cacheSettings.timeout();
                            }
                            maxEntries = cacheSettings.maxEntries();
                            strategy = cacheSettings.strategy();

                            //                            if (cacheSettings.overridable()) {
                            //                                ConfigElement setting = morphium.getConfig().getConfigManager().getConfigElement(MONGODBLAYER_CACHE);
                            //                                Map<String, String> map = setting.getMapValue();
                            //                                String v = null;
                            //                                if (map != null) {
                            //                                    v = map.get(clz.getName());
                            //                                }
                            //                                if (v == null) {
                            //                                    if (map == null) {
                            //                                        map = new Hashtable<String, String>();
                            //                                        setting.setMapValue(map);
                            //                                    }
                            //                                    setting.getMapValue().put(clz.getName(), "" + time);
                            //                                    setting.getMapValue().put(clz.getName() + "_max_entries", maxEntries + "");
                            //                                    setting.getMapValue().put(clz.getName() + "_clear_strategy", strategy.name());
                            //                                    morphium.getConfig().getConfigManager().storeSetting(setting);
                            //                                } else {
                            //                                    try {
                            //                                        time = Integer.parseInt(setting.getMapValue().get(clz.getName()));
                            //
                            //                                    } catch (Exception e1) {
                            //                                        LoggerFactory.getLogger("MongoDbLayer").warn("Timout could not be parsed for class " + clz.getName());
                            //                                    }
                            //                                    try {
                            //                                        maxEntries = Integer.parseInt(setting.getMapValue().get(clz.getName() + "_max_entries"));
                            //                                    } catch (Exception e1) {
                            //                                        LoggerFactory.getLogger("MongoDbLayer").warn("Max Entries could not be parsed for class " + clz.getName() + " Using " + maxEntries);
                            //                                        setting.getMapValue().put(clz.getName() + "_max_entries", "" + maxEntries);
                            //                                        morphium.getConfig().getConfigManager().storeSetting(setting);
                            //                                    }
                            //                                    try {
                            //                                        strategy = ClearStrategy.valueOf(setting.getMapValue().get(clz.getName() + "_clear_strategy"));
                            //                                    } catch (Exception e2) {
                            //                                        LoggerFactory.getLogger("MongoDbLayer").warn("STrategycould not be parsed for class " + clz.getName() + " Using " + strategy.name());
                            //                                        setting.getMapValue().put(clz.getName() + "_clear_strategy", strategy.name());
                            //                                        morphium.getConfig().getConfigManager().storeSetting(setting);
                            //                                    }
                            //                                }
                            //                            }
                            validTimeForClass.putIfAbsent(clz, time);
                        }
                    }
                    if (validTimeForClass.get(clz) != null) {
                        time = validTimeForClass.get(clz);
                    }

                    int del = 0;
                    for (Map.Entry<String, CacheEntry> est : ch.entrySet()) {
                        String k = est.getKey();
                        CacheEntry e = est.getValue(); //ch.get(k);

                        if (e == null || e.getResult() == null || System.currentTimeMillis() - e.getCreated() > time) {
                            toDelete.putIfAbsent(clz, new ArrayList<>());
                            toDelete.get(clz).add(k);
                            del++;
                        } else {
                            lruTime.putIfAbsent(e.getLru(), new ArrayList<>());
                            lruTime.get(e.getLru()).add(k);
                            long fifo = System.currentTimeMillis() - e.getCreated();
                            fifoTime.putIfAbsent(fifo, new ArrayList<>());
                            fifoTime.get(fifo).add(k);
                        }
                    }
                    //                    cache.put(clz, ch);
                    if (maxEntries > 0 && cache.get(clz).size() - del > maxEntries) {
                        Long[] array;
                        int idx;
                        switch (strategy) {
                            case LRU:
                                array = lruTime.keySet().toArray(new Long[0]);
                                Arrays.sort(array);
                                idx = 0;
                                while (cache.get(clz).size() - del > maxEntries) {
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
                                while (cache.get(clz).size() - del > maxEntries) {
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
                                while (cache.get(clz).size() - del > maxEntries) {
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

                }

                //                Map<Class<?>, Map<Object, Object>> idCacheClone = morphium.getCache().getIdCache();
                for (Map.Entry<Class, List<String>> et : toDelete.entrySet()) {
                    Class cls = et.getKey();


                    for (String k : et.getValue()) {
                        if (k.endsWith("idlist")) {
                            continue;
                        }
                            //remove objects from id cache
                            for (Object f : (List) cache.get(cls).get(k).getResult()) {
                                morphiumCache.removeEntryFromIdCache(cls, annotationHelper.getId(f));
                            }
                        cache.get(cls).remove(k);
                    }
                }
                //                morphium.getCache().setCache(cache);
                //                morphium.getCache().setIdCache(idCacheClone);


            } catch (Throwable e) {
                log.warn("Error:" + e.getMessage(), e);
            }
            try {
                sleep(housekeepingIntervalPause);
            } catch (InterruptedException e) {
                log.info("Ignoring InterruptedException");
            }
        }

    }

    @Override
    public void onShutdown(Morphium m) {
        end();
    }
}
