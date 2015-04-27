/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.cache;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.annotations.caching.Cache.ClearStrategy;
import de.caluga.morphium.annotations.caching.NoCache;

import java.util.*;

@SuppressWarnings("UnusedDeclaration")
public class CacheHousekeeper extends Thread {

    private static final String MONGODBLAYER_CACHE = "mongodblayer.cache";
    private int timeout;
    private Map<Class<?>, Integer> validTimeForClass;
    private int gcTimeout;
    private boolean running = true;
    private Logger log = new Logger(CacheHousekeeper.class);
    private Morphium morphium;
    private AnnotationAndReflectionHelper annotationHelper;

    @SuppressWarnings("unchecked")
    public CacheHousekeeper(Morphium m, int houseKeepingTimeout, int globalCacheTimout) {
        this.timeout = houseKeepingTimeout;
        gcTimeout = globalCacheTimout;
        morphium = m;
        annotationHelper = m.getARHelper();
        validTimeForClass = new HashMap<>();
        setDaemon(true);

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

    public void setValidCacheTime(Class<?> cls, int timeout) {
        HashMap<Class<?>, Integer> v = new HashMap<>(validTimeForClass);
        v.put(cls, timeout);
        validTimeForClass = v;
    }

    public Integer getValidCacheTime(Class<?> cls) {
        return validTimeForClass.get(cls);
    }

    public void end() {
        running = false;

    }

    private Map cloneMap(Map source) {
        return cloneMap(source, new HashMap());
    }

    private Map cloneMap(Map source, Map dest) {
        for (Object k : source.keySet()) {
            dest.put(k, source.get(k));
        }
        return dest;
    }

    @SuppressWarnings({"unchecked", "ConstantConditions"})
    public void run() {
        while (running) {
            try {
                Map<Class, List<String>> toDelete = new HashMap<Class, List<String>>();
                Map<Class<?>, Map<String, CacheElement>> cache = morphium.getCache().cloneCache();
                for (Map.Entry<Class<?>, Map<String, CacheElement>> es : cache.entrySet()) {
                    Class<?> clz = es.getKey();
                    Map<String, CacheElement> ch = (Map<String, CacheElement>) cloneMap(es.getValue());


                    int maxEntries = -1;
                    Cache cacheSettings = annotationHelper.getAnnotationFromHierarchy(clz, Cache.class);//clz.getAnnotation(Cache.class);
                    NoCache noCache = annotationHelper.getAnnotationFromHierarchy(clz, NoCache.class);// clz.getAnnotation(NoCache.class);
                    int time = gcTimeout;
                    HashMap<Long, List<String>> lruTime = new HashMap<>();
                    HashMap<Long, List<String>> fifoTime = new HashMap<>();
                    ClearStrategy strategy = null;
                    if (noCache == null) {
                        if (cacheSettings != null) {
                            time = cacheSettings.timeout();
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
//                                        new Logger("MongoDbLayer").warn("Timout could not be parsed for class " + clz.getName());
//                                    }
//                                    try {
//                                        maxEntries = Integer.parseInt(setting.getMapValue().get(clz.getName() + "_max_entries"));
//                                    } catch (Exception e1) {
//                                        new Logger("MongoDbLayer").warn("Max Entries could not be parsed for class " + clz.getName() + " Using " + maxEntries);
//                                        setting.getMapValue().put(clz.getName() + "_max_entries", "" + maxEntries);
//                                        morphium.getConfig().getConfigManager().storeSetting(setting);
//                                    }
//                                    try {
//                                        strategy = ClearStrategy.valueOf(setting.getMapValue().get(clz.getName() + "_clear_strategy"));
//                                    } catch (Exception e2) {
//                                        new Logger("MongoDbLayer").warn("STrategycould not be parsed for class " + clz.getName() + " Using " + strategy.name());
//                                        setting.getMapValue().put(clz.getName() + "_clear_strategy", strategy.name());
//                                        morphium.getConfig().getConfigManager().storeSetting(setting);
//                                    }
//                                }
//                            }
                            if (validTimeForClass.get(clz) == null) {
                                validTimeForClass.put(clz, time);
                            }
                        }
                    }
                    if (validTimeForClass.get(clz) != null) {
                        time = validTimeForClass.get(clz);
                    }

                    int del = 0;
                    for (Map.Entry<String, CacheElement> est : ch.entrySet()) {
                        String k = est.getKey();
                        CacheElement e = est.getValue(); //ch.get(k);

                        if (e == null || e.getFound() == null || System.currentTimeMillis() - e.getCreated() > time) {
                            if (toDelete.get(clz) == null) {
                                toDelete.put(clz, new ArrayList<String>());
                            }
                            toDelete.get(clz).add(k);
                            del++;
                        } else {
                            if (lruTime.get(e.getLru()) == null) {
                                lruTime.put(e.getLru(), new ArrayList<String>());
                            }
                            lruTime.get(e.getLru()).add(k);
                            long fifo = System.currentTimeMillis() - e.getCreated();
                            if (fifoTime.get(fifo) == null) {
                                fifoTime.put(fifo, new ArrayList<String>());
                            }
                            fifoTime.get(fifo).add(k);
                        }
                    }
                    cache.put(clz, ch);
                    if (maxEntries > 0 && cache.get(clz).size() - del > maxEntries) {
                        Long[] array;
                        int idx;
                        switch (strategy) {
                            case LRU:
                                array = lruTime.keySet().toArray(new Long[lruTime.keySet().size()]);
                                Arrays.sort(array);
                                idx = 0;
                                while (cache.get(clz).size() - del > maxEntries) {
                                    if (lruTime.get(array[idx]) != null && lruTime.get(array[idx]).size() != 0) {
                                        if (toDelete.get(clz) == null) {
                                            toDelete.put(clz, new ArrayList<String>());
                                        }
                                        toDelete.get(clz).add(lruTime.get(array[idx]).get(0));
                                        lruTime.get(array[idx]).remove(0);
                                        del++;
                                        if (lruTime.get(array[idx]).size() == 0) {
                                            idx++;
                                        }

                                    }
                                }
                                break;
                            case FIFO:
                                array = fifoTime.keySet().toArray(new Long[fifoTime.keySet().size()]);
                                Arrays.sort(array);
                                idx = 0;
                                while (cache.get(clz).size() - del > maxEntries) {
                                    if (fifoTime.get(array[array.length - 1 - idx]) != null && fifoTime.get(array[array.length - 1 - idx]).size() != 0) {
                                        if (toDelete.get(clz) == null) {
                                            toDelete.put(clz, new ArrayList<String>());
                                        }
                                        toDelete.get(clz).add(fifoTime.get(array[array.length - 1 - idx]).get(0));
                                        fifoTime.get(array[array.length - 1 - idx]).remove(0);
                                        del++;
                                        if (fifoTime.get(array[array.length - 1 - idx]).size() == 0) {
                                            idx++;
                                        }
                                    }
                                }
                                break;
                            case RANDOM:
                                array = fifoTime.keySet().toArray(new Long[fifoTime.keySet().size()]);
                                List<Long> lst = Arrays.asList(array);
                                Collections.shuffle(lst);
                                array = lst.toArray(new Long[lst.size()]);
                                idx = 0;
                                while (cache.get(clz).size() - del > maxEntries) {
                                    if (lruTime.get(array[idx]) != null && lruTime.get(array[idx]).size() != 0) {
                                        if (toDelete.get(clz) == null) {
                                            toDelete.put(clz, new ArrayList<String>());
                                        }
                                        toDelete.get(clz).add(lruTime.get(array[idx]).get(0));
                                        del++;
                                        if (lruTime.get(array[idx]).size() == 0) {
                                            idx++;
                                        }
                                    }
                                }
                                break;
                        }

                    }

                }

                Map<Class<?>, Map<Object, Object>> idCacheClone = morphium.getCache().cloneIdCache();
                for (Map.Entry<Class, List<String>> et : toDelete.entrySet()) {
                    Class cls = et.getKey();

                    boolean inIdCache = idCacheClone.get(cls) != null;

                    for (String k : et.getValue()) {
                        if (k.endsWith("idlist")) continue;
                        if (inIdCache) {
                            //remove objects from id cache
                            for (Object f : cache.get(cls).get(k).getFound()) {
                                idCacheClone.get(cls).remove(morphium.getId(f));
                            }
                        }
                        cache.get(cls).remove(k);
                    }
                }
                morphium.getCache().setCache(cache);
                morphium.getCache().setIdCache(idCacheClone);


            } catch (Throwable e) {
                log.warn("Error:" + e.getMessage(), e);
            }
            try {
                sleep(timeout);
            } catch (InterruptedException e) {
                log.info("Ignoring InterruptedException");
            }
        }

    }
}
