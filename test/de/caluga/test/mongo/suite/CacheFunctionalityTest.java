package de.caluga.test.mongo.suite;/**
 * Created by stephan on 02.03.16.
 */

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.StatisticKeys;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.cache.MorphiumCache;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO: Add Documentation here
 **/
public class CacheFunctionalityTest extends MongoTest {

    @Test
    public void accessTest() throws Exception {
        int amount = 10000;
        createCachedObjects(amount);
        Thread.sleep(500);
        for (int i = 0; i < amount; i++) {
            CachedObject o = morphium.createQueryFor(CachedObject.class).f("counter").eq(i + 1).get();
            assert (o != null) : "Not found: " + i;
            if (i % 1000 == 0) {
                log.info("Read " + i);
                log.info("Cached: " + morphium.getStatistics().get(StatisticKeys.CACHE_ENTRIES.name()));
            }
        }

        //cache warming finished...
        log.info("cache warmed... starting...");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            CachedObject o = morphium.createQueryFor(CachedObject.class).f("counter").eq((int) (Math.random() * amount) + 1).get();
        }
        long dur = System.currentTimeMillis() - start;

        log.info("Duration: " + dur + "ms");

        log.info("Cache hit ratio: " + morphium.getStatistics().get(StatisticKeys.CHITSPERC.name()));
        log.info("Cache hits     : " + morphium.getStatistics().get(StatisticKeys.CHITS.name()));
        log.info("Cache miss     : " + morphium.getStatistics().get(StatisticKeys.CMISS.name()));

        assert (morphium.getStatistics().get(StatisticKeys.CHITS.name()) >= 90); //first 10000 reads for cache warming!
    }

    @Test
    public void multiThreadAccessTest() throws Exception {
        int amount = 1000;
        createCachedObjects(amount);
        Thread.sleep(500);
        for (int i = 0; i < amount; i++) {
            CachedObject o = morphium.createQueryFor(CachedObject.class).f("counter").eq(i + 1).get();
            assert (o != null) : "Not found: " + i;
            if (i % 500 == 0) {
                log.info("Read " + i);
                log.info("Cached: " + morphium.getStatistics().get(StatisticKeys.CACHE_ENTRIES.name()));
            }
        }

        //cache warming finished...
        log.info("cache warmed... starting threads...");

        List<Thread> threads = new ArrayList<>();

        long start = System.currentTimeMillis();

        for (int j = 0; j < 100; j++) {
            Thread t = new Thread(() -> {
                Query<CachedObject> q = morphium.createQueryFor(CachedObject.class).f("counter").eq((int) (Math.random() * amount) + 1);
                for (int i = 0; i < 100000; i++) {
                    CachedObject o = q.get();
                }
            });

            t.start();
            threads.add(t);
        }

        for (Thread tr : threads) tr.join();

        long dur = System.currentTimeMillis() - start;

        log.info("Duration: " + dur + "ms");

        log.info("Cache hit ratio: " + morphium.getStatistics().get(StatisticKeys.CHITSPERC.name()));
        log.info("Cache hits     : " + morphium.getStatistics().get(StatisticKeys.CHITS.name()));
        log.info("Cache miss     : " + morphium.getStatistics().get(StatisticKeys.CMISS.name()));

        assert (morphium.getStatistics().get(StatisticKeys.CHITS.name()) >= 90); //first 10000 reads for cache warming!
    }

    @Test
    public void cachePerformanceTest() throws Exception {
        MorphiumCache c = morphium.getCache();
        CachedObject o = new CachedObject();
        List<CachedObject> lst = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            c.addToCache("key " + i, CachedObject.class, lst);
        }

        log.info("cache filled...");

        for (int t = 1; t <= 500; t += 10) {
            long start = System.currentTimeMillis();
            List<Thread> thr = new ArrayList<>();
            //create Threads according to t
            for (int i = 0; i < t; i++) {
                Thread thread = new Thread(() -> {
                    for (int l = 0; l < 10000; l++) {
                        assert (c.isCached(CachedObject.class, "key " + l));
                        assert (c.getFromCache(CachedObject.class, "key " + l) != null);
                    }
                });
                thread.start();
                thr.add(thread);
            }
            for (Thread thread : thr) thread.join();
            long dur = System.currentTimeMillis() - start;
            log.info("Reading with " + t + " threads took " + dur + "ms");

        }


    }

    @Test
    public void arHelperPerformanceTest() throws Exception {
        AnnotationAndReflectionHelper c = morphium.getARHelper();

        c.getAllAnnotationsFromHierachy(CachedObject.class, Cache.class);
        c.getAnnotationFromHierarchy(CachedObject.class, Cache.class);
        c.isAnnotationPresentInHierarchy(CachedObject.class, Cache.class);
        c.isEntity(new CachedObject());

        for (int t = 1; t <= 250; t += 10) {
            long start = System.currentTimeMillis();
            List<Thread> thr = new ArrayList<>();
            //create Threads according to t
            for (int i = 0; i < t; i++) {
                Thread thread = new Thread(() -> {
                    for (int l = 0; l < 10000; l++) {
                        assert (c.isEntity(new CachedObject()));
                        assert (c.getAnnotationFromHierarchy(CachedObject.class, Cache.class) != null);
                    }
                });
                thread.start();
                thr.add(thread);
            }
            for (Thread thread : thr) thread.join();
            long dur = System.currentTimeMillis() - start;
            log.info("Reading with " + t + " threads took " + dur + "ms");

        }


    }
}