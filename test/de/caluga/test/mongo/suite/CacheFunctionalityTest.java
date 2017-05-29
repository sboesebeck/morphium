package de.caluga.test.mongo.suite;/**
 * Created by stephan on 02.03.16.
 */

import de.caluga.morphium.StatisticKeys;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.cache.CacheObject;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO: Add Documentation here
 **/
public class CacheFunctionalityTest extends MongoTest {

    @Test
    public void accessTest() throws Exception {
        morphium.getCache().setValidCacheTime(CachedObject.class, 1000000);
        int amount = 1000;
        createCachedObjects(amount);
        Thread.sleep(5000);
        for (int i = 0; i < amount; i++) {
            CachedObject o = morphium.createQueryFor(CachedObject.class).f("counter").eq(i + 1).get();
            assert (o != null) : "Not found: " + i;
            if (i % 100 == 0) {
                log.info("Read " + i);
                log.info("Cached: " + morphium.getStatistics().get(StatisticKeys.CACHE_ENTRIES.name()));
            }
        }

        //cache warming finished...
        log.info("cache warmed... starting...");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 15000; i++) {
            if (i % 500 == 0) {
                log.info("Reached " + i);
            }
            CachedObject o = morphium.createQueryFor(CachedObject.class).f("counter").eq((int) (Math.random() * amount) + 1).get();
        }
        long dur = System.currentTimeMillis() - start;

        checkStats(dur);
        morphium.getCache().setDefaultCacheTime(CacheObject.class);
    }

    @Test
    public void emptyResultTest() throws Exception {
        morphium.getCache().setDefaultCacheTime(CacheObject.class);
        int amount = 100;
        createCachedObjects(amount);
        Thread.sleep(1500);

        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            if (i % 100 == 0) {
                log.info("Reached " + i);
            }
            CachedObject o = morphium.createQueryFor(CachedObject.class).f("counter").eq(amount + 1).get();
            assert (o == null);
            List<CachedObject> lst = morphium.createQueryFor(CachedObject.class).f("counter").gt(amount + 1).asList();
            assert (lst == null || lst.size() == 0);
        }
        long dur = System.currentTimeMillis() - start;

        checkStats(dur);
    }

    private void checkStats(long dur) {
        log.info("Duration: " + dur + "ms");
        log.info("Duration: " + dur + "ms");

        log.info("Cache hit ratio: " + morphium.getStatistics().get(StatisticKeys.CHITSPERC.name()));
        log.info("Cache hits     : " + morphium.getStatistics().get(StatisticKeys.CHITS.name()));
        log.info("Cache miss     : " + morphium.getStatistics().get(StatisticKeys.CMISS.name()));
        assert (morphium.getStatistics().get(StatisticKeys.CHITS.name()) >= 90);
    }

    @Test
    public void globalCacheSettingsTest() throws Exception {
        int gcTime = morphium.getConfig().getGlobalCacheValidTime();
        int hcTime = morphium.getConfig().getHousekeepingTimeout();
        Cache cache = morphium.getARHelper().getAnnotationFromHierarchy(SpecCacedOjbect.class, Cache.class);
        log.info("Housekeeping: " + hcTime);
        log.info("Cache valid:  " + gcTime);
        assert (cache.timeout() == -1);

        int amount = 100;
        for (int i = 0; i < amount; i++) {
            SpecCacedOjbect sp = new SpecCacedOjbect();
            sp.setCounter(i);
            sp.setValue("Value " + i);
            morphium.store(sp);
        }
        for (int i = 0; i < amount; i++) {
            assert (morphium.createQueryFor(SpecCacedOjbect.class).f("counter").eq(i).get() != null);
        }
        assert (morphium.getCache().getCache().get(SpecCacedOjbect.class).size() > 0);
        Thread.sleep(hcTime + 100);
        assert (morphium.getCache().getCache().get(SpecCacedOjbect.class).size() > 0);
        Thread.sleep(gcTime);
        assert (morphium.getCache().getCache().get(SpecCacedOjbect.class).size() == 0);

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

        checkStats(dur);
    }


    @Cache
    public static class SpecCacedOjbect extends UncachedObject {

    }


}
