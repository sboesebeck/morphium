package de.caluga.test.mongo.suite.base;/**
 * Created by stephan on 02.03.16.
 */

import de.caluga.morphium.StatisticKeys;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.cache.jcache.CacheEntry;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

/**
 * TODO: Add Documentation here
 **/
@Tag("core")
@Tag("cache")
public class CacheFunctionalityTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void accessTest(Morphium morphium) throws Exception  {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping test %s for InMemoryDriver", tstName);
            return;
        }

        morphium.getCache().setValidCacheTime(CachedObject.class, 1000000);
        int amount = 1000;
        createCachedObjects(morphium, amount);
        TestUtils.wait(5);
        for (int i = 0; i < amount; i++) {
            CachedObject o = morphium.createQueryFor(CachedObject.class).f("counter").eq(i ).get();
            assertNotNull(o, "Not found: " + i);
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

        checkStats(morphium, dur);
        morphium.getCache().setDefaultCacheTime(CacheEntry.class);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void emptyResultTest(Morphium morphium) throws Exception  {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping test %s for InMemoryDriver", tstName);
            return;
        }
        morphium.getCache().setDefaultCacheTime(CacheEntry.class);
        int amount = 100;
        createCachedObjects(morphium, amount);
        TestUtils.waitForConditionToBecomeTrue(5000, "CachedObjects not persisted",
            () -> morphium.createQueryFor(CachedObject.class).countAll() == amount);

        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            if (i % 100 == 0) {
                log.info("Reached " + i);
            }
            CachedObject o = morphium.createQueryFor(CachedObject.class).f(CachedObject.Fields.counter).eq(amount + 1).get();
            assert (o == null);
            List<CachedObject> lst = morphium.createQueryFor(CachedObject.class).f("counter").gt(amount + 1).asList();
            assert (lst == null || lst.size() == 0);
        }
        long dur = System.currentTimeMillis() - start;

        checkStats(morphium, dur);
    }

    private void checkStats(Morphium morphium, long dur) {
        log.info("Duration: " + dur + "ms");
        log.info("Duration: " + dur + "ms");

        log.info("Cache hit ratio: " + morphium.getStatistics().get(StatisticKeys.CHITSPERC.name()));
        log.info("Cache hits     : " + morphium.getStatistics().get(StatisticKeys.CHITS.name()));
        log.info("Cache miss     : " + morphium.getStatistics().get(StatisticKeys.CMISS.name()));
        assert (morphium.getStatistics().get(StatisticKeys.CHITS.name()) >= 90);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void globalCacheSettingsTest(Morphium morphium) throws Exception  {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping test %s for InMemoryDriver", tstName);
            return;
        }
        // Skip for MorphiumServer - cache sync doesn't work over network
        if (morphium.getDriver().isInMemoryBackend()) {
            log.info("Skipping cache test for MorphiumServer - cache sync not supported over network");
            morphium.close();
            return;
        }
        int gcTime = morphium.getConfig().getGlobalCacheValidTime();
        int hcTime = morphium.getConfig().getHousekeepingTimeout();
        Cache cache = morphium.getARHelper().getAnnotationFromHierarchy(SpecCachedOjbect.class, Cache.class);
        log.info("Housekeeping: " + hcTime);
        log.info("Cache valid:  " + gcTime);
        assert (cache.timeout() == -1);

        int amount = 100;
        for (int i = 0; i < amount; i++) {
            SpecCachedOjbect sp = new SpecCachedOjbect();
            sp.setCounter(i);
            sp.setStrValue("Value " + i);
            morphium.store(sp);
        }
        TestUtils.waitForConditionToBecomeTrue(morphium.getConfig().getMaxWaitTime(),
            "SpecCachedOjbect not persisted",
            () -> morphium.createQueryFor(SpecCachedOjbect.class).countAll() >= amount);
        for (int i = 0; i < amount; i++) {
            final int counter = i;
            TestUtils.waitForConditionToBecomeTrue(morphium.getConfig().getMaxWaitTime(),
                "SpecCachedOjbect with counter " + i + " not found",
                () -> morphium.createQueryFor(SpecCachedOjbect.class).f("counter").eq(counter).get() != null);
            assertNotNull(morphium.createQueryFor(SpecCachedOjbect.class).f("counter").eq(i).get());
            ;
        }
        assert (morphium.getCache().getSizes().get("idCache|" + SpecCachedOjbect.class.getName()) > 0);
        TestUtils.waitForConditionToBecomeTrue(hcTime + 1000, "Cache not maintained after housekeeping",
            () -> morphium.getCache().getSizes().get("idCache|" + SpecCachedOjbect.class.getName()) > 0);
        assert (morphium.getCache().getSizes().get("idCache|" + SpecCachedOjbect.class.getName()) > 0);
        TestUtils.waitForConditionToBecomeTrue(gcTime + 2000, "Cache not cleared after global cache time",
            () -> morphium.getCache().getSizes().get("idCache|" + SpecCachedOjbect.class.getName()) == 0);
        assert (morphium.getCache().getSizes().get("idCache|" + SpecCachedOjbect.class.getName()) == 0) : "Stored still: " + morphium.getCache().getSizes().get("idCache|" + SpecCachedOjbect.class.getName());

    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void multiThreadAccessTest(Morphium morphium) throws Exception  {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping test %s for InMemoryDriver", tstName);
            return;
        }
        morphium.dropCollection(CachedObject.class);
        TestUtils.waitForWrites(morphium, log);
        int amount = 1000;
        createCachedObjects(morphium, amount);
        TestUtils.waitForConditionToBecomeTrue(30000, "CachedObjects not persisted for multithread test",
            () -> morphium.createQueryFor(CachedObject.class).countAll() == amount);
        for (int i = 0; i < amount; i++) {
            CachedObject o = morphium.createQueryFor(CachedObject.class).f("counter").eq(i).get();
            assertNotNull(o, "Not found: " + i);
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

        checkStats(morphium, dur);
    }


    @Cache
    public static class SpecCachedOjbect extends UncachedObject {

    }


}
