package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.cache.CacheListener;
import de.caluga.morphium.cache.jcache.CacheEntry;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.test.mongo.suite.data.CachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 15.04.14
 * Time: 11:00
 * To change this template use File | Settings | File Templates.
 */
@SuppressWarnings("AssertWithSideEffects")
@Tag("core")
@Tag("cache")
public class CacheListenerTest extends MultiDriverTestBase {
    private boolean wouldAdd = false;
    private boolean wouldClear = false;
    private boolean wouldRemove = false;


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void callbackTest(Morphium morphium) throws Exception  {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping test %s for InMemoryDriver", tstName);
            return;
        }
        CacheListener cl;
        cl = new CacheListener() {

            @Override
            public <T> CacheEntry<T> wouldAddToCache(Object k, CacheEntry<T> toCache, boolean updated) {
                wouldAdd = true;
                return toCache;
            }

            @Override
            public <T> boolean wouldClearCache(Class<T> affectedEntityType) {
                log.info("Would clear cache!");
                wouldClear = true;
                return true;
            }

            @Override
            public <T> boolean wouldRemoveEntryFromCache(Object key, CacheEntry<T> toRemove, boolean expired) {
                wouldRemove = true;
                return true;
            }

        };
        try {
            morphium.getCache().addCacheListener(cl);
            assert (morphium.getCache().isListenerRegistered(cl));


            super.createCachedObjects(morphium, 100);

            for (int i = 0; i < 10; i++) {
                morphium.createQueryFor(CachedObject.class).f("counter").lte(i).asList();
            }
            TestUtils.waitForWrites(morphium, log);
            Thread.sleep(1000);
            assert (wouldAdd);

            super.createCachedObjects(morphium, 10);
            TestUtils.waitForWrites(morphium, log);
            log.info("Waiting for would clear message");
            Thread.sleep(1500);
            assert (wouldClear);
        } finally {
            morphium.getCache().removeCacheListener(cl);

        }


    }

}
