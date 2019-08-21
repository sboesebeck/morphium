package de.caluga.test.mongo.suite;

import de.caluga.morphium.cache.CacheListener;
import de.caluga.morphium.cache.jcache.CacheEntry;
import de.caluga.test.mongo.suite.data.CachedObject;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 15.04.14
 * Time: 11:00
 * To change this template use File | Settings | File Templates.
 */
@SuppressWarnings("AssertWithSideEffects")
public class CacheListenerTest extends MorphiumTestBase {
    private boolean wouldAdd = false;
    private boolean wouldClear = false;
    private boolean wouldRemove = false;


    @Test
    public void callbackTest() throws Exception {
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


            super.createCachedObjects(100);

            for (int i = 0; i < 10; i++) {
                morphium.createQueryFor(CachedObject.class).f("counter").lte(i).asList();
            }
            waitForWrites();
            Thread.sleep(1000);
            assert (wouldAdd);

            super.createCachedObjects(10);
            waitForWrites();
            log.info("Waiting for would clear message");
            Thread.sleep(1500);
            assert (wouldClear);
        } finally {
            morphium.getCache().removeCacheListener(cl);

        }


    }

}
