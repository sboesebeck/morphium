package de.caluga.test.mongo.suite;

import de.caluga.morphium.cache.CacheListener;
import de.caluga.morphium.cache.CacheObject;
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
public class CacheListenerTest extends MongoTest {
    boolean wouldAdd = false;
    boolean wouldClear = false;
    boolean wouldRemove = false;


    @Test
    public void callbackTest() throws Exception {
        CacheListener cl;
        cl = new CacheListener() {
            @Override
            public <T> CacheObject<T> wouldAddToCache(CacheObject<T> toCache) {
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
            public <T> boolean wouldRemoveEntryFromCache(Class cls, Object id, Object entity) {
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
