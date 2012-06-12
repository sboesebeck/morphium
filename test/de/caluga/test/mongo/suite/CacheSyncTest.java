package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.Query;
import de.caluga.morphium.cache.CacheSynchronizer;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 12.06.12
 * Time: 16:40
 * <p/>
 * TODO: Add documentation here
 */
public class CacheSyncTest extends MongoTest {
    @Test
    public void sendClearMsgTest() throws Exception {
        Messaging msg = new Messaging(MorphiumSingleton.get(), 100, true);
        msg.start();
        CacheSynchronizer cs = new CacheSynchronizer(msg, MorphiumSingleton.get());

        Query<Msg> q = MorphiumSingleton.get().createQueryFor(Msg.class);
        long cnt = q.countAll();
        assert (cnt == 0) : "Already a message?!?!" + cnt;

        cs.sendClearMessage(CachedObject.class.getName(), "test");
        Thread.sleep(5000);
        waitForWrites();
        cnt = q.countAll();
        assert (cnt == 1) : "there should be one msg, there are " + cnt;
    }

    @Test
    public void clearCacheTest() throws Exception {
        Messaging msg1 = new Messaging(MorphiumSingleton.get(), 100, true);
        msg1.start();
        Messaging msg2 = new Messaging(MorphiumSingleton.get(), 100, true);
        msg2.start();
        CacheSynchronizer cs1 = new CacheSynchronizer(msg1, MorphiumSingleton.get());
        CacheSynchronizer cs2 = new CacheSynchronizer(msg2, MorphiumSingleton.get());


        for (int i = 0; i < 100; i++) {
            CachedObject o = new CachedObject();
            o.setCounter(i);
            o.setValue("a value");
            MorphiumSingleton.get().store(o);
        }
        waitForWrites();
        for (int i = 0; i < 100; i++) {
            Query<CachedObject> c = MorphiumSingleton.get().createQueryFor(CachedObject.class);
            c = c.f("counter").eq(i);
            c.asList();
        }
        System.out.println("Stats " + MorphiumSingleton.get().getStatistics().toString());
        assert (MorphiumSingleton.get().getStatistics().get(Morphium.StatisticKeys.CACHE_ENTRIES.name()) != null) : "Cache entries not set?";
        cs1.sendClearMessage("ALL", "test");
        Thread.sleep(1500);
        assert (MorphiumSingleton.get().getStatistics().get(Morphium.StatisticKeys.CACHE_ENTRIES.name()) == 0) : "Cache entries not set?";
    }

}
