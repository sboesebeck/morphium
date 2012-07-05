package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.Query;
import de.caluga.morphium.StatisticKeys;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.cache.CacheSynchronizer;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import org.bson.types.ObjectId;
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
        msg.setRunning(false);
        cs.detach();
    }

    @Test
    public void removeFromCacheTest() throws Exception {
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
        assert (MorphiumSingleton.get().getStatistics().get(StatisticKeys.CACHE_ENTRIES.name()) != null) : "Cache entries not set?";
        assert (MorphiumSingleton.get().getStatistics().get(StatisticKeys.CACHE_ENTRIES.name()) == 100) : "Cache entries not set?";
        Query<CachedObject> c = MorphiumSingleton.get().createQueryFor(CachedObject.class);
        c = c.f("counter").eq(10);
        ObjectId id = c.get().getId();
        MorphiumSingleton.get().removeEntryFromCache(CachedObject.class, id);
        assert (MorphiumSingleton.get().getStatistics().get(StatisticKeys.CACHE_ENTRIES.name()) == 99) : "Cache entries not set?";

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
        assert (MorphiumSingleton.get().getStatistics().get(StatisticKeys.CACHE_ENTRIES.name()) != null) : "Cache entries not set?";
        cs1.sendClearMessage("ALL", "test");
        Thread.sleep(1500);
        assert (MorphiumSingleton.get().getStatistics().get(StatisticKeys.CACHE_ENTRIES.name()) == 0) : "Cache entries not set?";
        msg1.setRunning(false);
        msg2.setRunning(false);
        cs1.detach();
        cs2.detach();
    }

    @Test
    public void idCacheTest() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            IdCachedObject o = new IdCachedObject();
            o.setCounter(i);
            o.setValue("a value");
            MorphiumSingleton.get().store(o);
        }
        waitForWrites();
        long dur = System.currentTimeMillis() - start;
        log.info("Storing without synchronizer: " + dur + " ms");

        start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            Query<IdCachedObject> q = MorphiumSingleton.get().createQueryFor(IdCachedObject.class);
            IdCachedObject obj = (IdCachedObject) q.f("counter").eq(i).get();
            obj.setCounter(i + 1000);
            MorphiumSingleton.get().store(obj);
        }
        waitForWrites();
        dur = System.currentTimeMillis() - start;
        log.info("Updating without synchronizer: " + dur + " ms");


        MorphiumSingleton.get().dropCollection(IdCachedObject.class);
        Messaging msg1 = new Messaging(MorphiumSingleton.get(), 100, true);
        msg1.start();

        CacheSynchronizer cs1 = new CacheSynchronizer(msg1, MorphiumSingleton.get());
        start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            IdCachedObject o = new IdCachedObject();
            o.setCounter(i);
            o.setValue("a value");
            MorphiumSingleton.get().store(o);
        }
        waitForWrites();
        dur = System.currentTimeMillis() - start;
        log.info("Storing with synchronizer: " + dur + " ms");


        start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            Query<IdCachedObject> q = MorphiumSingleton.get().createQueryFor(IdCachedObject.class);
            IdCachedObject obj = (IdCachedObject) q.f("counter").eq(i).get();
            obj.setCounter(i + 2000);
            MorphiumSingleton.get().store(obj);
        }
        dur = System.currentTimeMillis() - start;
        log.info("Updates queued... " + dur + "ms");
        waitForWrites();
        dur = System.currentTimeMillis() - start;
        log.info("Updating with synchronizer: " + dur + " ms");


        msg1.setRunning(false);
        cs1.detach();

    }


    @Cache(readCache = true, writeCache = true, syncCache = Cache.SyncCacheStrategy.UPDATE_ENTRY)
    public static class IdCachedObject extends CachedObject {

    }

}
