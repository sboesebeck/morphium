package de.caluga.test.mongo.suite;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.query.Query;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 23.07.13
 * Time: 10:44
 * <p/>
 * TODO: Add documentation here
 */
public class NetworkRetryTest extends MongoTest {
    private boolean doTest = false;

    @Test
    public void networRetryTestAsList() throws Exception {
        if (!doTest) return;
        createUncachedObjects(1000);
        MorphiumSingleton.get().getConfig().setRetriesOnNetworkError(10);
        MorphiumSingleton.get().getConfig().setSleepBetweenNetworkErrorRetries(500);

        log.info("Now disconnect some mongo nodes, please");
        for (int i = 1; i <= 1000; i++) {
            Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
            q = q.f("counter").eq(i);
            q.asList();
            log.info("read " + i);
            Thread.sleep(500);
        }
    }

    @Test
    public void networkRetryTestGet() throws Exception {
        if (!doTest) return;
        createUncachedObjects(1000);
        MorphiumSingleton.get().getConfig().setRetriesOnNetworkError(10);
        MorphiumSingleton.get().getConfig().setSleepBetweenNetworkErrorRetries(500);
        waitForAsyncOperationToStart(1000);
        waitForWrites();
        log.info("Now disconnect some mongo nodes, please");
        for (int i = 1; i <= 1000; i++) {
            Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
            q = q.f("counter").eq(i);
            assert (q.get().getCounter() == i);
            log.info("read " + i);
            Thread.sleep(500);
        }
    }

    @Test
    public void networkRetryTestComplexQuery() throws Exception {
        if (!doTest) return;
        createUncachedObjects(1000);
        MorphiumSingleton.get().getConfig().setRetriesOnNetworkError(10);
        MorphiumSingleton.get().getConfig().setSleepBetweenNetworkErrorRetries(2000);

        log.info("Now disconnect some mongo nodes, please");
        for (int i = 0; i < 1000; i++) {
            Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
            DBObject o = new BasicDBObject("counter", i + 1);
            List<UncachedObject> lst = q.complexQuery(o);
            log.info("read " + i);
            assert (lst.get(0).getCounter() == i + 1);
            Thread.sleep(500);
        }
    }

    @Test
    public void networkRetryTestIterator() throws Exception {
        if (!doTest) return;
        createUncachedObjects(1000);
        MorphiumSingleton.get().getConfig().setRetriesOnNetworkError(10);
        MorphiumSingleton.get().getConfig().setSleepBetweenNetworkErrorRetries(500);

        log.info("Now disconnect some mongo nodes, please");
        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q.sort("counter");
        int last = 0;
        Iterable<UncachedObject> it = q.asIterable(10);
        for (UncachedObject ob : it) {
            last++;
            assert (ob.getCounter() == last);
            Thread.sleep(500);
        }
    }

    @Test
    public void networkRetryTestSave() throws Exception {
        if (!doTest) return;
        MorphiumSingleton.get().getConfig().setRetriesOnNetworkError(10);
        MorphiumSingleton.get().getConfig().setSleepBetweenNetworkErrorRetries(2500);

        log.info("Now disconnect some mongo nodes, please");
        for (int i = 0; i < 1000; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i + 1);
            o.setValue("A test");
            MorphiumSingleton.get().store(o);
            log.info("Stored...");
            Thread.sleep(1000);
        }
    }

    @Test
    public void networkRetryTestBulkSave() throws Exception {
        if (!doTest) return;
        MorphiumSingleton.get().getConfig().setRetriesOnNetworkError(10);
        MorphiumSingleton.get().getConfig().setSleepBetweenNetworkErrorRetries(2500);

        log.info("Now disconnect some mongo nodes, please");
        for (int i = 0; i < 1000; i++) {
            List<UncachedObject> lst = new ArrayList<UncachedObject>();
            for (int cnt = 0; cnt < 10000; cnt += 1000) {
                UncachedObject o = null;
                o = new UncachedObject();
                o.setCounter(i + cnt + 1);
                o.setValue("A test");
                lst.add(o);
            }
            MorphiumSingleton.get().storeList(lst);
            log.info("Stored...");
            Thread.sleep(1000);
        }
    }

    @Test
    public void networkRetryTestUpdate() throws Exception {
        if (!doTest) return;
        MorphiumSingleton.get().getConfig().setRetriesOnNetworkError(10);
        MorphiumSingleton.get().getConfig().setSleepBetweenNetworkErrorRetries(2500);

        log.info("Now disconnect some mongo nodes, please");
        for (int i = 0; i < 1000; i++) {
            Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
            q = q.f("counter").eq(i + 1);
            log.info("updating... " + i);
            MorphiumSingleton.get().set(q, "counter", i + 1000);
            Thread.sleep(500);
        }
    }


}
