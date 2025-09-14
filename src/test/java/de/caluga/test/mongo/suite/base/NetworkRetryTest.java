package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.ListContainer;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 23.07.13
 * Time: 10:44
 * <p/>
 * TODO: Add documentation here
 */
@Tag("driver")
@Tag("external")
public class NetworkRetryTest extends MorphiumTestBase {
    private final boolean doTest = false;

    @Test
    public void networRetryTestAsList() throws Exception {
        if (!doTest) {
            return;
        }
        createUncachedObjects(1000);
        morphium.getConfig().setRetriesOnNetworkError(10);
        morphium.getConfig().setSleepBetweenNetworkErrorRetries(500);

        log.info("Now disconnect some mongo nodes, please");
        for (int i = 1; i <= 1000; i++) {
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("counter").eq(i);
            q.asList();
            log.info("read " + i);
            Thread.sleep(500);
        }
    }

    @Test
    public void networkRetryTestGet() throws Exception {
        if (!doTest) {
            return;
        }
        createUncachedObjects(1000);
        morphium.getConfig().setRetriesOnNetworkError(10);
        morphium.getConfig().setSleepBetweenNetworkErrorRetries(500);
        waitForAsyncOperationsToStart(morphium, 1000);
        TestUtils.waitForWrites(morphium, log);
        log.info("Now disconnect some mongo nodes, please");
        for (int i = 1; i <= 1000; i++) {
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("counter").eq(i);
            assert (q.get().getCounter() == i);
            log.info("read " + i);
            Thread.sleep(500);
        }
    }

    @Test
    public void networkRetryTestComplexQuery() throws Exception {
        if (!doTest) {
            return;
        }
        createUncachedObjects(1000);
        morphium.getConfig().setRetriesOnNetworkError(10);
        morphium.getConfig().setSleepBetweenNetworkErrorRetries(2000);

        log.info("Now disconnect some mongo nodes, please");
        for (int i = 0; i < 1000; i++) {
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            Map<String, Object> o = UtilsMap.of("counter", i + 1);
            List<UncachedObject> lst = q.complexQuery(o);
            log.info("read " + i);
            assert (lst.get(0).getCounter() == i + 1);
            Thread.sleep(500);
        }
    }

    @Test
    public void networkRetryTestIterator() throws Exception {
        if (!doTest) {
            return;
        }
        createUncachedObjects(1000);
        morphium.getConfig().setRetriesOnNetworkError(10);
        morphium.getConfig().setSleepBetweenNetworkErrorRetries(500);

        log.info("Now disconnect some mongo nodes, please");
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
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
        if (!doTest) {
            return;
        }
        morphium.getConfig().setRetriesOnNetworkError(10);
        morphium.getConfig().setSleepBetweenNetworkErrorRetries(2500);

        log.info("Now disconnect some mongo nodes, please");
        for (int i = 0; i < 1000; i++) {
            UncachedObject o = new UncachedObject();
            o.setCounter(i + 1);
            o.setStrValue("A test");
            morphium.store(o);
            log.info("Stored...");
            Thread.sleep(1000);
        }
    }

    @Test
    public void networkRetryTestBulkSave() throws Exception {
        if (!doTest) {
            return;
        }
        morphium.getConfig().setRetriesOnNetworkError(10);
        morphium.getConfig().setSleepBetweenNetworkErrorRetries(2500);

        log.info("Now disconnect some mongo nodes, please");
        for (int i = 0; i < 1000; i++) {
            List<UncachedObject> lst = new ArrayList<>();
            for (int cnt = 0; cnt < 10000; cnt += 1000) {
                UncachedObject o;
                o = new UncachedObject();
                o.setCounter(i + cnt + 1);
                o.setStrValue("A test");
                lst.add(o);
            }
            morphium.storeList(lst);
            log.info("Stored...");
            Thread.sleep(1000);
        }
    }

    @Test
    public void networkRetryTestUpdate() throws Exception {
        if (!doTest) {
            return;
        }
        morphium.getConfig().setRetriesOnNetworkError(10);
        morphium.getConfig().setSleepBetweenNetworkErrorRetries(2500);

        log.info("Now disconnect some mongo nodes, please");
        for (int i = 0; i < 1000; i++) {
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("counter").eq(i + 1);
            log.info("updating... " + i);
            morphium.set(q, "counter", i + 1000);
            Thread.sleep(500);
        }
    }


    @Test
    public void pushTest() throws Exception {
        if (!doTest) {
            return;
        }
        morphium.getConfig().setRetriesOnNetworkError(10);
        morphium.getConfig().setSleepBetweenNetworkErrorRetries(2500);
        morphium.dropCollection(ListContainer.class);
        for (int i = 1; i <= 1000; i++) {
            ListContainer lc = new ListContainer();
            lc.addLong(12 + i);
            lc.addString("string");
            lc.setName("LC" + i);
            morphium.store(lc);
        }
        for (int i = 1; i < 1000; i++) {
            Query<ListContainer> lc = morphium.createQueryFor(ListContainer.class);
            lc = lc.f("name").eq("LC" + i);
            morphium.push(lc, "long_list", 12345L);
            morphium.push(lc, "long_list", 12346L);
            morphium.push(lc, "long_list", 12347L);
            ListContainer cont = lc.get();
            assert (cont.getLongList().contains(12345L)) : "No push?";
            log.info("Pushed...");
            Thread.sleep(1000);
        }

    }

    @Test
    public void pushAllTest() throws Exception {
        if (!doTest) {
            return;
        }
        morphium.getConfig().setRetriesOnNetworkError(10);
        morphium.getConfig().setSleepBetweenNetworkErrorRetries(2500);
        morphium.dropCollection(ListContainer.class);
        for (int i = 1; i <= 1000; i++) {
            ListContainer lc = new ListContainer();
            lc.addLong(12 + i);
            lc.addString("string");
            lc.setName("LC" + i);
            morphium.store(lc);
        }
        for (int i = 1; i < 1000; i++) {
            Query<ListContainer> lc = morphium.createQueryFor(ListContainer.class);
            lc = lc.f("name").eq("LC" + i);
            List<Long> lst = new ArrayList<>();
            lst.add(12345L);
            lst.add(123456L);
            lst.add(123L);
            lst.add(12L);
            morphium.pushAll(lc, "long_list", lst, false, false);
            ListContainer cont = lc.get();
            assert (cont.getLongList().contains(12345L)) : "No push?";
            log.info("Pushed...");
            Thread.sleep(1000);
        }

    }

    @Test
    public void incTest() throws Exception {
        if (!doTest) {
            return;
        }
        morphium.getConfig().setRetriesOnNetworkError(10);
        morphium.getConfig().setSleepBetweenNetworkErrorRetries(2500);
        createUncachedObjects(1000);

        for (int i = 1; i < 1000; i++) {
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("counter").eq(i);
            morphium.inc(q, "counter", 10000);
            log.info("increased...");
            Thread.sleep(1000);
        }
    }

    @Test
    public void decTest() throws Exception {
        if (!doTest) {
            return;
        }
        morphium.getConfig().setRetriesOnNetworkError(10);
        morphium.getConfig().setSleepBetweenNetworkErrorRetries(2500);
        createUncachedObjects(1000);

        for (int i = 500; i < 1000; i++) {
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("counter").eq(i);
            morphium.dec(q, "counter", 500);
            log.info("decreased...");
            Thread.sleep(1000);
        }
    }
}
