package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.ListContainer;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("core")
public class QueryCountDistinctTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void distinctTest(Morphium morphium) throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            morphium.store(new UncachedObject("uc", i % 3));
        }

        Thread.sleep(100);
        List lt = morphium.createQueryFor(UncachedObject.class).distinct("counter");
        assert (lt.size() == 3);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSize(Morphium morphium) throws InterruptedException {
        ListContainer lc = new ListContainer();

        for (int i = 0; i < 10; i++) {
            lc.addLong(i);
        }

        lc.setName("A test");
        morphium.store(lc);
        lc = new ListContainer();

        for (int i = 0; i < 5; i++) {
            lc.addLong(i);
        }

        lc.setName("A test2");
        morphium.store(lc);
        Thread.sleep(100);
        Query<ListContainer> q = morphium.createQueryFor(ListContainer.class);
        q = q.f(ListContainer.Fields.longList).size(10);
        lc = q.get();
        assert (lc.getLongList().size() == 10);
        assert (lc.getName().equals("A test"));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testCountAll(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 10);
        Thread.sleep(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.f(UncachedObject.Fields.counter).lt(100);
        q.limit(1);
        assert (q.countAll() == 10) : "Wrong amount: " + q.countAll();
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testCountAllWhere(Morphium morphium) throws Exception {
        // When connected through MorphiumServer (via PooledDriver), skip because
        // the MorphiumServer process doesn't have GraalJS available
        if (morphium.getDriver().isInMemoryBackend()) {
            log.info("Connected to in-memory backend (MorphiumServer) - skipping $where test (no JavaScript support)");
            return;
        }
        createUncachedObjects(morphium, 10);
        Thread.sleep(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.where("this.counter<100");
        q.limit(1);
        assert (q.countAll() == 10) : "Wrong amount: " + q.countAll();
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testAsyncCountAll(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 10);
        Thread.sleep(100);
        final AtomicLong c = new AtomicLong(0);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.f(UncachedObject.Fields.counter).lt(100);
        q.limit(1);
        q.countAll(new de.caluga.morphium.async.AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(de.caluga.morphium.async.AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                c.set((Long) param[0]);
            }

            @Override
            public void onOperationError(de.caluga.morphium.async.AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {
            }
        });

        long s = System.currentTimeMillis();

        while (c.get() != 10) {
            Thread.sleep(100);
            assert (System.currentTimeMillis() - s < morphium.getConfig().connectionSettings().getMaxWaitTime());
        }

        assert (c.get() == 10);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testRawQuery(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 100);
        Thread.sleep(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        List<UncachedObject> lst = q.rawQuery(UtilsMap.of("counter", 12)).asList();
        assertEquals(lst.size(), 1);
    }
}
