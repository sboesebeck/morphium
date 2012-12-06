package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.query.MorphiumIterator;
import de.caluga.morphium.query.Query;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 23.11.12
 * Time: 12:04
 * <p/>
 */
public class IteratorTest extends MongoTest {

    @Test
    public void basicIteratorTest() throws Exception {
        createUncachedObjects(1000);

        Query<UncachedObject> qu = getUncachedObjectQuery();

        MorphiumIterator<UncachedObject> it = qu.asIterable(2);
        assert (it.hasNext());
        UncachedObject u = it.next();
        assert (u.getCounter() == 1);
        log.info("Got one: " + u.getCounter() + "  / " + u.getValue());
        log.info("Current Buffersize: " + it.getCurrentBufferSize());
        assert (it.getCurrentBufferSize() == 2);

        u = it.next();
        assert (u.getCounter() == 2);
        u = it.next();
        assert (u.getCounter() == 3);
        assert (it.getCount() == 1000);
        assert (it.getCursor() == 3);

        u = it.next();
        assert (u.getCounter() == 4);
        u = it.next();
        assert (u.getCounter() == 5);

        while (it.hasNext()) {
            u = it.next();
            log.info("Object: " + u.getCounter());
        }

        assert (u.getCounter() == 1000);
    }

    @Test
    public void iteratorBoundaryTest() throws Exception {
        createUncachedObjects(17);

        Query<UncachedObject> qu = getUncachedObjectQuery();

        MorphiumIterator<UncachedObject> it = qu.asIterable(3);
        assert (it.hasNext());
        UncachedObject u = it.next();
        assert (u.getCounter() == 1);
        log.info("Got first one: " + u.getCounter() + "  / " + u.getValue());

        u = new UncachedObject();
        u.setCounter(800);
        u.setValue("Should not be read");
        MorphiumSingleton.get().store(u);
        waitForWrites();

        while (it.hasNext()) {
            u = it.next();
            log.info("Object: " + u.getCounter() + "/" + u.getValue());
        }

        assert (u.getCounter() == 17);
        assert (it.getCurrentBufferSize() == 2);
    }

    @Test
    public void iterableTest() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> qu = getUncachedObjectQuery();

        int cnt = 0;
        for (UncachedObject u : qu.asIterable(10)) {
            cnt++;
            assert (u.getCounter() == cnt);
        }

    }

    @Test
    public void expectedBehaviorTest() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> qu = MorphiumSingleton.get().createQueryFor(UncachedObject.class).sort("-counter");
        for (UncachedObject u : qu.asIterable()) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(u.getCounter() + 1);
            uc.setValue("WRONG!");
            MorphiumSingleton.get().store(uc);
            waitForWrites();
            log.info("Current Counter: " + u.getCounter() + " and Value: " + u.getValue());
        }
    }


    private Query<UncachedObject> getUncachedObjectQuery() {
        Query<UncachedObject> qu = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        qu = qu.f("counter").lte(1000);
        qu = qu.sort("counter");
        return qu;
    }
}
