package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.query.MorphiumIterator;
import de.caluga.morphium.query.Query;
import org.junit.Test;

import java.util.HashMap;

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
    public void iteratorByIdTest() throws Exception {
        MorphiumSingleton.get().dropCollection(UncachedObject.class);
        createUncachedObjects(100000);
        Query<UncachedObject> qu = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        qu.sort("_id");

        MorphiumIterator<UncachedObject> it = qu.asIterable(107);

        int read = 0;
        UncachedObject u = null;
        while (it.hasNext()) {
            read++;
            u = it.next();
            log.info("Object: " + u.getCounter());
            assert (u.getCounter() == read);
        }

        assert (read == 100000) : "Last counter wrong: " + u.getCounter();
    }

    @Test
    public void iteratorRepeatTest() throws Exception {
        createUncachedObjects(278);
        Query<UncachedObject> qu = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        qu = qu.sort("_id");
//        MorphiumIterator<UncachedObject> it = qu.asIterable(10);
        HashMap<String, String> hash = new HashMap<String, String>();
        boolean error = false;
        int count = 0;
        for (UncachedObject o : qu.asIterable(20)) {
            count++;
            if (hash.get(o.getMongoId().toString()) == null) {
                hash.put(o.getMongoId().toString(), "found");
            } else {
                log.error("Element read multiple times. Number " + count);
                error = true;
            }
        }
        assert (!error);
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
    public void iteratorLimitTest() throws Exception {
        createUncachedObjects(17);

        Query<UncachedObject> qu = getUncachedObjectQuery();
        qu.limit(10);

        MorphiumIterator<UncachedObject> it = qu.asIterable(3);
        int count = 0;
        while (it.hasNext()) {
            count++;
            it.next();
        }
        assert (count == 10) : "Count wrong: " + count;
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
    public void iterableSkipsTest() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> qu = getUncachedObjectQuery();

        MorphiumIterator<UncachedObject> it = qu.asIterable(10);
        boolean back = false;
        for (UncachedObject u : it) {
            log.info("Object: " + u.getCounter());
            if (u.getCounter() == 8) {
                it.ahead(15);
                log.info("Skipping 15 elements");
            }
            if (u.getCounter() == 30 && !back) {
                log.info("and Back 22");
                it.back(22);
                back = true;
            }


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
            //Will write out some Wrong!-Values... this is expected and GOOD!
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
