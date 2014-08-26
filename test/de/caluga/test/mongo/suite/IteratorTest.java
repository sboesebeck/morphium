package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.query.MorphiumIterator;
import de.caluga.morphium.query.Query;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 23.11.12
 * Time: 12:04
 * <p/>
 */
public class IteratorTest extends MongoTest {

    @Test
    public void iterationSpeedTest() throws Exception {
        Query<UncachedObject> qu = MorphiumSingleton.get().createQueryFor(UncachedObject.class).sort("_id");
        qu.setCollectionName("test_uc");
        if (qu.countAll() != 15000) {
            MorphiumSingleton.get().dropCollection(UncachedObject.class, "test_uc", null);
            log.info("Creating uncached objects");

            List<UncachedObject> lst = new ArrayList<>();
            for (int i = 0; i < 15000; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i + 1);
                o.setValue("V" + i);
                lst.add(o);
            }
            MorphiumSingleton.get().storeList(lst, "test_uc");
        }
        log.info("creation finished");
        log.info("creating iterator");
        MorphiumIterator<UncachedObject> it = qu.asIterable(1000, 1);
        log.info("iterating 1000/1");
        long start = System.currentTimeMillis();
        while (it.hasNext()) {
            UncachedObject o = it.next();
//            log.info("." + it.getCursor()+": "+o.getCounter());
            assert (it.getCursor() == o.getCounter()) : "cursor=" + it.getCursor() + " != counter=" + o.getCounter();
        }

        log.info("iterator 1000/1 Took " + (System.currentTimeMillis() - start) + " ms");


        log.info("iterating 1000/5");
        start = System.currentTimeMillis();
        it = qu.asIterable(1000, 5);

        while (it.hasNext()) {
            UncachedObject o = it.next();
            assert (it.getCursor() == o.getCounter());

        }
        log.info("iterator 1000/5 Took " + (System.currentTimeMillis() - start) + " ms");


        log.info("iterating 1000/10");
        start = System.currentTimeMillis();
        it = qu.asIterable(1000, 10);

        while (it.hasNext()) {
            UncachedObject o = it.next();
            assert (it.getCursor() == o.getCounter());

        }
        log.info("iterator 1000/5 Took " + (System.currentTimeMillis() - start) + " ms");


        log.info("iterating 1000/15");
        start = System.currentTimeMillis();
        it = qu.asIterable(1000, 5);

        while (it.hasNext()) {
            UncachedObject o = it.next();
            assert (it.getCursor() == o.getCounter());

        }
        log.info("iterator 1000/5 Took " + (System.currentTimeMillis() - start) + " ms");



        
        log.info("iterating 100/1");
        start = System.currentTimeMillis();
        it = qu.asIterable(100, 1);

        while (it.hasNext()) {
            UncachedObject o = it.next();
            assert (it.getCursor() == o.getCounter());

        }

        log.info("iterator 100/1 Took " + (System.currentTimeMillis() - start) + " ms");


        log.info("iterating 100/5");
        start = System.currentTimeMillis();
        it = qu.asIterable(100, 5);

        while (it.hasNext()) {
            UncachedObject o = it.next();
            assert (it.getCursor() == o.getCounter());

        }

        log.info("iterator 100/5 Took " + (System.currentTimeMillis() - start) + " ms");


        log.info("iterating 100/15");
        start = System.currentTimeMillis();
        it = qu.asIterable(100, 15);

        while (it.hasNext()) {
            UncachedObject o = it.next();
            assert (it.getCursor() == o.getCounter());

        }

        log.info("iterator 100/15 Took " + (System.currentTimeMillis() - start) + " ms");


        log.info("iterating 100/100");
        start = System.currentTimeMillis();
        it = qu.asIterable(100, 100);

        while (it.hasNext()) {
            UncachedObject o = it.next();
            assert (it.getCursor() == o.getCounter());

        }

        log.info("iterator 100/100 Took " + (System.currentTimeMillis() - start) + " ms");


        
        log.info("iterating 5000/5");
        start = System.currentTimeMillis();
        it = qu.asIterable(5000, 5);

        while (it.hasNext()) {
            UncachedObject o = it.next();
            assert (it.getCursor() == o.getCounter());
        }

        log.info("iterator 5000/5 Took " + (System.currentTimeMillis() - start) + " ms");


        log.info("iterating 10000/5");
        start = System.currentTimeMillis();
        it = qu.asIterable(10000, 5);

        while (it.hasNext()) {
            UncachedObject o = it.next();
            assert (it.getCursor() == o.getCounter());
        }

        log.info("iterator 10000/5 Took " + (System.currentTimeMillis() - start) + " ms");


        log.info("iterating 15000/5");
        start = System.currentTimeMillis();
        it = qu.asIterable(15000, 5);

        while (it.hasNext()) {
            UncachedObject o = it.next();
            assert (it.getCursor() == o.getCounter());
        }

        log.info("iterator 15000/5 Took " + (System.currentTimeMillis() - start) + " ms");


        log.info("Iterating directly over list");
        start = System.currentTimeMillis();
        List<UncachedObject> lst = qu.asList();
        for (UncachedObject u : lst) {
            assert (u.getValue() != null);
        }
        log.info("iterating direktly took " + (System.currentTimeMillis() - start) + " ms");

    }


    @Test
    public void basicIteratorTest() throws Exception {
        createUncachedObjects(1000);

        Query<UncachedObject> qu = getUncachedObjectQuery();
        long start = System.currentTimeMillis();
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
        log.info("Took " + (System.currentTimeMillis() - start) + " ms");
    }

    @Test
    public void iteratorByIdTest() throws Exception {
        MorphiumSingleton.get().dropCollection(UncachedObject.class);
        createUncachedObjects(10000);
        Query<UncachedObject> qu = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        qu.sort("_id");
        long start = System.currentTimeMillis();
        MorphiumIterator<UncachedObject> it = qu.asIterable(107);

        int read = 0;
        UncachedObject u = null;
        while (it.hasNext()) {
            read++;
            u = it.next();
            log.info("Object: " + u.getCounter());
            assert (u.getCounter() == read);
        }

        assert (read == 10000) : "Last counter wrong: " + u.getCounter();
        log.info("Took " + (System.currentTimeMillis() - start) + " ms");
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
        long start = System.currentTimeMillis();
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
        log.info("Took " + (System.currentTimeMillis() - start) + " ms");
    }

    @Test
    public void iteratorBoundaryTest() throws Exception {
        createUncachedObjects(17);

        Query<UncachedObject> qu = getUncachedObjectQuery();

        long start = System.currentTimeMillis();
        MorphiumIterator<UncachedObject> it = qu.asIterable(3);
        assert (it.hasNext());
        UncachedObject u = it.next();
        assert (u.getCounter() == 1);
        log.info("Got first one: " + u.getCounter() + "  / " + u.getValue());

        u = new UncachedObject();
        u.setCounter(1800);
        u.setValue("Should not be read");
        MorphiumSingleton.get().store(u);
        waitForWrites();

        while (it.hasNext()) {
            u = it.next();
            log.info("Object: " + u.getCounter() + "/" + u.getValue());
        }

        assert (u.getCounter() == 17);
        assert (it.getCurrentBufferSize() == 2);
        log.info("Took " + (System.currentTimeMillis() - start) + " ms");
    }

    @Test
    public void iteratorLimitTest() throws Exception {
        createUncachedObjects(17);

        Query<UncachedObject> qu = getUncachedObjectQuery();
        qu.limit(10);

        long start = System.currentTimeMillis();
        MorphiumIterator<UncachedObject> it = qu.asIterable(3);
        int count = 0;
        while (it.hasNext()) {
            count++;
            it.next();
        }
        assert (count == 10) : "Count wrong: " + count;

        log.info("Took " + (System.currentTimeMillis() - start) + " ms");
    }

    @Test
    public void iterableTest() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> qu = getUncachedObjectQuery();
        long start = System.currentTimeMillis();
        int cnt = 0;
        for (UncachedObject u : qu.asIterable(10)) {
            cnt++;
            assert (u.getCounter() == cnt);
        }
        log.info("Took " + (System.currentTimeMillis() - start) + " ms");
    }


    @Test
    public void iterableSkipsTest() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> qu = getUncachedObjectQuery();

        long start = System.currentTimeMillis();
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
        log.info("Took " + (System.currentTimeMillis() - start) + " ms");
    }


    @Test
    public void expectedBehaviorTest() throws Exception {
        createUncachedObjects(10);
        long start = System.currentTimeMillis();
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

        log.info("Took " + (System.currentTimeMillis() - start) + " ms");
    }


    private Query<UncachedObject> getUncachedObjectQuery() {
        Query<UncachedObject> qu = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        qu = qu.f("counter").lte(1000);
        qu = qu.sort("counter");
        return qu;
    }
}
