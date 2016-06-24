package de.caluga.test.mongo.suite;

import de.caluga.morphium.driver.bson.MorphiumId;
import de.caluga.morphium.query.MorphiumDriverIterator;
import de.caluga.morphium.query.MorphiumIterator;
import de.caluga.morphium.query.PrefetchingDriverIterator;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.*;

/**
 * User: Stephan Bösebeck
 * Date: 23.11.12
 * Time: 12:04
 * <p/>
 */
public class IteratorTest extends MongoTest {

    private List<MorphiumId> data = Collections.synchronizedList(new ArrayList<>());

    private int runningThreads = 0;


    @Test
    public void concurrentAccessTest() throws Exception {
        createTestUc();
        Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class).sort("counter");
        qu.setCollectionName("test_uc");

        MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(), qu.asIterable(1000), qu.asIterable(1000, 15)};
        for (final MorphiumIterator<UncachedObject> it : toTest) {

            log.info("Running test with " + it.getClass().getName());
            //        final MorphiumIterator<UncachedObject> it = qu.asIterable(1000, 15);
            it.setMultithreaddedAccess(true);
            //            data = Collections.synchronizedList(new ArrayList<>());

            final Vector<Thread> threads = new Vector<>();
            for (int i = 0; i < 3; i++) {
                log.info("Starting thread..." + i);
                Thread t = new Thread() {
                    @Override
                    public void run() {
                        try {
                            int cnt = 0;
                            while (it.hasNext()) {
                                UncachedObject uc = it.next();
                                //                                assert (!data.contains(uc.getMorphiumId())); //cannot guarantee that as hasNext() and nex() are not executed atomically!
                                //                                data.add(uc.getMorphiumId());
                                cnt++;
                                if (cnt % 1000 == 0) {
                                    log.info("Got " + cnt);
                                }
                            }
                        } finally {
                            log.info("Thread finished");
                            threads.remove(this);

                        }
                    }
                };
                threads.add(t);
                t.start();
                Thread.sleep(100);
            }

            while (!threads.isEmpty()) {
                Thread.sleep(200);
            }
        }
    }

    @Test
    public void emptyResultIteratorTest() throws Exception {
        for (UncachedObject uc : morphium.createQueryFor(UncachedObject.class).asIterable(1000)) {
            assert (false);
        }

        for (UncachedObject uc : morphium.createQueryFor(UncachedObject.class).sort("-counter").asIterable(1000)) {
            assert (false);
        }
    }

    @Test
    public void parallelIteratorAccessTest() throws Exception {
        createTestUc();
        runningThreads = 0;


        for (int i = 0; i < 3; i++) {
            new Thread() {
                @Override
                public void run() {
                    int myNum = runningThreads++;
                    log.info("Starting thread..." + myNum);
                    Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class).sort("counter");
                    qu.setCollectionName("test_uc");
                    //                    MorphiumIterator<UncachedObject> it = qu.asIterable(5000, 15);
                    MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(), qu.asIterable(1000, 1), qu.asIterable(1000)};
                    for (MorphiumIterator<UncachedObject> it : toTest) {
                        for (UncachedObject uc : it) {
                            assert (it.getCursor() == uc.getCounter());
                            if (it.getCursor() % 2500 == 0) {
                                log.info("Thread " + myNum + " read " + it.getCursor() + "/" + it.getCount());
                                Thread.yield();
                            }
                        }
                    }
                    runningThreads--;
                    log.info("Thread finished");
                }
            }.start();
            Thread.sleep(250);
        }
        Thread.sleep(1000);
        while (runningThreads > 0) {
            Thread.sleep(100);
        }
    }


    @Test
    public void doubleIteratorTest() throws Exception {
        createTestUc();
        createCachedObjects(1000);

        Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class).f("counter").lt(10000).sort("counter");
        qu.setCollectionName("test_uc");
        MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(10, 1), qu.asIterable(10)};
        for (MorphiumIterator<UncachedObject> it : toTest) {
            //        MorphiumIterator<UncachedObject> it = qu.asIterable(1000, 3);
            for (UncachedObject u : it) {
                Query<CachedObject> other = morphium.createQueryFor(CachedObject.class).f("counter").gt(u.getCounter() % 100).f("counter").lt(u.getCounter() % 100 + 10).sort("counter");
                MorphiumIterator<CachedObject> otherIt = other.asIterable();
                assert (it.getCursor() == u.getCounter());
                for (CachedObject co : otherIt) {
                    //                log.info("iterating otherIt: "+otherIt.getNumberOfThreads()+" "+co.getCounter());
                    //                Thread.sleep(200);
                    assert (co.getValue() != null);
                    assert (co.getCounter() > u.getCounter() % 100 && co.getCounter() < u.getCounter() % 100 + 10);
                }
                if (it.getCursor() % 100 == 0) {
                    log.info("Iteration it: " + it.getCursor() + "/" + it.getCount());
                }
            }
        }


    }

    @Test
    public void iterationSpeedTest() throws Exception {
        createTestUc();
        Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class).sort("_id");
        qu.setCollectionName("test_uc");
        log.info("creating iterator");
        MorphiumIterator<UncachedObject> it = qu.asIterable(5000, 1);
        log.info("iterating 5000/1");
        long start = System.currentTimeMillis();
        while (it.hasNext()) {
            UncachedObject o = it.next();
            //            log.info("." + it.getCursor()+": "+o.getCounter());
            //            if (it.getCursor()%1000==0) {
            //                ThreadMXBean thbean = ManagementFactory.getThreadMXBean();
            //                log.info("Running threads: " + thbean.getThreadCount());
            //                log.info("Buffers: "+it.getCurrentBufferSize());
            //            }
            assert (it.getCursor() == o.getCounter()) : "cursor=" + it.getCursor() + " != counter=" + o.getCounter();
        }

        log.info("iterator 5000/1 Took " + (System.currentTimeMillis() - start) + " ms");


        log.info("iterating 1000/5");
        start = System.currentTimeMillis();
        it = qu.asIterable(1000, 5);


        while (it.hasNext()) {
            UncachedObject o = it.next();
            assert (it.getCursor() == o.getCounter());
            //            if (it.getCursor()%1000==0) {
            //                ThreadMXBean thbean = ManagementFactory.getThreadMXBean();
            //                log.info("Running threads: " + thbean.getThreadCount());
            //            }

        }
        log.info("iterator 1000/5 Took " + (System.currentTimeMillis() - start) + " ms");


        log.info("iterating 1000/10");
        start = System.currentTimeMillis();
        it = qu.asIterable(1000, 10);
        while (it.hasNext()) {
            UncachedObject o = it.next();
            assert (it.getCursor() == o.getCounter());

        }
        log.info("iterator 1000/10 Took " + (System.currentTimeMillis() - start) + " ms");


        log.info("iterating 1000/15");
        start = System.currentTimeMillis();
        it = qu.asIterable(1000, 15);

        while (it.hasNext()) {
            UncachedObject o = it.next();
            assert (it.getCursor() == o.getCounter());

        }
        log.info("iterator 1000/15 Took " + (System.currentTimeMillis() - start) + " ms");

        log.info("iterating singlethreadded no prefetch, window size 1000");
        start = System.currentTimeMillis();
        it = qu.asIterable(1000);

        while (it.hasNext()) {
            UncachedObject o = it.next();
            assert (it.getCursor() == o.getCounter());

        }
        log.info("iterator singlethreadded Took " + (System.currentTimeMillis() - start) + " ms");

        //
        //
        //        log.info("iterating 100/5");
        //        start = System.currentTimeMillis();
        //        it = qu.asIterable(100, 5);
        //
        //        while (it.hasNext()) {
        //            UncachedObject o = it.next();
        //            assert (it.getCursor() == o.getCounter());
        //
        //        }
        //
        //        log.info("iterator 100/5 Took " + (System.currentTimeMillis() - start) + " ms");

        //
        //        log.info("iterating 100/15");
        //        start = System.currentTimeMillis();
        //        it = qu.asIterable(100, 15);
        //
        //        while (it.hasNext()) {
        //            UncachedObject o = it.next();
        //            assert (it.getCursor() == o.getCounter());
        //
        //        }
        //
        //        log.info("iterator 100/15 Took " + (System.currentTimeMillis() - start) + " ms");


        //        log.info("iterating 100/100");
        //        start = System.currentTimeMillis();
        //        it = qu.asIterable(100, 100);
        //
        //        while (it.hasNext()) {
        //            UncachedObject o = it.next();
        //            assert (it.getCursor() == o.getCounter());
        //
        //        }
        //
        //        log.info("iterator 100/100 Took " + (System.currentTimeMillis() - start) + " ms");


        log.info("iterating 1000/50");
        start = System.currentTimeMillis();
        it = qu.asIterable(5000, 5);

        while (it.hasNext()) {
            UncachedObject o = it.next();
            assert (it.getCursor() == o.getCounter());
        }

        log.info("iterator 5000/5 Took " + (System.currentTimeMillis() - start) + " ms");


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
        List<UncachedObject> lst = qu.sort("-counter").asList(); //force new query
        for (UncachedObject u : lst) {
            assert (u.getValue() != null);
        }
        log.info("iterating directly took " + (System.currentTimeMillis() - start) + " ms");

    }

    private void createTestUc() {
        Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class);
        qu.setCollectionName("test_uc");
        if (qu.countAll() != 25000) {
            morphium.dropCollection(UncachedObject.class, "test_uc", null);
            log.info("Creating uncached objects");

            List<UncachedObject> lst = new ArrayList<>();
            for (int i = 0; i < 25000; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i + 1);
                o.setValue("V" + i);
                lst.add(o);
            }
            morphium.storeList(lst, "test_uc");
            log.info("creation finished");
        } else {
            log.info("Testdata already filled...");
        }
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

        for (UncachedObject uc : qu.asIterable(100)) {
            if (uc.getCounter() % 100 == 0) {
                log.info("Got msg " + uc.getCounter());
            }
        }
        morphium.dropCollection(UncachedObject.class);
        u = new UncachedObject();
        u.setValue("Hello");
        u.setCounter(1900);
        morphium.store(u);
        Thread.sleep(1500);
        for (UncachedObject uc : morphium.createQueryFor(UncachedObject.class).asIterable(100)) {
            log.info("Got another " + uc.getCounter());
        }

    }

    @Test
    public void basicPefetchIteratorTest() throws Exception {
        createUncachedObjects(1000);

        Query<UncachedObject> qu = getUncachedObjectQuery();
        long start = System.currentTimeMillis();
        MorphiumIterator<UncachedObject> it = qu.asIterable(2, 10);
        assert (it.hasNext());
        UncachedObject u = it.next();
        assert (u.getCounter() == 1) : "Counter wrong: " + u.getCounter();
        log.info("Got one: " + u.getCounter() + "  / " + u.getValue());
        log.info("Current Buffersize: " + it.getCurrentBufferSize());
        assert (it.getCurrentBufferSize() <= 20) : "buffer is " + it.getCurrentBufferSize();
        u = it.next();
        assert (u.getCounter() == 2) : "Counter wrong: " + u.getCounter();
        it.hasNext();
        u = it.next();
        assert (u.getCounter() == 3) : "Counter wrong: " + u.getCounter() + " cursor: " + it.getCursor();
        assert (it.getCount() == 1000);
        assert (it.getCursor() == 3);

        u = it.next();
        assert (u.getCounter() == 4) : "Counter wrong: " + u.getCounter();
        u = it.next();
        assert (u.getCounter() == 5);

        int lastCounter = 5;
        while (it.hasNext()) {
            u = it.next();
            lastCounter++;
            assert (u.getCounter() == lastCounter) : "counter mismatch: is " + u.getCounter() + " should be:" + lastCounter;
            log.info("Object: " + u.getCounter());
        }

        assert (u.getCounter() == 1000);
        log.info("Took " + (System.currentTimeMillis() - start) + " ms");
    }

    @Test
    public void iteratorByIdTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        createUncachedObjects(10000);
        Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class);
        qu.sort("_id");
        while (qu.countAll() != 10000) {
            Thread.sleep(1000);
            log.info("not stored yet...");
        }
        long start = System.currentTimeMillis();
        MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(107, 2), qu.asIterable(107)};
        for (MorphiumIterator<UncachedObject> it : toTest) {

            int read = 0;
            UncachedObject u = new UncachedObject();
            while (it.hasNext()) {
                read++;
                u = it.next();
                log.info("Object: " + u.getCounter());
                assert (u.getCounter() == read);
            }

            assert (read == 10000) : "Last counter wrong: " + u.getCounter();
            log.info("Took " + (System.currentTimeMillis() - start) + " ms");
        }
    }

    @Test
    public void iteratorRepeatTest() throws Exception {
        createUncachedObjects(278);
        Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class);
        qu = qu.sort("_id");
        //        MorphiumIterator<UncachedObject> it = qu.asIterable(10);

        MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(20, 1), qu.asIterable(20, 10), qu.asIterable(20)};
        for (MorphiumIterator<UncachedObject> it : toTest) {
            HashMap<String, String> hash = new HashMap<>();
            boolean error = false;
            int count = 0;
            long start = System.currentTimeMillis();
            for (UncachedObject o : it) {
                count++;
                if (hash.get(o.getMorphiumId().toString()) == null) {
                    hash.put(o.getMorphiumId().toString(), "found");
                } else {
                    log.error("Element read multiple times. Number " + count);
                    error = true;
                }
            }
            assert (!error);
            log.info("Took " + (System.currentTimeMillis() - start) + " ms");
        }
    }

    @Test
    public void iteratorBoundaryTest() throws Exception {
        createUncachedObjects(17);

        Thread.sleep(1000);
        Query<UncachedObject> qu = getUncachedObjectQuery();

        MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(3, 1), qu.asIterable(3, 5), qu.asIterable(3)};
        for (final MorphiumIterator<UncachedObject> it : toTest) {
            long start = System.currentTimeMillis();

            //        MorphiumIterator<UncachedObject> it = qu.asIterable(3);
            assert (it.hasNext());
            UncachedObject u = it.next();
            assert (u.getCounter() == 1);
            log.info("Got first one: " + u.getCounter() + "  / " + u.getValue());

            u = new UncachedObject();
            u.setCounter(1800);
            u.setValue("Should not be read");
            morphium.store(u);
            waitForWrites();

            while (it.hasNext()) {
                u = it.next();
                log.info("Object: " + u.getCounter() + "/" + u.getValue());
            }

            assert (u.getCounter() == 17);
            //cannot check buffersize anymore
            log.info("Took " + (System.currentTimeMillis() - start) + " ms");
        }
    }

    @Test
    public void iteratorLimitTest() throws Exception {
        createUncachedObjects(17);
        Thread.sleep(400);
        Query<UncachedObject> qu = getUncachedObjectQuery();
        qu.limit(10);

        long start = System.currentTimeMillis();
        MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(3, 1), qu.asIterable(3, 3), qu.asIterable(3)};
        for (MorphiumIterator<UncachedObject> it : toTest) {
            //        MorphiumIterator<UncachedObject> it = qu.asIterable(3);
            int count = 0;
            while (it.hasNext()) {
                count++;
                it.next();
            }
            assert (count == 10) : "Count wrong: " + count;

            log.info("Took " + (System.currentTimeMillis() - start) + " ms");
        }
    }

    @Test
    public void iterableTest() throws Exception {
        createUncachedObjects(1000);
        Thread.sleep(500);
        Query<UncachedObject> qu = getUncachedObjectQuery();
        qu.limit(20);
        MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(10, 1), qu.asIterable(10)};
        for (MorphiumIterator<UncachedObject> it : toTest) {
            long start = System.currentTimeMillis();
            int cnt = 0;
            for (UncachedObject u : it) {
                cnt++;
                assert (u.getCounter() == cnt);
            }
            assert (cnt == 20);
            log.info("Took " + (System.currentTimeMillis() - start) + " ms");
        }
    }


    @Test
    public void iterableSkipsTest() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> qu = getUncachedObjectQuery();


        MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(10, 5), qu.asIterable(10)};
        for (MorphiumIterator<UncachedObject> it : toTest) {
            log.info("Testing " + it.getClass());
            long start = System.currentTimeMillis();
            boolean back = false;
            for (UncachedObject u : it) {
                log.info("Object: " + u.getCounter());
                if (u.getCounter() == 8) {
                    it.ahead(15);
                    log.info("Skipping 15 elements");
                    u = it.next();
                    log.info("After skip, counter: " + u.getCounter());
                    assert (u.getCounter() == 24) : "Value is " + u.getCounter();
                }
                if (u.getCounter() == 9 && !back) {
                    log.info("and Back 22");
                    it.back(3);
                    back = true;
                    u = it.next();
                    log.info("After skip, counter: " + u.getCounter());
                    assert (u.getCounter() == 6);
                }


            }
            log.info("Took " + (System.currentTimeMillis() - start) + " ms");
        }
    }


    @Test
    public void expectedBehaviorTest() throws Exception {
        createUncachedObjects(10);
        long start = System.currentTimeMillis();
        Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class).sort("-counter");
        for (UncachedObject u : qu.asIterable()) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(u.getCounter() + 1);
            uc.setValue("expected WRONG!");
            morphium.store(uc);
            waitForWrites();
            //Will write out some Wrong!-Values... this is expected and GOOD!
            log.info("Current Counter: " + u.getCounter() + " and Value: " + u.getValue());
        }

        log.info("Took " + (System.currentTimeMillis() - start) + " ms");
    }


    private Query<UncachedObject> getUncachedObjectQuery() {
        Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class);
        qu = qu.f("counter").lte(1000);
        qu = qu.sort("counter");
        return qu;
    }


    @Test
    public void multithreaddedIteratorTest() {
        createUncachedObjects(1000);

        Query<UncachedObject> query = morphium.createQueryFor(UncachedObject.class).sort("counter");
        //        MorphiumIterator<UncachedObject> it = query.asIterable(10, 10);
        MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{query.asIterable(10, 10), query.asIterable(10)};
        for (MorphiumIterator<UncachedObject> it : toTest) {
            it.setMultithreaddedAccess(false);

            int cnt = 0;
            while (it.hasNext()) {
                UncachedObject uc = it.next();
                assert (uc.getCounter() == it.getCursor());
                cnt++;
                assert (uc.getCounter() == cnt);
            }
            assert (cnt == it.getCount());
        }
    }


    @Test
    public void prefetchTest() throws Exception {
        createUncachedObjects(1000);
        Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class).sort("counter");

        MorphiumIterator<UncachedObject> it = qu.asIterable(10, 10);
        for (UncachedObject u : it) {
            log.info("." + it.getCursor());
        }


    }

    @Test
    public void iteratorTypeTest() throws Exception {
        Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class).sort("counter");
        assert (qu.asIterable().getClass().equals(MorphiumDriverIterator.class));
        assert (qu.asIterable(10).getClass().equals(MorphiumDriverIterator.class));
        assert (qu.asIterable(10, 5).getClass().equals(PrefetchingDriverIterator.class));
    }
}
