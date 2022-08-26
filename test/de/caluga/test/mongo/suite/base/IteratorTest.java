package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.QueryIterator;
import de.caluga.morphium.query.MorphiumIterator;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.query.geospatial.MultiLineString;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * User: Stephan Bösebeck
 * Date: 23.11.12
 * Time: 12:04
 * <p/>
 */
public class IteratorTest extends MultiDriverTestBase {

    private final List<MorphiumId> data = Collections.synchronizedList(new ArrayList<>());

    private int runningThreads = 0;

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void iteratorSortTest(Morphium morphium) throws Exception {
        try (morphium) {
            for (int i = 0; i < 100; i++) {
                morphium.store(new SimpleEntity(((int) (Math.random() * 5.0)), (long) (Math.random() * 100000.0)));
            }
            Thread.sleep(1000);

            Query<SimpleEntity> q = morphium.createQueryFor(SimpleEntity.class);

            q.or(q.q().f("v2").lt(100L), q.q().f("v2").gt(200L));
            List<Integer> lst = new ArrayList<>();
            lst.add(100);
            lst.add(1001);
            q.f("v2").nin(lst);
            Map<String, Integer> map = UtilsMap.of("v1", 1);
            map.put("v2", 1);
            q.sort(map);


            long lastv2 = -1;
            int lastv1 = 0;
            QueryIterator<SimpleEntity> it = q.asIterable(1000);
            boolean error = false;
            for (SimpleEntity u : it) {
                log.info(u.v1 + " ---- " + u.v2);
                if (lastv1 > u.v1) {
                    error = true;
                }

                if (lastv1 != u.v1) {
                    lastv2 = -1;
                }
                if (lastv2 > u.v2) {
                    error = true;
                }
                lastv2 = u.v2;
                lastv1 = u.v1;
            }
            assert (!error);
        }
    }

    @Entity
    public static class SimpleEntity {
        @Id
        public MorphiumId id;
        public int v1;
        public long v2;
        public String v1str;

        public SimpleEntity(int v1, long v2) {
            this.v1 = v1;
            this.v2 = v2;
            v1str = "" + v1;
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void concurrentAccessTest(Morphium morphium) throws Exception {
        try (morphium) {
            createTestUc(morphium);
            Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class).sort("counter");
            qu.setCollectionName("test_uc");
            Thread.sleep(250);
            long totals = qu.countAll();

            MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(), qu.asIterable(1000)};
            for (final MorphiumIterator<UncachedObject> it : toTest) {
                final AtomicInteger count = new AtomicInteger(0);
                log.info("Running test with " + it.getClass().getName());
                //        final MorphiumIterator<UncachedObject> it = qu.asIterable(1000, 15);
                //            data = Collections.synchronizedList(new ArrayList<>());
                final Vector<Thread> threads = new Vector<>();
                for (int i = 0; i < 3; i++) {
                    log.info("Starting thread..." + i);
                    Thread t = new Thread() {
                        @Override
                        public void run() {
                            try {
                                while (it.hasNext()) {
                                    UncachedObject uc = it.next();
                                    if (uc == null) {
                                        log.info("reached end concurrently - some other thread won!");
                                    } else {
                                        //                                assert (!data.contains(uc.getMorphiumId())); //cannot guarantee that as hasNext() and nex() are not executed atomically!
                                        //                                data.add(uc.getMorphiumId());
                                        int cnt = count.incrementAndGet();
                                        if (cnt % 1000 == 0) {
                                            log.info("Got " + cnt);
                                        }
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
                assert (count.get() == totals) : "Count wrong, " + count.get() + " should be " + totals;
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void emptyResultIteratorTest(Morphium morphium) {
        try (morphium) {
            for (UncachedObject uc : morphium.createQueryFor(UncachedObject.class).asIterable(1000)) {
                //noinspection ConstantConditions
                assert (false);
            }

            for (UncachedObject uc : morphium.createQueryFor(UncachedObject.class).sort("-counter").asIterable(1000)) {
                //noinspection ConstantConditions
                assert (false);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void parallelIteratorAccessTest(Morphium morphium) throws Exception {
        try (morphium) {
            createTestUc(morphium);
            runningThreads = 0;


            for (int i = 0; i < 3; i++) {
                new Thread(() -> {
                    int myNum = runningThreads++;
                    log.info("Starting thread..." + myNum);
                    Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class).sort("counter");
                    qu.setCollectionName("test_uc");
                    //                    MorphiumIterator<UncachedObject> it = qu.asIterable(5000, 15);
                    MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(), qu.asIterable(1000)};
                    long count = qu.countAll();
                    for (MorphiumIterator<UncachedObject> it : toTest) {
                        for (UncachedObject uc : it) {
                            assert (it.getCursor() == uc.getCounter());
                            if (it.getCursor() % 2500 == 0) {
                                log.info("Thread " + myNum + " read " + it.getCursor() + "/" + count);
                                Thread.yield();
                            }
                        }
                    }
                    runningThreads--;
                    log.info("Thread finished");
                }).start();
                Thread.sleep(250);
            }
            Thread.sleep(1000);
            while (runningThreads > 0) {
                Thread.sleep(100);
            }
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void doubleIteratorTest(Morphium morphium) {
        try (morphium) {
            createTestUc(morphium);
            createCachedObjects(morphium, 1000);

            Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class).f("counter").lt(10000).sort("counter");
            qu.setCollectionName("test_uc");
            MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(10)};
            long count = qu.countAll();
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
                        log.info("Iteration it: " + it.getCursor() + "/" + count);
                    }
                }
            }
        }


    }


    private void createTestUc(Morphium morphium) {
        Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class);
        qu.setCollectionName("test_uc");
        if (qu.countAll() != 25000) {
            morphium.dropCollection(UncachedObject.class, "test_uc", null);
            log.info("Creating uncached objects");

            List<UncachedObject> lst = new ArrayList<>();
            for (int i = 0; i < 25000; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i + 1);
                o.setStrValue("V" + i);
                lst.add(o);
            }
            morphium.storeList(lst, "test_uc");
            log.info("creation finished");
        } else {
            log.info("Testdata already filled...");
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void basicIteratorTest(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 1000);
            Thread.sleep(500);
            Query<UncachedObject> qu = getUncachedObjectQuery(morphium);
            long start = System.currentTimeMillis();
            MorphiumIterator<UncachedObject> it = qu.asIterable(2);
            assertThat(it.hasNext());
            UncachedObject u = it.next();
            assertThat(u.getCounter()).isEqualTo(1);
            log.info("Got one: " + u.getCounter() + "  / " + u.getStrValue());
            log.info("Current Buffersize: " + it.available());
            if (!(morphium.getDriver() instanceof InMemoryDriver)) {
                assertThat(it.available()).isEqualTo(1);
            }
            u = it.next();
            assertThat(u.getCounter()).isEqualTo(2);
            u = it.next();
            assertThat(u.getCounter()).isEqualTo(3);
            assertThat(it.getCursor()).isEqualTo(3);

            u = it.next();
            assertThat(u.getCounter()).isEqualTo(4);
            u = it.next();
            assertThat(u.getCounter()).isEqualTo(5);

            while (it.hasNext()) {
                u = it.next();
                log.info("Object: " + u.getCounter());
            }

            assertThat(u.getCounter()).isEqualTo(1000);
            log.info("Took " + (System.currentTimeMillis() - start) + " ms");

            for (UncachedObject uc : qu.asIterable(100)) {
                if (uc.getCounter() % 100 == 0) {
                    log.info("Got msg " + uc.getCounter());
                }
            }
            morphium.dropCollection(UncachedObject.class);
            u = new UncachedObject();
            u.setStrValue("Hello");
            u.setCounter(1900);
            morphium.store(u);
            Thread.sleep(1500);
            for (UncachedObject uc : morphium.createQueryFor(UncachedObject.class).asIterable(100)) {
                log.info("Got another " + uc.getCounter());
            }
        }

    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void iteratorByIdTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(UncachedObject.class);
            createUncachedObjects(morphium, 10000);
            Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class);
            qu.sort("_id");
            while (qu.countAll() != 10000) {
                Thread.sleep(1000);
                log.info("not stored yet...");
            }
            long start = System.currentTimeMillis();
            MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(107)};
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
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void iteratorRepeatTest(Morphium morphium) {
        try (morphium) {
            createUncachedObjects(morphium, 278);
            Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class);
            qu = qu.sort("_id");
            //        MorphiumIterator<UncachedObject> it = qu.asIterable(10);

            MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(20)};
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
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void iteratorBoundaryTest(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 17);

            Thread.sleep(1000);
            Query<UncachedObject> qu = getUncachedObjectQuery(morphium);

            MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(3)};
            for (final MorphiumIterator<UncachedObject> it : toTest) {
                long start = System.currentTimeMillis();

                //        MorphiumIterator<UncachedObject> it = qu.asIterable(3);
                assert (it.hasNext());
                UncachedObject u = it.next();
                assert (u.getCounter() == 1);
                log.info("Got first one: " + u.getCounter() + "  / " + u.getStrValue());

                u = new UncachedObject();
                u.setCounter(1800);
                u.setStrValue("Should not be read");
                morphium.store(u);
                waitForWrites(morphium);

                while (it.hasNext()) {
                    u = it.next();
                    log.info("Object: " + u.getCounter() + "/" + u.getStrValue());
                }

                assert (u.getCounter() == 17);
                //cannot check buffersize anymore
                log.info("Took " + (System.currentTimeMillis() - start) + " ms");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void iteratorLimitTest(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 17);
            Thread.sleep(400);
            Query<UncachedObject> qu = getUncachedObjectQuery(morphium);
            qu.limit(10);

            long start = System.currentTimeMillis();
            MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(3)};
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
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void iterableTest(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 1000);
            Thread.sleep(500);
            Query<UncachedObject> qu = getUncachedObjectQuery(morphium);
            qu.limit(20);
            MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(10)};
            for (MorphiumIterator<UncachedObject> it : toTest) {
                long start = System.currentTimeMillis();
                int cnt = 0;
                for (UncachedObject u : it) {
                    cnt++;
                    assertThat(u.getCounter()).isEqualTo(cnt);
                }
                assertThat(cnt).isEqualTo(20);
                log.info("Took " + (System.currentTimeMillis() - start) + " ms");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void iteratorFunctionalityTest(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 1000);
            Thread.sleep(500);
            Query<UncachedObject> qu = getUncachedObjectQuery(morphium);

            var it = qu.asIterable(100);
            int cnt = 0;
            while (it.hasNext()) {
                cnt++;
                var u = it.next();
                assertThat(u.getCounter()).isEqualTo(cnt);
            }
            it.close();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void iterableSkipsTest(Morphium morphium) {
        try (morphium) {
            createUncachedObjects(morphium, 100);
            Query<UncachedObject> qu = getUncachedObjectQuery(morphium);


            MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{qu.asIterable(10)};
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
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void expectedBehaviorTest(Morphium morphium) {
        try (morphium) {
            createUncachedObjects(morphium, 10);
            long start = System.currentTimeMillis();
            Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class).sort("-counter");
            for (UncachedObject u : qu.asIterable()) {
                UncachedObject uc = new UncachedObject();
                uc.setCounter(u.getCounter() + 1);
                uc.setStrValue("expected WRONG!");
                morphium.store(uc);
                //Will write out some Wrong!-Values... depending on Driver: singleconnection, no "WRONG" is shown,
                // multithreadded it is - this is expected and GOOD!
                log.info("Current Counter: " + u.getCounter() + " and Value: " + u.getStrValue());
            }

            log.info("Took " + (System.currentTimeMillis() - start) + " ms");
        }
    }


    private Query<UncachedObject> getUncachedObjectQuery(Morphium morphium) {
        Query<UncachedObject> qu = morphium.createQueryFor(UncachedObject.class);
        qu = qu.f("counter").lte(1000);
        qu = qu.sort("counter");
        return qu;
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void multithreaddedIteratorTest(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 1000);
            Thread.sleep(500);
            Query<UncachedObject> query = morphium.createQueryFor(UncachedObject.class).sort("counter");
            //        MorphiumIterator<UncachedObject> it = query.asIterable(10, 10);
            MorphiumIterator<UncachedObject>[] toTest = new MorphiumIterator[]{query.asIterable(10)};
            for (MorphiumIterator<UncachedObject> it : toTest) {
                int cnt = 0;
                while (it.hasNext()) {
                    UncachedObject uc = it.next();
                    assert (uc.getCounter() == it.getCursor());
                    cnt++;
                    assert (uc.getCounter() == cnt);
                }
                assert (cnt == query.countAll());
            }
        }
    }


}
