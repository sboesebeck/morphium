/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

/**
 * @author stephan
 */
@Tag("core")
@Tag("cache")
public class MassCacheTest extends MultiDriverTestBase {

    public static final int NO_OBJECTS = 100;
    public static final int WRITING_THREADS = 5;
    public static final int READING_THREADS = 5;
    private static final Logger log = LoggerFactory.getLogger(MassCacheTest.class);

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void massiveParallelWritingTest(Morphium morphium)  throws Exception  {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping test %s for InMemoryDriver", tstName);
            return;
        }

        log.info("\nMassive parallel writing - single creating objects");
        long start = System.currentTimeMillis();
        ArrayList<Thread> thr = new ArrayList<>();
        for (int i = 0; i < WRITING_THREADS; i++) {
            Thread t = new Thread() {

                @Override
                public void run() {
                    for (int j = 0; j < NO_OBJECTS; j++) {
                        CachedObject o = new CachedObject();
                        o.setCounter(j + 1);
                        o.setValue(getName() + " " + j);
                        log.info("Storing...");
                        morphium.store(o);
                        log.info("Stored object..." + j + ": " + getId() + " / " + getName());
                    }
                }
            };
            t.setName("Writing thread " + i);
            t.start();
            thr.add(t);

        }
        long dur = System.currentTimeMillis() - start;
        log.info("Starting threads. Took " + dur + " ms");

        //Waiting for threads
        for (Thread t : thr) {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        TestUtils.waitForWrites(morphium, log);
        log.info("Waiting for changes to be propagated...");
        dur = System.currentTimeMillis() - start;
        int goal = NO_OBJECTS * WRITING_THREADS;
        long waitStart = System.currentTimeMillis();
        long maxWaitTime = 120000; // 2 minute timeout
        while (true) {
            Thread.sleep(1500);
            long l = morphium.createQueryFor(CachedObject.class).countAll();
            log.info("Waiting for writes..." + l + "/" + goal);
            if (l == goal) break;
            if (System.currentTimeMillis() - waitStart > maxWaitTime) {
                throw new AssertionError("Timeout waiting for writes to propagate: got " + l + " of " + goal);
            }
        }
        dur = System.currentTimeMillis() - start;
        log.info("Writing took " + dur + " ms");
        log.info("Checking consistency");
        start = System.currentTimeMillis();
        for (int i = 0; i < WRITING_THREADS; i++) {
            for (int j = 0; j < NO_OBJECTS; j++) {
                //                CachedObject o = new CachedObject();
                //                o.setCounter(j + 1);
                //                o.setValue("Writing thread " + i + " " + j);
                Query<CachedObject> q = morphium.createQueryFor(CachedObject.class);
                q.f("counter").eq(j + 1).f("value").eq("Writing thread " + i + " " + j);
                List<CachedObject> lst = q.asList();

                assert (lst != null && !lst.isEmpty()) : "List is null - Thread " + i + " Element " + (j + 1) + " not found";

            }
            log.info(i + "" + "/" + WRITING_THREADS);
        }
        dur = System.currentTimeMillis() - start;
        log.info("Reading all objects took: " + dur + " ms");
        printStats(morphium);


    }

    private void printStats(Morphium morphium) {
        final Map<String, Double> statistics = morphium.getStatistics();
        for (String k : statistics.keySet()) {
            log.info(k + ": " + statistics.get(k));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void massiveParallelAccessTest(Morphium morphium) {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping test %s for InMemoryDriver", tstName);
            return;
        }
        log.info("\nMassive parallel reading & writing - single creating objects");
        long start = System.currentTimeMillis();
        ArrayList<Thread> thr = new ArrayList<>();
        for (int i = 0; i < WRITING_THREADS; i++) {
            Thread t = new Thread() {

                @Override
                public void run() {
                    for (int j = 0; j < NO_OBJECTS; j++) {
                        CachedObject o = new CachedObject();
                        o.setCounter(j + 1);
                        o.setValue(getName() + " " + j);
                        morphium.store(o);
                    }
                }
            };
            t.setName("Writing thread " + i);
            t.start();
            thr.add(t);
        }
        log.info("Waiting a bit...");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
            LoggerFactory.getLogger(MassCacheTest.class.getName()).error(ex.getMessage(), ex);
        }

        log.info("Creating reader threads (random read)...");
        for (int i = 0; i < READING_THREADS; i++) {
            Thread t = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < NO_OBJECTS * 2; j++) {
                        int rnd = (int) (Math.random() * NO_OBJECTS);
                        int trnd = (int) (Math.random() * WRITING_THREADS);
                        CachedObject o = new CachedObject();
                        o.setCounter(rnd + 1);
                        o.setValue("Writing thread " + trnd + " " + rnd);
                        Query<CachedObject> q = morphium.createQueryFor(CachedObject.class);
                        q.f("counter").eq(rnd + 1).f("value").eq("Writing thread " + trnd + " " + rnd);
                        List<CachedObject> lst = q.asList();
                        if (lst == null || lst.isEmpty()) {
                            log.info("Not written yet: " + (rnd + 1) + " Thread: " + trnd);
                        } else {
                            o = lst.get(0);
                            o.setValue(o.getValue() + " altered by Thread " + getName());
                            morphium.store(o);
                        }
                    }
                }
            };
            t.setName("alter thread " + i);
            t.start();
            thr.add(t);
        }
        //Waiting for threads
        for (Thread t : thr) {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        TestUtils.waitForWrites(morphium, log);
        long dur = System.currentTimeMillis() - start;
        log.info("Writing took " + dur + " ms\n");

        printStats(morphium);

        log.info("Test finished!");
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void disableCacheTest(Morphium morphium) {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping test %s for InMemoryDriver", tstName);
            return;
        }
        morphium.getConfig().disableReadCache();
        morphium.getConfig().disableBufferedWrites();
        morphium.resetStatistics();
        try {
            log.info("Preparing test data...");
            for (int j = 0; j < NO_OBJECTS; j++) {
                CachedObject o = new CachedObject();
                o.setCounter(j + 1);
                o.setValue("Test " + j);
                morphium.store(o);
            }
            TestUtils.waitForWrites(morphium, log);
            log.info("Done.");

            for (int j = 0; j < 3; j++) {
                for (int i = 0; i < NO_OBJECTS; i++) {
                    Query<CachedObject> q = morphium.createQueryFor(CachedObject.class);
                    q.f("value").eq("Test " + i);
                    List<CachedObject> lst = q.asList();
                    assertNotNull(lst, "List is NULL????");
                    assert (!lst.isEmpty()) : "Not found?!?!? Value: Test " + i;
                    assert (lst.get(0).getValue().equals("Test " + i)) : "Wrong value!";
                    log.info("found " + lst.size() + " elements for value: " + lst.get(0).getValue());

                }
            }
            printStats(morphium);

            Map<String, Double> statistics = morphium.getStatistics();
            assert (statistics.get("X-Entries for: resultCache|de.caluga.test.mongo.suite.data.CachedObject") == null || statistics.get("X-Entries for: resultCache|de.caluga.test.mongo.suite.data.CachedObject") == 0);
            assert (statistics.get("WRITES_CACHED") == 0);
            morphium.getConfig().enableReadCache();
            for (int j = 0; j < 3; j++) {
                for (int i = 0; i < NO_OBJECTS; i++) {
                    Query<CachedObject> q = morphium.createQueryFor(CachedObject.class);
                    q.f("value").eq("Test " + i);
                    List<CachedObject> lst = q.asList();
                    assertNotNull(lst, "List is NULL????");
                    assert (!lst.isEmpty()) : "Not found?!?!? Value: Test " + i;
                    assert (lst.get(0).getValue().equals("Test " + i)) : "Wrong value!";
                    log.info("found " + lst.size() + " elements for value: " + lst.get(0).getValue());

                }
            }
            printStats(morphium);
            statistics = morphium.getStatistics();
            assert (statistics.get("CACHE_ENTRIES") != 0);
            assert (statistics.get("X-Entries for: resultCache|de.caluga.test.mongo.suite.data.CachedObject") > 0);
            assert (statistics.get("CHITS") != 0);
        } finally {
            morphium.getConfig().enableReadCache();
            morphium.getConfig().enableBufferedWrites();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void cacheTest(Morphium morphium) throws Exception  {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping test %s for InMemoryDriver", tstName);
            return;
        }
        morphium.getCache().setValidCacheTime(CachedObject.class, 1000000);
        log.info("Preparing test data...");
        for (int j = 0; j < NO_OBJECTS; j++) {
            CachedObject o = new CachedObject();
            o.setCounter(j + 1);
            o.setValue("Test " + j);
            morphium.store(o);
        }
        Thread.sleep(1200);
        TestUtils.waitForWrites(morphium, log);
        TestUtils.waitForConditionToBecomeTrue(60000, "CachedObjects not persisted for cache test",
            () -> morphium.createQueryFor(CachedObject.class).countAll() == NO_OBJECTS);
        log.info("Done.");

        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < NO_OBJECTS; i++) {
                Query<CachedObject> q = morphium.createQueryFor(CachedObject.class);
                q.f("value").eq("Test " + i);
                List<CachedObject> lst = q.asList();
                assertNotNull(lst, "List is NULL????");
                assert (!lst.isEmpty()) : "Not found?!?!? Value: Test " + i;
                assert (lst.get(0).getValue().equals("Test " + i)) : "Wrong value!";
                log.info("found " + lst.size() + " elements for value: " + lst.get(0).getValue());

            }

        }

        printStats(morphium);
        Map<String, Double> stats = morphium.getStatistics();
        assert (stats.get("CACHE_ENTRIES") >= 100);
        assert (stats.get("CHITS") >= 200);
        assert (stats.get("CHITSPERC") >= 40);
        morphium.getCache().setDefaultCacheTime(CachedObject.class);
        morphium.getCache().clearCachefor(CachedObject.class);
    }


}
