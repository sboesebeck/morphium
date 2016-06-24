/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.test.mongo.suite;

import de.caluga.morphium.Logger;
import de.caluga.morphium.ProfilingListener;
import de.caluga.morphium.ReadAccessType;
import de.caluga.morphium.WriteAccessType;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author stephan
 */
public class MassCacheTest extends MongoTest {

    public static final int NO_OBJECTS = 100;
    public static final int WRITING_THREADS = 5;
    public static final int READING_THREADS = 5;
    private static final Logger log = new Logger(MassCacheTest.class);

    @Test
    public void massiveParallelWritingTest() throws InterruptedException {

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
                        log.info("Stored object..." + getId() + " / " + getName());
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

        waitForWrites();

        dur = System.currentTimeMillis() - start;
        log.info("Writing took " + dur + " ms");
        Thread.sleep(2500);
        log.info("Checking consistency");
        start = System.currentTimeMillis();
        for (int i = 0; i < WRITING_THREADS; i++) {
            for (int j = 0; j < NO_OBJECTS; j++) {
                //                CachedObject o = new CachedObject();
                //                o.setCounter(j + 1);
                //                o.setValue("Writing thread " + i + " " + j);
                Query<CachedObject> q = morphium.createQueryFor(CachedObject.class);
                q.f("counter").eq(j + 1).f("value").eq("Writing thread " + i + " " + j);
                List<CachedObject> lst = morphium.find(q);

                assert (lst != null && !lst.isEmpty()) : "List is null - Thread " + i + " Element " + (j + 1) + " not found";

            }
            log.info(i + "" + "/" + WRITING_THREADS);
        }
        dur = System.currentTimeMillis() - start;
        log.info("Reading all objects took: " + dur + " ms");
        printStats();


    }

    private void printStats() {
        final Map<String, Double> statistics = morphium.getStatistics();
        for (String k : statistics.keySet()) {
            log.info(k + ": " + statistics.get(k));
        }
    }

    @Test
    public void massiveParallelAccessTest() {
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
            new Logger(MassCacheTest.class.getName()).fatal(ex);
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
                        List<CachedObject> lst = morphium.find(q);
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
        waitForWrites();
        long dur = System.currentTimeMillis() - start;
        log.info("Writing took " + dur + " ms\n");

        printStats();

        log.info("Test finished!");
    }


    @Test
    public void disableCacheTest() {
        morphium.getConfig().disableReadCache();
        morphium.getConfig().disableBufferedWrites();
        morphium.resetStatistics();
        log.info("Preparing test data...");
        for (int j = 0; j < NO_OBJECTS; j++) {
            CachedObject o = new CachedObject();
            o.setCounter(j + 1);
            o.setValue("Test " + j);
            morphium.store(o);
        }
        waitForWrites();
        log.info("Done.");

        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < NO_OBJECTS; i++) {
                Query<CachedObject> q = morphium.createQueryFor(CachedObject.class);
                q.f("value").eq("Test " + i);
                List<CachedObject> lst = q.asList();
                assert (lst != null) : "List is NULL????";
                assert (!lst.isEmpty()) : "Not found?!?!? Value: Test " + i;
                assert (lst.get(0).getValue().equals("Test " + i)) : "Wrong value!";
                log.info("found " + lst.size() + " elements for value: " + lst.get(0).getValue());

            }
        }
        printStats();

        Map<String, Double> statistics = morphium.getStatistics();
        assert (statistics.get("CACHE_ENTRIES") == 0);
        assert (statistics.get("WRITES_CACHED") == 0);
        morphium.getConfig().enableReadCache();
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < NO_OBJECTS; i++) {
                Query<CachedObject> q = morphium.createQueryFor(CachedObject.class);
                q.f("value").eq("Test " + i);
                List<CachedObject> lst = q.asList();
                assert (lst != null) : "List is NULL????";
                assert (!lst.isEmpty()) : "Not found?!?!? Value: Test " + i;
                assert (lst.get(0).getValue().equals("Test " + i)) : "Wrong value!";
                log.info("found " + lst.size() + " elements for value: " + lst.get(0).getValue());

            }
        }
        printStats();
        statistics = morphium.getStatistics();
        assert (statistics.get("CACHE_ENTRIES") != 0);
        assert (statistics.get("CHITS") != 0);
        morphium.getConfig().enableReadCache();
        morphium.getConfig().enableBufferedWrites();
    }

    @Test
    public void cacheTest() throws Exception {
        morphium.getCache().setValidCacheTime(CachedObject.class, 1000000);
        log.info("Preparing test data...");
        for (int j = 0; j < NO_OBJECTS; j++) {
            CachedObject o = new CachedObject();
            o.setCounter(j + 1);
            o.setValue("Test " + j);
            morphium.store(o);
        }
        Thread.sleep(1200);
        waitForWrites();
        Thread.sleep(25000);
        log.info("Done.");
        ProfilingListener pl = new ProfilingListener() {
            @Override
            public void readAccess(Query query, long time, ReadAccessType t) {
                log.info("Read Access...");
                //Should never be called as cache hits won't trigger profiling
            }

            @Override
            public void writeAccess(Class type, Object o, long time, boolean isNew, WriteAccessType t) {
                log.info("Write access...");
            }
        };

        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < NO_OBJECTS; i++) {
                Query<CachedObject> q = morphium.createQueryFor(CachedObject.class);
                q.f("value").eq("Test " + i);
                List<CachedObject> lst = q.asList();
                assert (lst != null) : "List is NULL????";
                assert (!lst.isEmpty()) : "Not found?!?!? Value: Test " + i;
                assert (lst.get(0).getValue().equals("Test " + i)) : "Wrong value!";
                log.info("found " + lst.size() + " elements for value: " + lst.get(0).getValue());

            }
            morphium.removeProfilingListener(pl);
            morphium.addProfilingListener(pl);

        }
        morphium.removeProfilingListener(pl);

        printStats();
        Map<String, Double> stats = morphium.getStatistics();
        assert (stats.get("CACHE_ENTRIES") >= 100);
        assert (stats.get("CHITS") >= 200);
        assert (stats.get("CHITSPERC") >= 40);
        morphium.getCache().setDefaultCacheTime(CachedObject.class);
        morphium.getCache().clearCachefor(CachedObject.class);
    }


}
