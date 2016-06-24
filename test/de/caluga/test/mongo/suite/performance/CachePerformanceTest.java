package de.caluga.test.mongo.suite.performance;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.cache.MorphiumCache;
import de.caluga.test.mongo.suite.MongoTest;
import de.caluga.test.mongo.suite.data.CachedObject;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 11.03.16
 * Time: 22:14
 * <p>
 * TODO: Add documentation here
 */
@State(Scope.Benchmark)
public class CachePerformanceTest {

    private Morphium morphium;
    private Logger log = new Logger(CachePerformanceTest.class);


    @Setup
    public void setup() {
        try {
            if (MongoTest.morphium == null) {
                MongoTest.setUpClass();
            }
            morphium = MongoTest.morphium;
        } catch (Exception e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        }
    }

    @TearDown
    public void teardown() {
        morphium.close();
    }

    @Benchmark
    @Warmup(iterations = 2)
    @Threads(10)
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void cachePerformanceTest() throws Exception {
        MorphiumCache c = morphium.getCache();
        CachedObject o = new CachedObject();
        List<CachedObject> lst = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            c.addToCache("key " + i, CachedObject.class, lst);
        }

        log.info("cache filled...");

        for (int t = 1; t <= 500; t += 10) {
            long start = System.currentTimeMillis();
            List<Thread> thr = new ArrayList<>();
            //create Threads according to t
            for (int i = 0; i < t; i++) {
                Thread thread = new Thread(() -> {
                    for (int l = 0; l < 10000; l++) {
                        assert (c.isCached(CachedObject.class, "key " + l));
                        assert (c.getFromCache(CachedObject.class, "key " + l) != null);
                    }
                });
                thread.start();
                thr.add(thread);
            }
            for (Thread thread : thr) thread.join();
            long dur = System.currentTimeMillis() - start;
            log.info("Reading with " + t + " threads took " + dur + "ms");

        }


    }

    @Benchmark
    @Warmup(iterations = 2)
    @Threads(10)
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void arHelperPerformanceTest() throws Exception {
        AnnotationAndReflectionHelper c = morphium.getARHelper();

        c.getAllAnnotationsFromHierachy(CachedObject.class, Cache.class);
        c.getAnnotationFromHierarchy(CachedObject.class, Cache.class);
        c.isAnnotationPresentInHierarchy(CachedObject.class, Cache.class);
        c.isEntity(new CachedObject());

        for (int t = 1; t <= 250; t += 10) {
            long start = System.currentTimeMillis();
            List<Thread> thr = new ArrayList<>();
            //create Threads according to t
            for (int i = 0; i < t; i++) {
                Thread thread = new Thread(() -> {
                    for (int l = 0; l < 10000; l++) {
                        assert (c.isEntity(new CachedObject()));
                        assert (c.getAnnotationFromHierarchy(CachedObject.class, Cache.class) != null);
                    }
                });
                thread.start();
                thr.add(thread);
            }
            for (Thread thread : thr) thread.join();
            long dur = System.currentTimeMillis() - start;
            log.info("Reading with " + t + " threads took " + dur + "ms");

        }


    }


}
