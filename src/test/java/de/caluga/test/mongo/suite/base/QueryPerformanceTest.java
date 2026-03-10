package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;

@Tag("core")
public class QueryPerformanceTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void speedTest(Morphium morphium) throws Exception {
        int numThr = 100;
        List<Thread> threads = new ArrayList<>();
        long start = System.currentTimeMillis();

        for (int t = 0; t < numThr; t++) {
            Thread thread = new Thread(() -> {
                for (int i = 0; i < 100000; i++) {
                    Query q = morphium.createQueryFor(CachedObject.class);
                }
            });
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        threads.clear();
        long dur = System.currentTimeMillis() - start;
        log.info("Creating the query with " + numThr + " threads took " + dur + "ms");
        start = System.currentTimeMillis();

        for (int t = 0; t < numThr; t++) {
            Thread thread = new Thread(() -> {
                for (int i = 0; i < 100000; i++) {
                    morphium.createQueryFor(CachedObject.class).f("counter");
                }
            });
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        threads.clear();
        dur = System.currentTimeMillis() - start;
        log.info("Creating the query+field with " + numThr + " threads took " + dur + "ms");
        start = System.currentTimeMillis();

        for (int t = 0; t < numThr; t++) {
            Thread thread = new Thread(() -> {
                for (int i = 0; i < 100000; i++) {
                    morphium.createQueryFor(CachedObject.class).f("counter").eq(109);
                }
            });
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        threads.clear();
        dur = System.currentTimeMillis() - start;
        log.info("Creating the query+field+op with " + numThr + " threads took " + dur + "ms");
    }
}
