package de.caluga.test.mongo.suite.performance;/**
 * Created by stephan on 04.04.16.
 */

import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.PrefetchingMorphiumIterator;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.MongoTest;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.openjdk.jmh.annotations.*;


/**
 * TODO: Add Documentation here
 **/
@State(Scope.Benchmark)
public class IteratorSpeedTest {
    private Logger log = new Logger(IteratorSpeedTest.class);
    private Morphium morphium;

    @Setup
    public void setup() {
        try {
            if (morphium == null) {
                MongoTest.setUpClass();
            }
            morphium = MongoTest.morphiumMeta;
            for (int i = 0; i < 10000; i++) {
                UncachedObject uc = new UncachedObject();
                uc.setCounter(i);
                uc.setValue("V-" + i);
                morphium.store(uc);
            }
        } catch (Exception e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        }
    }

    @TearDown
    public void teardown() {
        morphium.dropCollection(UncachedObject.class);

        //        morphium.close();
        try {
            MongoTest.getMorphiums().forEach(Morphium::close);
        } catch (Exception e) {
        }
    }

    @Benchmark
    @Warmup(iterations = 2)
    @Threads(1)
    @BenchmarkMode({Mode.AverageTime})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void defaultIteratorTest() throws Exception {
        log.info("Running default iterator test");
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).sort("counter");
        int sum = 0;
        for (UncachedObject o : q.asIterable()) {
            //running through it
            //calculating sum
            sum += o.getCounter();
        }
        log.info("Sum =" + sum);
    }

    @Benchmark
    @Warmup(iterations = 2)
    @Threads(1)
    @BenchmarkMode({Mode.AverageTime})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void prefetchingIteratorTest() throws Exception {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).sort("counter");
        int sum = 0;
        for (UncachedObject o : q.asIterable(1000, 5)) {
            //running through it
            //calculating sum
            sum += o.getCounter();
        }
    }

    @Benchmark
    @Warmup(iterations = 2)
    @Threads(1)
    @BenchmarkMode({Mode.AverageTime})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void bufferedIteratorTest() throws Exception {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).sort("counter");
        int sum = 0;
        for (UncachedObject o : q.asIterable(1000)) {
            //running through it
            //calculating sum
            sum += o.getCounter();
        }
    }

    @Benchmark
    @Warmup(iterations = 2)
    @Threads(1)
    @BenchmarkMode({Mode.AverageTime})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void oldPrefetchingIteratorTest() throws Exception {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).sort("counter");
        int sum = 0;
        for (UncachedObject o : q.asIterable(1000, new PrefetchingMorphiumIterator<>())) {
            //running through it
            //calculating sum
            sum += o.getCounter();
        }
    }
}
