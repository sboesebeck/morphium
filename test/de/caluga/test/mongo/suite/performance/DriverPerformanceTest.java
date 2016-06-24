package de.caluga.test.mongo.suite.performance;/**
 * Created by stephan on 28.04.16.
 */

import de.caluga.test.mongo.suite.MongoTest;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.openjdk.jmh.annotations.*;

/**
 * TODO: Add Documentation here
 **/
@State(Scope.Benchmark)
public class DriverPerformanceTest extends MongoTest {

    private final static int numToRead = 100;

    @Setup
    public void preFill() throws Exception {
        MongoTest.setUpClass();
        log.info("Set up class.... storing data");
        for (int i = 0; i < numToRead; i++) {
            UncachedObject o = new UncachedObject();
            o.setValue("V - " + i);
            o.setCounter(i);
            morphium.store(o);
            morphiumInMemeory.store(o);
        }
        log.info("Preparation done....");
    }

    @TearDown
    public void teardown() {
        //        log.info("Tearing down things...");
        //        if (Thread.currentThread().isInterrupted()) log.error("Already interrupted!");
        //        try {
        ////            Thread.sleep(3000);
        //            for (Morphium m:getMorphiums()){
        //                m.close();
        //                if (Thread.currentThread().isInterrupted()){
        //                    log.error("Stopping morphium "+m.getClass().getName()+" interrupts current thread!");
        //                }
        //            }
        //        } catch (Exception e) {
        //
        //        }
    }

    @Benchmark
    @Warmup(iterations = 2)
    @Threads(15)
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void inMemoryMTTest() {
        try {
            for (int i = 0; i < numToRead; i++) {
                //            if (i % 500 == 0)
                //                log.info("Thread: " + Thread.currentThread().getId() + " " + i);
                UncachedObject u = morphiumInMemeory.createQueryFor(UncachedObject.class).f("counter").eq(i).get();
                assert (u.getCounter() == i);
            }
        } catch (Exception e) {
            log.error("Got an Exception...", e);
        }
    }

    @Benchmark
    @Warmup(iterations = 2)
    @Threads(1)
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void inMemorySTTest() {
        try {
            for (int i = 0; i < numToRead; i++) {
                //            if (i % 500 == 0)
                //                log.info("Thread: " + Thread.currentThread().getId() + " " + i);
                UncachedObject u = morphiumInMemeory.createQueryFor(UncachedObject.class).f("counter").eq(i).get();
                assert (u.getCounter() == i);
            }
        } catch (Exception e) {
            log.error("Got an Exception...", e);
        }
    }

    @Benchmark
    @Warmup(iterations = 2)
    @Threads(15)
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void metaMTTest() {
        try {
            for (int i = 0; i < numToRead; i++) {
                //            if (i % 500 == 0)
                //                log.info("Thread: " + Thread.currentThread().getId() + " " + i);
                UncachedObject u = morphiumMeta.createQueryFor(UncachedObject.class).f("counter").eq(i).get();
                assert (u.getCounter() == i);
            }
        } catch (Exception e) {
            log.error("Got an Exception...", e);
        }
    }

    @Benchmark
    @Warmup(iterations = 2)
    @Threads(1)
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void metaySTTest() {
        try {
            for (int i = 0; i < numToRead; i++) {
                //            if (i % 500 == 0)
                //                log.info("Thread: " + Thread.currentThread().getId() + " " + i);
                UncachedObject u = morphiumMeta.createQueryFor(UncachedObject.class).f("counter").eq(i).get();
                assert (u.getCounter() == i);
            }
        } catch (Exception e) {
            log.error("Got an Exception...", e);
        }
    }


    @Benchmark
    @Warmup(iterations = 2)
    @Threads(15)
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void mongodbMTTest() {
        try {
            for (int i = 0; i < numToRead; i++) {
                //            if (i % 500 == 0)
                //                log.info("Thread: " + Thread.currentThread().getId() + " " + i);
                UncachedObject u = morphiumMongodb.createQueryFor(UncachedObject.class).f("counter").eq(i).get();
                assert (u.getCounter() == i);
            }
        } catch (Exception e) {
            log.error("Got an Exception...", e);
        }
    }

    @Benchmark
    @Warmup(iterations = 2)
    @Threads(1)
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void mongodbSTTest() {
        try {
            for (int i = 0; i < numToRead; i++) {
                //            if (i % 500 == 0)
                //                log.info("Thread: " + Thread.currentThread().getId() + " " + i);
                UncachedObject u = morphiumMongodb.createQueryFor(UncachedObject.class).f("counter").eq(i).get();
                assert (u.getCounter() == i);
            }
        } catch (Exception e) {
            log.error("Got an Exception...", e);
        }
    }

    @Benchmark
    @Warmup(iterations = 2)
    @Threads(15)
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void singleConnectThrMTTest() {
        try {
            for (int i = 0; i < numToRead; i++) {
                //            if (i % 500 == 0)
                //                log.info("Thread: " + Thread.currentThread().getId() + " " + i);
                UncachedObject u = morphiumSingleConnectThreadded.createQueryFor(UncachedObject.class).f("counter").eq(i).get();
                assert (u.getCounter() == i);
            }
        } catch (Exception e) {
            log.error("Got an Exception...", e);
        }
    }

    @Benchmark
    @Warmup(iterations = 2)
    @Threads(1)
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void singleConnectThrSTTest() {
        try {
            for (int i = 0; i < numToRead; i++) {
                //            if (i % 500 == 0)
                //                log.info("Thread: " + Thread.currentThread().getId() + " " + i);
                UncachedObject u = morphiumSingleConnectThreadded.createQueryFor(UncachedObject.class).f("counter").eq(i).get();
                assert (u.getCounter() == i);
            }
        } catch (Exception e) {
            log.error("Got an Exception...", e);
        }
    }
}
