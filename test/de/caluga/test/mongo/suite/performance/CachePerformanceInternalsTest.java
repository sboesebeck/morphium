package de.caluga.test.mongo.suite.performance;

import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

/**
 * User: Stephan Bösebeck
 * Date: 11.03.16
 * Time: 22:32
 * <p>
 * TODO: Add documentation here
 */
@State(Scope.Benchmark)
public class CachePerformanceInternalsTest {
    private List<String> synchronizedList = Collections.synchronizedList(new ArrayList<>());
    private List<String> vector = new Vector();


    @Setup
    public void preFill() {
        writeVectorTest();
        writeSynchronizedList();
    }

    @Benchmark
    @Warmup(iterations = 2)
    @Threads(100)
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void writeVectorTest() {
        for (int i = 0; i < 10000; i++) {
            vector.add("tst " + i);
        }
    }

    @Benchmark
    @Warmup(iterations = 2)
    @Threads(100)
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void writeSynchronizedList() {
        for (int i = 0; i < 10000; i++) {
            synchronizedList.add("tst " + i);
        }
    }

    @Benchmark
    @Warmup(iterations = 2)
    @Threads(100)
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void zReadVectorTest() {
        for (String s : vector) {
            //just zooming through
            int l = s.length();
        }
    }

    @Benchmark
    @Warmup(iterations = 2)
    @Threads(100)
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void zReadSynchronizedListTest() {
        for (String s : synchronizedList) {
            //just zooming through
            int l = s.length();
        }
    }
}
