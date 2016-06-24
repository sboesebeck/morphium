package de.caluga.test.mongo.suite.performance;/**
 * Created by stephan on 21.10.15.
 */

import de.caluga.morphium.Morphium;
import de.caluga.test.mongo.suite.MongoTest;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO: Add Documentation here
 **/
@State(Scope.Benchmark)
public class BasicReadWriteTest {
    private Morphium morphium;


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
    @Warmup(iterations = 1)
    @Threads(1)
    @BenchmarkMode({Mode.SampleTime, Mode.All, Mode.AverageTime, Mode.Throughput})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void basicWriteTestAutoCappingEnabled() {

        morphium.getConfig().setAutoIndexAndCappedCreationOnWrite(true);
        doWriteTest();

    }

    @Benchmark
    @Warmup(iterations = 1)
    @Threads(1)
    @BenchmarkMode({Mode.SampleTime, Mode.All, Mode.AverageTime, Mode.Throughput})
    @OperationsPerInvocation()
    @Fork(1)
    @Measurement(iterations = 5, time = -1)
    public void basicWriteTestAutoCappingDisabled() {

        morphium.getConfig().setAutoIndexAndCappedCreationOnWrite(false);
        doWriteTest();
        morphium.getConfig().setAutoIndexAndCappedCreationOnWrite(true);

    }


    public void doWriteTest() {
        morphium.dropCollection(UncachedObject.class);
        for (int i = 0; i <
                1000; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i);
            uc.setValue("V" + i);
            morphium.store(uc);
        }

        for (int i = 0; i < 1000; i++) {
            UncachedObject uc = new UncachedObject();
            int c = (int) (Math.random() * 100000.0);
            uc.setCounter(c);
            uc.setValue("V" + c);
            morphium.store(uc);

        }


        List<UncachedObject> buffer = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            UncachedObject uc = new UncachedObject();
            int c = (int) (Math.random() * 100000.0);
            uc.setCounter(c);
            uc.setValue("n" + c);
            buffer.add(uc);
        }
        morphium.storeList(buffer);
    }
}
