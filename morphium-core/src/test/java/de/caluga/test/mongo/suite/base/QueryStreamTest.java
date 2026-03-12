package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@Tag("core")
public class QueryStreamTest extends MultiDriverTestBase {

    private void createTestData(Morphium morphium, int count) {
        for (int i = 1; i <= count; i++) {
            UncachedObject o = new UncachedObject("value_" + i, i);
            morphium.store(o);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    void streamReturnsAllElements(Morphium morphium) {
        createTestData(morphium, 20);

        try (Stream<UncachedObject> s = morphium.createQueryFor(UncachedObject.class).stream()) {
            long count = s.count();
            assertEquals(20, count);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    void streamWithFilterAndMap(Morphium morphium) {
        createTestData(morphium, 20);

        try (Stream<UncachedObject> s = morphium.createQueryFor(UncachedObject.class).stream()) {
            List<String> names = s
                .filter(u -> u.getCounter() > 15)
                .map(UncachedObject::getStrValue)
                .toList();
            assertEquals(5, names.size());
            assertTrue(names.contains("value_16"));
            assertTrue(names.contains("value_20"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    void streamWithQueryModifiers(Morphium morphium) {
        createTestData(morphium, 50);

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.f(UncachedObject.Fields.counter).gte(10).f(UncachedObject.Fields.counter).lte(20);
        q.sort(UncachedObject.Fields.counter);

        try (Stream<UncachedObject> s = q.stream()) {
            List<Integer> counters = s.map(UncachedObject::getCounter).toList();
            assertEquals(11, counters.size());
            assertEquals(10, counters.get(0));
            assertEquals(20, counters.get(counters.size() - 1));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    void streamWithBatchSize(Morphium morphium) {
        createTestData(morphium, 30);

        try (Stream<UncachedObject> s = morphium.createQueryFor(UncachedObject.class).stream(5)) {
            long count = s.count();
            assertEquals(30, count);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    void streamEmptyResult(Morphium morphium) {
        // No data stored — stream should be empty
        try (Stream<UncachedObject> s = morphium.createQueryFor(UncachedObject.class).stream()) {
            assertEquals(0, s.count());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    void streamFindFirst(Morphium morphium) {
        createTestData(morphium, 10);

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.sort(UncachedObject.Fields.counter);

        try (Stream<UncachedObject> s = q.stream()) {
            var first = s.findFirst();
            assertTrue(first.isPresent());
            assertEquals(1, first.get().getCounter());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    void streamWithLimit(Morphium morphium) {
        createTestData(morphium, 50);

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.sort(UncachedObject.Fields.counter);
        q.limit(10);

        try (Stream<UncachedObject> s = q.stream()) {
            List<Integer> counters = s.map(UncachedObject::getCounter).toList();
            assertEquals(10, counters.size());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    void streamCloseWithoutConsuming(Morphium morphium) {
        createTestData(morphium, 10);

        // Creating and immediately closing a stream must not throw
        try (Stream<UncachedObject> s = morphium.createQueryFor(UncachedObject.class).stream()) {
            // intentionally not consuming
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    void streamDoubleClose(Morphium morphium) {
        createTestData(morphium, 5);

        Stream<UncachedObject> s = morphium.createQueryFor(UncachedObject.class).stream();
        // Consume fully, then close twice — must not throw
        long count = s.count();
        assertEquals(5, count);
        s.close();
    }
}
