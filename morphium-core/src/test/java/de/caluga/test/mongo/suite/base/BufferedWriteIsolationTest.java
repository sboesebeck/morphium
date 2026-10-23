package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-059: the buffered writer flushes one bulk per collection. A collection whose bulk is rejected must not keep the others from being written, and the callbacks of the
 * rejected entries hear about the error.
 */
@Tag("core")
public class BufferedWriteIsolationTest extends MultiDriverTestBase {

    @Entity
    @WriteBuffer(size = 100, timeout = 300)
    public static class BufA {
        @Id
        public String id;
        public String code;
    }

    @Entity
    @WriteBuffer(size = 100, timeout = 300)
    public static class BufB {
        @Id
        public String id;
        public int counter;
    }

    private static class Cb<T> implements AsyncOperationCallback<T> {
        final AtomicInteger errors = new AtomicInteger();
        final AtomicInteger successes = new AtomicInteger();
        final AtomicReference<Throwable> error = new AtomicReference<>();

        @Override
        public void onOperationSucceeded(AsyncOperationType type, Query<T> q, long duration, List<T> result, T entity, Object... param) {
            successes.incrementAndGet();
        }

        @Override
        public void onOperationError(AsyncOperationType type, Query<T> q, long duration, String message, Throwable t, T entity, Object... param) {
            errors.incrementAndGet();
            error.set(t);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aRejectedCollectionDoesNotKeepTheOthersFromBeingWritten(Morphium m) throws Exception {
        try (m) {
            m.dropCollection(BufA.class);
            m.dropCollection(BufB.class);

            for (int i = 0; i < 2; i++) {
                BufA a = new BufA();
                a.id = "a" + i;
                a.code = "c" + i;
                m.store(a);
            }

            BufB b = new BufB();
            b.id = "b0";
            m.store(b);
            TestUtils.waitForConditionToBecomeTrue(30000, "initial documents not written",
                () -> m.createQueryFor(BufA.class).countAll() == 2 && m.createQueryFor(BufB.class).countAll() == 1);
            // after the first flush: the buffered writer creates collections and indexes on its first write
            m.ensureIndex(BufA.class, m.getMapper().getCollectionName(BufA.class), Map.of("code", 1), Map.of("unique", true));
            assertTrue(m.getIndexesFromMongo(BufA.class).stream().anyMatch(i -> Boolean.TRUE.equals(i.getUnique())), "the unique index exists");

            Cb<BufA> cbA = new Cb<>();
            Cb<BufB> cbB = new Cb<>();
            // both writes are queued and flushed together; the first one violates the unique index of its collection
            m.createQueryFor(BufA.class).f("id").eq("a0").set("code", "c1", false, false, cbA);
            m.createQueryFor(BufB.class).f("id").eq("b0").set("counter", 5, false, false, cbB);

            TestUtils.waitForConditionToBecomeTrue(30000, "the second collection was not written",
                () -> m.createQueryFor(BufB.class).f("id").eq("b0").get().counter == 5);
            TestUtils.waitForConditionToBecomeTrue(30000, "the callback of the rejected write did not hear about the error", () -> cbA.errors.get() > 0);

            assertEquals("c0", m.createQueryFor(BufA.class).f("id").eq("a0").get().code, "the rejected write changed nothing");
            assertTrue(cbA.error.get() instanceof MorphiumDriverException, String.valueOf(cbA.error.get()));
            assertTrue(((MorphiumDriverException) cbA.error.get()).isDuplicateKey());
            assertEquals(0, cbB.errors.get());
            assertNull(cbB.error.get());
        }
    }
}
