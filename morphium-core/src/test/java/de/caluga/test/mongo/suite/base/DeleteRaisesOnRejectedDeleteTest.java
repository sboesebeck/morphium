package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.SafetyLevel;
import de.caluga.morphium.annotations.WriteSafety;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.WriteConcernNotSatisfiedException;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * MOR-065: the {@code delete} command reports a statement that the server rejects (an unknown query operator, an invalid collation) in {@code writeErrors} of its reply
 * and a delete that missed its write concern in {@code writeConcernError}, exactly like {@code update}. {@code Query.remove} and {@code Morphium.remove} used to ignore the
 * reply; they raise a {@link MorphiumDriverException} now, with the same switch and the same per-query override as the update paths.
 */
@Tag("core")
public class DeleteRaisesOnRejectedDeleteTest extends MultiDriverTestBase {

    @Entity
    public static class Doc {
        @Id
        public String id;
        public String code;
    }

    @Entity
    @WriteSafety(level = SafetyLevel.WAIT_FOR_SLAVE, timeout = 300)
    public static class Guarded {
        @Id
        public String id;
        public String code;
    }

    private void setup(Morphium m) {
        m.dropCollection(Doc.class);

        for (int i = 0; i < 3; i++) {
            Doc d = new Doc();
            d.id = "d" + i;
            d.code = "c" + i;
            m.store(d);
        }
    }

    /** a statement that every server rejects at parse time: code 2, "unknown operator" */
    private Query<Doc> rejected(Morphium m) {
        return m.createQueryFor(Doc.class).rawQuery(Map.of("code", Map.of("$badOperator", 1)));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aRejectedQueryRemoveRaisesAndDeletesNothing(Morphium m) throws Exception {
        try (m) {
            setup(m);
            MorphiumDriverException e = assertThrows(MorphiumDriverException.class, () -> rejected(m).remove());
            assertEquals(2, ((Number) e.getMongoCode()).intValue(), String.valueOf(e));
            assertEquals(3, m.createQueryFor(Doc.class).countAll(), "nothing was deleted");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aRejectedMorphiumRemoveOfAQueryRaises(Morphium m) throws Exception {
        try (m) {
            setup(m);
            assertThrows(MorphiumDriverException.class, () -> m.remove(rejected(m)));
            assertEquals(3, m.createQueryFor(Doc.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anAsyncCallbackHearsAboutItOnce(Morphium m) throws Exception {
        try (m) {
            setup(m);
            AtomicInteger errors = new AtomicInteger();
            AtomicInteger successes = new AtomicInteger();
            AtomicReference<Throwable> error = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            m.remove(rejected(m), new AsyncOperationCallback<Doc>() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query<Doc> q, long duration, List<Doc> result, Doc entity, Object... param) {
                    successes.incrementAndGet();
                    done.countDown();
                }

                @Override
                public void onOperationError(AsyncOperationType type, Query<Doc> q, long duration, String message, Throwable t, Doc entity, Object... param) {
                    errors.incrementAndGet();
                    error.set(t);
                    done.countDown();
                }
            });
            assertTrue(done.await(30, TimeUnit.SECONDS));
            assertEquals(1, errors.get());
            assertEquals(0, successes.get());
            assertTrue(error.get() instanceof MorphiumDriverException, String.valueOf(error.get()));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void thePerQueryOverrideBringsTheOldBehaviourBack(Morphium m) throws Exception {
        try (m) {
            setup(m);
            Map<String, Object> reply = rejected(m).setRaiseOnWriteErrors(false).remove();
            assertEquals(3, m.createQueryFor(Doc.class).countAll());
            assertNotNull(reply);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theQueryOverrideBringsTheOldBehaviourBack(Morphium m) throws Exception {
        try (m) {
            setup(m);
            rejected(m).setRaiseOnWriteErrors(false).remove();
            assertEquals(3, m.createQueryFor(Doc.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aSuccessfulRemoveStillWorks(Morphium m) throws Exception {
        try (m) {
            setup(m);
            m.createQueryFor(Doc.class).f("code").eq("c0").remove();
            Doc d = m.createQueryFor(Doc.class).f("id").eq("d1").get();
            m.remove(d);
            m.remove(new java.util.ArrayList<>(List.of(m.createQueryFor(Doc.class).f("id").eq("d2").get())), m.getMapper().getCollectionName(Doc.class));
            assertEquals(0, m.createQueryFor(Doc.class).countAll());
        }
    }

    private void setupGuarded(Morphium m) {
        assumeTrue(m.getDriver().isReplicaSet(), "needs a replica set");
        assumeTrue(m.getDriver().getHostSeed().size() < 2, "the replica set must not be able to satisfy w:2");
        m.dropCollection(Guarded.class);

        for (int i = 0; i < 2; i++) {
            Guarded g = new Guarded();
            g.id = "g" + i;
            g.code = "c" + i;

            try {
                m.store(g);
            } catch (MorphiumDriverException e) {
                // the insert has the same problem; the document is there
            }
        }

        assertEquals(2, m.createQueryFor(Guarded.class).asList().size(), "the setup stored both documents");
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aQueryRemoveThatMissesItsWriteConcernRaisesTheSubtypeAndTheDataIsDeleted(Morphium m) throws Exception {
        try (m) {
            setupGuarded(m);
            WriteConcernNotSatisfiedException e = assertThrows(WriteConcernNotSatisfiedException.class, () -> m.createQueryFor(Guarded.class).f("id").eq("g0").remove());
            assertTrue(e.getMessage().contains("WAS APPLIED"), e.getMessage());
            assertEquals(1, m.createQueryFor(Guarded.class).asList().size(), "the delete is applied");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anEntityRemoveThatMissesItsWriteConcernRaisesTheSubtypeToo(Morphium m) throws Exception {
        try (m) {
            setupGuarded(m);
            Guarded g = m.createQueryFor(Guarded.class).f("id").eq("g1").get();
            assertThrows(WriteConcernNotSatisfiedException.class, () -> m.remove(g));
            assertEquals(1, m.createQueryFor(Guarded.class).asList().size(), "the delete is applied");
        }
    }
}
