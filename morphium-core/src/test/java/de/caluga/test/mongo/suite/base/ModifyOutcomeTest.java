package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.query.ReturnDocument;
import de.caluga.morphium.query.UpdateOutcome;
import de.caluga.morphium.query.UpdateOutcome.Kind;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Lease;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Plain;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * MOR-010: {@code UpdateBuilder.tryFindOneAndUpdate(ReturnDocument)} returns a typed {@link UpdateOutcome} (inserted, updated, not matched, duplicate key)
 * instead of a document that may be {@code null} for three different reasons and an exception that has to be classified by its code.
 */
@Tag("core")
public class ModifyOutcomeTest extends MultiDriverTestBase {

    private static final Instant NOW = Instant.parse("2026-10-15T12:00:00Z");

    private Query<Lease> leaseFilter(Morphium m, String id, String me, Instant now) {
        Query<Lease> q = m.createQueryFor(Lease.class).f("id").eq(id);
        q.or(m.createQueryFor(Lease.class).f("expiresAt").lt(now), m.createQueryFor(Lease.class).f("ownerName").eq(me));
        return q;
    }

    private UpdateOutcome<Lease> tryAcquire(Morphium m, String id, String me, Instant now, ReturnDocument which) {
        return leaseFilter(m, id, me, now).modify().set("ownerName", me).set("expiresAt", now.plusSeconds(60)).upsert().tryFindOneAndUpdate(which);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anInsertWithAfterReturnsTheNewDocumentAndTheUpsertedId(Morphium m) throws Exception {
        try (m) {
            UpdateOutcome<Lease> o = tryAcquire(m, "job", "A", NOW, ReturnDocument.AFTER);
            assertEquals(Kind.INSERTED, o.getKind());
            assertTrue(o.wasApplied() && o.wasInserted() && !o.isDuplicateKey());
            assertNotNull(o.getDocument());
            assertEquals("A", o.getDocument().ownerName);
            assertEquals("job", o.getDocument().id);
            assertEquals("job", o.getUpsertedId());
            assertEquals(1L, o.getDocument().entityVersion);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anInsertWithBeforeHasNoDocumentButIsStillTold(Morphium m) throws Exception {
        try (m) {
            UpdateOutcome<Lease> o = tryAcquire(m, "job", "A", NOW, ReturnDocument.BEFORE);
            assertEquals(Kind.INSERTED, o.getKind(), "the plain findOneAndUpdate would return null here, like for no match");
            assertNull(o.getDocument(), "an insert has no pre-image");
            assertEquals("job", o.getUpsertedId());
            assertEquals("A", m.createQueryFor(Lease.class).f("id").eq("job").get().ownerName);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anUpdateOfAnExistingDocumentIsUpdatedForBothReturnDocuments(Morphium m) throws Exception {
        try (m) {
            tryAcquire(m, "job", "A", NOW, ReturnDocument.AFTER);
            UpdateOutcome<Lease> renewed = tryAcquire(m, "job", "A", NOW.plusSeconds(10), ReturnDocument.AFTER);
            assertEquals(Kind.UPDATED, renewed.getKind());
            assertTrue(renewed.wasApplied() && !renewed.wasInserted());
            assertNull(renewed.getUpsertedId());
            assertEquals(NOW.plusSeconds(70), renewed.getDocument().expiresAt);
            assertEquals(2L, renewed.getDocument().entityVersion);

            UpdateOutcome<Lease> before = tryAcquire(m, "job", "A", NOW.plusSeconds(20), ReturnDocument.BEFORE);
            assertEquals(Kind.UPDATED, before.getKind());
            assertEquals(NOW.plusSeconds(70), before.getDocument().expiresAt, "the state before the update");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void noMatchWithoutUpsertIsNotMatchedAndWritesNothing(Morphium m) throws Exception {
        try (m) {
            UpdateOutcome<Plain> o = m.createQueryFor(Plain.class).f("key").eq("none").modify().set("payload", "x").tryFindOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(Kind.NOT_MATCHED, o.getKind());
            assertFalse(o.wasApplied());
            assertNull(o.getDocument());
            assertNull(o.getUpsertedId());
            assertEquals(0, m.createQueryFor(Plain.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aLostConditionalUpsertIsADuplicateKeyOutcomeWithoutAnyCodeInspection(Morphium m) throws Exception {
        try (m) {
            assertEquals(Kind.INSERTED, tryAcquire(m, "job", "A", NOW, ReturnDocument.AFTER).getKind());

            UpdateOutcome<Lease> held = tryAcquire(m, "job", "B", NOW.plusSeconds(10), ReturnDocument.AFTER);
            assertEquals(Kind.DUPLICATE_KEY, held.getKind());
            assertTrue(held.isDuplicateKey());
            assertFalse(held.wasApplied());
            assertNull(held.getDocument());
            assertNull(held.getUpsertedId());

            Lease stored = m.createQueryFor(Lease.class).f("id").eq("job").get();
            assertEquals("A", stored.ownerName, "the holder is untouched");
            assertEquals(1L, stored.entityVersion);
            assertEquals(1, m.createQueryFor(Lease.class).countAll());

            assertEquals(Kind.DUPLICATE_KEY, tryAcquire(m, "job", "B", NOW.plusSeconds(10), ReturnDocument.BEFORE).getKind(), "also with BEFORE");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anExpiredLeaseCanBeTakenOverAndIsReportedAsUpdated(Morphium m) throws Exception {
        try (m) {
            tryAcquire(m, "job", "A", NOW, ReturnDocument.AFTER);
            UpdateOutcome<Lease> taken = tryAcquire(m, "job", "B", NOW.plusSeconds(120), ReturnDocument.AFTER);
            assertEquals(Kind.UPDATED, taken.getKind());
            assertEquals("B", taken.getDocument().ownerName);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aDuplicateKeyWithoutUpsertIsNotTheLostInsertOfThisCallAndIsRethrown(Morphium m) throws Exception {
        try (m) {
            for (String id : List.of("p1", "p2")) {
                Plain p = new Plain();
                p.id = id;
                p.key = "k";
                p.payload = "distinct-" + id;
                m.store(p);
            }

            m.ensureIndex(Plain.class, m.getMapper().getCollectionName(Plain.class), java.util.Map.of("payload", 1), java.util.Map.of("unique", true));
            assertTrue(m.getIndexesFromMongo(Plain.class).stream().anyMatch(i -> Boolean.TRUE.equals(i.getUnique())), "the unique index exists");
            MorphiumDriverException e = assertThrows(MorphiumDriverException.class,
                                                      () -> m.createQueryFor(Plain.class).f("id").eq("p1").modify().set("payload", "distinct-p2").tryFindOneAndUpdate(ReturnDocument.AFTER));
            assertTrue(e.isDuplicateKey(), String.valueOf(e));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aChangeOfTheImmutableIdIsAFailureAndNotAnOutcome(Morphium m) throws Exception {
        try (m) {
            // the InMemoryDriver does not enforce the immutability of _id (a known difference to mongod, not part of MOR-010)
            assumeFalse(m.getDriver() instanceof InMemoryDriver);
            Plain p = new Plain();
            p.id = "x";
            m.store(p);
            assertThrows(RuntimeException.class, () -> m.createQueryFor(Plain.class).f("id").eq("x").modify().set("id", "y").tryFindOneAndUpdate(ReturnDocument.AFTER), "_id is immutable: a server error, not an outcome");
            assertEquals(1, m.createQueryFor(Plain.class).f("id").eq("x").countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theBuilderValidationApplies(Morphium m) throws Exception {
        try (m) {
            assertThrows(IllegalStateException.class, () -> m.createQueryFor(Plain.class).f("id").eq("x").modify().tryFindOneAndUpdate(ReturnDocument.AFTER));
            assertThrows(IllegalStateException.class, () -> m.createQueryFor(Plain.class).f("id").eq("x").modify().setOnInsert("payload", "x").tryFindOneAndUpdate(ReturnDocument.AFTER));
            assertThrows(IllegalArgumentException.class, () -> m.createQueryFor(Plain.class).f("id").eq("x").modify().set("payload", "x").tryFindOneAndUpdate(null));
            assertThrows(UnsupportedOperationException.class, () -> m.createQueryFor(Plain.class).f("id").eq("x").skip(1).modify().set("payload", "x").upsert().tryFindOneAndUpdate(ReturnDocument.AFTER));
            assertEquals(0, m.createQueryFor(Plain.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void createIfAbsentIsInsertedOnceAndUpdatedAfterwards(Morphium m) throws Exception {
        try (m) {
            UpdateOutcome<Plain> first = m.createQueryFor(Plain.class).f("id").eq("once").modify().setOnInsert("payload", "first").upsert().tryFindOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(Kind.INSERTED, first.getKind());
            UpdateOutcome<Plain> second = m.createQueryFor(Plain.class).f("id").eq("once").modify().setOnInsert("payload", "second").upsert().tryFindOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(Kind.UPDATED, second.getKind(), "the document existed, the update was a no-op for it");
            assertEquals("first", second.getDocument().payload);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theGeneratedIdOfAnInsertIsConvertedToTheIdType(Morphium m) throws Exception {
        try (m) {
            UpdateOutcome<ModifyCountTest.Job> o = m.createQueryFor(ModifyCountTest.Job.class).f("key").eq("k").modify().set("state", "new").upsert().tryFindOneAndUpdate(ReturnDocument.BEFORE);
            assertEquals(Kind.INSERTED, o.getKind());
            assertTrue(o.getUpsertedId() instanceof de.caluga.morphium.driver.MorphiumId, String.valueOf(o.getUpsertedId()));
            assertEquals(o.getUpsertedId(), m.createQueryFor(ModifyCountTest.Job.class).get().id);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theCacheIsClearedAfterAnOutcome(Morphium m) throws Exception {
        try (m) {
            Plain p = new Plain();
            p.id = "c";
            p.key = "ck";
            m.store(p);
            assertNull(m.createQueryFor(Plain.class).f("key").eq("ck").get().payload);
            m.createQueryFor(Plain.class).f("key").eq("ck").modify().set("payload", "new").tryFindOneAndUpdate(ReturnDocument.AFTER);
            assertEquals("new", m.createQueryFor(Plain.class).f("key").eq("ck").get().payload);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void contendersRaceForOneLeaseAndExactlyOneIsInserted(Morphium m) throws Exception {
        try (m) {
            int contenders = 8;
            ExecutorService pool = Executors.newFixedThreadPool(contenders);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<Kind>> results = new ArrayList<>();

            for (int i = 0; i < contenders; i++) {
                String me = "C" + i;
                results.add(pool.submit(() -> {
                    go.await();
                    return tryAcquire(m, "race", me, NOW, ReturnDocument.AFTER).getKind();
                }));
            }

            go.countDown();
            int inserted = 0;
            int lost = 0;

            for (Future<Kind> f : results) {
                Kind k = f.get(60, TimeUnit.SECONDS);

                if (k == Kind.INSERTED) {
                    inserted++;
                } else if (k == Kind.DUPLICATE_KEY) {
                    lost++;
                } else {
                    throw new AssertionError("unexpected outcome " + k);
                }
            }

            pool.shutdown();
            assertEquals(1, inserted, "exactly one contender inserts the lease");
            assertEquals(1, m.createQueryFor(Lease.class).countAll());
            assertEquals(contenders - 1, lost, "the others are told that the lease is held");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void findOneAndUpdateIsUnchangedAndStillThrows(Morphium m) throws Exception {
        try (m) {
            tryAcquire(m, "job", "A", NOW, ReturnDocument.AFTER);
            MorphiumDriverException e = assertThrows(MorphiumDriverException.class,
                                                      () -> leaseFilter(m, "job", "B", NOW.plusSeconds(10)).modify().set("ownerName", "B").upsert().findOneAndUpdate(ReturnDocument.AFTER));
            assertTrue(e.isDuplicateKey());
        }
    }
}
