package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.VersionMismatchException;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.query.ReturnDocument;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Counter;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Lease;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Plain;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static de.caluga.test.mongo.suite.base.ModifyFixtures.raw;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MOR-006: the lease, counter and create-if-absent use cases of the update builder, on a real MongoDB and on the InMemoryDriver. */
@Tag("core")
public class ModifyLeaseAndCounterTest extends MultiDriverTestBase {

    private static final Instant NOW = Instant.parse("2026-10-15T12:00:00Z");

    private Query<Lease> leaseFilter(Morphium m, String id, String me, Instant now) {
        Query<Lease> q = m.createQueryFor(Lease.class).f("id").eq(id);
        q.or(m.createQueryFor(Lease.class).f("expiresAt").lt(now), m.createQueryFor(Lease.class).f("ownerName").eq(me));
        return q;
    }

    /** the conditional upsert of a lease acquisition, with Java field names */
    private Lease acquire(Morphium m, String id, String me, Instant now, long ttlSeconds) {
        return leaseFilter(m, id, me, now).modify()
               .set("ownerName", me)
               .set("expiresAt", now.plus(ttlSeconds, ChronoUnit.SECONDS))
               .upsert()
               .findOneAndUpdate(ReturnDocument.AFTER);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void firstAcquireInsertsUnderTheFilterIdWithoutStrayFields(Morphium m) throws Exception {
        try (m) {
            Lease l = acquire(m, "job", "A", NOW, 60);
            assertEquals("job", l.id);
            assertEquals("A", l.ownerName);
            assertEquals(NOW.plusSeconds(60), l.expiresAt);
            assertEquals(1L, l.entityVersion, "the version of an inserted document starts at 1");
            Map<String, Object> raw = raw(m, Lease.class, "job");
            assertEquals("A", raw.get("owner_name"));
            assertFalse(raw.containsKey("ownerName") || raw.containsKey("expiresAt"), String.valueOf(raw));
            assertEquals(1, m.createQueryFor(Lease.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void renewalByTheOwnerKeepsOneDocumentAndBumpsTheVersion(Morphium m) throws Exception {
        try (m) {
            acquire(m, "job", "A", NOW, 60);
            Lease renewed = acquire(m, "job", "A", NOW.plusSeconds(10), 60);
            assertEquals(NOW.plusSeconds(70), renewed.expiresAt);
            assertEquals(2L, renewed.entityVersion);
            assertEquals(1, m.createQueryFor(Lease.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aHeldLeaseRaisesADuplicateKeyAndLeavesTheDocumentUntouched(Morphium m) throws Exception {
        try (m) {
            acquire(m, "job", "A", NOW, 60);
            MorphiumDriverException e = assertThrows(MorphiumDriverException.class, () -> acquire(m, "job", "B", NOW.plusSeconds(1), 60));
            assertTrue(e.isDuplicateKey(), String.valueOf(e));
            Lease l = m.createQueryFor(Lease.class).f("id").eq("job").get();
            assertEquals("A", l.ownerName);
            assertEquals(1L, l.entityVersion, "a rejected acquisition does not bump the version");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anExpiredLeaseCanBeTakenOver(Morphium m) throws Exception {
        try (m) {
            acquire(m, "job", "A", NOW, 60);
            Lease l = acquire(m, "job", "B", NOW.plusSeconds(61), 60);
            assertEquals("B", l.ownerName);
            assertEquals(2L, l.entityVersion);
            assertEquals(1, m.createQueryFor(Lease.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void releaseWithUnsetOfAJavaNameRemovesTheRealField(Morphium m) throws Exception {
        try (m) {
            acquire(m, "job", "A", NOW, 60);
            leaseFilter(m, "job", "A", NOW).modify().unset("ownerName").set("expiresAt", NOW).findOneAndUpdate(ReturnDocument.AFTER);
            Map<String, Object> raw = raw(m, Lease.class, "job");
            assertFalse(raw.containsKey("owner_name") || raw.containsKey("ownerName"), String.valueOf(raw));
            assertEquals("B", acquire(m, "job", "B", NOW.plusSeconds(1), 60).ownerName);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void storeAfterABuilderUpsertWorksAndAStaleStoreIsRejected(Morphium m) throws Exception {
        try (m) {
            Lease created = acquire(m, "job", "A", NOW, 60);
            created.retryCount = 5;
            m.store(created); // the version of a builder-created document is 1: this is a versioned update, not an insert
            Lease read = m.createQueryFor(Lease.class).f("id").eq("job").get();
            assertEquals(5, read.retryCount);
            assertEquals(2L, read.entityVersion);

            Lease stale = acquire(m, "job", "A", NOW.plusSeconds(1), 60); // version 3
            read.retryCount = 6;
            assertThrows(VersionMismatchException.class, () -> m.store(read), "the stored version is 3, the entity still has 2");
            assertEquals(3L, stale.entityVersion);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aBuilderWriteAfterAStoreBumpsTheVersionAndMakesTheOldEntityStale(Morphium m) throws Exception {
        try (m) {
            Lease l = new Lease();
            l.id = "stored";
            l.ownerName = "A";
            m.store(l);
            Lease loaded = m.createQueryFor(Lease.class).f("id").eq("stored").get();
            assertEquals(1L, loaded.entityVersion);
            m.createQueryFor(Lease.class).f("id").eq("stored").modify().set("ownerName", "B").findOneAndUpdate(ReturnDocument.AFTER);
            loaded.retryCount = 1;
            assertThrows(VersionMismatchException.class, () -> m.store(loaded));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void racingContendersForAFreshLeaseHaveExactlyOneWinner(Morphium m) throws Exception {
        try (m) {
            for (int round = 0; round < 3; round++) {
                String id = "race" + round;
                int contenders = 8;
                ExecutorService pool = Executors.newFixedThreadPool(contenders);
                CountDownLatch go = new CountDownLatch(1);
                List<Future<String>> results = new ArrayList<>();

                for (int i = 0; i < contenders; i++) {
                    String me = "C" + i;
                    results.add(pool.submit(() -> {
                        go.await();

                        try {
                            return acquire(m, id, me, NOW, 60).ownerName;
                        } catch (MorphiumDriverException e) {
                            assertTrue(e.isDuplicateKey(), "unexpected failure: " + e);
                            return null;
                        }
                    }));
                }

                go.countDown();
                List<String> winners = new ArrayList<>();

                for (Future<String> f : results) {
                    String w = f.get(30, TimeUnit.SECONDS);

                    if (w != null) {
                        winners.add(w);
                    }
                }

                pool.shutdown();
                assertEquals(1, winners.size(), "round " + round + ": exactly one contender may win: " + winners);
                assertEquals(winners.get(0), m.createQueryFor(Lease.class).f("id").eq(id).get().ownerName);
            }

            assertEquals(3, m.createQueryFor(Lease.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void racingContendersForAnExpiredLeaseHaveExactlyOneWinner(Morphium m) throws Exception {
        try (m) {
            acquire(m, "old", "A", NOW, 60);
            Instant later = NOW.plusSeconds(120);
            int contenders = 8;
            ExecutorService pool = Executors.newFixedThreadPool(contenders);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<String>> results = new ArrayList<>();

            for (int i = 0; i < contenders; i++) {
                String me = "C" + i;
                results.add(pool.submit(() -> {
                    go.await();

                    try {
                        return acquire(m, "old", me, later, 60).ownerName;
                    } catch (MorphiumDriverException e) {
                        assertTrue(e.isDuplicateKey(), "unexpected failure: " + e);
                        return null;
                    }
                }));
            }

            go.countDown();
            List<String> winners = new ArrayList<>();

            for (Future<String> f : results) {
                String w = f.get(30, TimeUnit.SECONDS);

                if (w != null) {
                    winners.add(w);
                }
            }

            pool.shutdown();
            assertEquals(1, winners.size(), "exactly one contender may take the expired lease: " + winners);
            assertEquals(winners.get(0), m.createQueryFor(Lease.class).f("id").eq("old").get().ownerName);
            assertEquals(1, m.createQueryFor(Lease.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void incCreatesOnInsertAndAddsOnUpdate(Morphium m) throws Exception {
        try (m) {
            Query<Counter> q = m.createQueryFor(Counter.class).f("id").eq("c");
            assertEquals(1, q.modify().inc("hits", 1).upsert().findOneAndUpdate(ReturnDocument.AFTER).hits);
            assertEquals(6, q.modify().inc("hits", 5).upsert().findOneAndUpdate(ReturnDocument.AFTER).hits);
            assertEquals(4, q.modify().inc("hits", -2).findOneAndUpdate(ReturnDocument.AFTER).hits);
            Counter c = q.modify().inc("score", 1.5).findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(1.5, c.score, 0.0001);
            assertEquals(1, m.createQueryFor(Counter.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void beforeAndAfterReturnTheOldAndTheNewValue(Morphium m) throws Exception {
        try (m) {
            Query<Counter> q = m.createQueryFor(Counter.class).f("id").eq("c");
            q.modify().inc("hits", 10).upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(10, q.modify().inc("hits", 1).findOneAndUpdate(ReturnDocument.BEFORE).hits);
            assertEquals(12, q.modify().inc("hits", 1).findOneAndUpdate(ReturnDocument.AFTER).hits);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void concurrentIncrementsAreAtomic(Morphium m) throws Exception {
        try (m) {
            m.createQueryFor(Counter.class).f("id").eq("conc").modify().inc("hits", 0).upsert().findOneAndUpdate(ReturnDocument.AFTER);
            int threads = 8;
            int each = 50;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<?>> fs = new ArrayList<>();

            for (int t = 0; t < threads; t++) {
                fs.add(pool.submit(() -> {
                    go.await();

                    for (int i = 0; i < each; i++) {
                        m.createQueryFor(Counter.class).f("id").eq("conc").modify().inc("hits", 1).findOneAndUpdate(ReturnDocument.AFTER);
                    }

                    return null;
                }));
            }

            go.countDown();

            for (Future<?> f : fs) {
                f.get(60, TimeUnit.SECONDS);
            }

            pool.shutdown();
            assertEquals(threads * each, m.createQueryFor(Counter.class).f("id").eq("conc").get().hits);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setOnInsertCreatesOnlyWhenAbsentAndNeverOverwrites(Morphium m) throws Exception {
        try (m) {
            Query<Plain> q = m.createQueryFor(Plain.class).f("id").eq("cfg");
            Plain created = q.modify().setOnInsert("payload", "first").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals("first", created.payload);
            Plain again = q.modify().setOnInsert("payload", "second").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals("first", again.payload);
            assertEquals(1, m.createQueryFor(Plain.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void createIfAbsentWithBeforeReturnsNullOnCreateAndTheExistingDocumentOtherwise(Morphium m) throws Exception {
        try (m) {
            Query<Plain> q = m.createQueryFor(Plain.class).f("id").eq("cfg");
            assertNull(q.modify().setOnInsert("payload", "first").upsert().findOneAndUpdate(ReturnDocument.BEFORE), "an insert has no pre-image");
            Plain existing = q.modify().setOnInsert("payload", "second").upsert().findOneAndUpdate(ReturnDocument.BEFORE);
            assertNotNull(existing);
            assertEquals("first", existing.payload);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void noMatchWithoutUpsertReturnsNull(Morphium m) throws Exception {
        try (m) {
            assertNull(m.createQueryFor(Plain.class).f("id").eq("none").modify().set("payload", "x").findOneAndUpdate(ReturnDocument.AFTER));
            assertEquals(0, m.createQueryFor(Plain.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aNaturalKeyLeaseWithAUniqueIndexHasOneWinner(Morphium m) throws Exception {
        try (m) {
            m.ensureIndex(Plain.class, m.getMapper().getCollectionName(Plain.class), Map.of("key", 1), Map.of("unique", true));
            int contenders = 8;
            ExecutorService pool = Executors.newFixedThreadPool(contenders);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<Boolean>> results = new ArrayList<>();

            for (int i = 0; i < contenders; i++) {
                String me = "C" + i;
                results.add(pool.submit(() -> {
                    go.await();

                    try {
                        m.createQueryFor(Plain.class).f("key").eq("natural").modify().setOnInsert("payload", me).upsert().findOneAndUpdate(ReturnDocument.AFTER);
                        return true;
                    } catch (MorphiumDriverException e) {
                        assertTrue(e.isDuplicateKey(), "unexpected failure: " + e);
                        return false;
                    }
                }));
            }

            go.countDown();
            long ok = 0;

            for (Future<Boolean> f : results) {
                ok += f.get(30, TimeUnit.SECONDS) ? 1 : 0;
            }

            pool.shutdown();
            assertEquals(1, m.createQueryFor(Plain.class).f("key").eq("natural").countAll(), "the unique index keeps one document");
            assertTrue(ok >= 1);
        }
    }
}
