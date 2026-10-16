package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.query.ReturnDocument;
import de.caluga.morphium.query.UpdateBuilder;
import de.caluga.morphium.query.UpdateResult;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Child;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Lease;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Plain;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static de.caluga.test.mongo.suite.base.ModifyFixtures.raw;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MOR-050: {@code updateOne()} and {@code updateMany()} of the update builder and their {@link UpdateResult}, on a real MongoDB and on the InMemoryDriver. */
@Tag("core")
public class ModifyCountTest extends MultiDriverTestBase {

    @Entity
    public static class Job {
        @Id
        public MorphiumId id;
        public String state;
        public String key;
        public int attempts;
    }

    private void jobs(Morphium m, String state, int count) {
        for (int i = 0; i < count; i++) {
            Plain p = new Plain();
            p.id = state + i;
            p.key = state;
            p.count = i;
            m.store(p);
        }
    }

    private Query<Plain> byKey(Morphium m, String key) {
        return m.createQueryFor(Plain.class).f("key").eq(key);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void updateManyReportsMatchedAndModifiedAndASecondIdenticalCallModifiesNothing(Morphium m) throws Exception {
        try (m) {
            jobs(m, "pending", 3);
            jobs(m, "done", 2);
            UpdateResult r = byKey(m, "pending").modify().set("payload", "x").updateMany();
            assertEquals(3, r.getMatchedCount());
            assertEquals(3, r.getModifiedCount());
            assertNull(r.getUpsertedId());
            assertTrue(r.isAcknowledged());
            assertEquals(3, m.createQueryFor(Plain.class).f("payload").eq("x").countAll());

            UpdateResult again = byKey(m, "pending").modify().set("payload", "x").updateMany();
            assertEquals(3, again.getMatchedCount());
            assertEquals(0, again.getModifiedCount(), "the documents already have the value");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void updateOneUpdatesExactlyOneDocument(Morphium m) throws Exception {
        try (m) {
            jobs(m, "pending", 3);
            UpdateResult r = byKey(m, "pending").modify().set("payload", "taken").updateOne();
            assertEquals(1, r.getMatchedCount());
            assertEquals(1, r.getModifiedCount());
            assertEquals(1, m.createQueryFor(Plain.class).f("payload").eq("taken").countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void noMatchWithoutUpsertReportsZero(Morphium m) throws Exception {
        try (m) {
            UpdateResult r = byKey(m, "none").modify().set("payload", "x").updateMany();
            assertEquals(0, r.getMatchedCount());
            assertEquals(0, r.getModifiedCount());
            assertNull(r.getUpsertedId());
            assertTrue(r.isAcknowledged());
            assertEquals(0, m.createQueryFor(Plain.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anUpsertThatInsertsReportsTheUpsertedIdAndMatchedZero(Morphium m) throws Exception {
        try (m) {
            UpdateResult r = byKey(m, "fresh").modify().set("payload", "x").upsert().updateOne();
            assertEquals(0, r.getMatchedCount(), "n includes the upserted document, the matched count does not");
            assertEquals(0, r.getModifiedCount());
            assertNotNull(r.getUpsertedId());
            assertTrue(r.wasUpserted());
            assertEquals(1, m.createQueryFor(Plain.class).countAll());
            assertEquals(r.getUpsertedId(), m.createQueryFor(Plain.class).get().id, "the id is converted to the type of the @Id (String)");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theUpsertedIdOfAMorphiumIdEntityIsAMorphiumId(Morphium m) throws Exception {
        try (m) {
            UpdateResult r = m.createQueryFor(Job.class).f("key").eq("k").modify().set("state", "new").upsert().updateOne();
            assertTrue(r.getUpsertedId() instanceof MorphiumId, String.valueOf(r.getUpsertedId()));
            assertEquals(r.getUpsertedId(), m.createQueryFor(Job.class).get().id);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anUpsertWithAFilterIdKeepsTheFilterId(Morphium m) throws Exception {
        try (m) {
            UpdateResult r = m.createQueryFor(Plain.class).f("id").eq("fixed").modify().set("payload", "x").upsert().updateOne();
            assertEquals("fixed", r.getUpsertedId());
            assertEquals("fixed", raw(m, Plain.class, "fixed").get("_id"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aMatchingUpsertUpdatesAndDoesNotReportAnUpsertedId(Morphium m) throws Exception {
        try (m) {
            jobs(m, "pending", 2);
            UpdateResult r = byKey(m, "pending").modify().set("payload", "x").upsert().updateMany();
            assertEquals(2, r.getMatchedCount());
            assertFalse(r.wasUpserted());
            assertEquals(2, m.createQueryFor(Plain.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aLostConditionalUpsertRaisesADuplicateKeyOnBothDrivers(Morphium m) throws Exception {
        try (m) {
            Instant now = Instant.parse("2026-10-15T12:00:00Z");
            Query<Lease> first = m.createQueryFor(Lease.class).f("id").eq("job");
            first.or(m.createQueryFor(Lease.class).f("expiresAt").lt(now), m.createQueryFor(Lease.class).f("ownerName").eq("A"));
            assertTrue(first.modify().set("ownerName", "A").set("expiresAt", now.plusSeconds(60)).upsert().updateOne().wasUpserted());

            Query<Lease> second = m.createQueryFor(Lease.class).f("id").eq("job");
            second.or(m.createQueryFor(Lease.class).f("expiresAt").lt(now.plusSeconds(1)), m.createQueryFor(Lease.class).f("ownerName").eq("B"));
            MorphiumDriverException e = assertThrows(MorphiumDriverException.class, () -> second.modify().set("ownerName", "B").upsert().updateOne());
            assertTrue(e.isDuplicateKey(), String.valueOf(e));
            assertEquals("A", m.createQueryFor(Lease.class).f("id").eq("job").get().ownerName);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aUniqueIndexViolationInAMultiUpdateIsReportedAsDuplicateKey(Morphium m) throws Exception {
        try (m) {
            jobs(m, "pending", 2);

            for (Plain p : byKey(m, "pending").asList()) {
                p.payload = "distinct-" + p.id; // null values would collide in a unique index (they are stored as null, not missing)
                m.store(p);
            }

            m.ensureIndex(Plain.class, m.getMapper().getCollectionName(Plain.class), Map.of("payload", 1), Map.of("unique", true));
            assertTrue(m.getIndexesFromMongo(Plain.class).stream().anyMatch(i -> Boolean.TRUE.equals(i.getUnique())), "the unique index exists");
            MorphiumDriverException e = assertThrows(MorphiumDriverException.class, () -> byKey(m, "pending").modify().set("payload", "same").updateMany());
            assertTrue(e.isDuplicateKey(), String.valueOf(e));
            assertTrue(e.getMessage().contains("11000") || e.getMessage().contains("duplicate"), e.getMessage());
            assertEquals(1, m.createQueryFor(Plain.class).f("payload").eq("same").countAll(), "ordered: the first document was updated, the second was not, nothing is rolled back");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void arrayFiltersWorkWithUpdateMany(Morphium m) throws Exception {
        try (m) {
            for (String id : List.of("a", "b")) {
                Lease l = new Lease();
                l.id = id;
                l.children = new ArrayList<>(List.of(new Child("x"), new Child("y")));
                m.store(l);
            }

            Query<Lease> q = m.createQueryFor(Lease.class).setArrayFilters(Doc.of("c.file_name", "y"));
            UpdateResult r = q.modify().set("children.$[c].file_name", "Y").updateMany();
            assertEquals(2, r.getMatchedCount());

            for (String id : List.of("a", "b")) {
                assertEquals(List.of("x", "Y"), m.createQueryFor(Lease.class).f("id").eq(id).get().children.stream().map(c -> c.fileName).toList());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theVersionOfEveryMatchedDocumentIsIncremented(Morphium m) throws Exception {
        try (m) {
            for (String id : List.of("v1", "v2", "v3")) {
                Lease l = new Lease();
                l.id = id;
                m.store(l);
            }

            UpdateResult r = m.createQueryFor(Lease.class).modify().set("retryCount", 5).updateMany();
            assertEquals(3, r.getMatchedCount());
            assertEquals(3, r.getModifiedCount());

            for (String id : List.of("v1", "v2", "v3")) {
                assertEquals(2L, m.createQueryFor(Lease.class).f("id").eq(id).get().entityVersion);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theCacheIsClearedAfterTheWrite(Morphium m) throws Exception {
        try (m) {
            jobs(m, "pending", 1);
            assertNull(byKey(m, "pending").get().payload);
            byKey(m, "pending").modify().set("payload", "new").updateMany();
            assertEquals("new", byKey(m, "pending").get().payload);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theBuilderIsReusableForTheCountTerminalsToo(Morphium m) throws Exception {
        try (m) {
            jobs(m, "pending", 2);
            UpdateBuilder<Plain> b = byKey(m, "pending").modify().inc("count", 10);
            assertEquals(2, b.updateMany().getModifiedCount());
            assertEquals(2, b.updateMany().getModifiedCount());
            assertEquals(41, m.createQueryFor(Plain.class).f("key").eq("pending").asList().stream().mapToInt(p -> p.count).sum(), "(0 + 20) + (1 + 20): two documents, two runs of inc 10");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void optionsThatCannotBeHonouredAreRejectedAndNothingIsWritten(Morphium m) throws Exception {
        try (m) {
            jobs(m, "pending", 2);
            assertThrows(UnsupportedOperationException.class, () -> byKey(m, "pending").sort("count").modify().set("payload", "x").updateOne());
            assertThrows(UnsupportedOperationException.class, () -> byKey(m, "pending").sort("count").modify().set("payload", "x").updateMany());
            assertThrows(UnsupportedOperationException.class, () -> byKey(m, "pending").setHintString("_id_").modify().set("payload", "x").updateMany());
            assertThrows(UnsupportedOperationException.class, () -> byKey(m, "pending").setProjection("payload").modify().set("payload", "x").updateMany());
            assertThrows(UnsupportedOperationException.class, () -> byKey(m, "pending").skip(1).modify().set("payload", "x").updateMany());
            assertThrows(UnsupportedOperationException.class, () -> byKey(m, "pending").limit(1).modify().set("payload", "x").updateMany());
            assertThrows(UnsupportedOperationException.class, () -> byKey(m, "pending").limit(2).modify().set("payload", "x").updateOne());
            assertEquals(0, m.createQueryFor(Plain.class).f("payload").eq("x").countAll());
            // a limit of one is what updateOne does anyway
            assertEquals(1, byKey(m, "pending").limit(1).modify().set("payload", "x").updateOne().getModifiedCount());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anEmptyBuilderAndASetOnInsertWithoutUpsertAreRejected(Morphium m) throws Exception {
        try (m) {
            jobs(m, "pending", 1);
            assertThrows(IllegalStateException.class, () -> byKey(m, "pending").modify().updateMany());
            assertThrows(IllegalStateException.class, () -> byKey(m, "pending").modify().setOnInsert("payload", "x").updateOne());
            assertEquals("pending", byKey(m, "pending").get().key);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theFindOneAndUpdateTerminalIsUnaffected(Morphium m) throws Exception {
        try (m) {
            jobs(m, "pending", 1);
            Plain p = byKey(m, "pending").modify().set("payload", "x").findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals("x", p.payload);
        }
    }
}
