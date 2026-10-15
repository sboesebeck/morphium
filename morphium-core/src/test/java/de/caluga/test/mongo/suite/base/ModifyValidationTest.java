package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.VersionMismatchException;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.query.ReturnDocument;
import de.caluga.morphium.query.UpdateBuilder;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Lease;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Plain;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static de.caluga.test.mongo.suite.base.ModifyFixtures.raw;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MOR-006: validation, version injection and the documented limits of the update builder. */
@Tag("core")
public class ModifyValidationTest extends MultiDriverTestBase {

    @Entity
    public static class Named {
        @Id
        public String id;
        public String x;
        @Version(fieldName = "ver")
        public Long version;
    }

    private Query<Lease> q(Morphium m, String id) {
        return m.createQueryFor(Lease.class).f("id").eq(id);
    }

    private Query<Plain> p(Morphium m, String id) {
        return m.createQueryFor(Plain.class).f("id").eq(id);
    }

    // ---------------------------------------------------------------- empty / invalid builders

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anEmptyBuilderIsRejectedAndTheDocumentStaysIntact(Morphium m) throws Exception {
        try (m) {
            Plain d = new Plain();
            d.id = "x";
            d.payload = "keep";
            m.store(d);
            assertThrows(IllegalStateException.class, () -> p(m, "x").modify().findOneAndUpdate(ReturnDocument.AFTER));
            assertEquals("keep", p(m, "x").get().payload);
            assertEquals("keep", raw(m, Plain.class, "x").get("payload"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setOnInsertWithoutUpsertIsRejected(Morphium m) throws Exception {
        try (m) {
            assertThrows(IllegalStateException.class, () -> p(m, "x").modify().setOnInsert("payload", "p").findOneAndUpdate(ReturnDocument.AFTER));
            assertEquals(0, m.createQueryFor(Plain.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void invalidArgumentsAreRejectedAtTheCall(Morphium m) throws Exception {
        try (m) {
            UpdateBuilder<Plain> b = p(m, "x").modify();
            assertThrows(IllegalArgumentException.class, () -> b.inc("count", (Number) null));
            assertThrows(IllegalArgumentException.class, () -> b.inc("count", new BigDecimal("1.5")));
            assertThrows(IllegalArgumentException.class, () -> b.inc("count", new AtomicInteger(1)));
            assertThrows(IllegalArgumentException.class, () -> b.set((String) null, 1));
            assertThrows(IllegalArgumentException.class, () -> b.set("", 1));
            assertThrows(IllegalArgumentException.class, () -> b.unset(new String[0]));
            assertThrows(IllegalArgumentException.class, () -> b.unset((String[]) null));
            assertThrows(IllegalArgumentException.class, () -> b.unset("a", null));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void smallNumberTypesAreWidenedForInc(Morphium m) throws Exception {
        try (m) {
            Plain a = p(m, "n").modify().inc("count", (short) 2).upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(2, a.count);
            Plain b = p(m, "n").modify().inc("count", (byte) 3).findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(5, b.count);
            Plain c = p(m, "n").modify().inc("count", 2.0f).findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(7, c.count);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void conflictingPathsAreRejectedBeforeAnythingIsSent(Morphium m) throws Exception {
        try (m) {
            assertThrows(IllegalArgumentException.class, () -> p(m, "c").modify().set("payload", "a").inc("payload", 1));
            assertThrows(IllegalArgumentException.class, () -> p(m, "c").modify().set("payload", "a").unset("payload"));
            assertThrows(IllegalArgumentException.class, () -> p(m, "c").modify().set("tags", "a").set("tags.0", "b"));
            assertThrows(IllegalArgumentException.class, () -> p(m, "c").modify().inc("count", 1).inc("count", 1));
            assertEquals(0, m.createQueryFor(Plain.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void skipLimitHintAndProjectionAreRejected(Morphium m) throws Exception {
        try (m) {
            assertThrows(UnsupportedOperationException.class, () -> p(m, "r").skip(1).modify().set("payload", "x").findOneAndUpdate(ReturnDocument.AFTER));
            assertThrows(UnsupportedOperationException.class, () -> p(m, "r").limit(2).modify().set("payload", "x").findOneAndUpdate(ReturnDocument.AFTER));
            assertThrows(UnsupportedOperationException.class, () -> p(m, "r").setProjection("payload").modify().set("payload", "x").findOneAndUpdate(ReturnDocument.AFTER));
            assertThrows(UnsupportedOperationException.class, () -> p(m, "r").setHintString("_id_").modify().set("payload", "x").findOneAndUpdate(ReturnDocument.AFTER));
            // a limit of one is what findAndModify does anyway
            assertEquals(null, p(m, "r").limit(1).modify().set("payload", "x").findOneAndUpdate(ReturnDocument.AFTER));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void sortDecidesWhichDocumentIsUpdated(Morphium m) throws Exception {
        try (m) {
            for (int i = 1; i <= 3; i++) {
                Plain d = new Plain();
                d.id = "s" + i;
                d.key = "k";
                d.count = i;
                m.store(d);
            }

            Plain first = m.createQueryFor(Plain.class).f("key").eq("k").sort("count").modify().set("payload", "taken").findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals("s1", first.id);
            Plain last = m.createQueryFor(Plain.class).f("key").eq("k").sort("-count").modify().set("payload", "taken").findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals("s3", last.id);
        }
    }

    // ---------------------------------------------------------------- version injection

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theVersionIsIncrementedByAnUpdateAndInitialisedToOneOnInsert(Morphium m) throws Exception {
        try (m) {
            assertEquals(1L, q(m, "v").modify().set("retryCount", 1).upsert().findOneAndUpdate(ReturnDocument.AFTER).entityVersion);
            assertEquals(2L, q(m, "v").modify().set("retryCount", 2).findOneAndUpdate(ReturnDocument.AFTER).entityVersion);
            assertEquals(3L, q(m, "v").modify().inc("retryCount", 1).findOneAndUpdate(ReturnDocument.AFTER).entityVersion);
            assertEquals(4L, q(m, "v").modify().unset("mode").findOneAndUpdate(ReturnDocument.AFTER).entityVersion);
            assertEquals("entity_version", raw(m, Lease.class, "v").keySet().stream().filter(k -> k.contains("version")).findFirst().orElse(null));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void createIfAbsentSetsTheVersionOnInsertAndDoesNotBumpAnExistingDocument(Morphium m) throws Exception {
        try (m) {
            Lease created = q(m, "ci").modify().setOnInsert("ownerName", "A").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(1L, created.entityVersion);
            Lease again = q(m, "ci").modify().setOnInsert("ownerName", "B").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(1L, again.entityVersion, "a no-op must not bump the version");
            assertEquals("A", again.ownerName);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anExplicitTouchOfTheVersionWins(Morphium m) throws Exception {
        try (m) {
            q(m, "x").modify().set("retryCount", 1).upsert().findOneAndUpdate(ReturnDocument.AFTER);
            Lease l = q(m, "x").modify().set("entityVersion", 40L).set("retryCount", 2).findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(40L, l.entityVersion);
            Lease i = q(m, "x").modify().inc("entityVersion", 5).findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(45L, i.entityVersion);
            Lease so = q(m, "x2").modify().setOnInsert("entityVersion", 7L).set("retryCount", 1).upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(7L, so.entityVersion, "setOnInsert of the version is an explicit touch, no inc is added next to it");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void disabledAutoValuesSkipTheVersionInjection(Morphium m) throws Exception {
        try (m) {
            Lease l = q(m, "na").disableAutoValues().modify().set("retryCount", 1).upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(null, l.entityVersion);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void entitiesWithoutAVersionAreUntouched(Morphium m) throws Exception {
        try (m) {
            Plain d = p(m, "pl").modify().set("payload", "x").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertFalse(raw(m, Plain.class, "pl").keySet().stream().anyMatch(k -> k.contains("version")));
            assertNotNull(d);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aVersionFieldWithAFieldNameIsRejectedUntilItIsFixed(Morphium m) throws Exception {
        try (m) {
            IllegalStateException e = assertThrows(IllegalStateException.class,
                                                    () -> m.createQueryFor(Named.class).f("id").eq("n").modify().set("x", "y").upsert().findOneAndUpdate(ReturnDocument.AFTER));
            assertTrue(e.getMessage().contains("fieldName"), e.getMessage());
        }
    }

    // ---------------------------------------------------------------- misc contract

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theBuilderIsReusableAndDoesNotConsumeItsOperations(Morphium m) throws Exception {
        try (m) {
            UpdateBuilder<Plain> b = p(m, "re").modify().inc("count", 1).upsert();
            assertEquals(1, b.findOneAndUpdate(ReturnDocument.AFTER).count);
            assertEquals(2, b.findOneAndUpdate(ReturnDocument.AFTER).count);
            Map<String, Object> doc = b.toUpdateDocument();
            assertTrue(doc.containsKey("$inc"), String.valueOf(doc));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theQueryIsCapturedWhenTheBuilderIsCreated(Morphium m) throws Exception {
        try (m) {
            Query<Plain> q = p(m, "cap");
            UpdateBuilder<Plain> b = q.modify().set("payload", "x").upsert();
            q.f("payload").eq("never"); // changes the original after the builder exists
            assertEquals("cap", b.findOneAndUpdate(ReturnDocument.AFTER).id);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theCacheIsClearedAfterTheWrite(Morphium m) throws Exception {
        try (m) {
            Plain d = new Plain();
            d.id = "cache";
            d.payload = "old";
            m.store(d);
            assertEquals("old", p(m, "cache").get().payload);
            p(m, "cache").modify().set("payload", "new").findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals("new", p(m, "cache").get().payload);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aStaleStoreAfterABuilderWriteIsRejected(Morphium m) throws Exception {
        try (m) {
            Lease l = new Lease();
            l.id = "st";
            m.store(l);
            Lease loaded = q(m, "st").get();
            q(m, "st").modify().set("retryCount", 9).findOneAndUpdate(ReturnDocument.AFTER);
            loaded.retryCount = 1;
            assertThrows(VersionMismatchException.class, () -> m.store(loaded));
        }
    }
}
