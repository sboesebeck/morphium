package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.CreationTime;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.LastChange;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.query.ReturnDocument;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;

import static de.caluga.test.mongo.suite.base.ModifyFixtures.raw;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-009: {@code @LastChange} and {@code @CreationTime} are applied by the update builder, like {@code store()} and {@code Query.set} do: the last
 * change on every write that modifies (or inserts), the creation time only when the update inserts. An explicit touch of the field wins and the
 * auto values can be switched off.
 */
@Tag("core")
public class ModifyAutoValuesTest extends MultiDriverTestBase {

    @Entity
    public static class Audited {
        @Id
        public String id;
        public String name;
        public int counter;
        @LastChange
        public Date lastChange;
        @CreationTime
        public Date createdAt;
        @Version
        public Long entityVersion;
    }

    @Entity
    public static class AuditedLong {
        @Id
        public String id;
        public String name;
        @LastChange
        public Long modifiedMs;
        @CreationTime
        public long createdMs;
    }

    @Entity
    public static class AuditedMisc {
        @Id
        public String id;
        public String name;
        @LastChange(dateFormat = "yyyy-MM-dd HH:mm:ss")
        public String modifiedText;
        @CreationTime
        public LocalDateTime createdLocal;
        @LastChange
        public Instant modifiedInstant;
    }

    @Entity
    public static class AuditedBadType {
        @Id
        public String id;
        public String name;
        @LastChange
        public Integer modified;
    }

    private Query<Audited> q(Morphium m, String id) {
        return m.createQueryFor(Audited.class).f("id").eq(id);
    }

    private static long ms(Object date) {
        return ((Date) date).getTime();
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void lastChangeIsSetByAnUpdateAndCreationTimeIsLeftAlone(Morphium m) throws Exception {
        try (m) {
            Audited a = new Audited();
            a.id = "a";
            a.name = "x";
            a.createdAt = new Date(1_000_000_000_000L); // store() does not set the creation time of an entity with a preset id
            m.store(a);
            Map<String, Object> before = raw(m, Audited.class, "a");
            Thread.sleep(30);
            long start = System.currentTimeMillis();
            q(m, "a").modify().set("name", "y").findOneAndUpdate(ReturnDocument.AFTER);
            long end = System.currentTimeMillis();
            Map<String, Object> after = raw(m, Audited.class, "a");
            assertTrue(ms(after.get("last_change")) >= start && ms(after.get("last_change")) <= end, "last change within the call: " + after);
            assertTrue(ms(after.get("last_change")) > ms(before.get("last_change")));
            assertEquals(ms(before.get("created_at")), ms(after.get("created_at")), "the creation time never changes on an update");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anUpsertThatInsertsSetsBothAndALaterUpdateOnlyTheLastChange(Morphium m) throws Exception {
        try (m) {
            long start = System.currentTimeMillis();
            Audited created = q(m, "u").modify().set("name", "x").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            long end = System.currentTimeMillis();
            assertNotNull(created.lastChange);
            assertNotNull(created.createdAt);
            assertTrue(created.createdAt.getTime() >= start && created.createdAt.getTime() <= end);
            assertEquals(created.createdAt, created.lastChange, "both are taken from one clock reading on insert");

            Thread.sleep(30);
            Audited updated = q(m, "u").modify().set("name", "y").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(created.createdAt, updated.createdAt, "an existing document keeps its creation time");
            assertTrue(updated.lastChange.getTime() > created.lastChange.getTime());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void createIfAbsentSetsBothOnInsertAndTouchesNothingOnAnExistingDocument(Morphium m) throws Exception {
        try (m) {
            Audited created = q(m, "c").modify().setOnInsert("name", "first").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertNotNull(created.lastChange);
            assertNotNull(created.createdAt);
            assertEquals(1L, created.entityVersion);
            Thread.sleep(30);
            Audited again = q(m, "c").modify().setOnInsert("name", "second").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals("first", again.name);
            assertEquals(created.lastChange, again.lastChange, "a no-op for an existing document is not a change");
            assertEquals(created.createdAt, again.createdAt);
            assertEquals(1L, again.entityVersion);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void incAndUnsetCountAsChangesToo(Morphium m) throws Exception {
        try (m) {
            Audited a = new Audited();
            a.id = "i";
            m.store(a);
            long first = ms(raw(m, Audited.class, "i").get("last_change"));
            Thread.sleep(30);
            q(m, "i").modify().inc("counter", 1).findOneAndUpdate(ReturnDocument.AFTER);
            long second = ms(raw(m, Audited.class, "i").get("last_change"));
            assertTrue(second > first);
            Thread.sleep(30);
            q(m, "i").modify().unset("name").findOneAndUpdate(ReturnDocument.AFTER);
            assertTrue(ms(raw(m, Audited.class, "i").get("last_change")) > second);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anExplicitTouchOfTheFieldsWins(Morphium m) throws Exception {
        try (m) {
            Date fixed = new Date(1_000_000_000_000L);
            Audited a = q(m, "e").modify().set("name", "x").set("lastChange", fixed).setOnInsert("createdAt", fixed).upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(fixed, a.lastChange);
            assertEquals(fixed, a.createdAt);
            Audited b = q(m, "e").modify().set("lastChange", fixed).set("name", "y").findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(fixed, b.lastChange);
            Audited c = q(m, "e").modify().unset("lastChange").findOneAndUpdate(ReturnDocument.AFTER);
            assertNull(c.lastChange, "an explicit unset is respected");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void disabledAutoValuesSkipBoth(Morphium m) throws Exception {
        try (m) {
            Audited off = q(m, "d1").disableAutoValues().modify().set("name", "x").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertNull(off.lastChange);
            assertNull(off.createdAt);
            assertNull(off.entityVersion);

            m.disableAutoValuesForThread();

            try {
                Audited threadOff = q(m, "d2").modify().set("name", "x").upsert().findOneAndUpdate(ReturnDocument.AFTER);
                assertNull(threadOff.lastChange);
                assertNull(threadOff.createdAt);
            } finally {
                m.enableAutoValuesForThread();
            }

            Audited on = q(m, "d3").modify().set("name", "x").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertNotNull(on.lastChange);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void longFieldsGetMillisecondsAndPrimitivesAreHandledToo(Morphium m) throws Exception {
        try (m) {
            long start = System.currentTimeMillis();
            AuditedLong a = m.createQueryFor(AuditedLong.class).f("id").eq("l").modify().set("name", "x").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            long end = System.currentTimeMillis();
            assertTrue(a.modifiedMs >= start && a.modifiedMs <= end, String.valueOf(a.modifiedMs));
            assertTrue(a.createdMs >= start && a.createdMs <= end, String.valueOf(a.createdMs));
            Thread.sleep(30);
            AuditedLong b = m.createQueryFor(AuditedLong.class).f("id").eq("l").modify().set("name", "y").findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(a.createdMs, b.createdMs);
            assertTrue(b.modifiedMs > a.modifiedMs);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void stringLocalDateTimeAndInstantFieldsAreSupported(Morphium m) throws Exception {
        try (m) {
            AuditedMisc a = m.createQueryFor(AuditedMisc.class).f("id").eq("m").modify().set("name", "x").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertNotNull(a.modifiedText);
            assertTrue(a.modifiedText.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"), a.modifiedText);
            assertNotNull(a.createdLocal);
            assertTrue(java.time.Duration.between(a.createdLocal, LocalDateTime.now()).abs().getSeconds() < 60, String.valueOf(a.createdLocal));
            assertNotNull(a.modifiedInstant);
            assertTrue(java.time.Duration.between(a.modifiedInstant, Instant.now()).abs().getSeconds() < 60, String.valueOf(a.modifiedInstant));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aFieldTypeThatCannotHoldADateIsRejectedAndNothingIsWritten(Morphium m) throws Exception {
        try (m) {
            IllegalStateException e = assertThrows(IllegalStateException.class,
                                                    () -> m.createQueryFor(AuditedBadType.class).f("id").eq("b").modify().set("name", "x").upsert().findOneAndUpdate(ReturnDocument.AFTER));
            assertTrue(e.getMessage().contains("modified") && e.getMessage().contains("@LastChange"), e.getMessage());
            assertEquals(0, m.createQueryFor(AuditedBadType.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theUpdateDocumentShowsWhatIsInjected(Morphium m) throws Exception {
        try (m) {
            Map<String, Object> doc = q(m, "doc").modify().set("name", "x").toUpdateDocument();
            assertTrue(((Map<?, ?>) doc.get("$set")).containsKey("last_change"), String.valueOf(doc));
            assertTrue(((Map<?, ?>) doc.get("$setOnInsert")).containsKey("created_at"), String.valueOf(doc));
            assertEquals(1L, ((Map<?, ?>) doc.get("$inc")).get("entity_version"));
            assertFalse(((Map<?, ?>) doc.get("$set")).containsKey("created_at"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aStoreAfterABuilderWriteStillWorks(Morphium m) throws Exception {
        try (m) {
            Audited a = q(m, "s").modify().set("name", "x").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            a.counter = 3;
            m.store(a);
            Audited read = q(m, "s").get();
            assertEquals(3, read.counter);
            assertNotNull(read.createdAt);
            assertEquals(2L, read.entityVersion);
        }
    }
}
