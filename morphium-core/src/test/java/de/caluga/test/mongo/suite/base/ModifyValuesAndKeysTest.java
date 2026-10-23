package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.query.ReturnDocument;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Child;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Holder;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Lease;
import de.caluga.test.mongo.suite.base.ModifyFixtures.LeaseFields;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Mode;
import de.caluga.test.mongo.suite.base.ModifyFixtures.RawLease;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Secret;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Target;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static de.caluga.test.mongo.suite.base.ModifyFixtures.raw;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MOR-006: values are marshalled and keys are translated by the update builder, on a real MongoDB and on the InMemoryDriver. */
@Tag("core")
public class ModifyValuesAndKeysTest extends MultiDriverTestBase {

    private Query<Lease> q(Morphium m, String id) {
        return m.createQueryFor(Lease.class).f("id").eq(id);
    }

    private static void assertChild(Object o, String fileName) {
        assertTrue(o instanceof Map, String.valueOf(o));
        assertEquals(fileName, ((Map<?, ?>) o).get("file_name"), String.valueOf(o));
        assertEquals(Child.class.getName(), ((Map<?, ?>) o).get("class_name"), String.valueOf(o));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void embeddedListSetSingleAndMapValuesNeedNoConversion(Morphium m) throws Exception {
        try (m) {
            Map<String, Child> map = new LinkedHashMap<>();
            map.put("k", new Child("m"));
            Lease l = q(m, "v").modify()
                      .set("children", List.of(new Child("a"), new Child("b")))
                      .set("childSet", new LinkedHashSet<>(List.of(new Child("s"))))
                      .set("child", new Child("single"))
                      .set("childMap", map)
                      .upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(List.of("a", "b"), l.children.stream().map(c -> c.fileName).toList());
            assertEquals("s", l.childSet.iterator().next().fileName);
            assertEquals("single", l.child.fileName);
            assertEquals("m", l.childMap.get("k").fileName);
            Map<String, Object> raw = raw(m, Lease.class, "v");
            assertChild(((List<?>) raw.get("children")).get(0), "a");
            assertChild(((List<?>) raw.get("child_set")).get(0), "s");
            assertChild(raw.get("child"), "single");
            assertChild(((Map<?, ?>) raw.get("child_map")).get("k"), "m");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void instantAndEnumValuesReadBackAndEnumsAreStoredByName(Morphium m) throws Exception {
        try (m) {
            Instant at = Instant.parse("2026-10-15T12:34:56.789Z");
            Lease l = q(m, "t").modify().set("expiresAt", at).set("mode", Mode.SLOW).set("modes", List.of(Mode.FAST, Mode.SLOW)).upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(at, l.expiresAt);
            assertEquals(Mode.SLOW, l.mode);
            assertEquals(List.of(Mode.FAST, Mode.SLOW), l.modes);
            Map<String, Object> raw = raw(m, Lease.class, "t");
            assertEquals("SLOW", raw.get("mode"));
            assertEquals(List.of("FAST", "SLOW"), List.copyOf((List<?>) raw.get("modes")));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void enumFieldConstantsAndStringNamesAreEquivalent(Morphium m) throws Exception {
        try (m) {
            q(m, "e").modify().set(LeaseFields.ownerName, "A").set("retryCount", 1).upsert().findOneAndUpdate(ReturnDocument.AFTER);
            q(m, "e").modify().set(LeaseFields.ownerName, "A").set("retryCount", 1).findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals("A", raw(m, Lease.class, "e").get("owner_name"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void idIsStoredAsUnderscoreIdAndPinsTheIdOnInsert(Morphium m) throws Exception {
        try (m) {
            Lease l = m.createQueryFor(Lease.class).f("ownerName").eq("nobody").modify().setOnInsert("id", "fixed").set("retryCount", 1).upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals("fixed", l.id);
            assertEquals("fixed", raw(m, Lease.class, "fixed").get("_id"));
            assertEquals(1, m.createQueryFor(Lease.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aReferenceFieldRejectsAnEntityAndAnEncryptedFieldRejectsAWrite(Morphium m) throws Exception {
        try (m) {
            Target t = new Target();
            t.label = "t";
            m.store(t);
            assertThrows(IllegalArgumentException.class, () -> m.createQueryFor(Holder.class).f("id").eq("h").modify().set("target", t));
            assertThrows(IllegalArgumentException.class, () -> m.createQueryFor(Secret.class).f("id").eq("s").modify().set("secretValue", "plain"));
            assertEquals(0, m.createQueryFor(Holder.class).countAll() + m.createQueryFor(Secret.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void javaNamesAreStoredUnderTheirStoredNamesAndPropertyNamesAreHonoured(Morphium m) throws Exception {
        try (m) {
            q(m, "k").modify().set("ownerName", "A").set("retryCount", 3).set("renamed", "r").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            Map<String, Object> raw = raw(m, Lease.class, "k");
            assertEquals("A", raw.get("owner_name"));
            assertEquals(3, ((Number) raw.get("retry_count")).intValue());
            assertEquals("r", raw.get("custom_x"));
            assertFalse(raw.containsKey("ownerName") || raw.containsKey("retryCount") || raw.containsKey("renamed"), String.valueOf(raw));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aNonTranslatingEntityKeepsItsNamesAndItsEmbeddedClassKeepsItsOwn(Morphium m) throws Exception {
        try (m) {
            Query<RawLease> q = m.createQueryFor(RawLease.class).f("id").eq("r");
            q.modify().set("ownerName", "A").set("defaultChild", new Child("f")).upsert().findOneAndUpdate(ReturnDocument.AFTER);
            q.modify().set("defaultChild.fileName", "g").findOneAndUpdate(ReturnDocument.AFTER);
            Map<String, Object> raw = raw(m, RawLease.class, "r");
            assertEquals("A", raw.get("ownerName"));
            Map<?, ?> child = (Map<?, ?>) raw.get("defaultChild");
            assertEquals("g", child.get("file_name"), String.valueOf(child));
            assertFalse(child.containsKey("fileName"), String.valueOf(child));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aDottedPathThroughASingleEmbeddedFieldIsTranslated(Morphium m) throws Exception {
        try (m) {
            q(m, "d").modify().set("child", new Child("f")).upsert().findOneAndUpdate(ReturnDocument.AFTER);
            q(m, "d").modify().set("child.fileName", "g").findOneAndUpdate(ReturnDocument.AFTER);
            Lease l = q(m, "d").get();
            assertEquals("g", l.child.fileName);
            assertFalse(((Map<?, ?>) raw(m, Lease.class, "d").get("child")).containsKey("fileName"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void unknownAndAliasKeysAreWrittenAsGiven(Morphium m) throws Exception {
        try (m) {
            q(m, "u").modify().set("legacy_key", 1).set("legacy_owner", "L").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            Map<String, Object> raw = raw(m, Lease.class, "u");
            assertEquals(1, ((Number) raw.get("legacy_key")).intValue());
            assertEquals("L", raw.get("legacy_owner"));
            assertFalse(raw.containsKey("current_owner"), String.valueOf(raw));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theTranslationSwitchAndTheQueryOverrideDoNotAffectTheBuilder(Morphium m) throws Exception {
        try (m) {
            m.getConfig().objectMappingSettings().setTranslateUpdateFieldNames(false);
            Query<Lease> off = q(m, "o");
            off.setTranslateUpdateFieldNames(false);
            off.modify().set("ownerName", "A").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            Map<String, Object> raw = raw(m, Lease.class, "o");
            assertEquals("A", raw.get("owner_name"), "the builder always translates: " + raw);
            assertFalse(raw.containsKey("ownerName"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void javaNameAndStoredNameOfOneFieldMergeIfEqualAndAreRejectedIfDifferent(Morphium m) throws Exception {
        try (m) {
            Lease l = q(m, "mg").modify().set("ownerName", "A").set("owner_name", "A").upsert().findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals("A", l.ownerName);
            assertThrows(IllegalArgumentException.class, () -> q(m, "mg").modify().set("ownerName", "A").set("owner_name", "B"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void valuesAreCopiedAtTheCallNotAtTheTerminal(Morphium m) throws Exception {
        try (m) {
            List<Child> children = new java.util.ArrayList<>(List.of(new Child("a")));
            var builder = q(m, "c").modify().set("children", children).upsert();
            children.add(new Child("late"));
            Lease l = builder.findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(1, l.children.size(), "a collection changed after the call is not seen");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void arrayFiltersOfTheQueryAreAppliedByTheBuilder(Morphium m) throws Exception {
        try (m) {
            // behind a list segment the path is used as given (documented limit of resolveFieldPath): the stored name file_name is spelled out
            q(m, "af").modify().set("children", List.of(new Child("a"), new Child("b"))).upsert().findOneAndUpdate(ReturnDocument.AFTER);
            Query<Lease> q = q(m, "af").setArrayFilters(de.caluga.morphium.driver.Doc.of("c.file_name", "b"));
            Lease l = q.modify().set("children.$[c].file_name", "B").findOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(List.of("a", "B"), l.children.stream().map(c -> c.fileName).toList());
        }
    }
}
