package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Aliases;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * MOR-041: the keys of the operator maps handed to {@code Query.findOneAndUpdate} are translated to the stored field names, on the
 * InMemoryDriver and on a real MongoDB. Every test reads the raw document, because the defect it guards against is silent: a Java name
 * used to be written as a stray field next to the real one.
 */
@Tag("core")
public class UpdateKeyTranslationTest extends MultiDriverTestBase {

    @Embedded
    public static class Child {
        public String fileName;

        public Child() {
        }

        public Child(String fileName) {
            this.fileName = fileName;
        }
    }

    /** default naming */
    @Entity
    public static class Lease {
        @Id
        public String id;
        public String tag;
        public String ownerName;
        public Date expiresAt;
        public int retryCount;
        public int backupCount;
        public List<String> tagList;
        @Property(fieldName = "custom_x")
        public String renamed;
        @Aliases({"legacy_owner"})
        public String currentOwner;
        public Child childEntry;
        public List<Child> childList;
        public Map<String, Object> data;
    }

    /** owner keeps Java names, the embedded class translates (research hypothesis 2) */
    @Entity(translateCamelCase = false)
    public static class RawLease {
        @Id
        public String id;
        public String tag;
        public String ownerName;
        public Child defaultChild;
    }

    private Query<Lease> q(Morphium m, String tag) {
        return m.createQueryFor(Lease.class).f("tag").eq(tag);
    }

    private Lease up(Morphium m, String tag, Map<String, Object> update) {
        return q(m, tag).findOneAndUpdate(update, true, true);
    }

    /** Reads the documents without the projection to known fields that Query adds: a stray field would be invisible otherwise. */
    private List<Map<String, Object>> rawDocs(Query<?> q) throws Exception {
        FindCommand cmd = q.getFindCmd();

        try {
            cmd.setProjection(new Doc());
            return cmd.execute();
        } finally {
            cmd.releaseConnection();
        }
    }

    private Map<String, Object> raw(Morphium m, String tag) throws Exception {
        List<Map<String, Object>> docs = rawDocs(q(m, tag));
        assertEquals(1, docs.size(), String.valueOf(docs));
        return docs.get(0);
    }

    private static Map<String, Object> set(Object... kv) {
        Map<String, Object> f = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            f.put((String) kv[i], kv[i + 1]);
        }
        return UtilsMap.of("$set", f);
    }

    private static Map<String, Object> op(String op, Object... kv) {
        Map<String, Object> f = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            f.put((String) kv[i], kv[i + 1]);
        }
        return UtilsMap.of(op, f);
    }

    private static void assertNoStray(Map<String, Object> raw, String... strays) {
        for (String s : strays) {
            assertFalse(raw.containsKey(s), "stray field " + s + " in " + raw);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void camelCaseKeysWriteTheMappedFields(Morphium m) throws Exception {
        try (m) {
            Date d = new Date();
            Lease l = up(m, "t1", set("tag", "t1", "ownerName", "A", "expiresAt", d, "retryCount", 2));
            assertEquals("A", l.ownerName);
            Map<String, Object> raw = raw(m, "t1");
            assertEquals("A", raw.get("owner_name"));
            assertNotNull(raw.get("expires_at"));
            assertEquals(2, ((Number) raw.get("retry_count")).intValue());
            assertNoStray(raw, "ownerName", "expiresAt", "retryCount");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void storedNamesStayUnchanged(Morphium m) throws Exception {
        try (m) {
            up(m, "t2", set("tag", "t2", "owner_name", "A"));
            assertEquals("A", raw(m, "t2").get("owner_name"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void killSwitchWritesTheKeysVerbatim(Morphium m) throws Exception {
        try (m) {
            m.getConfig().objectMappingSettings().setTranslateUpdateFieldNames(false);
            up(m, "t3", set("tag", "t3", "ownerName", "A"));
            Map<String, Object> raw = raw(m, "t3");
            assertEquals("A", raw.get("ownerName"), "stray field of the previous behaviour");
            assertFalse(raw.containsKey("owner_name"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void perQueryOverrideWinsOverTheConfiguration(Morphium m) throws Exception {
        try (m) {
            Query<Lease> off = q(m, "t4a");
            off.setTranslateUpdateFieldNames(false);
            off.findOneAndUpdate(set("tag", "t4a", "ownerName", "A"), true, true);
            assertEquals("A", raw(m, "t4a").get("ownerName"));

            m.getConfig().objectMappingSettings().setTranslateUpdateFieldNames(false);
            Query<Lease> on = q(m, "t4b");
            on.setTranslateUpdateFieldNames(true);
            on.findOneAndUpdate(set("tag", "t4b", "ownerName", "A"), true, true);
            assertEquals("A", raw(m, "t4b").get("owner_name"));

            Query<Lease> follow = q(m, "t4c");
            follow.setTranslateUpdateFieldNames(null);
            follow.findOneAndUpdate(set("tag", "t4c", "ownerName", "A"), true, true);
            assertEquals("A", raw(m, "t4c").get("ownerName"), "null follows the configuration, which is off here");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theSwitchCanBeToggledBetweenCalls(Morphium m) throws Exception {
        try (m) {
            m.getConfig().objectMappingSettings().setTranslateUpdateFieldNames(false);
            up(m, "t5", set("tag", "t5", "ownerName", "A"));
            m.getConfig().objectMappingSettings().setTranslateUpdateFieldNames(true);
            up(m, "t5", set("ownerName", "B"));
            Map<String, Object> raw = raw(m, "t5");
            assertEquals("A", raw.get("ownerName"));
            assertEquals("B", raw.get("owner_name"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void nonTranslatingEntityKeepsItsKeysAndEmbeddedPathsFollowTheEmbeddedClass(Morphium m) throws Exception {
        try (m) {
            Query<RawLease> q = m.createQueryFor(RawLease.class).f("tag").eq("t6");
            q.findOneAndUpdate(set("tag", "t6", "ownerName", "A", "defaultChild", new Child("f")), true, true);
            q.findOneAndUpdate(set("defaultChild.fileName", "g"), true, true);
            Map<String, Object> raw = rawDocs(q).get(0);
            assertEquals("A", raw.get("ownerName"));
            Map<?, ?> child = (Map<?, ?>) raw.get("defaultChild");
            assertEquals("g", child.get("file_name"), "the dotted key reaches the field the value was written to: " + child);
            assertFalse(child.containsKey("fileName"), String.valueOf(child));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void propertyFieldNameIsHonoured(Morphium m) throws Exception {
        try (m) {
            up(m, "t7", set("tag", "t7", "renamed", "r"));
            Map<String, Object> raw = raw(m, "t7");
            assertEquals("r", raw.get("custom_x"));
            assertNoStray(raw, "renamed");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void idKeyBecomesUnderscoreIdAndPinsTheId(Morphium m) throws Exception {
        try (m) {
            Lease l = q(m, "t8").findOneAndUpdate(UtilsMap.of("$set", UtilsMap.of("tag", "t8"), "$setOnInsert", UtilsMap.of("id", "fixed")), true, true);
            assertEquals("fixed", l.id);
            assertEquals(1, m.createQueryFor(Lease.class).countAll());
            Map<String, Object> raw = raw(m, "t8");
            assertEquals("fixed", raw.get("_id"));
            assertNoStray(raw, "id");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void unknownAndAliasKeysAreWrittenVerbatim(Morphium m) throws Exception {
        try (m) {
            up(m, "t9", set("tag", "t9", "legacy_key", 1, "legacy_owner", "L"));
            Map<String, Object> raw = raw(m, "t9");
            assertEquals(1, ((Number) raw.get("legacy_key")).intValue());
            assertEquals("L", raw.get("legacy_owner"), "an alias is a stored name of old documents and is not resolved");
            assertNoStray(raw, "current_owner");
            up(m, "t9", op("$unset", "legacy_owner", ""));
            assertFalse(raw(m, "t9").containsKey("legacy_owner"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void unsetOfAJavaNameRemovesTheRealField(Morphium m) throws Exception {
        try (m) {
            up(m, "t10", set("tag", "t10", "ownerName", "A", "expiresAt", new Date()));
            up(m, "t10", op("$unset", "ownerName", "", "expiresAt", ""));
            Map<String, Object> raw = raw(m, "t10");
            assertNoStray(raw, "owner_name", "expires_at", "ownerName", "expiresAt");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void strayFieldsOfThePreviousBehaviourCanBeRemovedWithTheOverride(Morphium m) throws Exception {
        try (m) {
            m.getConfig().objectMappingSettings().setTranslateUpdateFieldNames(false);
            up(m, "t11", set("tag", "t11", "ownerName", "stray"));
            m.getConfig().objectMappingSettings().setTranslateUpdateFieldNames(true);
            up(m, "t11", set("ownerName", "real"));
            Query<Lease> cleanup = q(m, "t11");
            cleanup.setTranslateUpdateFieldNames(false);
            cleanup.findOneAndUpdate(op("$unset", "ownerName", ""), false, true);
            Map<String, Object> raw = raw(m, "t11");
            assertFalse(raw.containsKey("ownerName"));
            assertEquals("real", raw.get("owner_name"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void renameTranslatesKeyAndTarget(Morphium m) throws Exception {
        try (m) {
            up(m, "t12", set("tag", "t12", "retryCount", 3));
            up(m, "t12", op("$rename", "retryCount", "backupCount"));
            Map<String, Object> raw = raw(m, "t12");
            assertEquals(3, ((Number) raw.get("backup_count")).intValue());
            assertNoStray(raw, "retry_count", "retryCount", "backupCount");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void numericOperatorsTranslateTheirKeys(Morphium m) throws Exception {
        try (m) {
            up(m, "t13", set("tag", "t13", "retryCount", 3));
            up(m, "t13", op("$inc", "retryCount", 2));
            assertEquals(5, ((Number) raw(m, "t13").get("retry_count")).intValue());
            up(m, "t13", op("$mul", "retryCount", 2));
            assertEquals(10, ((Number) raw(m, "t13").get("retry_count")).intValue());
            up(m, "t13", op("$min", "retryCount", 4));
            assertEquals(4, ((Number) raw(m, "t13").get("retry_count")).intValue());
            up(m, "t13", op("$max", "retryCount", 9));
            assertEquals(9, ((Number) raw(m, "t13").get("retry_count")).intValue());
            assertNoStray(raw(m, "t13"), "retryCount");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void currentDateBitAndPopTranslateTheirKeys(Morphium m) throws Exception {
        try (m) {
            up(m, "t14", set("tag", "t14", "retryCount", 4, "tagList", List.of("a", "b", "c")));
            up(m, "t14", UtilsMap.of("$currentDate", UtilsMap.of("expiresAt", true), "$pop", UtilsMap.of("tagList", 1), "$bit", UtilsMap.of("retryCount", UtilsMap.of("or", 3))));
            Map<String, Object> raw = raw(m, "t14");
            assertNotNull(raw.get("expires_at"));
            assertEquals(List.of("a", "b"), List.copyOf((List<?>) raw.get("tag_list")));
            assertEquals(7, ((Number) raw.get("retry_count")).intValue());
            assertNoStray(raw, "expiresAt", "tagList", "retryCount");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void arrayOperatorsTranslateTheirKeys(Morphium m) throws Exception {
        try (m) {
            up(m, "t15", set("tag", "t15", "tagList", List.of("a", "b", "c")));
            up(m, "t15", op("$push", "tagList", "d"));
            up(m, "t15", op("$addToSet", "tagList", "a"));
            up(m, "t15", op("$pull", "tagList", "b"));
            up(m, "t15", op("$pullAll", "tagList", List.of("c")));
            Map<String, Object> raw = raw(m, "t15");
            assertEquals(List.of("a", "d"), List.copyOf((List<?>) raw.get("tag_list")));
            assertNoStray(raw, "tagList");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void dottedKeysThroughASingleEmbeddedField(Morphium m) throws Exception {
        try (m) {
            up(m, "t16", set("tag", "t16", "childEntry", new Child("f")));
            up(m, "t16", set("childEntry.fileName", "g"));
            Map<?, ?> child = (Map<?, ?>) raw(m, "t16").get("child_entry");
            assertEquals("g", child.get("file_name"), String.valueOf(child));
            assertFalse(child.containsKey("fileName"), String.valueOf(child));
            assertNoStray(raw(m, "t16"), "childEntry");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void indexedPathsThroughListsTranslateOnlyTheLeadingField(Morphium m) throws Exception {
        try (m) {
            up(m, "t17", set("tag", "t17", "tagList", List.of("a", "b")));
            up(m, "t17", set("tagList.0", "z"));
            assertEquals(List.of("z", "b"), List.copyOf((List<?>) raw(m, "t17").get("tag_list")));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void embeddedElementsInListsAndMapValuesAreMarshalledAndTheirKeysAreData(Morphium m) throws Exception {
        try (m) {
            up(m, "t18", UtilsMap.of("$set", UtilsMap.of("tag", "t18", "data", UtilsMap.of("fooBar", 1)), "$push", UtilsMap.of("childList", new Child("f"))));
            Map<String, Object> raw = raw(m, "t18");
            Map<?, ?> element = (Map<?, ?>) ((List<?>) raw.get("child_list")).get(0);
            assertEquals("f", element.get("file_name"));
            assertEquals(1, ((Number) ((Map<?, ?>) raw.get("data")).get("fooBar")).intValue(), "keys of a map value are data");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void valueShapeDoesNotDependOnTheSwitch(Morphium m) throws Exception {
        try (m) {
            up(m, "t19a", set("tag", "t19a", "childList", List.of(new Child("f"))));
            m.getConfig().objectMappingSettings().setTranslateUpdateFieldNames(false);
            up(m, "t19b", set("tag", "t19b", "childList", List.of(new Child("f"))));
            assertEquals(String.valueOf(raw(m, "t19a").get("child_list")), String.valueOf(raw(m, "t19b").get("childList")));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void keysThatCollideAreMergedIfEqualAndRejectedIfDifferent(Morphium m) throws Exception {
        try (m) {
            up(m, "t20", set("tag", "t20", "ownerName", "A", "owner_name", "A"));
            assertEquals("A", raw(m, "t20").get("owner_name"));
            assertThrows(IllegalArgumentException.class, () -> up(m, "t20", set("ownerName", "B", "owner_name", "C")));
            assertEquals("A", raw(m, "t20").get("owner_name"), "nothing may have been written");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void conflictsAcrossOperatorsAreLeftToTheServer(Morphium m) throws Exception {
        try (m) {
            assumeFalse(m.getDriver() instanceof InMemoryDriver, "the InMemoryDriver does not detect path conflicts");
            up(m, "t21", set("tag", "t21", "ownerName", "A"));
            Throwable t = assertThrows(RuntimeException.class, () -> up(m, "t21", UtilsMap.of("$set", UtilsMap.of("ownerName", "B"), "$inc", UtilsMap.of("owner_name", 1))));
            boolean server = false;
            for (Throwable c = t; c != null; c = c.getCause()) {
                server |= c instanceof MorphiumDriverException;
            }
            assertTrue(server, String.valueOf(t));
            assertEquals("A", raw(m, "t21").get("owner_name"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theOneArgumentOverloadTranslatesToo(Morphium m) throws Exception {
        try (m) {
            up(m, "t22", set("tag", "t22", "ownerName", "A"));
            Lease old = q(m, "t22").findOneAndUpdate(set("ownerName", "B"));
            assertNotNull(old);
            assertEquals("B", raw(m, "t22").get("owner_name"));
            assertNoStray(raw(m, "t22"), "ownerName");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void leaseScenarioWithJavaNamedKeys(Morphium m) throws Exception {
        try (m) {
            Lease first = leaseFilter(m, "A").findOneAndUpdate(set("ownerName", "A", "expiresAt", new Date(System.currentTimeMillis() + 60_000)), true, true);
            assertEquals("job", first.id);
            assertEquals("A", first.ownerName);
            Lease renewed = leaseFilter(m, "A").findOneAndUpdate(set("ownerName", "A", "expiresAt", new Date(System.currentTimeMillis() + 120_000)), true, true);
            assertEquals("A", renewed.ownerName);
            assertEquals(1, m.createQueryFor(Lease.class).countAll());

            if (!(m.getDriver() instanceof InMemoryDriver)) {
                // MOR-030: the InMemoryDriver takes a held lease over instead of raising a duplicate key error
                Throwable t = assertThrows(RuntimeException.class, () -> leaseFilter(m, "B").findOneAndUpdate(set("ownerName", "B", "expiresAt", new Date(System.currentTimeMillis() + 60_000)), true, true));
                assertNotNull(t);
                assertEquals("A", m.createQueryFor(Lease.class).get().ownerName);
            }

            // release: expire the lease and drop the owner, with Java names
            leaseFilter(m, "A").findOneAndUpdate(UtilsMap.of("$set", UtilsMap.of("expiresAt", new Date(System.currentTimeMillis() - 1000)), "$unset", UtilsMap.of("ownerName", "")), false, true);
            Map<String, Object> raw = rawDocs(m.createQueryFor(Lease.class)).get(0);
            assertFalse(raw.containsKey("owner_name"), String.valueOf(raw));
            assertFalse(raw.containsKey("ownerName"), String.valueOf(raw));
            Lease b = leaseFilter(m, "B").findOneAndUpdate(set("ownerName", "B", "expiresAt", new Date(System.currentTimeMillis() + 60_000)), true, true);
            assertEquals("B", b.ownerName);
            assertEquals(1, m.createQueryFor(Lease.class).countAll());
        }
    }

    private Query<Lease> leaseFilter(Morphium m, String me) {
        Query<Lease> q = m.createQueryFor(Lease.class).f("id").eq("job");
        q.or(m.createQueryFor(Lease.class).f("expiresAt").lt(new Date()), m.createQueryFor(Lease.class).f("ownerName").eq(me));
        return q;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void filterAndUpdateAgreeOnEveryPersistentTopLevelField(Morphium m) throws Exception {
        try (m) {
            AnnotationAndReflectionHelper h = m.getARHelper();
            for (String name : List.of("id", "tag", "ownerName", "expiresAt", "retryCount", "backupCount", "tagList", "renamed", "currentOwner", "childEntry", "childList", "data")) {
                Map<String, Object> filter = m.createQueryFor(Lease.class).f(name).eq("x").toQueryObject();
                assertEquals(List.of(h.resolveStoredFieldName(Lease.class, name)), List.copyOf(filter.keySet()), name);
            }
        }
    }
}
