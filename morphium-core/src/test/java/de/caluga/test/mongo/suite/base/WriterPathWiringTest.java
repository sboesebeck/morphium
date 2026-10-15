package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * MOR-042: one test per entry point of the writer path ({@code Query.set}, {@code push}, {@code pushAll}, {@code addToSet},
 * {@code addAllToSet}, {@code pull}, {@code pullAll}, {@code setEnum}, upsert) with the expected stored document written out. The documents are read
 * without the projection to known fields. These tests describe the behaviour that must not change; the tests for the holes that are closed
 * by MOR-042 are in {@code WriterPathMarshallingTest}.
 */
@Tag("core")
public class WriterPathWiringTest extends MultiDriverTestBase {

    public enum Color {
        RED, GREEN
    }

    public enum Fields {
        text, counter
    }

    @Embedded(translateCamelCase = false)
    public static class Item {
        public String name;
        public int amount;

        public Item() {
        }

        public Item(String name, int amount) {
            this.name = name;
            this.amount = amount;
        }
    }

    @Embedded(translateCamelCase = false, polymorph = true, typeId = "wanimal")
    public static class Animal {
        public String name;
    }

    @Embedded(translateCamelCase = false, polymorph = true, typeId = "wdog")
    public static class Dog extends Animal {
        public int barks;
    }

    @Entity(translateCamelCase = false, typeId = "wtarget")
    public static class Target {
        @Id
        public MorphiumId id;
        public String label;
    }

    @Entity(translateCamelCase = false)
    public static class WDoc {
        @Id
        public String id;
        public String key;
        public String text;
        public int counter;
        public Date when;
        public Color color;
        public Item item;
        public Animal animal;
        public Object anything;
        public List<Item> items;
        public List<String> tags;
        public List<Long> nums;
        public Map<String, Item> itemMap;
        public Map<String, String> strMap;
    }

    /** the shape of the MorphiumMigrationRunner lock: Map.of(String, Date) with a @Property entity */
    @Entity(translateCamelCase = false)
    public static class Lock {
        @Id
        public String id;
        @Property(fieldName = "owner")
        public String ownerName;
        @Property(fieldName = "expires_at")
        public Date expiresAt;
    }

    private Query<WDoc> q(Morphium m, String key) {
        return m.createQueryFor(WDoc.class).f("key").eq(key);
    }

    private Map<String, Object> raw(Query<?> q) throws Exception {
        FindCommand cmd = q.getFindCmd();

        try {
            cmd.setProjection(new Doc());
            List<Map<String, Object>> docs = cmd.execute();
            assertEquals(1, docs.size(), String.valueOf(docs));
            return docs.get(0);
        } finally {
            cmd.releaseConnection();
        }
    }

    private Map<String, Object> raw(Morphium m, String key) throws Exception {
        return raw(q(m, key));
    }

    private WDoc store(Morphium m, String key) {
        // array operators need arrays, a null field is rejected by MongoDB ("non-array type null")
        WDoc d = new WDoc();
        d.key = key;
        d.tags = new ArrayList<>();
        d.items = new ArrayList<>();
        d.nums = new ArrayList<>();
        d.anything = new ArrayList<>();
        m.store(d);
        return d;
    }

    private static void assertItem(Object o, String name, int amount) {
        assertTrue(o instanceof Map, String.valueOf(o));
        Map<?, ?> m = (Map<?, ?>) o;
        assertEquals(name, m.get("name"));
        assertEquals(amount, ((Number) m.get("amount")).intValue());
        assertEquals(Item.class.getName(), m.get("class_name"), String.valueOf(m));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setStoresScalarsDatesAndEnumNames(Morphium m) throws Exception {
        try (m) {
            store(m, "s1");
            Date d = new Date(1_700_000_000_000L);
            Map<String, Object> values = new HashMap<>();
            values.put("text", "hello");
            values.put("counter", 7);
            values.put("when", d);
            values.put("color", Color.GREEN);
            q(m, "s1").set(values, false, false);
            Map<String, Object> raw = raw(m, "s1");
            assertEquals("hello", raw.get("text"));
            assertEquals(7, ((Number) raw.get("counter")).intValue());
            assertEquals(d.getTime(), ((Date) raw.get("when")).getTime());
            assertEquals("GREEN", raw.get("color"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setWithANullValueInAMapStoresNull(Morphium m) throws Exception {
        try (m) {
            store(m, "s2");
            q(m, "s2").set(Map.of("text", "x"), false, false);
            Map<String, Object> values = new HashMap<>();
            values.put("text", null);
            q(m, "s2").set(values, false, false);
            assertEquals(null, raw(m, "s2").get("text"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setOfASingleEmbeddedValueKeepsClassName(Morphium m) throws Exception {
        try (m) {
            store(m, "s3");
            q(m, "s3").set("item", new Item("a", 1), false, false);
            assertItem(raw(m, "s3").get("item"), "a", 1);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setOfAPolymorphicSubclassStoresItsTypeId(Morphium m) throws Exception {
        try (m) {
            store(m, "s4");
            Dog dog = new Dog();
            dog.name = "rex";
            dog.barks = 3;
            q(m, "s4").set("animal", dog, false, false);
            Map<?, ?> stored = (Map<?, ?>) raw(m, "s4").get("animal");
            assertEquals("wdog", stored.get("class_name"));
            assertEquals(3, ((Number) stored.get("barks")).intValue());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setOfASingleEntityKeepsItsIdAndClassName(Morphium m) throws Exception {
        try (m) {
            store(m, "s5");
            Target t = new Target();
            t.label = "t";
            m.store(t);
            q(m, "s5").set("anything", t, false, false);
            Map<?, ?> stored = (Map<?, ?>) raw(m, "s5").get("anything");
            assertEquals("wtarget", stored.get("class_name"));
            assertEquals("t", stored.get("label"));
            assertNotNull(stored.get("_id"), "the writer keeps the id of a single entity: " + stored);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setOfListsAndMapsOfEmbeddedAndScalarValues(Morphium m) throws Exception {
        try (m) {
            store(m, "s6");
            Map<String, Object> values = new HashMap<>();
            values.put("items", new ArrayList<>(List.of(new Item("a", 1), new Item("b", 2))));
            values.put("tags", new ArrayList<>(List.of("x", "y")));
            values.put("nums", new ArrayList<>(List.of(1L, 2L)));
            Map<String, Item> itemMap = new LinkedHashMap<>();
            itemMap.put("p", new Item("a", 1));
            values.put("itemMap", itemMap);
            values.put("strMap", new LinkedHashMap<>(Map.of("k", "v")));
            q(m, "s6").set(values, false, false);
            Map<String, Object> raw = raw(m, "s6");
            List<?> items = (List<?>) raw.get("items");
            assertEquals(2, items.size());
            assertItem(items.get(0), "a", 1);
            assertItem(items.get(1), "b", 2);
            assertEquals(List.of("x", "y"), List.copyOf((List<?>) raw.get("tags")));
            assertEquals(List.of(1L, 2L), ((List<?>) raw.get("nums")).stream().map(o -> ((Number) o).longValue()).toList());
            assertItem(((Map<?, ?>) raw.get("itemMap")).get("p"), "a", 1);
            assertEquals("v", ((Map<?, ?>) raw.get("strMap")).get("k"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setWithUpsertOnANonIdFilterInsertsTheDocument(Morphium m) throws Exception {
        try (m) {
            Map<String, Object> values = new HashMap<>();
            values.put("text", "inserted");
            values.put("item", new Item("a", 1));
            q(m, "s7").set(values, true, false);
            Map<String, Object> raw = raw(m, "s7");
            assertEquals("inserted", raw.get("text"));
            assertItem(raw.get("item"), "a", 1);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setEnumUsesTheConstantNameAsFieldName(Morphium m) throws Exception {
        try (m) {
            store(m, "s8");
            q(m, "s8").setEnum(Map.of(Fields.text, "viaEnum"), false, false);
            assertEquals("viaEnum", raw(m, "s8").get("text"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushOfScalarEmbeddedAndPolymorphicValues(Morphium m) throws Exception {
        try (m) {
            store(m, "p1");
            q(m, "p1").push("tags", "a");
            q(m, "p1").push("items", new Item("a", 1));
            Dog dog = new Dog();
            dog.name = "rex";
            q(m, "p1").push("anything", dog);
            Map<String, Object> raw = raw(m, "p1");
            assertEquals(List.of("a"), List.copyOf((List<?>) raw.get("tags")));
            assertItem(((List<?>) raw.get("items")).get(0), "a", 1);
            assertEquals("wdog", ((Map<?, ?>) ((List<?>) raw.get("anything")).get(0)).get("class_name"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushWithEachOfScalars(Morphium m) throws Exception {
        try (m) {
            store(m, "p2");
            q(m, "p2").push("tags", Map.of("$each", List.of("a", "b")), false, false, null);
            assertEquals(List.of("a", "b"), List.copyOf((List<?>) raw(m, "p2").get("tags")));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushAllOfScalarsAndEmbeddedValues(Morphium m) throws Exception {
        try (m) {
            store(m, "p3");
            q(m, "p3").pushAll("tags", List.of("a", "b"));
            q(m, "p3").pushAll("items", List.of(new Item("a", 1), new Item("b", 2)));
            Map<String, Object> raw = raw(m, "p3");
            assertEquals(List.of("a", "b"), List.copyOf((List<?>) raw.get("tags")));
            assertItem(((List<?>) raw.get("items")).get(1), "b", 2);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void addToSetOfAnEqualEmbeddedValueDoesNotDuplicate(Morphium m) throws Exception {
        try (m) {
            store(m, "a1");
            m.addToSet(q(m, "a1"), "items", new Item("a", 1));
            m.addToSet(q(m, "a1"), "items", new Item("a", 1));
            m.addToSet(q(m, "a1"), "tags", "t");
            m.addToSet(q(m, "a1"), "tags", "t");
            Map<String, Object> raw = raw(m, "a1");
            assertEquals(1, ((List<?>) raw.get("items")).size());
            assertEquals(1, ((List<?>) raw.get("tags")).size());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void addAllToSetOfEmbeddedValues(Morphium m) throws Exception {
        try (m) {
            store(m, "a2");
            m.addAllToSet(q(m, "a2"), "items", List.of(new Item("a", 1), new Item("b", 2)), false, false);
            m.addAllToSet(q(m, "a2"), "items", List.of(new Item("a", 1), new Item("c", 3)), false, false);
            assertEquals(3, ((List<?>) raw(m, "a2").get("items")).size());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pullOfScalarEmbeddedAndConditionValues(Morphium m) throws Exception {
        try (m) {
            store(m, "l1");
            q(m, "l1").pushAll("tags", List.of("a", "b", "c"));
            q(m, "l1").pushAll("items", List.of(new Item("a", 1), new Item("b", 2)));
            q(m, "l1").pushAll("nums", List.of(1L, 2L, 5L));
            q(m, "l1").pull("tags", "b");
            q(m, "l1").pull("items", new Item("a", 1));
            q(m, "l1").pull("nums", Map.of("$gt", 3));
            Map<String, Object> raw = raw(m, "l1");
            assertEquals(List.of("a", "c"), List.copyOf((List<?>) raw.get("tags")));
            assertEquals(1, ((List<?>) raw.get("items")).size());
            assertItem(((List<?>) raw.get("items")).get(0), "b", 2);
            assertEquals(2, ((List<?>) raw.get("nums")).size());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pullAllWithTheSixArgumentFormRemovesEveryElement(Morphium m) throws Exception {
        try (m) {
            store(m, "l2");
            q(m, "l2").pushAll("tags", List.of("a", "b", "c", "d"));
            q(m, "l2").pullAll("tags", List.of("a", "c"), false, false, null);
            assertEquals(List.of("b", "d"), List.copyOf((List<?>) raw(m, "l2").get("tags")));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void migrationLockShapedUpsertAcquiresAndRenews(Morphium m) throws Exception {
        try (m) {
            Query<Lock> acquire = m.createQueryFor(Lock.class).f("_id").eq("migrations");
            Date until = new Date(System.currentTimeMillis() + 60_000);
            acquire.set(Map.of("owner", "node-a", "expires_at", until), true, false);
            Map<String, Object> raw = raw(acquire);
            assertEquals("migrations", raw.get("_id"));
            assertEquals("node-a", raw.get("owner"));
            assertEquals(until.getTime(), ((Date) raw.get("expires_at")).getTime());

            Date later = new Date(System.currentTimeMillis() + 120_000);
            m.createQueryFor(Lock.class).f("_id").eq("migrations").f("owner").eq("node-a").set(Map.of("expires_at", later), false, false);
            assertEquals(later.getTime(), ((Date) raw(acquire).get("expires_at")).getTime());
            assertEquals(1, m.createQueryFor(Lock.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void currentDateStillDoesNotSetTheField(Morphium m) throws Exception {
        try (m) {
            // pinned defect MOR-004, see CurrentDateReproductionTest; the value function must not change it
            assumeFalse(m.getDriver() instanceof InMemoryDriver, "the InMemoryDriver reports a success");
            store(m, "c1");
            m.currentDate(q(m, "c1"), "when", false, false);
            assertFalse(raw(m, "c1").get("when") instanceof Date);
        }
    }
}
