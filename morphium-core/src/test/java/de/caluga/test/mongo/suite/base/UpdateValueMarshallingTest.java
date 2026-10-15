package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Reference;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * MOR-002: update documents handed to {@code Query.findOneAndUpdate} are marshalled (POJOs and enums are rewritten,
 * everything else is passed through), on the InMemoryDriver and on a real MongoDB.
 */
@Tag("core")
public class UpdateValueMarshallingTest extends MultiDriverTestBase {

    public enum Color {
        RED, GREEN, BLUE {
            @Override
            public String toString() {
                return "blue!";
            }
        }
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

        @Override
        public boolean equals(Object o) {
            return o instanceof Item i && amount == i.amount && java.util.Objects.equals(name, i.name);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, amount);
        }
    }

    @Embedded(translateCamelCase = false, polymorph = true)
    public static class Animal {
        public String name;
    }

    @Embedded(translateCamelCase = false, polymorph = true)
    public static class Dog extends Animal {
        public int barks;
    }

    @Entity(translateCamelCase = false)
    public static class Target {
        @Id
        public MorphiumId id;
        public String label;
    }

    @Entity(translateCamelCase = false)
    public static class Doc1 {
        @Id
        public MorphiumId id;
        public String key;
        public List<Item> items;
        public Set<Item> itemSet;
        public List<Animal> animals;
        public Color color;
        public List<Color> colors;
        public List<MorphiumId> ids;
        public AtomicLong counter;
        public BigInteger big;
        public String text;
        @Reference
        public Target ref;
        @Reference
        public List<Target> refs;
    }

    private Query<Doc1> q(Morphium m, String key) {
        return m.createQueryFor(Doc1.class).f("key").eq(key);
    }

    private Doc1 upsert(Morphium m, String key, Map<String, Object> update) {
        return q(m, key).findOneAndUpdate(update, true, true);
    }

    private Map<String, Object> raw(Morphium m, String key) {
        List<Map<String, Object>> docs = q(m, key).asMapList();
        assertEquals(1, docs.size(), "one raw document expected for " + key + ": " + docs);
        return docs.get(0);
    }

    private static Map<String, Object> setOf(String key, String field, Object value) {
        Map<String, Object> set = new HashMap<>();
        set.put("key", key);
        set.put(field, value);
        return UtilsMap.of("$set", set);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void typedResultIsDeserialisedForAllShapes(Morphium m) throws Exception {
        try (m) {
            Doc1 d = upsert(m, "list", setOf("list", "items", List.of(new Item("a", 1), new Item("b", 2))));
            assertEquals(List.of(new Item("a", 1), new Item("b", 2)), d.items);
            d = upsert(m, "set", setOf("set", "itemSet", new LinkedHashSet<>(List.of(new Item("a", 1), new Item("b", 2)))));
            assertEquals(new LinkedHashSet<>(List.of(new Item("a", 1), new Item("b", 2))), d.itemSet);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void callersUpdateMapIsNotMutatedAndMayBeImmutable(Morphium m) throws Exception {
        try (m) {
            Map<String, Object> update = Map.of("$set", Map.of("key", "imm", "items", List.of(new Item("a", 1))));
            Doc1 d = upsert(m, "imm", update);
            assertEquals(List.of(new Item("a", 1)), d.items);
            assertTrue(((Map<?, ?>) update.get("$set")).get("items") instanceof List<?> l && l.get(0) instanceof Item,
                       "the caller's map must still hold the POJO");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void alreadySerialisedElementsAreStoredUnchanged(Morphium m) throws Exception {
        try (m) {
            Map<String, Object> pre = m.getMapper().serialize(new Item("a", 1));
            upsert(m, "pre", setOf("pre", "items", new ArrayList<>(List.of(pre))));
            Object stored = ((List<?>) raw(m, "pre").get("items")).get(0);
            assertEquals(pre, new HashMap<>((Map<?, ?>) stored), "pre-serialised document must not be changed (no class_name added)");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void marshallingIsIdempotent(Morphium m) throws Exception {
        try (m) {
            List<Object> values = new ArrayList<>();
            values.add(new Item("a", 1));
            values.add(List.of(new Item("a", 1), Color.RED));
            values.add(new LinkedHashSet<>(List.of(new Item("b", 2))));
            values.add(new Item[] {new Item("c", 3)});
            values.add(Map.of("x", Map.of("y", new Item("d", 4)), "e", Color.GREEN));
            values.add("plain");
            values.add(42);
            values.add(null);

            for (Object v : values) {
                Object once = m.getMapper().marshallUpdateValue(v);
                Object twice = m.getMapper().marshallUpdateValue(once);
                assertEquals(String.valueOf(once), String.valueOf(twice), "not idempotent for " + v);
                assertFalse(containsPojo(once), "POJO left in " + once);
            }
        }
    }

    private static boolean containsPojo(Object o) {
        if (o instanceof Item || o instanceof Enum) {
            return true;
        }
        if (o instanceof Map<?, ?> mm) {
            return mm.values().stream().anyMatch(UpdateValueMarshallingTest::containsPojo);
        }
        if (o instanceof Iterable<?> it) {
            for (Object e : it) {
                if (containsPojo(e)) {
                    return true;
                }
            }
        }
        return false;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void replacementDocumentWithoutOperatorsIsSentUnchanged(Morphium m) throws Exception {
        try (m) {
            // the InMemoryDriver cannot apply a replacement document at all (GitHub #399)
            assumeFalse(m.getDriver() instanceof InMemoryDriver);
            Map<String, Object> replacement = new HashMap<>();
            replacement.put("key", "repl");
            replacement.put("text", "hello");
            upsert(m, "repl", replacement);
            Map<String, Object> r = raw(m, "repl");
            assertEquals("hello", r.get("text"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void enumsAreStoredByNameAlsoInsideListsAndForConstantsWithBody(Morphium m) throws Exception {
        try (m) {
            upsert(m, "enum", setOf("enum", "colors", List.of(Color.RED, Color.BLUE)));
            Map<String, Object> r = raw(m, "enum");
            assertEquals(List.of("RED", "BLUE"), new ArrayList<>((List<?>) r.get("colors")));
            Doc1 d = upsert(m, "enum2", setOf("enum2", "color", Color.BLUE));
            assertEquals("BLUE", raw(m, "enum2").get("color"));
            assertEquals(Color.BLUE, d.color);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushAndAddToSetOfASinglePojoStoreAnElementWithClassName(Morphium m) throws Exception {
        try (m) {
            upsert(m, "add", UtilsMap.of("$setOnInsert", UtilsMap.of("key", "add"), "$addToSet", UtilsMap.of("items", new Item("a", 1))));
            upsert(m, "add", UtilsMap.of("$addToSet", UtilsMap.of("items", new Item("a", 1))));
            upsert(m, "add", UtilsMap.of("$push", UtilsMap.of("items", new Item("b", 2))));
            List<?> items = (List<?>) raw(m, "add").get("items");
            assertEquals(2, items.size(), "addToSet of an equal element must not duplicate, push appends: " + items);
            for (Object o : items) {
                assertEquals(Item.class.getName(), ((Map<?, ?>) o).get("class_name"), String.valueOf(o));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void addToSetWithEachMarshalsEveryElementOnce(Morphium m) throws Exception {
        try (m) {
            Map<String, Object> each = UtilsMap.of("$each", List.of(new Item("a", 1), new Item("b", 2)));
            upsert(m, "each", UtilsMap.of("$setOnInsert", UtilsMap.of("key", "each"), "$addToSet", UtilsMap.of("items", each)));
            upsert(m, "each", UtilsMap.of("$addToSet", UtilsMap.of("items", UtilsMap.of("$each", List.of(new Item("a", 1), new Item("c", 3))))));
            List<?> items = (List<?>) raw(m, "each").get("items");
            assertEquals(3, items.size(), String.valueOf(items));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void polymorphicSubclassKeepsItsTypeId(Morphium m) throws Exception {
        try (m) {
            Dog dog = new Dog();
            dog.name = "rex";
            dog.barks = 3;
            Doc1 d = upsert(m, "dog", setOf("dog", "animals", List.of(dog)));
            Map<?, ?> stored = (Map<?, ?>) ((List<?>) raw(m, "dog").get("animals")).get(0);
            assertEquals(Dog.class.getName(), stored.get("class_name"));
            assertInstanceOf(Dog.class, d.animals.get(0));
            assertEquals(3, ((Dog) d.animals.get(0)).barks);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void oneArgumentFindOneAndUpdateAlsoMarshals(Morphium m) throws Exception {
        try (m) {
            Doc1 d = new Doc1();
            d.key = "one";
            d.text = "before";
            m.store(d);
            Doc1 old = q(m, "one").findOneAndUpdate(UtilsMap.of("$set", UtilsMap.of("items", List.of(new Item("a", 1)))));
            assertNotNull(old, "legacy overload used to swallow the encoder error and return null");
            assertEquals("before", old.text);
            assertEquals(1, ((List<?>) raw(m, "one").get("items")).size());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void morphiumIdsInAddToSetAndPullMatchStoredElements(Morphium m) throws Exception {
        try (m) {
            MorphiumId a = new MorphiumId();
            MorphiumId b = new MorphiumId();
            Doc1 d = new Doc1();
            d.key = "ids";
            d.ids = new ArrayList<>(List.of(a));
            m.store(d);
            q(m, "ids").findOneAndUpdate(UtilsMap.of("$addToSet", UtilsMap.of("ids", b)), false, true);
            q(m, "ids").findOneAndUpdate(UtilsMap.of("$addToSet", UtilsMap.of("ids", a)), false, true);
            Doc1 afterAdd = q(m, "ids").get();
            assertEquals(List.of(a, b), afterAdd.ids, "a must not be duplicated, b appended");
            Doc1 afterPull = q(m, "ids").findOneAndUpdate(UtilsMap.of("$pull", UtilsMap.of("ids", a)), false, true);
            assertEquals(List.of(b), afterPull.ids);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void typesWithoutNativeEncoderSupportAreMarshalledByTheirMapper(Morphium m) throws Exception {
        try (m) {
            Map<String, Object> set = new HashMap<>();
            set.put("key", "leaf");
            set.put("counter", new AtomicLong(5));
            set.put("big", BigInteger.TEN);
            Doc1 d = upsert(m, "leaf", UtilsMap.of("$set", set));
            assertEquals(5L, d.counter.get());
            assertEquals(BigInteger.TEN, d.big);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void entityValueForAReferenceFieldIsRejectedInsteadOfEmbedded(Morphium m) throws Exception {
        try (m) {
            Target t = new Target();
            t.label = "t";
            m.store(t);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> upsert(m, "ref", setOf("ref", "ref", t)));
            assertTrue(e.getMessage().contains("ref") && e.getMessage().contains("@Reference"), e.getMessage());
            assertThrows(IllegalArgumentException.class, () -> upsert(m, "refs", setOf("refs", "refs", List.of(t))));
            assertEquals(0, q(m, "ref").countAll(), "nothing may have been written");

            // the supported way: pass the id
            Doc1 d = upsert(m, "refok", setOf("refok", "ref", t.id));
            assertNotNull(d);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void nonStringMapKeyIsRejectedBeforeAnythingIsSent(Morphium m) throws Exception {
        try (m) {
            Map<Object, Object> bad = new LinkedHashMap<>();
            bad.put(1, new Item("a", 1));
            assertThrows(IllegalArgumentException.class, () -> upsert(m, "badkey", setOf("badkey", "items", bad)));
            assertEquals(0, q(m, "badkey").countAll());
        }
    }
}
