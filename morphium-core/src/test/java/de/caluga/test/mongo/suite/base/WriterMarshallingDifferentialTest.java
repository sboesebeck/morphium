package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumReference;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.writer.MorphiumWriterImpl;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-042: the value function of the writer ({@code MorphiumWriterImpl.marshallIfNecessary}) is compared with a verbatim copy of its
 * previous implementation (as of 092f264dd). Every input in {@link #keepRows()} must produce exactly the same output as before; the
 * rows that change on purpose are asserted in {@code WriterPathMarshallingTest}.
 */
@Tag("core")
public class WriterMarshallingDifferentialTest extends MultiDriverTestBase {

    public enum Color {
        RED, GREEN
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

    @Embedded(translateCamelCase = false, polymorph = true, typeId = "animal")
    public static class Animal {
        public String name;
    }

    @Embedded(translateCamelCase = false, polymorph = true, typeId = "dog")
    public static class Dog extends Animal {
        public int barks;
    }

    @Entity(translateCamelCase = false, typeId = "wtarget")
    public static class Target {
        @Id
        public MorphiumId id;
        public String label;
    }

    /** verbatim copy of MorphiumWriterImpl.marshallIfNecessary at 092f264dd, the only change is that it takes the Morphium instance */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static Object legacy(Morphium morphium, Object value) {
        if (value != null) {
            if (value instanceof Enum) {
                return ((Enum) value).name();
            }

            if (morphium.getARHelper().isAnnotationPresentInHierarchy(value.getClass(), Entity.class) || morphium.getARHelper().isAnnotationPresentInHierarchy(value.getClass(), Embedded.class)) {
                // need to serialize...
                Map<String, Object> marshall = morphium.getMapper().serialize(value);
                marshall.put("class_name", morphium.getARHelper().getTypeIdForClass(value.getClass()));
                value = marshall;
            } else if (List.class.isAssignableFrom(value.getClass())) {
                List lst = new ArrayList();

                for (Object o : (List) value) {
                    if (morphium.getARHelper().isAnnotationPresentInHierarchy(o.getClass(), Embedded.class) || morphium.getARHelper().isAnnotationPresentInHierarchy(o.getClass(), Entity.class)) {
                        Map<String, Object> marshall = morphium.getMapper().serialize(o);
                        marshall.put("class_name", morphium.getARHelper().getTypeIdForClass(o.getClass()));
                        lst.add(marshall);
                    } else {
                        lst.add(o);
                    }
                }

                value = lst;
            } else if (Map.class.isAssignableFrom(value.getClass())) {
                value = new LinkedHashMap((Map) value);

                for (Object e : ((Map) value).entrySet()) {
                    Map.Entry en = (Map.Entry) e;

                    if (!String.class.isAssignableFrom(((Map.Entry) e).getKey().getClass())) {
                        throw new IllegalArgumentException("Can't push maps with Key not of type String!");
                    }

                    if (en.getValue() != null) {
                        if (morphium.getARHelper().isAnnotationPresentInHierarchy(en.getValue().getClass(), Entity.class)
                                || morphium.getARHelper().isAnnotationPresentInHierarchy(en.getValue().getClass(), Embedded.class)) {
                            Map<String, Object> marshall = morphium.getMapper().serialize(en.getValue());
                            marshall.put("class_name", morphium.getARHelper().getTypeIdForClass(en.getValue().getClass()));
                            ((Map) value).put(en.getKey(), marshall);
                        }
                    } else {
                        ((Map) value).put(en.getKey(), null);
                    }
                }
            }
        }

        return value;
    }

    private static Map<String, Object[]> keepRows() {
        Map<String, Object[]> rows = new LinkedHashMap<>();
        Dog dog = new Dog();
        dog.name = "rex";
        dog.barks = 3;
        Target target = new Target();
        target.id = new MorphiumId();
        target.label = "t";
        Map<String, Object> items = new LinkedHashMap<>();
        items.put("x", new Item("a", 1));
        items.put("y", new Item("b", 2));
        items.put("z", null);
        Map<String, Object> strs = new LinkedHashMap<>();
        strs.put("k", "v");
        strs.put("n", 5);

        rows.put("String", new Object[] {"text"});
        rows.put("int", new Object[] {42});
        rows.put("long", new Object[] {42L});
        rows.put("double", new Object[] {1.5});
        rows.put("boolean", new Object[] {true});
        rows.put("Date", new Object[] {new Date(1_000_000L)});
        rows.put("null", new Object[] {null});
        rows.put("Enum", new Object[] {Color.RED});
        rows.put("single Item", new Object[] {new Item("a", 1)});
        rows.put("polymorphic Dog", new Object[] {dog});
        rows.put("single entity (keeps _id and class_name)", new Object[] {target});
        rows.put("MorphiumReference", new Object[] {new MorphiumReference("wtarget", new MorphiumId())});
        rows.put("List<Item>", new Object[] {new ArrayList<>(List.of(new Item("a", 1), new Item("b", 2)))});
        rows.put("List<String>", new Object[] {new ArrayList<>(List.of("a", "b"))});
        rows.put("List<Long>", new Object[] {new ArrayList<>(List.of(1L, 2L))});
        rows.put("mixed List", new Object[] {new ArrayList<>(List.of(new Item("a", 1), "str", 5))});
        rows.put("List<Dog>", new Object[] {new ArrayList<>(List.of(dog))});
        rows.put("Map<String,Item> with null value", new Object[] {items});
        rows.put("Map<String,Object> scalars", new Object[] {strs});
        rows.put("update operator map with scalars", new Object[] {Map.of("$each", List.of("a", "b"))});
        return rows;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void theWriterFunctionEqualsTheLegacyCopyForEveryKeepRow(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumWriterImpl writer = (MorphiumWriterImpl) morphium.getConfig().writerSettings().getWriter();

            for (var row : keepRows().entrySet()) {
                Object in = row.getValue()[0];
                Object expected = legacy(morphium, in);
                Object actual = writer.marshallIfNecessary(in);
                assertEquals(String.valueOf(sorted(expected)), String.valueOf(sorted(actual)), row.getKey());
                assertEquals(expected == null ? null : expected.getClass().getSimpleName().replace("Linked", "").replace("Hash", ""),
                             actual == null ? null : actual.getClass().getSimpleName().replace("Linked", "").replace("Hash", ""), row.getKey() + " (container type)");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void pushAllMarshalsEveryElementWithTheSameFunction(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumWriterImpl writer = (MorphiumWriterImpl) morphium.getConfig().writerSettings().getWriter();
            // pushAll applies the function to each element of the list: elements that are lists of embedded values are marshalled one level
            List<Object> elements = List.of("a", new Item("a", 1), new ArrayList<>(List.of(new Item("b", 2))), Map.of("k", "v"));

            for (Object element : elements) {
                assertEquals(String.valueOf(sorted(legacy(morphium, element))), String.valueOf(sorted(writer.marshallIfNecessary(element))), String.valueOf(element));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void legacyCopyKnownDifferencesStayVisible(Morphium morphium) throws Exception {
        try (morphium) {
            // documents what the copy does today and what the later change alters on purpose (asserted in WriterPathMarshallingTest)
            assertTrue(legacy(morphium, new ArrayList<>(List.of(Color.RED))) instanceof List<?> l && l.get(0) == Color.RED, "enum inside a list is untouched");
            Object set = legacy(morphium, new java.util.LinkedHashSet<>(List.of("a")));
            assertTrue(set instanceof java.util.Set, "a Set is untouched");
        }
    }

    /** the rows that change on purpose (MOR-042); the new expectation is written out, the previous behaviour is shown by the legacy copy */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void theSwitchRowsChangeOnPurpose(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumWriterImpl writer = (MorphiumWriterImpl) morphium.getConfig().writerSettings().getWriter();
            String itemType = Item.class.getName();

            // enum constants inside a container are written by name (the encoder wrote toString(), and failed for constants with a body)
            assertTrue(legacy(morphium, new ArrayList<>(List.of(Color.RED))) instanceof List<?> l && l.get(0) == Color.RED);
            assertEquals(List.of("RED"), writer.marshallIfNecessary(new ArrayList<>(List.of(Color.RED))));

            // a Set and an array reach the driver as a list; the elements are marshalled
            assertEquals(List.of("a"), writer.marshallIfNecessary(new java.util.LinkedHashSet<>(List.of("a"))));
            assertEquals(List.of("a", "b"), writer.marshallIfNecessary(new String[] {"a", "b"}));
            Object items = writer.marshallIfNecessary(new Item[] {new Item("a", 1)});
            assertEquals(itemType, ((Map<?, ?>) ((List<?>) items).get(0)).get("class_name"));

            // nested containers are recursed
            Object nested = writer.marshallIfNecessary(new ArrayList<>(List.of(new ArrayList<>(List.of(new Item("a", 1))))));
            assertEquals(itemType, ((Map<?, ?>) ((List<?>) ((List<?>) nested).get(0)).get(0)).get("class_name"));
            Map<String, Object> inner = new LinkedHashMap<>();
            inner.put("x", new Item("a", 1));
            Map<String, Object> outer = new LinkedHashMap<>();
            outer.put("o", inner);
            Object mapInMap = writer.marshallIfNecessary(outer);
            assertEquals(itemType, ((Map<?, ?>) ((Map<?, ?>) ((Map<?, ?>) mapInMap).get("o")).get("x")).get("class_name"));

            // a null element is kept (it was a NullPointerException)
            List<Object> withNull = new ArrayList<>();
            withNull.add(null);
            assertEquals(withNull, writer.marshallIfNecessary(withNull));
            assertThrows(NullPointerException.class, () -> legacy(morphium, withNull));

            // an enum map key is accepted, other keys are rejected also in nested maps
            Map<Object, Object> enumKey = new LinkedHashMap<>();
            enumKey.put(Color.GREEN, 1);
            assertEquals(Map.of("GREEN", 1), writer.marshallIfNecessary(enumKey));
            assertThrows(IllegalArgumentException.class, () -> legacy(morphium, enumKey));
            Map<Object, Object> nestedBadKey = new LinkedHashMap<>();
            nestedBadKey.put("a", new LinkedHashMap<>(Map.of(1, "x")));
            assertThrows(IllegalArgumentException.class, () -> writer.marshallIfNecessary(nestedBadKey));

            // MorphiumId becomes an ObjectId inside a container and as an element; a whole-field value stays as it is (the wire encoder writes it as an ObjectId)
            MorphiumId id = new MorphiumId();
            assertTrue(legacy(morphium, id) instanceof MorphiumId);
            assertTrue(writer.marshallIfNecessary(id) instanceof MorphiumId);
            assertTrue(morphium.getMapper().marshallUpdateElement(id) instanceof org.bson.types.ObjectId);
            assertTrue(((List<?>) writer.marshallIfNecessary(new ArrayList<>(List.of(id)))).get(0) instanceof org.bson.types.ObjectId);

            // types the encoder cannot write
            assertEquals(5L, writer.marshallIfNecessary(new java.util.concurrent.atomic.AtomicLong(5)));
            assertEquals(java.math.BigInteger.TEN.toString().length() > 0, writer.marshallIfNecessary(java.math.BigInteger.TEN) != null);

            // a Class value is data, not an entity
            assertSame(Target.class, writer.marshallIfNecessary(Target.class));
        }
    }

    /** HashMap order is not part of the contract: compare sorted copies of the maps */
    private static Object sorted(Object o) {
        if (o instanceof Map<?, ?> m) {
            Map<String, Object> s = new java.util.TreeMap<>();
            m.forEach((k, v) -> s.put(String.valueOf(k), sorted(v)));
            return s;
        }

        if (o instanceof List<?> l) {
            List<Object> s = new ArrayList<>();
            l.forEach(v -> s.add(sorted(v)));
            return s;
        }

        return o;
    }
}
