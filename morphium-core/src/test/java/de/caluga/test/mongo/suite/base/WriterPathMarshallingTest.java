package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumStorageListener;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.writer.MorphiumWriterImpl;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * MOR-042: the writer path ({@code Query.set}, {@code push}, {@code pushAll}, {@code addToSet}, {@code addAllToSet}, {@code pull}, {@code pullAll}) marshals
 * the values with the shared update-value walk, on the calling thread. These tests cover what changed on purpose; the behaviour that must not
 * change is in {@code WriterMarshallingDifferentialTest} and {@code WriterPathWiringTest}.
 */
@Tag("core")
public class WriterPathMarshallingTest extends MultiDriverTestBase {

    public enum Color {
        RED, GREEN,
        BLUE {
            @Override
            public String toString() {
                return "blue!";
            }
        }
    }

    public enum Fields {
        text {
            @Override
            public String toString() {
                return "TEXT-LABEL";
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
    }

    @Entity(translateCamelCase = false, typeId = "mtarget")
    public static class Target {
        @Id
        public MorphiumId id;
        public String label;
    }

    @Entity(translateCamelCase = false)
    public static class MDoc {
        @Id
        public MorphiumId id;
        public String key;
        public String text;
        public Object anything;
        public Object other;
        public Item single;
        public List<Item> items;
        public Set<Item> itemSet;
        public Item[] itemArray;
        public Map<String, Object> data;
        public List<Color> colors;
        public List<MorphiumId> ids;
        public List<Object> objs;
        public List<String> tags;
        public AtomicLong counter;
        public BigInteger big;
    }

    private Query<MDoc> q(Morphium m, String key) {
        return m.createQueryFor(MDoc.class).f("key").eq(key);
    }

    private Map<String, Object> raw(Morphium m, String key) throws Exception {
        FindCommand cmd = q(m, key).getFindCmd();

        try {
            cmd.setProjection(new Doc());
            List<Map<String, Object>> docs = cmd.execute();
            assertEquals(1, docs.size(), String.valueOf(docs));
            return docs.get(0);
        } finally {
            cmd.releaseConnection();
        }
    }

    private MDoc store(Morphium m, String key) {
        MDoc d = new MDoc();
        d.key = key;
        d.tags = new ArrayList<>();
        d.items = new ArrayList<>();
        d.objs = new ArrayList<>();
        d.ids = new ArrayList<>();
        m.store(d);
        return d;
    }

    private static void assertItem(Object o, String name, int amount) {
        assertTrue(o instanceof Map, String.valueOf(o));
        Map<?, ?> m = (Map<?, ?>) o;
        assertEquals(name, m.get("name"), String.valueOf(m));
        assertEquals(amount, ((Number) m.get("amount")).intValue(), String.valueOf(m));
        assertEquals(Item.class.getName(), m.get("class_name"), String.valueOf(m));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setOfASetAnArrayAndNestedContainersOfEmbeddedValues(Morphium m) throws Exception {
        try (m) {
            store(m, "h1");
            Map<String, Object> values = new HashMap<>();
            values.put("itemSet", new LinkedHashSet<>(List.of(new Item("a", 1), new Item("b", 2))));
            values.put("itemArray", new Item[] {new Item("a", 1), new Item("b", 2)});
            values.put("data", new LinkedHashMap<>(Map.of("outer", new LinkedHashMap<>(Map.of("x", new Item("a", 1))))));
            values.put("objs", new ArrayList<>(List.of(new ArrayList<>(List.of(new Item("c", 3))))));
            q(m, "h1").set(values, false, false);
            Map<String, Object> raw = raw(m, "h1");
            List<?> set = (List<?>) raw.get("itemSet");
            assertEquals(2, set.size());
            assertItem(set.get(0), "a", 1);
            assertItem(set.get(1), "b", 2);
            assertItem(((List<?>) raw.get("itemArray")).get(1), "b", 2);
            assertItem(((Map<?, ?>) ((Map<?, ?>) raw.get("data")).get("outer")).get("x"), "a", 1);
            assertItem(((List<?>) ((List<?>) raw.get("objs")).get(0)).get(0), "c", 3);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void enumsInsideContainersAreStoredByNameAlsoWithToStringOverridesAndBodies(Morphium m) throws Exception {
        try (m) {
            store(m, "h2");
            Map<String, Object> values = new HashMap<>();
            values.put("colors", new ArrayList<>(List.of(Color.RED, Color.BLUE)));
            values.put("data", new LinkedHashMap<>(Map.of("c", Color.BLUE)));
            q(m, "h2").set(values, false, false);
            Map<String, Object> raw = raw(m, "h2");
            assertEquals(List.of("RED", "BLUE"), List.copyOf((List<?>) raw.get("colors")));
            assertEquals("BLUE", ((Map<?, ?>) raw.get("data")).get("c"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aNullElementInAListIsKept(Morphium m) throws Exception {
        try (m) {
            store(m, "h3");
            List<Object> withNull = new ArrayList<>();
            withNull.add("a");
            withNull.add(null);
            withNull.add(new Item("b", 2));
            q(m, "h3").set("objs", withNull, false, false, null);
            List<?> stored = (List<?>) raw(m, "h3").get("objs");
            assertEquals(3, stored.size());
            assertEquals("a", stored.get(0));
            assertEquals(null, stored.get(1));
            assertItem(stored.get(2), "b", 2);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void enumMapKeysAreAcceptedAndOtherKeysAreRejectedDirectly(Morphium m) throws Exception {
        try (m) {
            store(m, "h4");
            Map<Object, Object> enumKeys = new LinkedHashMap<>();
            enumKeys.put(Color.RED, "r");
            q(m, "h4").set("data", enumKeys, false, false, null);
            assertEquals("r", ((Map<?, ?>) raw(m, "h4").get("data")).get("RED"));

            Map<Object, Object> bad = new LinkedHashMap<>();
            bad.put(1, new Item("a", 1));
            // thrown from the call itself, not wrapped by the writer task, and nothing is written
            assertThrows(IllegalArgumentException.class, () -> q(m, "h4").set("data", bad, false, false, null));
            Map<Object, Object> nested = new LinkedHashMap<>();
            nested.put("outer", new LinkedHashMap<>(Map.of(1, "x")));
            assertThrows(IllegalArgumentException.class, () -> q(m, "h4").set("data", nested, false, false, null));
            assertThrows(IllegalArgumentException.class, () -> q(m, "h4").push("objs", bad));
            assertThrows(IllegalArgumentException.class, () -> q(m, "h4").pushAll("objs", List.of("ok", bad)));
            assertEquals("r", ((Map<?, ?>) raw(m, "h4").get("data")).get("RED"));
            assertTrue(((List<?>) raw(m, "h4").get("objs")).isEmpty());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anInvalidValueFiresNoUpdateEvent(Morphium m) throws Exception {
        try (m) {
            store(m, "h5");
            AtomicInteger pre = new AtomicInteger();
            MorphiumStorageListener<?> listener = (MorphiumStorageListener<?>) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {MorphiumStorageListener.class}, (proxy, method, args) -> {
                switch (method.getName()) {
                    case "preUpdate" -> pre.incrementAndGet();
                    case "equals" -> {
                        return proxy == args[0];
                    }
                    case "hashCode" -> {
                        return System.identityHashCode(proxy);
                    }
                    default -> {
                    }
                }

                return null;
            });
            m.addListener(listener);

            try {
                Map<Object, Object> bad = new LinkedHashMap<>();
                bad.put(1, "x");
                assertThrows(IllegalArgumentException.class, () -> q(m, "h5").set("data", bad, false, false, null));
                assertEquals(0, pre.get(), "the value is rejected before the update starts");
                q(m, "h5").set("text", "ok", false, false, null);
                assertEquals(1, pre.get());
            } finally {
                m.removeListener(listener);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void morphiumIdsInContainersAndAsElementsMatchStoredOnes(Morphium m) throws Exception {
        try (m) {
            MorphiumId a = new MorphiumId();
            MorphiumId b = new MorphiumId();
            store(m, "h6");
            q(m, "h6").set("ids", new ArrayList<>(List.of(a)), false, false, null);
            q(m, "h6").push("ids", b);
            m.addToSet(q(m, "h6"), "ids", a);
            assertEquals(List.of(a, b), q(m, "h6").get().ids);
            q(m, "h6").pull("ids", a);
            assertEquals(List.of(b), q(m, "h6").get().ids);

            if (m.getDriver() instanceof InMemoryDriver) {
                // the stored element type is the one store() writes
                assertEquals("ObjectId", ((List<?>) raw(m, "h6").get("ids")).get(0).getClass().getSimpleName());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushWithEachOfEmbeddedValuesAndPushAllOfNestedLists(Morphium m) throws Exception {
        try (m) {
            store(m, "h7");
            q(m, "h7").push("items", Map.of("$each", List.of(new Item("a", 1), new Item("b", 2))), false, false, null);
            q(m, "h7").pushAll("objs", List.of("s", List.of(new Item("c", 3))));
            Map<String, Object> raw = raw(m, "h7");
            assertItem(((List<?>) raw.get("items")).get(1), "b", 2);
            List<?> objs = (List<?>) raw.get("objs");
            assertEquals("s", objs.get(0));
            assertItem(((List<?>) objs.get(1)).get(0), "c", 3);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pullAllRemovesEveryElementInAllFourForms(Morphium m) throws Exception {
        try (m) {
            store(m, "h8");
            q(m, "h8").pushAll("tags", List.of("a", "b", "c", "d", "e", "f"));
            q(m, "h8").pullAll("tags", List.of("a", "b"));
            q(m, "h8").pullAll(Tags.tags, List.of("c"));
            m.pullAll(q(m, "h8"), "tags", new ArrayList<Object>(List.of("d")), false, false);
            m.pullAll(q(m, "h8"), Tags.tags, new ArrayList<Object>(List.of("e")), false, false);
            assertEquals(List.of("f"), List.copyOf((List<?>) raw(m, "h8").get("tags")));

            q(m, "h8").pushAll("items", List.of(new Item("a", 1), new Item("b", 2), new Item("c", 3)));
            q(m, "h8").pullAll("items", List.of(new Item("a", 1), new Item("c", 3)));
            List<?> items = (List<?>) raw(m, "h8").get("items");
            assertEquals(1, items.size());
            assertItem(items.get(0), "b", 2);
        }
    }

    public enum Tags {
        tags
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setWithANullValueAndWithAnEnumFieldUsesTheConstantName(Morphium m) throws Exception {
        try (m) {
            store(m, "h9");
            q(m, "h9").set("text", "x", false, false, null);
            q(m, "h9").set("text", null, false, false, null);
            assertEquals(null, raw(m, "h9").get("text"));
            // Fields.text overrides toString(), the field is still "text"
            q(m, "h9").set(Fields.text, "viaEnum", false, false, null);
            assertEquals("viaEnum", raw(m, "h9").get("text"));
            q(m, "h9").set(Fields.text, "again");
            assertEquals("again", raw(m, "h9").get("text"));
            q(m, "h9").set(Fields.text, "third", false, false);
            assertEquals("third", raw(m, "h9").get("text"));
            q(m, "h9").set(Fields.text, "fourth", null);
            assertEquals("fourth", raw(m, "h9").get("text"));
            q(m, "h9").push(Tags.tags, "t");
            assertEquals(List.of("t"), List.copyOf((List<?>) raw(m, "h9").get("tags")));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aClassValueIsDataAndNotAnEntity(Morphium m) throws Exception {
        try (m) {
            assertSame(Target.class, m.getMapper().marshallUpdateValue(Target.class));
            assertSame(Target.class, m.getMapper().marshallUpdateElement(Target.class));
            MorphiumWriterImpl writer = (MorphiumWriterImpl) m.getConfig().writerSettings().getWriter();
            assertSame(Target.class, writer.marshallIfNecessary(Target.class));
            assertSame(Item.class, m.getMapper().marshallUpdateValue(Item.class));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void typesTheEncoderCannotWriteAreMarshalledByTheirMapper(Morphium m) throws Exception {
        try (m) {
            store(m, "h10");
            Map<String, Object> values = new HashMap<>();
            values.put("counter", new AtomicLong(5));
            values.put("big", BigInteger.TEN);
            q(m, "h10").set(values, false, false);
            MDoc read = q(m, "h10").get();
            assertEquals(5L, read.counter.get());
            assertEquals(BigInteger.TEN, read.big);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void immutableInputsAreAccepted(Morphium m) throws Exception {
        try (m) {
            store(m, "h11");
            q(m, "h11").set(Map.of("tags", List.of("a", "b"), "text", "t"), false, false);
            q(m, "h11").set("data", Map.of("k", List.of(new Item("a", 1))), false, false, null);
            q(m, "h11").pushAll("tags", List.of("c"));
            Map<String, Object> raw = raw(m, "h11");
            assertEquals(List.of("a", "b", "c"), List.copyOf((List<?>) raw.get("tags")));
            assertItem(((List<?>) ((Map<?, ?>) raw.get("data")).get("k")).get(0), "a", 1);
        }
    }

    /** a list that remembers on which threads it was iterated */
    public static class RecordingList extends ArrayList<Object> {
        final Set<Thread> threads = Collections.synchronizedSet(new HashSet<>());

        @Override
        public Iterator<Object> iterator() {
            threads.add(Thread.currentThread());
            return super.iterator();
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void valuesAreMarshalledOnTheCallingThreadAlsoWithACallback(Morphium m) throws Exception {
        try (m) {
            store(m, "h12");
            RecordingList list = new RecordingList();
            list.add(new Item("a", 1));
            CountDownLatch done = new CountDownLatch(1);
            q(m, "h12").set("objs", list, false, false, new de.caluga.morphium.async.AsyncOperationCallback<MDoc>() {
                @Override
                public void onOperationSucceeded(de.caluga.morphium.async.AsyncOperationType type, Query<MDoc> q, long duration, List<MDoc> result, MDoc entity, Object... param) {
                    done.countDown();
                }

                @Override
                public void onOperationError(de.caluga.morphium.async.AsyncOperationType type, Query<MDoc> q, long duration, String error, Throwable t, MDoc entity, Object... param) {
                    done.countDown();
                }
            });
            assertTrue(done.await(30, TimeUnit.SECONDS));
            assertNotNull(list.threads);
            assertEquals(Set.of(Thread.currentThread()), list.threads, "the list is read on the calling thread only");

            // a collection changed after the call is not seen by the write
            RecordingList later = new RecordingList();
            later.add("a");
            q(m, "h12").pushAll("tags", later);
            later.add("b");
            assertEquals(List.of("a"), List.copyOf((List<?>) raw(m, "h12").get("tags")));
            assertFalse(((List<?>) raw(m, "h12").get("objs")).isEmpty());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void findOneAndUpdateAndTheWriterStoreSingleValuesInTheSameShape(Morphium m) throws Exception {
        try (m) {
            store(m, "h13a");
            store(m, "h13b");
            Target t = new Target();
            t.label = "t";
            m.store(t);
            q(m, "h13a").set("single", new Item("a", 1), false, false, null);
            q(m, "h13a").set("anything", t, false, false, null);
            q(m, "h13b").findOneAndUpdate(Map.of("$set", Map.of("single", new Item("a", 1), "anything", t)));
            Map<String, Object> a = raw(m, "h13a");
            Map<String, Object> b = raw(m, "h13b");
            assertEquals(a.get("single"), b.get("single"));
            assertEquals(a.get("anything"), b.get("anything"));
            assertEquals("mtarget", ((Map<?, ?>) b.get("anything")).get("class_name"));
            assertNotNull(((Map<?, ?>) b.get("anything")).get("_id"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aSetOfStringsAndAnArrayOfStringsAreStoredAsLists(Morphium m) throws Exception {
        try (m) {
            assumeTrue(true);
            store(m, "h14");
            q(m, "h14").set("data", Map.of("s", new LinkedHashSet<>(List.of("a", "b")), "arr", new String[] {"x", "y"}), false, false, null);
            Map<?, ?> data = (Map<?, ?>) raw(m, "h14").get("data");
            assertEquals(List.of("a", "b"), List.copyOf((List<?>) data.get("s")));
            assertEquals(List.of("x", "y"), List.copyOf((List<?>) data.get("arr")));
        }
    }
}
