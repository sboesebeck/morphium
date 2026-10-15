package de.caluga.test.mongo.suite.repro;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.repro.ReproSupport.Outcome;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reproduction of hypotheses 1 and 4 of the Morphium update-path research (docs/research/reproduction-results.md):
 * <ol>
 * <li>A POJO (here an {@code @Embedded} class) inside a raw {@code $set} map handed to
 * {@link Query#findOneAndUpdate(Map, boolean, boolean)} - as a List, a Set, an array, a single value and
 * as the value of a nested Map - is not marshalled; the wire encoder cannot encode it
 * ("Unhandled Data type").</li>
 * <li>The InMemoryDriver does not go through the wire encoder, so it may accept what a real MongoDB
 * connection rejects.</li>
 * </ol>
 * The test asserts the CURRENT behaviour per driver (it documents a defect, it does not fix it). It runs
 * against the InMemoryDriver by default and, in addition, against a real MongoDB when one is configured
 * (e.g. {@code ./runtests.sh --uri mongodb://127.0.0.1:27099/repro --driver pooled,inmem --test PojoInSetOperatorReproductionTest}).
 * If a defect gets fixed, the matching assertion fails and must be turned into the positive expectation.
 */
@Tag("core")
public class PojoInSetOperatorReproductionTest extends MultiDriverTestBase {

    @Entity(translateCamelCase = false)
    public static class Holder {
        @Id
        public de.caluga.morphium.driver.MorphiumId id;
        public String key;
        public List<Item> items;
        public Set<Item> itemSet;
        public Item single;
        public Item[] itemArray;
        public Map<String, Item> itemMap;
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

    private static boolean isInMem(Morphium m) {
        return m.getDriver() instanceof InMemoryDriver;
    }

    /** Runs findOneAndUpdate(upsert=true, returnNew=true) with {$set: {key: ..., field: value}}. */
    private Outcome<Holder> upsertWith(Morphium m, String key, String field, Object value) {
        Query<Holder> q = m.createQueryFor(Holder.class).f("key").eq(key);
        Map<String, Object> set = new HashMap<>();
        set.put("key", key);
        set.put(field, value);
        return ReproSupport.attempt(() -> q.findOneAndUpdate(UtilsMap.of("$set", set), true, true));
    }

    private Map<String, Object> rawDoc(Morphium m, String key) {
        List<Map<String, Object>> docs = m.createQueryFor(Holder.class).f("key").eq(key).asMapList();
        assertEquals(1, docs.size(), "exactly one raw document expected for key " + key);
        return docs.get(0);
    }

    private static List<Item> list() {
        return new ArrayList<>(List.of(new Item("a", 1), new Item("b", 2)));
    }

    /**
     * Hypothesis 1 for the failing shapes. Real MongoDB: the encoder throws "Unhandled Data type" wrapped in
     * a MorphiumDriverException ("Error sending Request") and nothing is stored. InMemoryDriver (hypothesis 4):
     * see the second branch of each assertion.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void rawPojoShapesInSet(Morphium morphium) throws Exception {
        try (morphium) {
            String driver = morphium.getDriver().getName();

            // shape -> (field, value)
            Map<String, Object[]> shapes = new LinkedHashMap<>();
            shapes.put("list", new Object[] {"items", list()});
            shapes.put("set", new Object[] {"itemSet", new LinkedHashSet<>(list())});
            shapes.put("array", new Object[] {"itemArray", new Item[] {new Item("a", 1), new Item("b", 2)}});
            shapes.put("single", new Object[] {"single", new Item("a", 1)});
            Map<String, Object> nestedMap = new LinkedHashMap<>();
            nestedMap.put("x", new Item("a", 1));
            nestedMap.put("y", new Item("b", 2));
            shapes.put("map", new Object[] {"itemMap", nestedMap});
            Map<String, Object> mapInMap = new LinkedHashMap<>();
            mapInMap.put("outer", new LinkedHashMap<>(nestedMap));
            shapes.put("mapInMap", new Object[] {"itemMap", mapInMap});

            for (var e : shapes.entrySet()) {
                String shape = e.getKey();
                String key = "k-" + shape;
                Outcome<Holder> o = upsertWith(morphium, key, (String) e.getValue()[0], e.getValue()[1]);
                ReproSupport.report(driver, "H1/H4 shape=" + shape, o.describe());

                if (isInMem(morphium)) {
                    recordInMemBehaviour(morphium, shape, key, o);
                } else {
                    // real MongoDB: encoder failure
                    assertTrue(o.failed(), shape + ": expected failure on real MongoDB but got " + o.describe());
                    MorphiumDriverException mde = o.find(MorphiumDriverException.class);
                    assertNotNull(mde, shape + ": expected a MorphiumDriverException in the cause chain: " + o.describe());
                    assertTrue(ReproSupport.describe(o.error).contains("Unhandled Data type"),
                               shape + ": expected 'Unhandled Data type' in the chain: " + o.describe());
                    assertTrue(morphium.createQueryFor(Holder.class).f("key").eq(key).asMapList().isEmpty(),
                               shape + ": nothing may have been stored");
                }
            }
        }
    }

    /**
     * InMemoryDriver branch (hypothesis 4). Observed: List, array and (nested) Map are ACCEPTED and the POJO
     * instances are stored as live Java objects inside the in-memory document (no BSON encoding step happens);
     * a Set and a single POJO also reach the store (the POJO is persisted as a Java object), but the call then
     * fails with an incidental ClassCastException while the driver builds the response, so the caller sees an
     * error AND a half-written document. There is no deliberate contract check.
     */
    private void recordInMemBehaviour(Morphium morphium, String shape, String key, Outcome<Holder> o) {
        boolean accepted = switch (shape) {
            case "list", "array", "map", "mapInMap" -> true;
            default -> false;
        };

        if (!accepted) {
            ReproSupport.report("InMemoryDriver", "H4 shape=" + shape + " REJECTED", o.describe());
            assertTrue(o.failed(), shape + ": expected InMem to reject, got " + o.describe());
            assertTrue(ReproSupport.describe(o.error).contains("ClassCastException"),
                       shape + ": expected an incidental ClassCastException: " + o.describe());
            List<Map<String, Object>> after = morphium.createQueryFor(Holder.class).f("key").eq(key).asMapList();
            ReproSupport.report("InMemoryDriver", "H4 shape=" + shape + " REJECTED, raw documents afterwards", String.valueOf(after));
            // the exception is thrown only AFTER the document was written: the POJO is already stored as a Java object
            assertEquals(1, after.size(), shape + ": partial write expected");
            Object stored = after.get(0).get("set".equals(shape) ? "itemSet" : "single");
            Object first = stored instanceof java.util.Collection<?> c ? c.iterator().next() : stored;
            assertTrue(first instanceof Item, shape + ": the raw POJO was stored, got " + first);
            return;
        }

        assertFalse(o.failed(), shape + ": expected InMem to accept, got " + o.describe());
        Map<String, Object> raw = rawDoc(morphium, key);
        ReproSupport.report("InMemoryDriver", "H4 shape=" + shape + " ACCEPTED raw", String.valueOf(raw));
        Object stored = switch (shape) {
            case "list" -> ((List<?>) raw.get("items")).get(0);
            case "array" -> ((Object[]) raw.get("itemArray"))[0];
            case "map" -> ((Map<?, ?>) raw.get("itemMap")).get("x");
            default -> ((Map<?, ?>) ((Map<?, ?>) raw.get("itemMap")).get("outer")).get("x");
        };
        // the stored value is the Java object itself, not a BSON document
        assertTrue(stored instanceof Item, shape + ": InMem keeps the raw POJO in the document, got " + stored.getClass());
        assertNotNull(o.value, shape + ": InMem returned no document");
    }

    /**
     * For comparison: the writer path ({@code Query.set(field, value, upsert, multiple)}) goes through
     * {@code MorphiumWriterImpl.marshallIfNecessary}, which handles a single @Embedded, a List and a Map of
     * @Embedded values - but not a Set, an array or a Map nested in a Map. Documents which shapes option B
     * ("make marshallIfNecessary handle any Iterable/array/nested Map") would additionally fix.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void writerPathShapes(Morphium morphium) throws Exception {
        try (morphium) {
            String driver = morphium.getDriver().getName();
            Map<String, Object> nestedMap = new LinkedHashMap<>();
            nestedMap.put("x", new Item("a", 1));
            Map<String, Object> mapInMap = new LinkedHashMap<>();
            mapInMap.put("outer", new LinkedHashMap<>(nestedMap));

            Map<String, Object[]> shapes = new LinkedHashMap<>();
            shapes.put("list", new Object[] {"items", list(), true});
            shapes.put("single", new Object[] {"single", new Item("a", 1), true});
            shapes.put("map", new Object[] {"itemMap", nestedMap, true});
            shapes.put("set", new Object[] {"itemSet", new LinkedHashSet<>(list()), false});
            shapes.put("array", new Object[] {"itemArray", new Item[] {new Item("a", 1)}, false});
            shapes.put("mapInMap", new Object[] {"itemMap", mapInMap, false});

            for (var e : shapes.entrySet()) {
                String shape = e.getKey();
                String key = "w-" + shape;
                boolean handled = (Boolean) e.getValue()[2];
                Query<Holder> q = morphium.createQueryFor(Holder.class).f("key").eq(key);
                Map<String, Object> set = new HashMap<>();
                set.put("key", key);
                set.put((String) e.getValue()[0], e.getValue()[1]);
                Outcome<Map<String, Object>> o = ReproSupport.attempt(() -> q.set(set, true, false));
                ReproSupport.report(driver, "H1 writer path (Query.set) shape=" + shape, o.describe());
                ReproSupport.report(driver, "H1 writer path (Query.set) shape=" + shape + " raw documents",
                                    String.valueOf(morphium.createQueryFor(Holder.class).f("key").eq(key).asMapList()));

                if (isInMem(morphium)) {
                    continue; // InMem is only documented here, see the REPRO-RESULT lines
                }
                if (handled) {
                    assertFalse(o.failed(), shape + ": " + o.describe());
                    assertEquals(1, morphium.createQueryFor(Holder.class).f("key").eq(key).asMapList().size());
                } else {
                    assertTrue(o.failed(), shape + ": expected the unhandled shape to fail: " + o.describe());
                    assertTrue(ReproSupport.describe(o.error).contains("Unhandled Data type"), o.describe());
                    assertTrue(morphium.createQueryFor(Holder.class).f("key").eq(key).asMapList().isEmpty());
                }
            }
        }
    }

    /** Control: the same shapes marshalled by hand through the mapper succeed on both drivers. */
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void preSerialisedShapesWork(Morphium morphium) throws Exception {
        try (morphium) {
            List<Map<String, Object>> serialised = new ArrayList<>();
            for (Item i : list()) {
                serialised.add(morphium.getMapper().serialize(i));
            }
            Outcome<Holder> o = upsertWith(morphium, "pre", "items", serialised);
            ReproSupport.report(morphium.getDriver().getName(), "H1 control (mapper.serialize per element)", o.describe());
            assertFalse(o.failed(), o.describe());
            Map<String, Object> raw = rawDoc(morphium, "pre");
            ReproSupport.report(morphium.getDriver().getName(), "H1 control raw document", String.valueOf(raw));
            assertEquals(2, ((List<?>) raw.get("items")).size());
            assertEquals(2, o.value.items.size());
            assertEquals(new Item("a", 1), o.value.items.get(0));
        }
    }
}
