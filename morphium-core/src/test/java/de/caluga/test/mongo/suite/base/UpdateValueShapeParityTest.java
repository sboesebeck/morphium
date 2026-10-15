package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.FindCommand;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The update entry points of Morphium ({@code Query.findOneAndUpdate(Map)}, the update builder, {@code Query.set/push/pull}) write the SAME on-disk shape as {@code store()}, per
 * position: a whole field holds the bare output of a custom type mapper, a container element holds {@code serialize()}'s shape ({@code {"value": scalar}} for a scalar
 * mapper). Compared on the raw persisted documents, with {@code useBsonDateForJavaTime} off and on (#335). Also: positional targets, {@code $pull} of an element and
 * of conditions, and a document written flat by an older version.
 */
@Tag("core")
public class UpdateValueShapeParityTest extends MultiDriverTestBase {
    private static final LocalDate D = LocalDate.ofEpochDay(18997L);
    private static final LocalDate D2 = LocalDate.ofEpochDay(19000L);
    private static final LocalDate D3 = LocalDate.ofEpochDay(19005L);
    private static final Instant I = Instant.ofEpochSecond(1600000000, 123456789);
    private static final BigDecimal AMT = new BigDecimal("2.500");

    @Embedded
    public static class Item {
        public String name;
        public LocalDate when;

        public Item() {
        }

        public Item(String name, LocalDate when) {
            this.name = name;
            this.when = when;
        }
    }

    @Entity
    public static class Shaped {
        @Id
        public MorphiumId id;
        public LocalDate date;
        public Instant instant;
        public List<LocalDate> dateList;
        public Map<String, LocalDate> dateMap;
        public BigDecimal amount;
        public List<BigDecimal> amounts;
        public Character ch;
        public Item item;
        public List<Item> items;
    }

    private static Shaped full() {
        Shaped e = new Shaped();
        e.id = new MorphiumId();
        e.date = D;
        e.instant = I;
        e.dateList = new ArrayList<>(List.of(D, D2));
        e.dateMap = new HashMap<>(Map.of("k", D));
        e.amount = AMT;
        e.amounts = new ArrayList<>(List.of(AMT));
        e.ch = 'A';
        e.item = new Item("a", D);
        e.items = new ArrayList<>(List.of(new Item("a", D), new Item("b", D2)));
        return e;
    }

    // "item" (a single embedded value) is deliberately not compared: store() omits class_name for a non-polymorphic single value, an update writes it always, see the last test
    private static final List<String> FIELDS = List.of("date", "instant", "dateList", "dateMap", "amount", "amounts", "ch", "items");

    private static Map<String, Object> values() {
        Shaped f = full();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("date", f.date);
        m.put("instant", f.instant);
        m.put("dateList", f.dateList);
        m.put("dateMap", f.dateMap);
        m.put("amount", f.amount);
        m.put("amounts", f.amounts);
        m.put("ch", f.ch);
        m.put("items", f.items);
        return m;
    }

    private Shaped blank(Morphium m) {
        Shaped s = new Shaped();
        s.id = new MorphiumId();
        m.store(s);
        return s;
    }

    /** a document whose containers exist (and are empty), the target of push and addToSet */
    private Shaped withEmptyContainers(Morphium m) {
        Shaped s = new Shaped();
        s.id = new MorphiumId();
        s.dateList = new ArrayList<>();
        s.amounts = new ArrayList<>();
        s.items = new ArrayList<>();
        m.store(s);
        return s;
    }

    private static String stored(Morphium m, String javaField) {
        return m.getARHelper().getMongoFieldName(Shaped.class, javaField);
    }

    private static Map<String, Object> raw(Morphium m, MorphiumId id) throws Exception {
        FindCommand find = new FindCommand(m.getDriver().getPrimaryConnection(null)).setDb(m.getDatabase()).setColl(m.getMapper().getCollectionName(Shaped.class)).setFilter(Map.of("_id", id));

        try {
            List<Map<String, Object>> res = find.execute();
            return res.isEmpty() ? null : res.get(0);
        } finally {
            find.releaseConnection();
        }
    }

    private static boolean same(Object a, Object b) {
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() == ((Number) b).doubleValue();
        }

        if (a instanceof Map && b instanceof Map) {
            Map<?, ?> ma = (Map<?, ?>) a;
            Map<?, ?> mb = (Map<?, ?>) b;

            if (ma.size() != mb.size()) {
                return false;
            }

            for (Map.Entry<?, ?> e : ma.entrySet()) {
                if (!mb.containsKey(e.getKey()) || !same(e.getValue(), mb.get(e.getKey()))) {
                    return false;
                }
            }

            return true;
        }

        if (a instanceof List && b instanceof List) {
            List<?> la = (List<?>) a;
            List<?> lb = (List<?>) b;

            if (la.size() != lb.size()) {
                return false;
            }

            for (int i = 0; i < la.size(); i++) {
                if (!same(la.get(i), lb.get(i))) {
                    return false;
                }
            }

            return true;
        }

        return a == null ? b == null : a.equals(b);
    }

    private void assertSameShape(Morphium m, MorphiumId expectedId, MorphiumId actualId, String how) throws Exception {
        Map<String, Object> expected = raw(m, expectedId);
        Map<String, Object> actual = raw(m, actualId);
        assertNotNull(expected);
        assertNotNull(actual);

        for (String f : FIELDS) {
            Object e = expected.get(stored(m, f));
            Object a = actual.get(stored(m, f));
            assertTrue(same(e, a), how + " '" + f + "' must have the shape store() writes" + System.lineSeparator() + "  store():  " + e + System.lineSeparator() + "  update:   " + a);
        }
    }

    private void withFlag(Morphium m, boolean flag, Runnable body) {
        boolean original = m.getConfig().objectMappingSettings().isUseBsonDateForJavaTime();
        m.getConfig().objectMappingSettings().setUseBsonDateForJavaTime(flag);

        try {
            body.run();
        } finally {
            m.getConfig().objectMappingSettings().setUseBsonDateForJavaTime(original);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void findOneAndUpdateSetWritesTheShapeOfStore(Morphium m) throws Exception {
        try (m) {
            for (boolean flag : new boolean[] {false, true}) {
                withFlag(m, flag, () -> {
                    try {
                        Shaped stored = full();
                        m.store(stored);
                        Shaped target = blank(m);
                        Map<String, Object> set = new LinkedHashMap<>();
                        values().forEach((k, v) -> set.put(k, v));
                        m.createQueryFor(Shaped.class).f("_id").eq(target.id).findOneAndUpdate(Map.of("$set", set), false, true);
                        assertSameShape(m, stored.id, target.id, "findOneAndUpdate $set (flag " + flag + ")");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theBuilderWritesTheShapeOfStore(Morphium m) throws Exception {
        try (m) {
            for (boolean flag : new boolean[] {false, true}) {
                withFlag(m, flag, () -> {
                    try {
                        Shaped stored = full();
                        m.store(stored);
                        Shaped target = blank(m);
                        var b = m.createQueryFor(Shaped.class).f("_id").eq(target.id).modify();
                        values().forEach(b::set);
                        b.findOneAndUpdate(de.caluga.morphium.query.ReturnDocument.AFTER);
                        assertSameShape(m, stored.id, target.id, "builder set (flag " + flag + ")");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void queryPushAndAddToSetWriteTheElementShapeOfStore(Morphium m) throws Exception {
        try (m) {
            for (boolean flag : new boolean[] {false, true}) {
                withFlag(m, flag, () -> {
                    try {
                        Shaped stored = full();
                        stored.dateList = new ArrayList<>(List.of(D, D2));
                        stored.amounts = new ArrayList<>(List.of(AMT));
                        stored.items = new ArrayList<>(List.of(new Item("a", D)));
                        m.store(stored);
                        Shaped viaWriter = withEmptyContainers(m);
                        m.createQueryFor(Shaped.class).f("_id").eq(viaWriter.id).push(stored(m, "dateList"), D);
                        m.createQueryFor(Shaped.class).f("_id").eq(viaWriter.id).push(stored(m, "dateList"), D2);
                        m.createQueryFor(Shaped.class).f("_id").eq(viaWriter.id).push(stored(m, "amounts"), AMT);
                        m.createQueryFor(Shaped.class).f("_id").eq(viaWriter.id).push(stored(m, "items"), new Item("a", D));
                        Shaped viaMap = withEmptyContainers(m);
                        m.createQueryFor(Shaped.class).f("_id").eq(viaMap.id).findOneAndUpdate(Map.of("$push", Map.of("dateList", Map.of("$each", List.of(D, D2)), "amounts", AMT, "items", new Item("a", D))), false, true);
                        Shaped viaAddToSet = withEmptyContainers(m);
                        m.createQueryFor(Shaped.class).f("_id").eq(viaAddToSet.id).findOneAndUpdate(Map.of("$addToSet", Map.of("dateList", Map.of("$each", List.of(D, D2, D)), "amounts", AMT, "items", new Item("a", D))), false, true);

                        for (MorphiumId id : List.of(viaWriter.id, viaMap.id, viaAddToSet.id)) {
                            for (String f : List.of("dateList", "amounts", "items")) {
                                assertTrue(same(raw(m, stored.id).get(stored(m, f)), raw(m, id).get(stored(m, f))), "element shape of '" + f + "' (flag " + flag + ", " + id + "): " + raw(m, id).get(stored(m, f)));
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aPositionalTargetHoldsTheElementShape(Morphium m) throws Exception {
        try (m) {
            Shaped stored = full();
            m.store(stored);
            Shaped target = full();
            target.dateList = new ArrayList<>(List.of(D3, D2));
            m.store(target);
            m.createQueryFor(Shaped.class).f("_id").eq(target.id).findOneAndUpdate(Map.of("$set", Map.of("dateList.0", D)), false, true);
            assertTrue(same(raw(m, stored.id).get(stored(m, "dateList")), raw(m, target.id).get(stored(m, "dateList"))), "dateList.0 holds an element: " + raw(m, target.id).get(stored(m, "dateList")));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pullRemovesAnElementAndMatchesConditions(Morphium m) throws Exception {
        try (m) {
            Shaped s = full();
            s.dateList = new ArrayList<>(List.of(D, D2, D3));
            m.store(s);
            m.createQueryFor(Shaped.class).f("_id").eq(s.id).findOneAndUpdate(Map.of("$pull", Map.of("dateList", D2)), false, true);
            assertEquals(List.of(D, D3), m.createQueryFor(Shaped.class).f("_id").eq(s.id).get().dateList, "pull of an element");
            m.createQueryFor(Shaped.class).f("_id").eq(s.id).findOneAndUpdate(Map.of("$pull", Map.of("items", Map.of("when", D))), false, true);
            assertEquals(List.of("b"), m.createQueryFor(Shaped.class).f("_id").eq(s.id).get().items.stream().map(i -> i.name).toList(), "pull with a condition on a field of embedded documents");

            // pinned deviation MOR-066: the InMemoryDriver does not apply query operators to the fields of the elements of a $pull with a document condition
            if (!(m.getDriver() instanceof de.caluga.morphium.driver.inmem.InMemoryDriver)) {
                m.createQueryFor(Shaped.class).f("_id").eq(s.id).findOneAndUpdate(Map.of("$pull", Map.of("dateList", Map.of("$gte", D3))), false, true);
                assertEquals(List.of(D), m.createQueryFor(Shaped.class).f("_id").eq(s.id).get().dateList, "pull with an operator condition on wrapped elements");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aSetWrittenDocumentReadsBackTyped(Morphium m) throws Exception {
        try (m) {
            Shaped target = blank(m);
            m.createQueryFor(Shaped.class).f("_id").eq(target.id).findOneAndUpdate(Map.of("$set", Map.of("dateList", List.of(D, D2), "dateMap", Map.of("k", D))), false, true);
            Shaped read = m.createQueryFor(Shaped.class).f("_id").eq(target.id).get();
            assertEquals(List.of(D, D2), read.dateList);
            assertEquals(D, read.dateMap.get("k"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aSingleEntityOrEmbeddedValueCarriesClassNameAndAnEntityKeepsItsId(Morphium m) throws Exception {
        try (m) {
            // the accepted difference to store(): an update does not know the declared type of the field
            Shaped target = blank(m);
            m.createQueryFor(Shaped.class).f("_id").eq(target.id).findOneAndUpdate(Map.of("$set", Map.of("item", new Item("a", D))), false, true);
            Object item = raw(m, target.id).get(stored(m, "item"));
            assertTrue(item instanceof Map && ((Map<?, ?>) item).containsKey("class_name"), String.valueOf(item));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pullRemovesTheFlatAndTheWrappedFormOfAnElement(Morphium m) throws Exception {
        try (m) {
            // an array written by an older version holds the elements flat (the epoch day), one written by store() holds them wrapped; a mixed array holds both
            MorphiumId id = new MorphiumId();
            var con = m.getDriver().getPrimaryConnection(null);

            try {
                Map<String, Object> doc = new LinkedHashMap<>();
                doc.put("_id", id);
                doc.put(stored(m, "dateList"), new ArrayList<>(List.of(D.toEpochDay(), Map.of("value", D2.toEpochDay()), D3.toEpochDay())));
                new de.caluga.morphium.driver.commands.InsertMongoCommand(con).setDb(m.getDatabase()).setColl(m.getMapper().getCollectionName(Shaped.class)).setDocuments(new ArrayList<>(List.of(doc))).execute();
            } finally {
                m.getDriver().releaseConnection(con);
            }

            m.createQueryFor(Shaped.class).f("_id").eq(id).findOneAndUpdate(Map.of("$pull", Map.of("dateList", D2)), false, true);
            m.createQueryFor(Shaped.class).f("_id").eq(id).findOneAndUpdate(Map.of("$pull", Map.of("dateList", D)), false, true);
            List<?> left = (List<?>) raw(m, id).get(stored(m, "dateList"));
            assertEquals(1, left.size(), String.valueOf(left));
            assertEquals(D3.toEpochDay(), ((Number) left.get(0)).longValue());
        }
    }
}
