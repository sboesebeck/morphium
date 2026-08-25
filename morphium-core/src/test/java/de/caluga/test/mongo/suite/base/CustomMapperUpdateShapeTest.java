package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.query.geospatial.Geo;
import de.caluga.morphium.query.geospatial.Point;
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
 * #335: the update APIs ({@code set()} / {@code push()} / {@code addToSet()}) must write the
 * SAME on-disk shape for custom-mapped fields as {@code store()} does - a document written by
 * one path has to be queryable by the same predicates that match documents written by the other.
 *
 * <p>This is deliberately NOT a round-trip test: a same-version read-back with the same code
 * that wrote the value cannot see this bug class at all (#334's mappers accept both shapes on
 * unmarshall, so a wrong-shaped value still reads back fine - only queries miss, silently).
 * Instead both write paths are compared on their RAW persisted documents, per structure
 * position: scalar fields store the bare mapper output, container elements the
 * {@code {"value": scalar}} wrapper (map-returning mappers their map with class_name).
 *
 * <p>The flag matrix matters because {@code useBsonDateForJavaTime} changes what {@code store()}
 * writes while - before the fix - {@code set()} bypassed the mappers entirely: with the flag on,
 * even scalar fields split into Date vs legacy sub-document, and MongoDB's type-bracketed range
 * operators then drop one shape from result sets entirely.
 */
@Tag("core")
public class CustomMapperUpdateShapeTest extends MultiDriverTestBase {

    private static final LocalDate D = LocalDate.ofEpochDay(18997L);
    private static final Instant I = Instant.ofEpochSecond(1600000000, 123456789);
    // Geo is the DUAL case: @Embedded AND custom-mapped (BsonGeoMapper). Its marshall() returns
    // a map, so store() carries class_name on it - the exact thing a polymorphic field
    // declaration (Geo holding a Point) needs to pick the subclass. A custom-mapper-first branch
    // in the update path silently dropped it (caught here after the fact).
    private static final Point P = new Point(13.4, 52.5);
    private static final BigDecimal AMT = new BigDecimal("2.500");

    @Entity
    public static class UpdateShapeEntity {
        @Id
        public MorphiumId id;
        public LocalDate date;
        public Instant instant;
        public List<LocalDate> dateList;
        public Map<String, LocalDate> dateMap;
        public Point loc;
        public List<Point> locs;
        public Geo polyLoc;
        public BigDecimal amount;
        public List<BigDecimal> amounts;
        public Character ch;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void setMatchesStoreShape(Morphium morphium) throws Exception {
        try (morphium) {
            UpdateShapeEntity stored = fullEntity();
            morphium.store(stored);

            UpdateShapeEntity updated = new UpdateShapeEntity();
            updated.id = new MorphiumId();
            morphium.store(updated);
            morphium.createQueryFor(UpdateShapeEntity.class)
                    .f("_id").eq(updated.id)
                    .set(allFieldValues(morphium));

            Map<String, Object> rawStored = findRaw(morphium, stored.id);
            Map<String, Object> rawUpdated = findRaw(morphium, updated.id);
            assertNotNull(rawStored);
            assertNotNull(rawUpdated);

            for (String f : ALL_FIELDS) {
                assertSameShape(rawStored.get(fieldName(morphium, f)),
                        rawUpdated.get(fieldName(morphium, f)),
                        "set()-written '" + f + "' must have the same on-disk shape as store()");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void pushMatchesStoreShape(Morphium morphium) throws Exception {
        try (morphium) {
            UpdateShapeEntity stored = fullEntity();
            morphium.store(stored);

            UpdateShapeEntity updated = new UpdateShapeEntity();
            updated.id = new MorphiumId();
            updated.dateList = new ArrayList<>();
            morphium.store(updated);
            morphium.push(morphium.createQueryFor(UpdateShapeEntity.class)
                    .f("_id").eq(updated.id), fieldName(morphium, "dateList"), D, false, true, null);

            Map<String, Object> rawStored = findRaw(morphium, stored.id);
            Map<String, Object> rawUpdated = findRaw(morphium, updated.id);
            assertNotNull(rawStored);
            assertNotNull(rawUpdated);

            // pushed elements land in the same array store() writes: identical element shape
            assertSameShape(rawStored.get(fieldName(morphium, "dateList")),
                    rawUpdated.get(fieldName(morphium, "dateList")),
                    "push()-written 'dateList' must have the same element shape as store()");
        }
    }

    /** The user-visible symptom: a predicate matching the store()-written doc must match the set()-written one. */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void queriesMatchBothWritePaths(Morphium morphium) throws Exception {
        try (morphium) {
            UpdateShapeEntity viaStore = fullEntity();
            morphium.store(viaStore);

            UpdateShapeEntity viaSet = new UpdateShapeEntity();
            viaSet.id = new MorphiumId();
            morphium.store(viaSet);
            morphium.createQueryFor(UpdateShapeEntity.class)
                    .f("_id").eq(viaSet.id)
                    .set(Map.of(
                            fieldName(morphium, "date"), D,
                            fieldName(morphium, "instant"), I,
                            fieldName(morphium, "dateList"), List.of(D),
                            fieldName(morphium, "dateMap"), Map.of("k", D)));

            UpdateShapeEntity other = fullEntity();
            other.date = D.plusDays(1);
            other.instant = I.plusSeconds(3600);
            other.dateList = new ArrayList<>(List.of(D.plusDays(1)));
            other.dateMap = new HashMap<>(Map.of("k", D.plusDays(1)));
            morphium.store(other);

            assertMatchesExactly(morphium, "date", D, viaStore.id, viaSet.id);
            assertMatchesExactly(morphium, "instant", I, viaStore.id, viaSet.id);
        }
    }

    /**
     * With {@code useBsonDateForJavaTime} on, store() writes BSON dates - the update APIs must
     * follow (pre-fix they wrote the legacy mapper format into the same field, and MongoDB's
     * type-bracketed comparison dropped those rows from range results entirely).
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void setMatchesStoreShapeWithBsonDateFlag(Morphium morphium) throws Exception {
        try (morphium) {
            boolean original = morphium.getConfig().objectMappingSettings().isUseBsonDateForJavaTime();
            morphium.getConfig().objectMappingSettings().setUseBsonDateForJavaTime(true);
            try {
                UpdateShapeEntity stored = fullEntity();
                morphium.store(stored);

                UpdateShapeEntity updated = new UpdateShapeEntity();
                updated.id = new MorphiumId();
                morphium.store(updated);
                morphium.createQueryFor(UpdateShapeEntity.class)
                        .f("_id").eq(updated.id)
                        .set(allFieldValues(morphium));

                Map<String, Object> rawStored = findRaw(morphium, stored.id);
                Map<String, Object> rawUpdated = findRaw(morphium, updated.id);
                assertNotNull(rawStored);
                assertNotNull(rawUpdated);

                for (String f : ALL_FIELDS) {
                    assertSameShape(rawStored.get(fieldName(morphium, f)),
                            rawUpdated.get(fieldName(morphium, f)),
                            "set()-written '" + f + "' must have the same on-disk shape as store() (bson date flag)");
                }
            } finally {
                morphium.getConfig().objectMappingSettings().setUseBsonDateForJavaTime(original);
            }
        }
    }

    /** A set()-written document must also READ BACK typed (#334's unwrapping applies to it). */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void setWrittenDocumentReadsBackTyped(Morphium morphium) throws Exception {
        try (morphium) {
            UpdateShapeEntity e = new UpdateShapeEntity();
            e.id = new MorphiumId();
            morphium.store(e);
            morphium.createQueryFor(UpdateShapeEntity.class)
                    .f("_id").eq(e.id)
                    .set(Map.of(
                            fieldName(morphium, "date"), D,
                            fieldName(morphium, "dateList"), List.of(D),
                            fieldName(morphium, "dateMap"), Map.of("k", D)));

            UpdateShapeEntity loaded = morphium.findById(UpdateShapeEntity.class, e.id);
            assertNotNull(loaded);
            assertEquals(D, loaded.date, "scalar custom-mapped field must round-trip through set()");
            assertEquals(1, loaded.dateList.size());
            assertEquals(D, loaded.dateList.get(0),
                    "container element written by set() must come back as " + LocalDate.class.getSimpleName()
                    + ", not as the raw marshalled scalar");
            assertEquals(D, loaded.dateMap.get("k"));
        }
    }

    // ---------- helpers ----------

    private static UpdateShapeEntity fullEntity() {
        UpdateShapeEntity e = new UpdateShapeEntity();
        e.id = new MorphiumId();
        e.date = D;
        e.instant = I;
        e.dateList = new ArrayList<>(List.of(D));
        e.dateMap = new HashMap<>(Map.of("k", D));
        e.loc = P;
        e.locs = new ArrayList<>(List.of(P));
        e.polyLoc = P;
        e.amount = AMT;
        e.amounts = new ArrayList<>(List.of(AMT));
        e.ch = 'A';
        return e;
    }

    /** All custom-mapped field values, keyed by mongo field name - the payload for set(). */
    private static Map<String, Object> allFieldValues(Morphium m) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put(fieldName(m, "date"), D);
        v.put(fieldName(m, "instant"), I);
        v.put(fieldName(m, "dateList"), List.of(D));
        v.put(fieldName(m, "dateMap"), Map.of("k", D));
        v.put(fieldName(m, "loc"), P);
        v.put(fieldName(m, "locs"), List.of(P));
        v.put(fieldName(m, "polyLoc"), P);
        v.put(fieldName(m, "amount"), AMT);
        v.put(fieldName(m, "amounts"), List.of(AMT));
        v.put(fieldName(m, "ch"), 'A');
        return v;
    }

    private static final List<String> ALL_FIELDS = List.of(
            "date", "instant", "dateList", "dateMap", "loc", "locs", "polyLoc", "amount", "amounts", "ch");

    private static String fieldName(Morphium m, String javaField) {
        return m.getARHelper().getMongoFieldName(UpdateShapeEntity.class, javaField);
    }

    private static Map<String, Object> findRaw(Morphium morphium, MorphiumId id) throws Exception {
        FindCommand fnd = new FindCommand(morphium.getDriver().getPrimaryConnection(null))
        .setDb(morphium.getDatabase())
        .setColl(morphium.getMapper().getCollectionName(UpdateShapeEntity.class))
        .setFilter(Map.of("_id", id));
        try {
            List<Map<String, Object>> res = fnd.execute();
            return res.isEmpty() ? null : res.get(0);
        } finally {
            fnd.releaseConnection();
        }
    }

    @SuppressWarnings("unused")
    private static void insertRaw(Morphium morphium, Map<String, Object> doc) throws Exception {
        InsertMongoCommand cmd = new InsertMongoCommand(morphium.getDriver().getPrimaryConnection(null))
        .setDb(morphium.getDatabase())
        .setColl(morphium.getMapper().getCollectionName(UpdateShapeEntity.class))
        .setDocuments(List.of(doc));
        cmd.execute();
        cmd.releaseConnection();
    }

    private void assertMatchesExactly(Morphium morphium, String javaField, Object value, MorphiumId... expectedIds) {
        List<MorphiumId> got = new ArrayList<>();
        for (var e : morphium.createQueryFor(UpdateShapeEntity.class).f(javaField).eq(value).asList()) {
            got.add(e.id);
        }
        List<MorphiumId> expected = new ArrayList<>(List.of(expectedIds));
        assertTrue(shapeListsEqual(expected, got),
                "f(\"" + javaField + "\").eq(...) must match exactly the docs written with " + value
                + " (store AND update path), got " + got + " expected " + expected);
    }

    /** Recursive comparison tolerant of numeric width differences between drivers (Integer vs Long). */
    private static void assertSameShape(Object expected, Object actual, String what) {
        assertTrue(shapesEqual(expected, actual), what + System.lineSeparator()
                + "  expected: " + expected + System.lineSeparator()
                + "  actual:   " + actual);
    }

    private static boolean shapesEqual(Object a, Object b) {
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
                if (!mb.containsKey(e.getKey()) || !shapesEqual(e.getValue(), mb.get(e.getKey()))) {
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
                if (!shapesEqual(la.get(i), lb.get(i))) {
                    return false;
                }
            }
            return true;
        }
        if (a == null || b == null) {
            return a == b;
        }
        return a.equals(b);
    }

    private static boolean shapeListsEqual(List<MorphiumId> a, List<MorphiumId> b) {
        if (a.size() != b.size()) {
            return false;
        }
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (MorphiumId id : a) {
            counts.merge(id.toString(), 1, Integer::sum);
        }
        for (MorphiumId id : b) {
            Integer c = counts.get(id.toString());
            if (c == null || c == 0) {
                return false;
            }
            counts.put(id.toString(), c - 1);
        }
        return true;
    }
}
