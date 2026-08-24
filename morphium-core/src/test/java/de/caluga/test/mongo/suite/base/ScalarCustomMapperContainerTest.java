package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.objectmapping.BigDecimalMapper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for #334 (symptom 1): container fields (List / array / Map) of types
 * whose custom mapper marshalls to a bare scalar (BigDecimal, Character, Atomic*, LocalDate,
 * LocalTime, Timestamp, ...) are written as a {@code {"value": scalar}} wrapper map without
 * class_name. The read path must unwrap that shape back to the declared element type.
 *
 * The fix is read-side only: the on-disk format MUST NOT change (older Morphium versions
 * must be able to read documents written by this version — see PR #333 for how a write-side
 * "fix" broke rollback/mixed-version operation). {@link #writeFormatUnchanged} pins the
 * written raw format so any future write-side change fails loudly.
 */
@Tag("core")
public class ScalarCustomMapperContainerTest extends MultiDriverTestBase {

    /**
     * Documents written by older Morphium versions (raw legacy shape, {"value": x} without
     * class_name) must deserialize into properly typed container elements.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void legacyScalarWrappersInContainers(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumId id = new MorphiumId();
            Map<String, Object> doc = new HashMap<>();
            doc.put("_id", id);
            doc.put(fn(morphium, "decimalList"), List.of(wrap(2.5d)));
            doc.put(fn(morphium, "charList"), List.of(wrap(65)));
            doc.put(fn(morphium, "atomicIntList"), List.of(wrap(42)));
            doc.put(fn(morphium, "atomicLongList"), List.of(wrap(43L)));
            doc.put(fn(morphium, "atomicBoolList"), List.of(wrap(true)));
            doc.put(fn(morphium, "dateList"), List.of(wrap(18997L)));  // epochDay
            doc.put(fn(morphium, "timeList"), List.of(wrap(3600000000000L))); // nanoOfDay = 01:00
            doc.put(fn(morphium, "tsList"), List.of(wrap(1234567890123L)));
            doc.put(fn(morphium, "decimalArray"), List.of(wrap(1.5d), wrap(2.5d)));
            doc.put(fn(morphium, "decimalMap"), Map.of("k", wrap(2.5d)));
            doc.put(fn(morphium, "dateMap"), Map.of("k", wrap(18997L)));
            insertRaw(morphium, doc);

            ScalarContainerEntity loaded = morphium.findById(ScalarContainerEntity.class, id);
            assertNotNull(loaded);
            assertInstanceOf(BigDecimal.class, loaded.decimalList.get(0));
            assertEquals(0, loaded.decimalList.get(0).compareTo(new BigDecimal("2.5")));
            assertInstanceOf(Character.class, loaded.charList.get(0));
            assertEquals(Character.valueOf('A'), loaded.charList.get(0));
            assertInstanceOf(AtomicInteger.class, loaded.atomicIntList.get(0));
            assertEquals(42, loaded.atomicIntList.get(0).get());
            assertInstanceOf(AtomicLong.class, loaded.atomicLongList.get(0));
            assertEquals(43L, loaded.atomicLongList.get(0).get());
            assertInstanceOf(AtomicBoolean.class, loaded.atomicBoolList.get(0));
            assertTrue(loaded.atomicBoolList.get(0).get());
            assertInstanceOf(LocalDate.class, loaded.dateList.get(0));
            assertEquals(LocalDate.ofEpochDay(18997L), loaded.dateList.get(0));
            assertInstanceOf(LocalTime.class, loaded.timeList.get(0));
            assertEquals(LocalTime.of(1, 0), loaded.timeList.get(0));
            assertInstanceOf(Timestamp.class, loaded.tsList.get(0));
            assertEquals(1234567890123L, loaded.tsList.get(0).getTime());
            assertNotNull(loaded.decimalArray);
            assertEquals(2, loaded.decimalArray.length);
            assertEquals(0, loaded.decimalArray[0].compareTo(new BigDecimal("1.5")));
            assertEquals(0, loaded.decimalArray[1].compareTo(new BigDecimal("2.5")));
            assertInstanceOf(BigDecimal.class, loaded.decimalMap.get("k"));
            assertEquals(0, loaded.decimalMap.get("k").compareTo(new BigDecimal("2.5")));
            assertInstanceOf(LocalDate.class, loaded.dateMap.get("k"));
            assertEquals(LocalDate.ofEpochDay(18997L), loaded.dateMap.get("k"));
        }
    }

    /**
     * The wrapper may also carry a class_name (polymorphic containers / hand-written data).
     * Both a typed list and a List&lt;Object&gt; must resolve the element via the class_name.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void legacyScalarWrapperWithClassName(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumId id = new MorphiumId();
            Map<String, Object> doc = new HashMap<>();
            doc.put("_id", id);
            Map<String, Object> typed = new HashMap<>(wrap(2.5d));
            typed.put("class_name", BigDecimal.class.getName());
            doc.put(fn(morphium, "decimalList"), List.of(typed));
            Map<String, Object> poly = new HashMap<>(wrap(1.5d));
            poly.put("class_name", BigDecimal.class.getName());
            doc.put(fn(morphium, "polymorphList"), List.of(poly));
            insertRaw(morphium, doc);

            ScalarContainerEntity loaded = morphium.findById(ScalarContainerEntity.class, id);
            assertNotNull(loaded);
            assertInstanceOf(BigDecimal.class, loaded.decimalList.get(0));
            assertEquals(0, loaded.decimalList.get(0).compareTo(new BigDecimal("2.5")));
            assertInstanceOf(BigDecimal.class, loaded.polymorphList.get(0));
            assertEquals(0, ((BigDecimal) loaded.polymorphList.get(0)).compareTo(new BigDecimal("1.5")));
        }
    }

    /** Normal store()/get() round trip must yield properly typed container elements. */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void roundTripContainers(Morphium morphium) throws Exception {
        try (morphium) {
            ScalarContainerEntity e = new ScalarContainerEntity();
            e.decimalList = new ArrayList<>(List.of(new BigDecimal("2.5")));
            e.charList = new ArrayList<>(List.of('A'));
            e.atomicIntList = new ArrayList<>(List.of(new AtomicInteger(42)));
            e.atomicLongList = new ArrayList<>(List.of(new AtomicLong(43L)));
            e.atomicBoolList = new ArrayList<>(List.of(new AtomicBoolean(true)));
            e.dateList = new ArrayList<>(List.of(LocalDate.ofEpochDay(18997L)));
            e.timeList = new ArrayList<>(List.of(LocalTime.of(1, 0)));
            e.tsList = new ArrayList<>(List.of(new Timestamp(1234567890123L)));
            e.decimalArray = new BigDecimal[] {new BigDecimal("1.5"), new BigDecimal("2.5")};
            e.decimalMap = new HashMap<>(Map.of("k", new BigDecimal("2.5")));
            e.dateMap = new HashMap<>(Map.of("k", LocalDate.ofEpochDay(18997L)));
            e.ldtList = new ArrayList<>(List.of(LocalDateTime.of(2026, 8, 24, 12, 30, 15)));
            morphium.store(e);

            ScalarContainerEntity loaded = morphium.findById(ScalarContainerEntity.class, e.id);
            assertNotNull(loaded);
            assertInstanceOf(BigDecimal.class, loaded.decimalList.get(0));
            assertEquals(0, loaded.decimalList.get(0).compareTo(new BigDecimal("2.5")));
            assertEquals(Character.valueOf('A'), loaded.charList.get(0));
            assertEquals(42, loaded.atomicIntList.get(0).get());
            assertEquals(43L, loaded.atomicLongList.get(0).get());
            assertTrue(loaded.atomicBoolList.get(0).get());
            assertEquals(LocalDate.ofEpochDay(18997L), loaded.dateList.get(0));
            assertEquals(LocalTime.of(1, 0), loaded.timeList.get(0));
            assertEquals(1234567890123L, loaded.tsList.get(0).getTime());
            assertEquals(2, loaded.decimalArray.length);
            assertEquals(0, loaded.decimalArray[1].compareTo(new BigDecimal("2.5")));
            assertEquals(0, loaded.decimalMap.get("k").compareTo(new BigDecimal("2.5")));
            assertEquals(LocalDate.ofEpochDay(18997L), loaded.dateMap.get("k"));
            assertEquals(LocalDateTime.of(2026, 8, 24, 12, 30, 15), loaded.ldtList.get(0));
        }
    }

    /**
     * Format stability: the on-disk shape written by store() must remain the legacy
     * {"value": scalar} wrapper WITHOUT class_name for scalar-returning custom mappers,
     * and the map shape WITH class_name for map-returning ones (LocalDateTime).
     * A failure here means the write format changed — that breaks rollback and
     * mixed-version operation (see PR #333) and must never happen silently.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void writeFormatUnchanged(Morphium morphium) throws Exception {
        try (morphium) {
            ScalarContainerEntity e = new ScalarContainerEntity();
            e.decimalList = new ArrayList<>(List.of(new BigDecimal("2.5")));
            e.dateList = new ArrayList<>(List.of(LocalDate.ofEpochDay(18997L)));
            e.decimalArray = new BigDecimal[] {new BigDecimal("1.5")};
            e.decimalMap = new HashMap<>(Map.of("k", new BigDecimal("2.5")));
            e.dateMap = new HashMap<>(Map.of("k", LocalDate.ofEpochDay(18997L)));
            e.ldtList = new ArrayList<>(List.of(LocalDateTime.of(2026, 8, 24, 12, 30, 15)));
            morphium.store(e);

            Map<String, Object> raw = findRaw(morphium, e.id);
            assertNotNull(raw);

            assertScalarWrapper(elem(raw, fn(morphium, "decimalList"), 0), 2.5d);
            assertScalarWrapper(elem(raw, fn(morphium, "dateList"), 0), 18997L);
            assertScalarWrapper(elem(raw, fn(morphium, "decimalArray"), 0), 1.5d);
            assertScalarWrapper(((Map<?, ?>) raw.get(fn(morphium, "decimalMap"))).get("k"), 2.5d);
            assertScalarWrapper(((Map<?, ?>) raw.get(fn(morphium, "dateMap"))).get("k"), 18997L);

            // map-returning mapper: {sec, n, class_name} — unchanged as well
            Object ldtElem = elem(raw, fn(morphium, "ldtList"), 0);
            assertInstanceOf(Map.class, ldtElem);
            Map<?, ?> ldtMap = (Map<?, ?>) ldtElem;
            assertEquals(Set.of("sec", "n", "class_name"), ldtMap.keySet());
        }
    }

    /**
     * A document may legitimately contain a field or key named "value": an embedded object
     * with a "value" property, or an untyped map holding a {"value": ...} sub-document.
     * Neither must be unwrapped.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void legitimateValueFieldsAreNotUnwrapped(Morphium morphium) throws Exception {
        try (morphium) {
            ScalarContainerEntity e = new ScalarContainerEntity();
            ValueHolder h = new ValueHolder();
            h.value = 3.14d;
            e.holderList = new ArrayList<>(List.of(h));
            morphium.store(e);

            ScalarContainerEntity loaded = morphium.findById(ScalarContainerEntity.class, e.id);
            assertInstanceOf(ValueHolder.class, loaded.holderList.get(0));
            assertEquals(3.14d, loaded.holderList.get(0).value);

            // untyped map: no declared element type, no custom mapper -> wrapper-shaped
            // sub-documents must stay maps (there is no type to unwrap them to)
            MorphiumId id2 = new MorphiumId();
            Map<String, Object> doc = new HashMap<>();
            doc.put("_id", id2);
            doc.put(fn(morphium, "untypedMap"), Map.of("k", wrap(3.14d)));
            insertRaw(morphium, doc);
            ScalarContainerEntity loaded2 = morphium.findById(ScalarContainerEntity.class, id2);
            assertInstanceOf(Map.class, loaded2.untypedMap.get("k"));
            assertEquals(3.14d, ((Map<?, ?>) loaded2.untypedMap.get("k")).get("value"));
        }
    }

    /**
     * If the custom mapper for the element type was deregistered at runtime, reading the
     * legacy wrapper must not throw (no NPE) — it falls back to the old behavior and
     * leaves the raw map in place.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void deregisteredMapperDoesNotThrow(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumId id = new MorphiumId();
            Map<String, Object> doc = new HashMap<>();
            doc.put("_id", id);
            doc.put(fn(morphium, "decimalList"), List.of(wrap(2.5d)));
            insertRaw(morphium, doc);

            morphium.getMapper().deregisterCustomMapperFor(BigDecimal.class);
            try {
                ScalarContainerEntity loaded = morphium.findById(ScalarContainerEntity.class, id);
                assertNotNull(loaded);
                // no mapper -> no unwrapping possible; raw map remains (pre-fix behavior)
                assertInstanceOf(Map.class, loaded.decimalList.get(0));
            } finally {
                morphium.getMapper().registerCustomMapperFor(BigDecimal.class, new BigDecimalMapper());
            }
        }
    }

    // ---------- helpers ----------

    private static Map<String, Object> wrap(Object scalar) {
        Map<String, Object> m = new HashMap<>();
        m.put("value", scalar);
        return m;
    }

    private static String fn(Morphium m, String javaField) {
        return m.getARHelper().getMongoFieldName(ScalarContainerEntity.class, javaField);
    }

    private static void insertRaw(Morphium morphium, Map<String, Object> doc) throws Exception {
        InsertMongoCommand cmd = new InsertMongoCommand(morphium.getDriver().getPrimaryConnection(null))
        .setDb(morphium.getDatabase())
        .setColl(morphium.getMapper().getCollectionName(ScalarContainerEntity.class))
        .setDocuments(List.of(doc));
        cmd.execute();
        cmd.releaseConnection();
    }

    private static Map<String, Object> findRaw(Morphium morphium, MorphiumId id) throws Exception {
        FindCommand fnd = new FindCommand(morphium.getDriver().getPrimaryConnection(null))
        .setDb(morphium.getDatabase())
        .setColl(morphium.getMapper().getCollectionName(ScalarContainerEntity.class))
        .setFilter(Map.of("_id", id));
        try {
            List<Map<String, Object>> res = fnd.execute();
            return res.isEmpty() ? null : res.get(0);
        } finally {
            fnd.releaseConnection();
        }
    }

    private static Object elem(Map<String, Object> raw, String field, int idx) {
        Object v = raw.get(field);
        assertInstanceOf(List.class, v, "field " + field + " should be a list");
        return ((List<?>) v).get(idx);
    }

    private static void assertScalarWrapper(Object dbValue, Object expectedScalar) {
        assertInstanceOf(Map.class, dbValue);
        Map<?, ?> m = (Map<?, ?>) dbValue;
        assertEquals(Set.of("value"), m.keySet(), "legacy wrapper must have exactly the key 'value', no class_name");
        Object v = m.get("value");
        if (expectedScalar instanceof Number && v instanceof Number) {
            assertEquals(((Number) expectedScalar).doubleValue(), ((Number) v).doubleValue());
        } else {
            assertEquals(expectedScalar, v);
        }
    }

    @Entity
    public static class ScalarContainerEntity {
        @Id
        public MorphiumId id;
        public List<BigDecimal> decimalList;
        public List<Character> charList;
        public List<AtomicInteger> atomicIntList;
        public List<AtomicLong> atomicLongList;
        public List<AtomicBoolean> atomicBoolList;
        public List<LocalDate> dateList;
        public List<LocalTime> timeList;
        public List<Timestamp> tsList;
        public BigDecimal[] decimalArray;
        public Map<String, BigDecimal> decimalMap;
        public Map<String, LocalDate> dateMap;
        public List<LocalDateTime> ldtList;
        public List<Object> polymorphList;
        public List<ValueHolder> holderList;
        public Map<String, Object> untypedMap;
    }

    @Embedded
    public static class ValueHolder {
        public Double value;
    }
}
