package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.FindCommand;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Pins the RAW on-disk format of the four java.time types for both values of
 * {@code ObjectMappingSettings#useBsonDateForJavaTime}.
 *
 * <p>The point of these tests is what they assert about the DEFAULT: with the flag off the written
 * documents must be byte-identical to what previous Morphium versions wrote, because an opt-in
 * feature must not change the storage format of applications that never opted in. A round-trip
 * test cannot show this — it pairs a writer and a reader of the same version, so both agree on any
 * format, including a broken one. These tests read the raw documents back through the driver
 * instead, so a write-side change fails here loudly.
 *
 * <p>Companion to {@code ScalarCustomMapperContainerTest#writeFormatUnchanged}, which pins the same
 * property for the scalar-mapped types in general (#334).
 */
@Tag("core")
public class JavaTimeBsonDateOptInFormatTest extends MultiDriverTestBase {

    private static final LocalDate DATE = LocalDate.of(2026, 8, 24);
    private static final LocalTime TIME = LocalTime.of(12, 30, 15);
    private static final LocalDateTime LDT = LocalDateTime.of(2026, 8, 24, 12, 30, 15);
    private static final Instant INSTANT = Instant.parse("2026-08-24T12:30:15Z");

    /**
     * flag=false (the default): every field keeps the legacy shape — bare epoch-day /
     * nano-of-day longs for LocalDate/LocalTime, {sec,n} and {type,seconds,nanos} sub-documents
     * for LocalDateTime/Instant. Container elements keep the {"value": scalar} wrapper that
     * serialize() has always produced for scalar-returning mappers.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void defaultFlagKeepsLegacyOnDiskFormat(Morphium morphium) throws Exception {
        try (morphium) {
            JavaTimeEntity e = store(morphium, false);
            Map<String, Object> raw = findRaw(morphium, e.id);
            assertNotNull(raw);

            assertEquals(DATE.toEpochDay(), num(raw, morphium, "date"),
                "LocalDate must stay an epoch-day long at the default flag value");
            assertEquals(TIME.toNanoOfDay(), num(raw, morphium, "time"),
                "LocalTime must stay a nano-of-day long at the default flag value");

            Map<?, ?> ldt = map(raw, morphium, "ldt");
            assertEquals(Set.of("sec", "n"), ldt.keySet(), "LocalDateTime keeps its {sec,n} shape");
            assertEquals(LDT.toEpochSecond(ZoneOffset.UTC), ((Number) ldt.get("sec")).longValue());

            Map<?, ?> inst = map(raw, morphium, "instant");
            assertEquals(Set.of("type", "seconds", "nanos"), inst.keySet(),
                "Instant keeps its {type,seconds,nanos} shape");
            assertEquals(INSTANT.getEpochSecond(), ((Number) inst.get("seconds")).longValue());

            // container element: the pre-existing {"value": scalar} wrapper, no class_name
            Object el = elem(raw, fn(morphium, "dateList"), 0);
            assertInstanceOf(Map.class, el);
            assertEquals(Set.of("value"), ((Map<?, ?>) el).keySet(),
                "container element keeps the plain {\"value\": ...} wrapper, no class_name added");
            assertEquals(DATE.toEpochDay(), ((Number) ((Map<?, ?>) el).get("value")).longValue());
        }
    }

    /**
     * flag=true: the four SCALAR fields become native BSON dates, i.e. the driver hands back a
     * {@link Date} rather than a long or a sub-document.
     *
     * <p>Container elements deliberately do NOT: they keep the {@code {"value": scalar}} wrapper
     * that {@code serialize()} produces for every scalar-returning mapper, with a native
     * {@link Date} inside it. So the round-trip is correct either way, but the "native date
     * queries/sorts work directly on the field" benefit only applies to scalar fields — a
     * container needs {@code field.value}. Pinned here because this is exactly the area that
     * regressed twice before.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void enabledFlagWritesNativeBsonDates(Morphium morphium) throws Exception {
        try (morphium) {
            JavaTimeEntity e = store(morphium, true);
            Map<String, Object> raw = findRaw(morphium, e.id);
            assertNotNull(raw);

            for (String field : List.of("date", "time", "ldt", "instant")) {
                assertInstanceOf(Date.class, raw.get(fn(morphium, field)),
                    field + " must be a native BSON date when useBsonDateForJavaTime is enabled");
            }

            assertEquals(Date.from(INSTANT), raw.get(fn(morphium, "instant")),
                "Instant must match the value the official driver's jsr310 codec would write");
            assertEquals(Date.from(DATE.atStartOfDay(ZoneOffset.UTC).toInstant()),
                raw.get(fn(morphium, "date")), "LocalDate is anchored at UTC start-of-day");

            // Container elements: still the {"value": ...} wrapper, now holding a native Date.
            Date expectedDate = Date.from(DATE.atStartOfDay(ZoneOffset.UTC).toInstant());
            assertWrappedDate(elem(raw, fn(morphium, "dateList"), 0), expectedDate,
                "a List<LocalDate> element keeps the {\"value\": ...} wrapper with the flag on");
            assertWrappedDate(((Map<?, ?>) raw.get(fn(morphium, "dateMap"))).get("k"), expectedDate,
                "a Map<String, LocalDate> value keeps the {\"value\": ...} wrapper with the flag on");
        }
    }

    private static void assertWrappedDate(Object dbValue, Date expected, String why) {
        assertInstanceOf(Map.class, dbValue, why);
        Map<?, ?> m = (Map<?, ?>) dbValue;
        assertEquals(Set.of("value"), m.keySet(), why + " -- exactly the key 'value', no class_name");
        assertInstanceOf(Date.class, m.get("value"),
            "the wrapped scalar itself IS a native date when the flag is on");
        assertEquals(expected, m.get("value"));
    }

    private static JavaTimeEntity store(Morphium morphium, boolean useBsonDate) {
        morphium.getConfig().objectMappingSettings().setUseBsonDateForJavaTime(useBsonDate);
        JavaTimeEntity e = new JavaTimeEntity();
        e.date = DATE;
        e.time = TIME;
        e.ldt = LDT;
        e.instant = INSTANT;
        e.dateList = new ArrayList<>(List.of(DATE));
        e.dateMap = new HashMap<>(Map.of("k", DATE));
        morphium.store(e);
        return e;
    }

    private static long num(Map<String, Object> raw, Morphium m, String field) {
        Object v = raw.get(fn(m, field));
        assertInstanceOf(Number.class, v, "field " + field + " should be a bare number, was " + v);
        return ((Number) v).longValue();
    }

    private static Map<?, ?> map(Map<String, Object> raw, Morphium m, String field) {
        Object v = raw.get(fn(m, field));
        assertInstanceOf(Map.class, v, "field " + field + " should be a sub-document, was " + v);
        return (Map<?, ?>) v;
    }

    private static Object elem(Map<String, Object> raw, String field, int idx) {
        Object v = raw.get(field);
        assertInstanceOf(List.class, v, "field " + field + " should be a list");
        return ((List<?>) v).get(idx);
    }

    private static String fn(Morphium m, String javaField) {
        return m.getARHelper().getMongoFieldName(JavaTimeEntity.class, javaField);
    }

    private static Map<String, Object> findRaw(Morphium morphium, MorphiumId id) throws Exception {
        FindCommand fnd = new FindCommand(morphium.getDriver().getPrimaryConnection(null))
        .setDb(morphium.getDatabase())
        .setColl(morphium.getMapper().getCollectionName(JavaTimeEntity.class))
        .setFilter(Map.of("_id", id));

        try {
            List<Map<String, Object>> res = fnd.execute();
            return res.isEmpty() ? null : res.get(0);
        } finally {
            fnd.releaseConnection();
        }
    }

    @Entity
    public static class JavaTimeEntity {
        @Id
        public MorphiumId id;
        public LocalDate date;
        public LocalTime time;
        public LocalDateTime ldt;
        public Instant instant;
        public List<LocalDate> dateList;
        public Map<String, LocalDate> dateMap;
    }
}
