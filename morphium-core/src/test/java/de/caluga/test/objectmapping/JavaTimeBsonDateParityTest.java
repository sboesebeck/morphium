package de.caluga.test.objectmapping;

import de.caluga.morphium.objectmapping.InstantMapper;
import de.caluga.morphium.objectmapping.LocalDateMapper;
import de.caluga.morphium.objectmapping.LocalDateTimeMapper;
import de.caluga.morphium.objectmapping.LocalTimeMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies BSON-Date parity to the official MongoDB Java driver's {@code org.bson.codecs.jsr310}
 * codecs (encode() behaviour re-derived from the actually pinned driver version, 4.11.5 -- see
 * morphium-core/docs/architecture/javatime-bson-date-parity-plan.md Section 5) for
 * LocalDate/LocalTime/LocalDateTime/Instant when {@code useBsonDate=true}, AND that the legacy
 * (default) format is unchanged when {@code useBsonDate=false} -- both directions matter, since
 * this is an opt-in flag, not a new default.
 */
public class JavaTimeBsonDateParityTest {

    @Test
    public void instant_useBsonDateTrue_isNativeDateAtEpochMilli() {
        Instant inst = Instant.parse("2025-06-15T10:30:45.123Z");
        InstantMapper mapper = new InstantMapper(true);

        Object result = mapper.marshall(inst);

        assertInstanceOf(Date.class, result, "must be a native java.util.Date, not the legacy Doc format");
        assertEquals(inst.toEpochMilli(), ((Date) result).getTime());
    }

    @Test
    public void instant_useBsonDateFalse_isLegacyDocFormat() {
        Instant inst = Instant.parse("2025-06-15T10:30:45.123Z");
        InstantMapper mapper = new InstantMapper(false);

        Object result = mapper.marshall(inst);

        assertInstanceOf(Map.class, result, "legacy format must remain a Doc/Map");
        Map<?, ?> doc = (Map<?, ?>) result;
        assertEquals("instant", doc.get("type"));
        assertEquals(inst.getEpochSecond(), doc.get("seconds"));
        assertEquals(inst.getNano(), doc.get("nanos"));
    }

    @Test
    public void instant_defaultConstructor_isLegacyFormat() {
        // The parameterless constructor must keep defaulting to legacy -- this is the
        // "unchanged default behaviour" regression guard for InstantMapper specifically.
        InstantMapper mapper = new InstantMapper();
        Object result = mapper.marshall(Instant.parse("2025-01-01T00:00:00Z"));
        assertInstanceOf(Map.class, result);
    }

    @Test
    public void localDateTime_useBsonDateTrue_matchesLocalDateTimeCodec() {
        LocalDateTime ldt = LocalDateTime.of(2025, 6, 15, 10, 30, 45, 123_000_000);
        LocalDateTimeMapper mapper = new LocalDateTimeMapper(true);

        Object result = mapper.marshall(ldt);

        assertInstanceOf(Date.class, result);
        // org.bson.codecs.jsr310.LocalDateTimeCodec#encode: writeDateTime(value.toInstant(UTC).toEpochMilli())
        long expected = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
        assertEquals(expected, ((Date) result).getTime());
    }

    @Test
    public void localDateTime_useBsonDateFalse_isLegacyMapFormat() {
        LocalDateTime ldt = LocalDateTime.of(2025, 6, 15, 10, 30, 45);
        LocalDateTimeMapper mapper = new LocalDateTimeMapper(false);

        Object result = mapper.marshall(ldt);

        assertInstanceOf(Map.class, result);
        Map<?, ?> doc = (Map<?, ?>) result;
        assertEquals(ldt.toEpochSecond(ZoneOffset.UTC), doc.get("sec"));
        assertEquals(ldt.getNano(), doc.get("n"));
    }

    @Test
    public void localDate_useBsonDateTrue_matchesLocalDateCodec() {
        LocalDate ld = LocalDate.of(2025, 6, 15);
        LocalDateMapper mapper = new LocalDateMapper(true);

        Object result = mapper.marshall(ld);

        assertInstanceOf(Date.class, result);
        // org.bson.codecs.jsr310.LocalDateCodec#encode: writeDateTime(atStartOfDay(UTC).toInstant().toEpochMilli())
        long expected = ld.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        assertEquals(expected, ((Date) result).getTime());
    }

    @Test
    public void localDate_useBsonDateFalse_isLegacyEpochDayLong() {
        LocalDate ld = LocalDate.of(2025, 6, 15);
        LocalDateMapper mapper = new LocalDateMapper(false);

        Object result = mapper.marshall(ld);

        assertInstanceOf(Long.class, result);
        assertEquals(ld.toEpochDay(), result);
    }

    @Test
    public void localTime_useBsonDateTrue_matchesLocalTimeCodec() {
        LocalTime lt = LocalTime.of(14, 30, 45, 500_000_000);
        LocalTimeMapper mapper = new LocalTimeMapper(true);

        Object result = mapper.marshall(lt);

        assertInstanceOf(Date.class, result);
        // org.bson.codecs.jsr310.LocalTimeCodec#encode: writeDateTime(atDate(epochDay 0).toInstant(UTC).toEpochMilli())
        long expected = lt.atDate(LocalDate.ofEpochDay(0L)).atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        assertEquals(expected, ((Date) result).getTime());
    }

    @Test
    public void localTime_useBsonDateFalse_isLegacyNanoOfDayLong() {
        LocalTime lt = LocalTime.of(14, 30, 45);
        LocalTimeMapper mapper = new LocalTimeMapper(false);

        Object result = mapper.marshall(lt);

        assertInstanceOf(Long.class, result);
        assertEquals(lt.toNanoOfDay(), result);
    }

    @Test
    public void allFourTypes_roundTripCorrectly_whenUseBsonDateTrue() {
        Instant inst = Instant.parse("2025-06-15T10:30:45.123Z");
        LocalDateTime ldt = LocalDateTime.of(2025, 6, 15, 10, 30, 45, 123_000_000);
        LocalDate ld = LocalDate.of(2025, 6, 15);
        LocalTime lt = LocalTime.of(14, 30, 45, 500_000_000);

        InstantMapper instantMapper = new InstantMapper(true);
        LocalDateTimeMapper ldtMapper = new LocalDateTimeMapper(true);
        LocalDateMapper ldMapper = new LocalDateMapper(true);
        LocalTimeMapper ltMapper = new LocalTimeMapper(true);

        // Instant round-trips exactly (millisecond precision matches the source -- .123s = 123ms).
        assertEquals(inst, instantMapper.unmarshall(instantMapper.marshall(inst)));
        // LocalDateTime/LocalDate/LocalTime lose sub-millisecond precision on the round-trip via
        // BSON Date, same as the official driver's codecs -- assert on the millisecond-truncated
        // expectation, not full equality, matching the documented trade-off (plan Section 5).
        assertEquals(ldt.truncatedTo(java.time.temporal.ChronoUnit.MILLIS),
                ldtMapper.unmarshall(ldtMapper.marshall(ldt)));
        assertEquals(ld, ldMapper.unmarshall(ldMapper.marshall(ld)));
        assertEquals(lt.truncatedTo(java.time.temporal.ChronoUnit.MILLIS),
                ltMapper.unmarshall(ltMapper.marshall(lt)));
    }

    @Test
    public void supplierBased_readsFlagFreshOnEveryCall_notCachedAtConstruction() {
        // Proves the lazy-read design from the plan's Section 2.3/3: toggling the backing
        // supplier's value AFTER the mapper was constructed must change marshall()'s behaviour
        // immediately -- a frozen constructor boolean could never do this.
        boolean[] flag = {false};
        InstantMapper mapper = new InstantMapper(() -> flag[0]);
        Instant inst = Instant.parse("2025-01-01T00:00:00Z");

        assertInstanceOf(Map.class, mapper.marshall(inst), "starts legacy (flag=false)");

        flag[0] = true;
        assertInstanceOf(Date.class, mapper.marshall(inst), "must switch to native Date once the supplier flips, without rebuilding the mapper");

        flag[0] = false;
        assertInstanceOf(Map.class, mapper.marshall(inst), "must switch back to legacy when the supplier flips again");
    }
}
