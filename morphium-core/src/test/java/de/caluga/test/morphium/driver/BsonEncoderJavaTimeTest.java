package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.bson.BsonDecoder;
import de.caluga.morphium.driver.bson.BsonEncoder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that BsonEncoder can directly encode java.time types
 * (LocalDateTime, LocalDate, LocalTime, Instant) without requiring
 * prior conversion through ObjectMapperImpl custom mappers.
 * <p>
 * This is essential for query filters where java.time values are
 * passed directly into the BSON query document.
 */
@Tag("driver")
public class BsonEncoderJavaTimeTest {

    @Test
    public void localDateTime_encodedAsDocument() throws Exception {
        LocalDateTime ldt = LocalDateTime.of(2025, 6, 15, 10, 30, 45, 123_000_000);

        Doc doc = Doc.of("ts", ldt);
        byte[] bytes = BsonEncoder.encodeDocument(doc);
        Map<String, Object> decoded = BsonDecoder.decodeDocument(bytes);

        assertThat(decoded).containsKey("ts");
        @SuppressWarnings("unchecked")
        Map<String, Object> tsDoc = (Map<String, Object>) decoded.get("ts");
        assertThat(tsDoc.get("sec")).isEqualTo(ldt.toEpochSecond(ZoneOffset.UTC));
        assertThat(tsDoc.get("n")).isEqualTo(ldt.getNano());
    }

    @Test
    public void localDateTime_roundTripsCorrectly() throws Exception {
        LocalDateTime original = LocalDateTime.of(2024, 1, 1, 0, 0, 0);

        Doc doc = Doc.of("ts", original);
        byte[] bytes = BsonEncoder.encodeDocument(doc);
        Map<String, Object> decoded = BsonDecoder.decodeDocument(bytes);

        @SuppressWarnings("unchecked")
        Map<String, Object> tsDoc = (Map<String, Object>) decoded.get("ts");
        LocalDateTime restored = LocalDateTime.ofEpochSecond(
            (long) tsDoc.get("sec"), (int) tsDoc.get("n"), ZoneOffset.UTC);

        assertThat(restored).isEqualTo(original);
    }

    @Test
    public void localDate_encodedAsLong() throws Exception {
        LocalDate ld = LocalDate.of(2025, 6, 15);

        Doc doc = Doc.of("d", ld);
        byte[] bytes = BsonEncoder.encodeDocument(doc);
        Map<String, Object> decoded = BsonDecoder.decodeDocument(bytes);

        assertThat(decoded.get("d")).isEqualTo(ld.toEpochDay());
    }

    @Test
    public void localTime_encodedAsLong() throws Exception {
        LocalTime lt = LocalTime.of(14, 30, 45, 500_000_000);

        Doc doc = Doc.of("t", lt);
        byte[] bytes = BsonEncoder.encodeDocument(doc);
        Map<String, Object> decoded = BsonDecoder.decodeDocument(bytes);

        assertThat(decoded.get("t")).isEqualTo(lt.toNanoOfDay());
    }

    @Test
    public void instant_encodedAsDocument() throws Exception {
        Instant inst = Instant.parse("2025-06-15T10:30:45.123Z");

        Doc doc = Doc.of("i", inst);
        byte[] bytes = BsonEncoder.encodeDocument(doc);
        Map<String, Object> decoded = BsonDecoder.decodeDocument(bytes);

        @SuppressWarnings("unchecked")
        Map<String, Object> iDoc = (Map<String, Object>) decoded.get("i");
        assertThat(iDoc.get("type")).isEqualTo("instant");
        assertThat(iDoc.get("seconds")).isEqualTo(inst.getEpochSecond());
        assertThat(iDoc.get("nanos")).isEqualTo(inst.getNano());
    }

    @Test
    public void javaTimeTypes_mixedInDocument() throws Exception {
        LocalDateTime ldt = LocalDateTime.of(2025, 3, 20, 8, 0);
        LocalDate ld = LocalDate.of(2025, 3, 20);
        LocalTime lt = LocalTime.of(8, 0);
        Instant inst = Instant.now();

        Doc doc = Doc.of("ldt", ldt, "ld", ld, "lt", lt, "inst", inst, "name", "test");
        byte[] bytes = BsonEncoder.encodeDocument(doc);
        Map<String, Object> decoded = BsonDecoder.decodeDocument(bytes);

        assertThat(decoded).containsKey("ldt");
        assertThat(decoded).containsKey("ld");
        assertThat(decoded).containsKey("lt");
        assertThat(decoded).containsKey("inst");
        assertThat(decoded.get("name")).isEqualTo("test");
    }
}
