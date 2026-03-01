package de.caluga.test.morphium;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for java.time types that cover the full encode/decode path:
 * entity field → ObjectMapperImpl (TypeMapper.marshall) → BsonEncoder → InMemoryDriver
 * → BsonDecoder → ObjectMapperImpl (TypeMapper.unmarshall) → entity field.
 * <p>
 * This complements the driver-level BsonEncoderJavaTimeTest by verifying that the
 * BsonEncoder format matches what the corresponding TypeMappers expect on unmarshall.
 * <p>
 * All tests run with InMemoryDriver — no MongoDB required.
 */
@Tag("inmemory")
public class JavaTimeEntityIntegrationTest {

    @Entity(collectionName = "java_time_entity")
    public static class JavaTimeEntity {
        @Id
        public MorphiumId id;

        public LocalDateTime localDateTime;
        public LocalDate localDate;
        public LocalTime localTime;
        public Instant instant;
        public String label;

        public JavaTimeEntity() {}

        public JavaTimeEntity(String label, LocalDateTime ldt, LocalDate ld, LocalTime lt, Instant inst) {
            this.label = label;
            this.localDateTime = ldt;
            this.localDate = ld;
            this.localTime = lt;
            this.instant = inst;
        }
    }

    private Morphium morphium;

    @BeforeEach
    public void setup() {
        MorphiumConfig cfg = new MorphiumConfig("java_time_test_db", 10, 10_000, 1_000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
    }

    @AfterEach
    public void tearDown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    @Test
    public void storeAndLoad_allJavaTimeFields_roundTripCorrectly() {
        LocalDateTime ldt = LocalDateTime.of(2025, 6, 15, 10, 30, 45, 123_000_000);
        LocalDate ld = LocalDate.of(2025, 6, 15);
        LocalTime lt = LocalTime.of(14, 30, 45, 500_000_000);
        Instant inst = Instant.parse("2025-06-15T10:30:45.123Z");

        JavaTimeEntity entity = new JavaTimeEntity("full-roundtrip", ldt, ld, lt, inst);
        morphium.store(entity);

        assertThat(entity.id).as("id must be set after store").isNotNull();

        JavaTimeEntity loaded = morphium.createQueryFor(JavaTimeEntity.class)
            .f("id").eq(entity.id)
            .get();

        assertThat(loaded).isNotNull();
        assertThat(loaded.localDateTime).isEqualTo(ldt);
        assertThat(loaded.localDate).isEqualTo(ld);
        assertThat(loaded.localTime).isEqualTo(lt);
        assertThat(loaded.instant).isEqualTo(inst);
        assertThat(loaded.label).isEqualTo("full-roundtrip");
    }

    @Test
    public void storeAndLoad_eachJavaTimeType_individually() {
        // LocalDateTime only
        JavaTimeEntity ldtEntity = new JavaTimeEntity("ldt-only",
            LocalDateTime.of(2024, 12, 31, 23, 59, 59, 999_000_000), null, null, null);
        morphium.store(ldtEntity);
        JavaTimeEntity ldtLoaded = morphium.createQueryFor(JavaTimeEntity.class).f("id").eq(ldtEntity.id).get();
        assertThat(ldtLoaded.localDateTime).isEqualTo(ldtEntity.localDateTime);

        // LocalDate only
        JavaTimeEntity ldEntity = new JavaTimeEntity("ld-only", null, LocalDate.of(2024, 1, 1), null, null);
        morphium.store(ldEntity);
        JavaTimeEntity ldLoaded = morphium.createQueryFor(JavaTimeEntity.class).f("id").eq(ldEntity.id).get();
        assertThat(ldLoaded.localDate).isEqualTo(ldEntity.localDate);

        // LocalTime only
        JavaTimeEntity ltEntity = new JavaTimeEntity("lt-only", null, null, LocalTime.of(0, 0, 0, 1), null);
        morphium.store(ltEntity);
        JavaTimeEntity ltLoaded = morphium.createQueryFor(JavaTimeEntity.class).f("id").eq(ltEntity.id).get();
        assertThat(ltLoaded.localTime).isEqualTo(ltEntity.localTime);

        // Instant only
        JavaTimeEntity instEntity = new JavaTimeEntity("inst-only", null, null, null, Instant.EPOCH);
        morphium.store(instEntity);
        JavaTimeEntity instLoaded = morphium.createQueryFor(JavaTimeEntity.class).f("id").eq(instEntity.id).get();
        assertThat(instLoaded.instant).isEqualTo(instEntity.instant);
    }

    @Test
    public void queryFilter_withLocalDateTime_gte_findsMatchingEntities() {
        LocalDateTime threshold = LocalDateTime.of(2025, 6, 1, 0, 0);
        JavaTimeEntity before = new JavaTimeEntity("before",
            LocalDateTime.of(2025, 5, 15, 12, 0), null, null, null);
        JavaTimeEntity after = new JavaTimeEntity("after",
            LocalDateTime.of(2025, 7, 15, 12, 0), null, null, null);

        morphium.store(before);
        morphium.store(after);

        // Query using java.time value directly in filter — hits BsonEncoder path
        List<JavaTimeEntity> results = morphium.createQueryFor(JavaTimeEntity.class)
            .f("local_date_time").gte(threshold)
            .asList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).label).isEqualTo("after");
    }

    @Test
    public void queryFilter_withInstant_gte_findsMatchingEntities() {
        Instant threshold = Instant.parse("2025-06-01T00:00:00Z");
        JavaTimeEntity before = new JavaTimeEntity("before",
            null, null, null, Instant.parse("2025-05-15T12:00:00Z"));
        JavaTimeEntity after = new JavaTimeEntity("after",
            null, null, null, Instant.parse("2025-07-15T12:00:00Z"));

        morphium.store(before);
        morphium.store(after);

        List<JavaTimeEntity> results = morphium.createQueryFor(JavaTimeEntity.class)
            .f("instant").gte(threshold)
            .asList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).label).isEqualTo("after");
    }

    @Test
    public void queryFilter_withLocalDate_gte_findsMatchingEntities() {
        LocalDate threshold = LocalDate.of(2025, 6, 1);
        JavaTimeEntity before = new JavaTimeEntity("before",
            null, LocalDate.of(2025, 5, 15), null, null);
        JavaTimeEntity after = new JavaTimeEntity("after",
            null, LocalDate.of(2025, 7, 15), null, null);

        morphium.store(before);
        morphium.store(after);

        // LocalDate is encoded as Long (epoch day) — Number comparison in QueryHelper
        List<JavaTimeEntity> results = morphium.createQueryFor(JavaTimeEntity.class)
            .f("local_date").gte(threshold)
            .asList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).label).isEqualTo("after");
    }

    @Test
    public void nullJavaTimeFields_storedAndLoadedAsNull() {
        JavaTimeEntity entity = new JavaTimeEntity("nulls-only", null, null, null, null);
        morphium.store(entity);

        JavaTimeEntity loaded = morphium.createQueryFor(JavaTimeEntity.class)
            .f("id").eq(entity.id)
            .get();

        assertThat(loaded).isNotNull();
        assertThat(loaded.localDateTime).isNull();
        assertThat(loaded.localDate).isNull();
        assertThat(loaded.localTime).isNull();
        assertThat(loaded.instant).isNull();
        assertThat(loaded.label).isEqualTo("nulls-only");
    }
}
