package de.caluga.test.objectmapping;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * End-to-end proof for morphium-core/docs/architecture/javatime-bson-date-parity-plan.md
 * Section 4: the regular, type-safe {@code Query<T>} API path (here:
 * {@code query.f("field").eq(someLocalDateTime)}) already runs through
 * {@code MongoFieldImpl#checkValue} -> {@code ObjectMapperImpl#marshallIfCustomMapped} -- the
 * SAME custom mapper the entity-persistence path uses -- BEFORE the filter document ever reaches
 * {@code de.caluga.morphium.driver.bson.BsonEncoder}. So once
 * {@code ObjectMappingSettings#setUseBsonDateForJavaTime(true)} is set, a query filter value
 * built through the field API is automatically consistent with how the same type would be
 * persisted -- no separate BsonEncoder change needed for this path.
 */
@Tag("core")
public class JavaTimeQueryFilterBsonDateTest {

    @Entity
    public static class EventEntity {
        @Id
        public MorphiumId id;
        public LocalDateTime occurredAt;
        public Instant recordedAt;
    }

    private Morphium morphium;

    @BeforeEach
    void setup() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings().setDatabase("javatime_query_filter_test");
        cfg.driverSettings().setDriverName("InMemDriver");
        morphium = new Morphium(cfg);
    }

    @AfterEach
    void teardown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    @Test
    public void localDateTimeFilterValue_isNativeDate_whenUseBsonDateEnabled() {
        morphium.getConfig().objectMappingSettings().setUseBsonDateForJavaTime(true);

        LocalDateTime value = LocalDateTime.of(2025, 6, 15, 10, 30, 45);
        Query<EventEntity> q = morphium.createQueryFor(EventEntity.class).f("occurredAt").eq(value);

        Map<String, Object> filter = q.toQueryObject();
        // camelCaseConversionEnabled (default true, ObjectMappingSettings) maps the Java field
        // name "occurredAt" to the Mongo field name "occurred_at" -- verified via a scratch debug
        // run against the real InMemoryDriver output before writing this assertion.
        Object filterValue = filter.get("occurred_at");

        assertInstanceOf(Date.class, filterValue,
            "the query filter value must already be a native Date -- MongoFieldImpl.checkValue "
            + "applies the same custom mapper the entity-persistence path uses, before BsonEncoder "
            + "is ever reached");
        assertEquals(value.toInstant(ZoneOffset.UTC).toEpochMilli(), ((Date) filterValue).getTime());
    }

    @Test
    public void instantFilterValue_isNativeDate_whenUseBsonDateEnabled() {
        morphium.getConfig().objectMappingSettings().setUseBsonDateForJavaTime(true);

        Instant value = Instant.parse("2025-06-15T10:30:45.123Z");
        Query<EventEntity> q = morphium.createQueryFor(EventEntity.class).f("recordedAt").eq(value);

        Map<String, Object> filter = q.toQueryObject();
        Object filterValue = filter.get("recorded_at");

        assertInstanceOf(Date.class, filterValue);
        assertEquals(value.toEpochMilli(), ((Date) filterValue).getTime());
    }

    @Test
    public void localDateTimeFilterValue_staysLegacyFormat_whenUseBsonDateDisabled() {
        // Default is false -- unchanged behaviour is the non-negotiable regression guard
        // (plan Section 6). Not explicitly setting the flag here IS the test.
        LocalDateTime value = LocalDateTime.of(2025, 6, 15, 10, 30, 45);
        Query<EventEntity> q = morphium.createQueryFor(EventEntity.class).f("occurredAt").eq(value);

        Map<String, Object> filter = q.toQueryObject();
        Object filterValue = filter.get("occurred_at");

        assertInstanceOf(Map.class, filterValue,
            "default (useBsonDateForJavaTime=false) must keep producing the legacy Doc{sec, n} format");
    }
}
