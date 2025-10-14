package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.UseIfNull;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for @UseIfNull annotation behavior
 */
public class UseIfNullTest extends MorphiumTestBase {

    @Entity
    public static class TestEntity {
        @Id
        private MorphiumId id;

        // Regular field - null values not stored
        private String regularField;

        // Field with @UseIfNull - null values stored
        @UseIfNull
        private String nullableField;

        // Field with default value - preserved when not in DB
        private Integer counter = 42;

        // Field with @UseIfNull and default value - overwritten by null from DB
        @UseIfNull
        private Integer nullCounter = 99;

        public MorphiumId getId() {
            return id;
        }

        public void setId(MorphiumId id) {
            this.id = id;
        }

        public String getRegularField() {
            return regularField;
        }

        public void setRegularField(String regularField) {
            this.regularField = regularField;
        }

        public String getNullableField() {
            return nullableField;
        }

        public void setNullableField(String nullableField) {
            this.nullableField = nullableField;
        }

        public Integer getCounter() {
            return counter;
        }

        public void setCounter(Integer counter) {
            this.counter = counter;
        }

        public Integer getNullCounter() {
            return nullCounter;
        }

        public void setNullCounter(Integer nullCounter) {
            this.nullCounter = nullCounter;
        }
    }

    @Test
    public void testNullValueNotStoredWithoutAnnotation() {
        // Test that fields without @UseIfNull don't store null values
        TestEntity entity = new TestEntity();
        entity.setRegularField(null);
        entity.setNullableField("not null");

        Map<String, Object> serialized = morphium.getMapper().serialize(entity);

        log.info("Serialized keys: " + serialized.keySet());
        log.info("Serialized content: " + serialized);

        // regularField should not be in the serialized map (camelCase -> snake_case)
        assertFalse(serialized.containsKey("regular_field"),
            "Field without @UseIfNull should not be stored when null");

        // nullableField should be in the map
        assertTrue(serialized.containsKey("nullable_field"),
            "Field with value should be stored. Actual keys: " + serialized.keySet());
    }

    @Test
    public void testNullValueStoredWithAnnotation() {
        // Test that fields with @UseIfNull store null values
        TestEntity entity = new TestEntity();
        entity.setRegularField("not null");
        entity.setNullableField(null);

        Map<String, Object> serialized = morphium.getMapper().serialize(entity);

        // nullableField should be in the map even though it's null (camelCase -> snake_case)
        assertTrue(serialized.containsKey("nullable_field"),
            "Field with @UseIfNull should be stored even when null");
        assertNull(serialized.get("nullable_field"),
            "Stored value should be null");

        // regularField should be in the map
        assertTrue(serialized.containsKey("regular_field"),
            "Field with value should be stored");
    }

    @Test
    public void testDefaultValuePreservedWhenFieldMissing() {
        // Simulate reading from DB where counter field is missing
        TestEntity entity = new TestEntity();
        entity.setRegularField("test");
        entity.setCounter(null); // null value, so won't be stored (no @UseIfNull)

        // Serialize to see what would be stored
        Map<String, Object> serialized = morphium.getMapper().serialize(entity);
        assertFalse(serialized.containsKey("counter"),
            "counter should not be in serialized map when null");

        // Deserialize from map without counter field
        TestEntity deserialized = morphium.getMapper().deserialize(TestEntity.class, serialized);
        assertNotNull(deserialized);
        assertEquals(42, deserialized.getCounter(),
            "Default value should be preserved when field is missing from DB");
    }

    @Test
    public void testDefaultValueOverwrittenWhenNullInDB() {
        // Simulate reading from DB where nullCounter is explicitly null
        TestEntity entity = new TestEntity();
        entity.setRegularField("test");
        entity.setNullCounter(null); // null value, but will be stored due to @UseIfNull

        // Serialize to see what would be stored
        Map<String, Object> serialized = morphium.getMapper().serialize(entity);
        assertTrue(serialized.containsKey("null_counter"),
            "null_counter should be in serialized map even when null");
        assertNull(serialized.get("null_counter"),
            "null_counter should be null in serialized map");

        // Deserialize from map with null_counter field present as null
        TestEntity deserialized = morphium.getMapper().deserialize(TestEntity.class, serialized);
        assertNotNull(deserialized);
        assertNull(deserialized.getNullCounter(),
            "Field with @UseIfNull should be null from DB, not default value");
    }

    @Test
    public void testFullRoundTrip() {
        // Create entity with various null/non-null values
        TestEntity entity = new TestEntity();
        entity.setRegularField(null);        // Not stored
        entity.setNullableField(null);       // Stored as null
        entity.setCounter(null);             // Not stored, default preserved
        entity.setNullCounter(null);         // Stored as null

        // Verify by serializing the entity to see what would be stored
        Map<String, Object> serialized = morphium.getMapper().serialize(entity);
        assertFalse(serialized.containsKey("regular_field"),
            "regular_field should not be serialized when null");
        assertTrue(serialized.containsKey("nullable_field"),
            "nullable_field should be serialized even when null");
        assertNull(serialized.get("nullable_field"),
            "nullable_field should be null in serialization");
        assertFalse(serialized.containsKey("counter"),
            "counter should not be serialized when null");
        assertTrue(serialized.containsKey("null_counter"),
            "null_counter should be serialized even when null");
        assertNull(serialized.get("null_counter"),
            "null_counter should be null in serialization");

        // Deserialize and verify behavior
        TestEntity loaded = morphium.getMapper().deserialize(TestEntity.class, serialized);
        assertNotNull(loaded);
        assertNull(loaded.getRegularField(),
            "regular_field should be null (not stored, so stays at default null)");
        assertNull(loaded.getNullableField(),
            "nullable_field should be null (from DB)");
        assertEquals(42, loaded.getCounter(),
            "counter should have default value (not in DB)");
        assertNull(loaded.getNullCounter(),
            "null_counter should be null (from DB, overriding default)");
    }
}
