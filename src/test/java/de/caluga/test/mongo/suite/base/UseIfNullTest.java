package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.IgnoreNullFromDB;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for null handling behavior with @IgnoreNullFromDB annotation.
 *
 * NEW BEHAVIOR: By default, null values are stored and accepted from DB.
 * Use @IgnoreNullFromDB to protect specific fields from null contamination.
 */
public class UseIfNullTest extends MorphiumTestBase {

    @Entity
    public static class TestEntity {
        @Id
        private MorphiumId id;

        // Regular field - null values stored and accepted (default behavior)
        private String regularField;

        // Field with @IgnoreNullFromDB - null values NOT stored, ignored from DB
        @IgnoreNullFromDB
        private String protectedField;

        // Field with default value - can be overwritten by null from DB (default)
        private Integer counter = 42;

        // Field with @IgnoreNullFromDB and default value - protected from null
        @IgnoreNullFromDB
        private Integer protectedCounter = 99;

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

        public String getProtectedField() {
            return protectedField;
        }

        public void setProtectedField(String protectedField) {
            this.protectedField = protectedField;
        }

        public Integer getCounter() {
            return counter;
        }

        public void setCounter(Integer counter) {
            this.counter = counter;
        }

        public Integer getProtectedCounter() {
            return protectedCounter;
        }

        public void setProtectedCounter(Integer protectedCounter) {
            this.protectedCounter = protectedCounter;
        }
    }

    @Test
    public void testNullValueStoredByDefault() {
        // NEW BEHAVIOR: Test that fields WITHOUT @IgnoreNullFromDB store null values
        TestEntity entity = new TestEntity();
        entity.setRegularField(null);
        entity.setProtectedField("not null");

        Map<String, Object> serialized = morphium.getMapper().serialize(entity);

        log.info("Serialized keys: " + serialized.keySet());
        log.info("Serialized content: " + serialized);

        // regularField should be in the serialized map with null (camelCase -> snake_case)
        assertTrue(serialized.containsKey("regular_field"),
            "Field without @IgnoreNullFromDB should be stored even when null (default behavior)");
        assertNull(serialized.get("regular_field"),
            "Stored value should be null");

        // protectedField should be in the map
        assertTrue(serialized.containsKey("protected_field"),
            "Field with value should be stored. Actual keys: " + serialized.keySet());
    }

    @Test
    public void testNullValueNotStoredWithProtection() {
        // NEW BEHAVIOR: Test that fields with @IgnoreNullFromDB don't store null values
        TestEntity entity = new TestEntity();
        entity.setRegularField("not null");
        entity.setProtectedField(null);

        Map<String, Object> serialized = morphium.getMapper().serialize(entity);

        // protectedField should NOT be in the map when null (camelCase -> snake_case)
        assertFalse(serialized.containsKey("protected_field"),
            "Field with @IgnoreNullFromDB should NOT be stored when null");

        // regularField should be in the map
        assertTrue(serialized.containsKey("regular_field"),
            "Field with value should be stored");
    }

    @Test
    public void testDefaultValuePreservedWhenFieldMissing() {
        // NEW BEHAVIOR: Simulate reading from DB where protectedCounter field is missing
        TestEntity entity = new TestEntity();
        entity.setRegularField("test");
        entity.setProtectedCounter(null); // null value, won't be stored (has @IgnoreNullFromDB)

        // Serialize to see what would be stored
        Map<String, Object> serialized = morphium.getMapper().serialize(entity);
        assertFalse(serialized.containsKey("protected_counter"),
            "protected_counter should not be in serialized map when null (has @IgnoreNullFromDB)");

        // Deserialize from map without protectedCounter field
        TestEntity deserialized = morphium.getMapper().deserialize(TestEntity.class, serialized);
        assertNotNull(deserialized);
        assertEquals(99, deserialized.getProtectedCounter(),
            "Default value should be preserved when field is missing from DB");
    }

    @Test
    public void testDefaultValueOverwrittenWhenNullInDB() {
        // NEW BEHAVIOR: Simulate reading from DB where counter is explicitly null
        TestEntity entity = new TestEntity();
        entity.setRegularField("test");
        entity.setCounter(null); // null value, will be stored (default behavior)

        // Serialize to see what would be stored
        Map<String, Object> serialized = morphium.getMapper().serialize(entity);
        assertTrue(serialized.containsKey("counter"),
            "counter should be in serialized map even when null (default behavior)");
        assertNull(serialized.get("counter"),
            "counter should be null in serialized map");

        // Deserialize from map with counter field present as null
        TestEntity deserialized = morphium.getMapper().deserialize(TestEntity.class, serialized);
        assertNotNull(deserialized);
        assertNull(deserialized.getCounter(),
            "Field without @IgnoreNullFromDB should be null from DB, overriding default value");
    }

    @Test
    public void testFullRoundTrip() {
        // NEW BEHAVIOR: Create entity with various null/non-null values
        TestEntity entity = new TestEntity();
        entity.setRegularField(null);        // Stored as null (default behavior)
        entity.setProtectedField(null);      // Not stored (has @IgnoreNullFromDB)
        entity.setCounter(null);             // Stored as null (default behavior)
        entity.setProtectedCounter(null);    // Not stored (has @IgnoreNullFromDB)

        // Verify by serializing the entity to see what would be stored
        Map<String, Object> serialized = morphium.getMapper().serialize(entity);
        assertTrue(serialized.containsKey("regular_field"),
            "regular_field should be serialized even when null (default behavior)");
        assertNull(serialized.get("regular_field"),
            "regular_field should be null in serialization");
        assertFalse(serialized.containsKey("protected_field"),
            "protected_field should not be serialized when null (has @IgnoreNullFromDB)");
        assertTrue(serialized.containsKey("counter"),
            "counter should be serialized even when null (default behavior)");
        assertNull(serialized.get("counter"),
            "counter should be null in serialization");
        assertFalse(serialized.containsKey("protected_counter"),
            "protected_counter should not be serialized when null (has @IgnoreNullFromDB)");

        // Deserialize and verify behavior
        TestEntity loaded = morphium.getMapper().deserialize(TestEntity.class, serialized);
        assertNotNull(loaded);
        assertNull(loaded.getRegularField(),
            "regular_field should be null (from DB)");
        assertNull(loaded.getProtectedField(),
            "protected_field should be null (not stored, so stays at default null)");
        assertNull(loaded.getCounter(),
            "counter should be null (from DB, overriding default)");
        assertEquals(99, loaded.getProtectedCounter(),
            "protected_counter should have default value (not in DB, protected by @IgnoreNullFromDB)");
    }
}
