package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.IgnoreNullFromDB;
import de.caluga.morphium.driver.MorphiumId;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

/**
 * Tests to verify that Morphium correctly distinguishes between:
 * 1. Field not present in MongoDB document (missing key)
 * 2. Field present in MongoDB document with null value (key exists, value is null)
 *
 * This distinction is CRITICAL for proper null handling:
 *
 * BEHAVIOR MATRIX:
 * ┌─────────────────────────┬──────────────────────────┬──────────────────────────┐
 * │ Scenario                │ Without @IgnoreNullFromDB│ With @IgnoreNullFromDB   │
 * ├─────────────────────────┼──────────────────────────┼──────────────────────────┤
 * │ Field missing from DB   │ Keep default value       │ Keep default value       │
 * │ Field in DB = null      │ Set to null              │ Keep default (protected) │
 * │ Field in DB = value     │ Use the value            │ Use the value            │
 * └─────────────────────────┴──────────────────────────┴──────────────────────────┘
 *
 * NEW BEHAVIOR (as of the null handling flip):
 * - By default, explicit null values from DB ARE accepted and override defaults
 * - Use @IgnoreNullFromDB to protect specific fields from null contamination
 * - Missing fields ALWAYS preserve default values (regardless of annotation)
 */
public class UseIfNullDistinctionTest extends MultiDriverTestBase {

    @Entity
    public static class TestEntity {
        @Id
        private MorphiumId id;

        // Field WITHOUT @IgnoreNullFromDB - accepts nulls (default behavior)
        private Integer regularField = 100;

        // Field WITH @IgnoreNullFromDB - protected from nulls
        @IgnoreNullFromDB
        private Integer protectedField = 200;

        public MorphiumId getId() {
            return id;
        }

        public void setId(MorphiumId id) {
            this.id = id;
        }

        public Integer getRegularField() {
            return regularField;
        }

        public void setRegularField(Integer regularField) {
            this.regularField = regularField;
        }

        public Integer getProtectedField() {
            return protectedField;
        }

        public void setProtectedField(Integer protectedField) {
            this.protectedField = protectedField;
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testFieldMissingFromDB(Morphium morphium) {
        // Create a document without the fields
        Map<String, Object> doc = new HashMap<>();
        doc.put("_id", new MorphiumId());

        // Deserialize - both fields should have their default values
        TestEntity entity = morphium.getMapper().deserialize(TestEntity.class, doc);

        assertNotNull(entity);
        assertEquals(100, entity.getRegularField(),
            "Regular field should have default value when missing from DB");
        assertEquals(200, entity.getProtectedField(),
            "Protected field should have default value when missing from DB");
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testFieldPresentWithNullValue(Morphium morphium) {
        // Create a document WITH the fields, but null values
        Map<String, Object> doc = new HashMap<>();
        doc.put("_id", new MorphiumId());
        doc.put("regular_field", null);     // Field present, value null
        doc.put("protected_field", null);    // Field present, value null

        // Deserialize - what happens?
        TestEntity entity = morphium.getMapper().deserialize(TestEntity.class, doc);

        assertNotNull(entity);

        log.info("Regular field value: " + entity.getRegularField());
        log.info("Protected field value: " + entity.getProtectedField());

        // NEW BEHAVIOR: Nulls are accepted by default, @IgnoreNullFromDB protects
        assertNull(entity.getRegularField(),
            "Field WITHOUT @IgnoreNullFromDB: null in DB is accepted and set to null (default behavior)");
        assertEquals(200, entity.getProtectedField(),
            "Field WITH @IgnoreNullFromDB: null in DB is ignored, default value preserved");
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testMixedScenario(Morphium morphium) {
        // Create a document with one field missing, one field null
        Map<String, Object> doc = new HashMap<>();
        doc.put("_id", new MorphiumId());
        doc.put("regular_field", null);     // Present as null
        // protected_field is missing entirely

        TestEntity entity = morphium.getMapper().deserialize(TestEntity.class, doc);

        assertNotNull(entity);
        log.info("Regular field (present as null): " + entity.getRegularField());
        log.info("Protected field (missing): " + entity.getProtectedField());

        // NEW behavior: nulls accepted by default, @IgnoreNullFromDB protects
        assertNull(entity.getRegularField(),
            "Field WITHOUT @IgnoreNullFromDB: null in DB accepted (default behavior)");
        assertEquals(200, entity.getProtectedField(),
            "Field missing from DB keeps default value");
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testAnnotationAffectsBothDirections(Morphium morphium) {
        // Demonstrate that @IgnoreNullFromDB affects BOTH writing AND reading

        // Scenario 1: Field WITHOUT @IgnoreNullFromDB manually set to null in DB
        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("_id", new MorphiumId());
        doc1.put("regular_field", null);  // Manually added null

        TestEntity entity1 = morphium.getMapper().deserialize(TestEntity.class, doc1);
        assertNull(entity1.getRegularField(),
            "Field WITHOUT @IgnoreNullFromDB: null in DB is accepted (default behavior)");

        // Scenario 2: Field WITH @IgnoreNullFromDB manually set to null in DB
        Map<String, Object> doc2 = new HashMap<>();
        doc2.put("_id", new MorphiumId());
        doc2.put("protected_field", null);  // Manually added null

        TestEntity entity2 = morphium.getMapper().deserialize(TestEntity.class, doc2);
        assertEquals(200, entity2.getProtectedField(),
            "Field WITH @IgnoreNullFromDB: null in DB is rejected, default preserved");

        // Scenario 3: Field WITH @IgnoreNullFromDB missing from DB
        Map<String, Object> doc3 = new HashMap<>();
        doc3.put("_id", new MorphiumId());
        // protected_field missing entirely

        TestEntity entity3 = morphium.getMapper().deserialize(TestEntity.class, doc3);
        assertEquals(200, entity3.getProtectedField(),
            "Field WITH @IgnoreNullFromDB: missing from DB keeps default");
    }
}
