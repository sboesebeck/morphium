package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.UseIfNull;
import de.caluga.morphium.driver.MorphiumId;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests to verify if implementation distinguishes between:
 * 1. Field not present in MongoDB
 * 2. Field present with null value
 */
public class UseIfNullDistinctionTest extends MorphiumTestBase {

    @Entity
    public static class TestEntity {
        @Id
        private MorphiumId id;

        // Field WITHOUT @UseIfNull
        private Integer regularField = 100;

        // Field WITH @UseIfNull
        @UseIfNull
        private Integer nullableField = 200;

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

        public Integer getNullableField() {
            return nullableField;
        }

        public void setNullableField(Integer nullableField) {
            this.nullableField = nullableField;
        }
    }

    @Test
    public void testFieldMissingFromDB() {
        // Create a document without the fields
        Map<String, Object> doc = new HashMap<>();
        doc.put("_id", new MorphiumId());

        // Deserialize - both fields should have their default values
        TestEntity entity = morphium.getMapper().deserialize(TestEntity.class, doc);

        assertNotNull(entity);
        assertEquals(100, entity.getRegularField(),
            "Regular field should have default value when missing from DB");
        assertEquals(200, entity.getNullableField(),
            "Nullable field should have default value when missing from DB");
    }

    @Test
    public void testFieldPresentWithNullValue() {
        // Create a document WITH the fields, but null values
        Map<String, Object> doc = new HashMap<>();
        doc.put("_id", new MorphiumId());
        doc.put("regular_field", null);     // Field present, value null
        doc.put("nullable_field", null);    // Field present, value null

        // Deserialize - what happens?
        TestEntity entity = morphium.getMapper().deserialize(TestEntity.class, doc);

        assertNotNull(entity);

        log.info("Regular field value: " + entity.getRegularField());
        log.info("Nullable field value: " + entity.getNullableField());

        // NEW BEHAVIOR: @UseIfNull is checked during deserialization
        assertEquals(100, entity.getRegularField(),
            "Field WITHOUT @UseIfNull: null in DB is ignored, default value preserved");
        assertNull(entity.getNullableField(),
            "Field WITH @UseIfNull: null in DB is accepted and set to null");
    }

    @Test
    public void testMixedScenario() {
        // Create a document with one field missing, one field null
        Map<String, Object> doc = new HashMap<>();
        doc.put("_id", new MorphiumId());
        doc.put("regular_field", null);     // Present as null
        // nullable_field is missing entirely

        TestEntity entity = morphium.getMapper().deserialize(TestEntity.class, doc);

        assertNotNull(entity);
        log.info("Regular field (present as null): " + entity.getRegularField());
        log.info("Nullable field (missing): " + entity.getNullableField());

        // NEW behavior: @UseIfNull protects from nulls during deserialization
        assertEquals(100, entity.getRegularField(),
            "Field WITHOUT @UseIfNull: null in DB ignored, default preserved");
        assertEquals(200, entity.getNullableField(),
            "Field missing from DB keeps default value");
    }

    @Test
    public void testAnnotationAffectsBothDirections() {
        // Demonstrate that @UseIfNull affects BOTH writing AND reading

        // Scenario 1: Field WITHOUT @UseIfNull manually set to null in DB
        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("_id", new MorphiumId());
        doc1.put("regular_field", null);  // Manually added null

        TestEntity entity1 = morphium.getMapper().deserialize(TestEntity.class, doc1);
        assertEquals(100, entity1.getRegularField(),
            "Field WITHOUT @UseIfNull: null in DB is rejected, default preserved");

        // Scenario 2: Field WITH @UseIfNull manually set to null in DB
        Map<String, Object> doc2 = new HashMap<>();
        doc2.put("_id", new MorphiumId());
        doc2.put("nullable_field", null);  // Manually added null

        TestEntity entity2 = morphium.getMapper().deserialize(TestEntity.class, doc2);
        assertNull(entity2.getNullableField(),
            "Field WITH @UseIfNull: null in DB is accepted");

        // Scenario 3: Field WITH @UseIfNull missing from DB
        Map<String, Object> doc3 = new HashMap<>();
        doc3.put("_id", new MorphiumId());
        // nullable_field missing entirely

        TestEntity entity3 = morphium.getMapper().deserialize(TestEntity.class, doc3);
        assertEquals(200, entity3.getNullableField(),
            "Field WITH @UseIfNull: missing from DB keeps default");
    }
}
