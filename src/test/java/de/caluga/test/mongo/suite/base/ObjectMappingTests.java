package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.Utils;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.objectmapping.MorphiumTypeMapper;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Consolidated object mapping tests combining functionality from ObjectMapperTest
 * and ObjectMapperImplTest, focusing on serialization/deserialization without
 * performance benchmarking.
 */
@Tag("core")
public class ObjectMappingTests extends MorphiumTestBase {

    private final ObjectMapperImpl mapper = new ObjectMapperImpl();

    @Test
    public void basicSerializationTest() {
        // Test simple object serialization
        UncachedObject obj = new UncachedObject();
        obj.setCounter(42);
        obj.setStrValue("Test String");
        obj.setMorphiumId(new MorphiumId());

        Map<String, Object> serialized = mapper.serialize(obj);
        assertNotNull(serialized);
        assertEquals(42, serialized.get("counter"));
        assertEquals("Test String", serialized.get("value"));
        assertTrue(serialized.get("_id") instanceof ObjectId);

        // Test deserialization
        UncachedObject deserialized = mapper.deserialize(UncachedObject.class, serialized);
        assertNotNull(deserialized);
        assertEquals(42, deserialized.getCounter());
        assertEquals("Test String", deserialized.getStrValue());
        assertNotNull(deserialized.getMorphiumId());
    }

    @Test
    public void embeddedObjectMappingTest() {
        // Test embedded object serialization
        ComplexEntity complex = new ComplexEntity();
        complex.name = "Test Complex";

        EmbeddedObject embedded = new EmbeddedObject();
        embedded.setName("Embedded Name");
        embedded.setValue("Embedded Value");
        embedded.setTest(123);
        complex.embedded = embedded;

        Map<String, Object> serialized = mapper.serialize(complex);
        assertNotNull(serialized);
        assertEquals("Test Complex", serialized.get("name"));

        @SuppressWarnings("unchecked")
        Map<String, Object> embeddedSerialized = (Map<String, Object>) serialized.get("embedded");
        assertNotNull(embeddedSerialized);
        assertEquals("Embedded Name", embeddedSerialized.get("name"));
        assertEquals("Embedded Value", embeddedSerialized.get("value"));
        assertEquals(123, embeddedSerialized.get("test"));

        // Test deserialization
        ComplexEntity deserialized = mapper.deserialize(ComplexEntity.class, serialized);
        assertNotNull(deserialized);
        assertEquals("Test Complex", deserialized.name);
        assertNotNull(deserialized.embedded);
        assertEquals("Embedded Name", deserialized.embedded.getName());
        assertEquals("Embedded Value", deserialized.embedded.getValue());
        assertEquals(123, deserialized.embedded.getTest());
    }

    @Test
    public void collectionMappingTest() {
        // Test collection serialization
        CollectionEntity entity = new CollectionEntity();
        entity.stringList = Arrays.asList("item1", "item2", "item3");
        entity.intSet = Set.of(1, 2, 3, 4, 5);
        entity.stringMap = Map.of("key1", "value1", "key2", "value2");

        Map<String, Object> serialized = mapper.serialize(entity);
        assertNotNull(serialized);

        @SuppressWarnings("unchecked")
        List<String> serializedList = (List<String>) serialized.get("stringList");
        assertEquals(3, serializedList.size());
        assertTrue(serializedList.contains("item2"));

        @SuppressWarnings("unchecked")
        Set<Integer> serializedSet = new HashSet<>((List<Integer>) serialized.get("intSet"));
        assertEquals(5, serializedSet.size());
        assertTrue(serializedSet.contains(3));

        @SuppressWarnings("unchecked")
        Map<String, String> serializedMap = (Map<String, String>) serialized.get("stringMap");
        assertEquals(2, serializedMap.size());
        assertEquals("value1", serializedMap.get("key1"));

        // Test deserialization
        CollectionEntity deserialized = mapper.deserialize(CollectionEntity.class, serialized);
        assertNotNull(deserialized);
        assertEquals(3, deserialized.stringList.size());
        assertEquals(5, deserialized.intSet.size());
        assertEquals(2, deserialized.stringMap.size());
        assertTrue(deserialized.stringList.contains("item3"));
        assertTrue(deserialized.intSet.contains(4));
        assertEquals("value2", deserialized.stringMap.get("key2"));
    }

    @Test
    public void typeConversionTest() {
        // Test various type conversions
        TypeConversionEntity entity = new TypeConversionEntity();
        entity.bigInteger = new BigInteger("12345678901234567890");
        entity.dateTime = LocalDateTime.now();
        entity.longValue = 9223372036854775807L;
        entity.doubleValue = 3.141592653589793;
        entity.booleanValue = true;

        Map<String, Object> serialized = mapper.serialize(entity);
        assertNotNull(serialized);
        assertNotNull(serialized.get("bigInteger"));
        assertNotNull(serialized.get("dateTime"));
        assertEquals(9223372036854775807L, serialized.get("longValue"));
        assertEquals(3.141592653589793, serialized.get("doubleValue"));
        assertEquals(true, serialized.get("booleanValue"));

        TypeConversionEntity deserialized = mapper.deserialize(TypeConversionEntity.class, serialized);
        assertNotNull(deserialized);
        assertEquals(new BigInteger("12345678901234567890"), deserialized.bigInteger);
        assertNotNull(deserialized.dateTime);
        assertEquals(9223372036854775807L, deserialized.longValue);
        assertEquals(3.141592653589793, deserialized.doubleValue, 0.0001);
        assertTrue(deserialized.booleanValue);
    }

    @Test
    public void nullValueHandlingTest() {
        // Test null value handling
        NullableEntity entity = new NullableEntity();
        entity.requiredString = "Required";
        entity.optionalString = null;
        entity.optionalInteger = null;
        entity.optionalList = null;

        Map<String, Object> serialized = mapper.serialize(entity);
        assertNotNull(serialized);
        assertEquals("Required", serialized.get("requiredString"));
        // Null values might or might not be present in serialized map depending on configuration

        NullableEntity deserialized = mapper.deserialize(NullableEntity.class, serialized);
        assertNotNull(deserialized);
        assertEquals("Required", deserialized.requiredString);
        assertNull(deserialized.optionalString);
        assertNull(deserialized.optionalInteger);
        assertNull(deserialized.optionalList);
    }

    @Test
    public void customPropertyNameTest() {
        // Test custom property names
        CustomPropertyEntity entity = new CustomPropertyEntity();
        entity.internalName = "Internal";
        entity.differentName = "Different";

        Map<String, Object> serialized = mapper.serialize(entity);
        assertNotNull(serialized);
        assertEquals("Internal", serialized.get("external_name"));
        assertEquals("Different", serialized.get("very_different"));
        // Should not contain the field names from the Java class
        assertFalse(serialized.containsKey("internalName"));
        assertFalse(serialized.containsKey("differentName"));

        CustomPropertyEntity deserialized = mapper.deserialize(CustomPropertyEntity.class, serialized);
        assertNotNull(deserialized);
        assertEquals("Internal", deserialized.internalName);
        assertEquals("Different", deserialized.differentName);
    }

    @Test
    public void nestedObjectMappingTest() {
        // Test deeply nested objects
        NestedEntity root = new NestedEntity();
        root.name = "Root";

        NestedEntity child = new NestedEntity();
        child.name = "Child";

        NestedEntity grandchild = new NestedEntity();
        grandchild.name = "Grandchild";

        child.child = grandchild;
        root.child = child;

        Map<String, Object> serialized = mapper.serialize(root);
        assertNotNull(serialized);
        assertEquals("Root", serialized.get("name"));

        @SuppressWarnings("unchecked")
        Map<String, Object> childSerialized = (Map<String, Object>) serialized.get("child");
        assertNotNull(childSerialized);
        assertEquals("Child", childSerialized.get("name"));

        @SuppressWarnings("unchecked")
        Map<String, Object> grandchildSerialized = (Map<String, Object>) childSerialized.get("child");
        assertNotNull(grandchildSerialized);
        assertEquals("Grandchild", grandchildSerialized.get("name"));

        NestedEntity deserialized = mapper.deserialize(NestedEntity.class, serialized);
        assertNotNull(deserialized);
        assertEquals("Root", deserialized.name);
        assertNotNull(deserialized.child);
        assertEquals("Child", deserialized.child.name);
        assertNotNull(deserialized.child.child);
        assertEquals("Grandchild", deserialized.child.child.name);
    }

    @Test
    public void arrayMappingTest() {
        // Test array serialization/deserialization
        ArrayEntity entity = new ArrayEntity();
        entity.stringArray = new String[] {"a", "b", "c"};
        entity.intArray = new int[] {1, 2, 3, 4, 5};
        entity.doubleArray = new double[] {1.1, 2.2, 3.3};

        Map<String, Object> serialized = mapper.serialize(entity);
        assertNotNull(serialized);

        @SuppressWarnings("unchecked")
        List<String> stringList = (List<String>) serialized.get("stringArray");
        assertEquals(3, stringList.size());
        assertEquals("b", stringList.get(1));

        ArrayEntity deserialized = mapper.deserialize(ArrayEntity.class, serialized);
        assertNotNull(deserialized);
        assertArrayEquals(new String[] {"a", "b", "c"}, deserialized.stringArray);
        assertArrayEquals(new int[] {1, 2, 3, 4, 5}, deserialized.intArray);
        assertArrayEquals(new double[] {1.1, 2.2, 3.3}, deserialized.doubleArray, 0.001);
    }

    @Test
    public void polymorphicMappingTest() {
        // Test polymorphic object mapping
        PolymorphicContainer container = new PolymorphicContainer();

        ConcreteTypeA typeA = new ConcreteTypeA();
        typeA.commonField = "Common A";
        typeA.specificToA = "Specific A";

        ConcreteTypeB typeB = new ConcreteTypeB();
        typeB.commonField = "Common B";
        typeB.specificToB = 42;

        container.objects = Arrays.asList(typeA, typeB);

        Map<String, Object> serialized = mapper.serialize(container);
        assertNotNull(serialized);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> objectList = (List<Map<String, Object >> ) serialized.get("objects");
        assertEquals(2, objectList.size());

        // Note: Actual polymorphic behavior depends on Morphium configuration
        // This test verifies basic serialization structure

        PolymorphicContainer deserialized = mapper.deserialize(PolymorphicContainer.class, serialized);
        assertNotNull(deserialized);
        assertNotNull(deserialized.objects);
        assertEquals(2, deserialized.objects.size());
    }

    @Test
    public void mapSerializationTest() {
        // Test direct map serialization (not entity-based)
        Map<String, Object> originalMap = new HashMap<>();
        originalMap.put("string", "test");
        originalMap.put("number", 42);
        originalMap.put("boolean", true);
        originalMap.put("list", Arrays.asList("a", "b", "c"));

        Map<String, Object> serializedMap = mapper.serializeMap(originalMap, null);
        assertNotNull(serializedMap);
        assertEquals("test", serializedMap.get("string"));
        assertEquals(42, serializedMap.get("number"));
        assertEquals(true, serializedMap.get("boolean"));

        @SuppressWarnings("unchecked")
        List<String> serializedList = (List<String>) serializedMap.get("list");
        assertEquals(3, serializedList.size());
        assertEquals("b", serializedList.get(1));
    }

    // Helper entity classes for testing

    @Entity
    public static class ComplexEntity {
        @Id
        public MorphiumId id;

        @Property
        public String name;

        public EmbeddedObject embedded;
    }

    @Entity
    public static class CollectionEntity {
        @Id
        public MorphiumId id;

        @Property
        public List<String> stringList;

        @Property
        public Set<Integer> intSet;

        @Property
        public Map<String, String> stringMap;
    }

    @Entity
    public static class TypeConversionEntity {
        @Id
        public MorphiumId id;

        @Property
        public BigInteger bigInteger;

        @Property
        public LocalDateTime dateTime;

        @Property
        public Long longValue;

        @Property
        public Double doubleValue;

        @Property
        public Boolean booleanValue;
    }

    @Entity
    public static class NullableEntity {
        @Id
        public MorphiumId id;

        @Property
        public String requiredString;

        @Property
        public String optionalString;

        @Property
        public Integer optionalInteger;

        @Property
        public List<String> optionalList;
    }

    @Entity
    public static class CustomPropertyEntity {
        @Id
        public MorphiumId id;

        @Property(fieldName = "external_name")
        public String internalName;

        @Property(fieldName = "very_different")
        public String differentName;
    }

    @Entity
    public static class NestedEntity {
        @Id
        public MorphiumId id;

        @Property
        public String name;

        public NestedEntity child;
    }

    @Entity
    public static class ArrayEntity {
        @Id
        public MorphiumId id;

        @Property
        public String[] stringArray;

        @Property
        public int[] intArray;

        @Property
        public double[] doubleArray;
    }

    @Entity
    public static class PolymorphicContainer {
        @Id
        public MorphiumId id;

        @Property
        public List<BaseType> objects;
    }

    public static abstract class BaseType {
        @Property
        public String commonField;
    }

    public static class ConcreteTypeA extends BaseType {
        @Property
        public String specificToA;
    }

    public static class ConcreteTypeB extends BaseType {
        @Property
        public Integer specificToB;
    }
}
