package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.*;
// import net.sf.cglib.proxy.Enhancer;
// import net.sf.cglib.proxy.FixedValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.FixedValue;
import java.lang.reflect.Field;
import java.util.*;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by stephan on 26.11.15.
 */
@SuppressWarnings({"AssertWithSideEffects", "unchecked"})
@Tag("core")
public class AnnotationAndReflectionHelperTest {

    private AnnotationAndReflectionHelper helper;

    @BeforeEach
    public void setup() {
        helper = new AnnotationAndReflectionHelper(true);
    }

    @Test
    public void testIsAnnotationPresentInHierarchy() {
        assertTrue(helper.isAnnotationPresentInHierarchy(UncachedObject.class, Entity.class));
        assertTrue(helper.isAnnotationPresentInHierarchy(CachedObject.class, Cache.class));
    }

    @Test
    public void returnsTheRealClassFromCache() {
        // Given
        Map < Class<?>, Class<? >> realClassCache = new HashMap<>();
        helper = new AnnotationAndReflectionHelper(true, realClassCache);
        helper.getRealClass(newProxy().getClass());
        // When
        helper.getRealClass(newProxy().getClass());
        // Then
        assertEquals(1, realClassCache.size());
        assertTrue(realClassCache.containsKey(newProxy().getClass()));
    }

    @Test
    public void returnsTheRealClassOfSuperClass() {
        assertEquals(UncachedObject.class, helper.getRealClass(UncachedObject.class));
    }

    @Test
    public void testIsBufferedWrite() {
        assertTrue(helper.isBufferedWrite(CachedObject.class));
    }

    @Test
    public void testGetAnnotationFromHierarchy() {
        assertNotNull(helper.getAnnotationFromHierarchy(UncachedObject.class, Entity.class));
        assertNotNull(helper.getAnnotationFromHierarchy(new UncachedObject() {
            private String justASubclass;
        } .getClass(), Entity.class));

    }

    @Test
    public void testHasAdditionalData() {
        assertTrue(helper.hasAdditionalData(AdditionalDataEntity.class));
    }

    @Test
    public void testGetFieldName() {
        String fieldName = helper.getMongoFieldName(UncachedObject.class, "counter");
        assertNotNull((fieldName));
        assertEquals("counter", fieldName);
        fieldName = helper.getMongoFieldName(UncachedObject.class, "intData");
        assertNotNull((fieldName));
        assertEquals("int_data", fieldName);

    }

    @Test
    public void testCreateCamelCase() {
        assertEquals("helloWorld", helper.createCamelCase("hello_world", false));
        assertEquals("HelloWorld", helper.createCamelCase("hello_world", true));
    }

    @Test
    public void testConvertCamelCase() {
        assertEquals("hello_world", helper.convertCamelCase("hello_world"));
    }

    @Test
    public void convertCamelCaseTest() {
        String n = helper.convertCamelCase("thisIsATestTT");
        assert (n.equals("this_is_a_test_t_t"));

    }

    @Test
    public void testGetAllFields() throws Exception {
        List<Field> fields = helper.getAllFields(UncachedObject.class);
        assertEquals(10, fields.size());
        assertTrue(fields.contains(UncachedObject.class.getDeclaredField("counter")));
    }

    @Test
    public void testGetField() {
        Field field = helper.getField(UncachedObject.class, "counter");
        assertNotNull((field));
        assertEquals("counter", field.getName());
    }

    @Test
    public void testIsEntity() {
        assertTrue(helper.isEntity(UncachedObject.class));
        assertTrue(helper.isEntity(EmbeddedObject.class));
    }

    @Test
    public void testGetValue() {
        UncachedObject uncachedObject = new UncachedObject();
        uncachedObject.setCounter(123);
        Object value = helper.getValue(uncachedObject, "counter");
        assertNotNull((value));
        assertEquals(123, value);
    }

    @Test
    public void testSetValue() {
        UncachedObject uncachedObject = new UncachedObject();
        helper.setValue(uncachedObject, 123, "counter");
        assertEquals(123, uncachedObject.getCounter());
    }

    @Test
    public void testSetUUIDFromJavaLegacyUIIDByteArray() {
        UUIDTestObject uuidTestObject = new UUIDTestObject();
        byte[] dbValue = new byte[] {78, 67, 47, -34, -114, 8, 62, -13, 72, -42, 126, -10, 38, 56, -106, -122};
        helper.setValue(uuidTestObject, dbValue, "uuidValue");
        assertEquals(UUID.fromString("4e432fde-8e08-3ef3-48d6-7ef626389686"), uuidTestObject.uuidValue);
    }

    @Test
    public void testGetId() {
        UncachedObject uncachedObject = new UncachedObject();
        uncachedObject.setMorphiumId(new MorphiumId());
        assertEquals(uncachedObject.getMorphiumId(), helper.getId(uncachedObject));
    }

    @Test
    public void testGetIdFieldName() {
        String fieldName = helper.getIdFieldName(new UncachedObject());
        assertEquals("morphium_id", fieldName);
    }

    @Test
    public void testGetIdField() throws Exception {
        assertEquals(UncachedObject.class.getDeclaredField("morphiumId"), helper.getIdField(new UncachedObject()));
    }

    @Test
    public void testGetFields() {
        assertEquals(1, helper.getFields(UncachedObject.class, Id.class).size());
        assertEquals(2, helper.getFields(UncachedObject.class, Index.class).size());
    }

    @Test
    public void testGetTypeOfField() {
        assertTrue(Objects.equals(helper.getTypeOfField(UncachedObject.class, "strValue"), String.class));
    }

    @Test
    public void testStoresLastChange() {
        assertFalse(helper.storesLastChange(UncachedObject.class));
        assertTrue(helper.storesLastChange(AutoVariableTest.LCTest.class));
    }

    @Test
    public void testStoresLastAccess() {
        assertFalse(helper.storesLastAccess(UncachedObject.class));
        assertTrue(helper.storesLastAccess(AutoVariableTest.LATest.class));
    }

    @Test
    public void testStoresCreation() {
        assertFalse(helper.storesCreation(UncachedObject.class));
        assertTrue(helper.storesCreation(AutoVariableTest.CTimeTest.class));
    }

    @Test
    public void testGetAllAnnotationsFromHierarchy() {
        assertEquals(4, helper.getAllAnnotationsFromHierachy(UncachedObject.class).size());
    }

    @Test
    public void testIgnoreFieldsAnnotation() {
        assertFalse(helper.getFields(TestClass.class).contains("var1"));
        assertTrue(helper.getFields(TestClass.class).contains("var2"));
    }

    @Test
    public void testLimitToFieldsAnnotationList() {
        assertFalse(helper.getFields(TestClass2.class).contains("var2"));
        assertFalse(helper.getFields(TestClass2.class).contains("var3"));
        assertTrue(helper.getFields(TestClass2.class).contains("var1"));
    }

    @Test
    public void testLimitToFieldsAnnotationType() {
        assertTrue(helper.getFields(TestClass3.class).contains("var1"));
        assertTrue(helper.getFields(TestClass3.class).contains("var2"));
        assertTrue(helper.getFields(TestClass3.class).contains("var3"));
        assertFalse(helper.getFields(TestClass3.class).contains("not_valid"));
        assertFalse(helper.getFields(TestClass3.class).contains("notValid"));

    }


    @Test
    public void testLimitToFieldsContains() {
        assertFalse(helper.getFields(TestClass4.class).contains("var1"));
        assertFalse(helper.getFields(TestClass4.class).contains("var2"));
        assertFalse(helper.getFields(TestClass4.class).contains("var3"));
        assertFalse(helper.getFields(TestClass4.class).contains("id"));
        //noinspection unchecked
        assertFalse(helper.getFields(TestClass4.class).contains("_id"));
        assertTrue(helper.getFields(TestClass4.class).contains("something"));

    }

    @Test
    public void testLimitToFieldsRegex() {
        assertFalse(helper.getFields(TestClass5.class).contains("var1"));
        assertFalse(helper.getFields(TestClass5.class).contains("var2"));
        assertTrue(helper.getFields(TestClass5.class).contains("var3"));
        assertFalse(helper.getFields(TestClass5.class).contains("id"));
        assertFalse(helper.getFields(TestClass5.class).contains("_id"));
        assertTrue(helper.getFields(TestClass5.class).contains("something"));

    }

    @Entity
    @IgnoreFields({"var1", "var3"})
    public class TestClass {
        @Id
        public MorphiumId id;
        public int var1;
        public int var2;
        public int var3;
    }

    @Entity
    @LimitToFields({"var1"})
    public class TestClass2 {
        @Id
        public MorphiumId id;
        public int var1;
        public int var2;
        public int var3;
    }

    @Entity
    @LimitToFields(type = TestClass2.class)
    public class TestClass3 extends TestClass2 {

        public String notValid;
    }

    @Entity
    @IgnoreFields({"~var", "id"})
    public class TestClass4 {
        @Id
        public MorphiumId id;
        public int var1;
        public int var2;
        public int var3;
        public int something;
    }

    @Entity
    @IgnoreFields({"/.*var[12].*/", "id"})
    public class TestClass5 {
        @Id
        public MorphiumId id;
        public int var1;
        public int var2;
        public int var3;
        public int something;
    }

    private UncachedObject newProxy() {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(UncachedObject.class);
        enhancer.setCallback((FixedValue) () -> "proxy");
        return (UncachedObject) enhancer.create();
    }
}
