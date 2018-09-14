package de.caluga.test.mongo.suite;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.FixedValue;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Created by stephan on 26.11.15.
 */
@SuppressWarnings("AssertWithSideEffects")
public class AnnotationAndReflectionHelperTest {

    private AnnotationAndReflectionHelper helper;

    @Before
    public void setup() {
        helper = new AnnotationAndReflectionHelper(true);
    }

    @Test
    public void testIsAnnotationPresentInHierarchy() {
        assertThat(helper.isAnnotationPresentInHierarchy(UncachedObject.class, Entity.class)).isTrue();
        assertThat(helper.isAnnotationPresentInHierarchy(CachedObject.class, Cache.class)).isTrue();
    }

    @Test
    public void returnsTheRealClassFromCache() {
        // Given
        Map<Class<?>, Class<?>> realClassCache = new HashMap<>();
        helper = new AnnotationAndReflectionHelper(true, realClassCache);
        helper.getRealClass(newProxy().getClass());
        // When
        helper.getRealClass(newProxy().getClass());
        // Then
        assertThat(realClassCache).hasSize(1);
        assertThat(realClassCache.containsKey(newProxy().getClass())).isTrue();
    }

    @Test
    public void returnsTheRealClassOfSuperClass() {
        assertThat(helper.getRealClass(UncachedObject.class)).isEqualTo(UncachedObject.class);
    }

    @Test
    public void throwsNullPointerExceptionWhenSuperClassIsNull() {
        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> helper.getRealClass(null));
    }

    @Test
    public void testIsBufferedWrite() {
        assertThat(helper.isBufferedWrite(CachedObject.class)).isTrue();
    }

    @Test
    public void testGetAnnotationFromHierarchy() {
        assertThat(helper.getAnnotationFromHierarchy(UncachedObject.class, Entity.class)).isNotNull();
        assertThat(helper.getAnnotationFromHierarchy(new UncachedObject() {
            private String justASubclass;
        }.getClass(), Entity.class)).isNotNull();

    }

    @Test
    public void testHasAdditionalData() {
        assertThat(helper.hasAdditionalData(AdditionalDataTest.AddDat.class)).isTrue();
    }

    @Test
    public void testGetFieldName() {
        String fieldName = helper.getFieldName(UncachedObject.class, "counter");
        assertThat((fieldName)).isNotNull();
        assertThat(fieldName).isEqualTo("counter");
        fieldName = helper.getFieldName(UncachedObject.class, "intData");
        assertThat((fieldName)).isNotNull();
        assertThat(fieldName).isEqualTo("int_data");

    }

    @Test
    public void testCreateCamelCase() {
        assertThat(helper.createCamelCase("hello_world", false)).isEqualTo("helloWorld");
        assertThat(helper.createCamelCase("hello_world", true)).isEqualTo("HelloWorld");
    }

    @Test
    public void testConvertCamelCase() {
        assertThat(helper.convertCamelCase("hello_world")).isEqualTo("hello_world");
    }

    @Test
    public void testGetAllFields() throws Exception {
        List<Field> fields = helper.getAllFields(UncachedObject.class);
        assertThat(fields).hasSize(10);
        assertThat(fields.contains(UncachedObject.class.getDeclaredField("counter"))).isTrue();
    }

    @Test
    public void testGetField() {
        Field field = helper.getField(UncachedObject.class, "counter");
        assertThat((field)).isNotNull();
        assertThat(field.getName()).isEqualTo("counter");
    }

    @Test
    public void testIsEntity() {
        assertThat(helper.isEntity(UncachedObject.class)).isTrue();
        assertThat(helper.isEntity(EmbeddedObject.class)).isTrue();
    }

    @Test
    public void testGetValue() {
        UncachedObject uncachedObject = new UncachedObject();
        uncachedObject.setCounter(123);
        Object value = helper.getValue(uncachedObject, "counter");
        assertThat((value)).isNotNull();
        assertThat(value).isEqualTo(123);
    }

    @Test
    public void testSetValue() {
        UncachedObject uncachedObject = new UncachedObject();
        helper.setValue(uncachedObject, 123, "counter");
        assertThat(uncachedObject.getCounter()).isEqualTo(123);
    }

    @Test
    public void testGetId() {
        UncachedObject uncachedObject = new UncachedObject();
        uncachedObject.setMorphiumId(new MorphiumId());
        assertThat(helper.getId(uncachedObject)).isEqualTo(uncachedObject.getMorphiumId());
    }

    @Test
    public void testGetIdFieldName() {
        String fieldName = helper.getIdFieldName(new UncachedObject());
        assertThat(fieldName).isEqualTo("morphium_id");
    }

    @Test
    public void testGetIdField() throws Exception {
        assertThat(helper.getIdField(new UncachedObject())).isEqualTo(UncachedObject.class.getDeclaredField("morphiumId"));
    }

    @Test
    public void testGetFields() {
        assertThat(helper.getFields(UncachedObject.class, Id.class)).hasSize(1);
        assertThat(helper.getFields(UncachedObject.class, Index.class)).hasSize(2);
    }

    @Test
    public void testGetTypeOfField() {
        assertThat(Objects.equals(helper.getTypeOfField(UncachedObject.class, "value"), String.class)).isTrue();
    }

    @Test
    public void testStoresLastChange() {
        assertThat(helper.storesLastChange(UncachedObject.class)).isFalse();
        assertThat(helper.storesLastChange(AutoVariableTest.LCTest.class)).isTrue();
    }

    @Test
    public void testStoresLastAccess() {
        assertThat(helper.storesLastAccess(UncachedObject.class)).isFalse();
        assertThat(helper.storesLastAccess(AutoVariableTest.LATest.class)).isTrue();
    }

    @Test
    public void testStoresCreation() {
        assertThat(helper.storesCreation(UncachedObject.class)).isFalse();
        assertThat(helper.storesCreation(AutoVariableTest.CTimeTest.class)).isTrue();
    }

    @Test
    public void testGetAllAnnotationsFromHierarchy() {
        assertThat(helper.getAllAnnotationsFromHierachy(UncachedObject.class)).hasSize(4);
    }

    @Test
    public void testIgnoreFieldsAnnotation() {
        assertThat(helper.getFields(TestClass.class).contains("var1")).isFalse();
        assertThat(helper.getFields(TestClass.class).contains("var2")).isTrue();
    }

    @Test
    public void testLimitToFieldsAnnotationList() {
        assertThat(helper.getFields(TestClass2.class).contains("var2")).isFalse();
        assertThat(helper.getFields(TestClass2.class).contains("var3")).isFalse();
        assertThat(helper.getFields(TestClass2.class).contains("var1")).isTrue();
    }

    @Test
    public void testLimitToFieldsAnnotationType() {
        assertThat(helper.getFields(TestClass3.class).contains("var1")).isTrue();
        assertThat(helper.getFields(TestClass3.class).contains("var2")).isTrue();
        assertThat(helper.getFields(TestClass3.class).contains("var3")).isTrue();
        assertThat(helper.getFields(TestClass3.class).contains("not_valid")).isFalse();
        assertThat(helper.getFields(TestClass3.class).contains("notValid")).isFalse();

    }


    @Test
    public void testLimitToFieldsContains() {
        assertThat(helper.getFields(TestClass4.class).contains("var1")).isFalse();
        assertThat(helper.getFields(TestClass4.class).contains("var2")).isFalse();
        assertThat(helper.getFields(TestClass4.class).contains("var3")).isFalse();
        assertThat(helper.getFields(TestClass4.class).contains("id")).isFalse();
        assertThat(helper.getFields(TestClass4.class).contains("_id")).isFalse();
        assertThat(helper.getFields(TestClass4.class).contains("something")).isTrue();

    }

    @Test
    public void testLimitToFieldsRegex() {
        assertThat(helper.getFields(TestClass5.class).contains("var1")).isFalse();
        assertThat(helper.getFields(TestClass5.class).contains("var2")).isFalse();
        assertThat(helper.getFields(TestClass5.class).contains("var3")).isTrue();
        assertThat(helper.getFields(TestClass5.class).contains("id")).isFalse();
        assertThat(helper.getFields(TestClass5.class).contains("_id")).isFalse();
        assertThat(helper.getFields(TestClass5.class).contains("something")).isTrue();

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