package de.caluga.test.mongo.suite;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Objects;

/**
 * Created by stephan on 26.11.15.
 */
@SuppressWarnings("AssertWithSideEffects")
public class AnnotationAndReflectionHelperTest {
    private AnnotationAndReflectionHelper arHelper = new AnnotationAndReflectionHelper(true);

    @Test
    public void testIsAnnotationPresentInHierarchy() {
        assert (arHelper.isAnnotationPresentInHierarchy(UncachedObject.class, Entity.class));
        assert (arHelper.isAnnotationPresentInHierarchy(CachedObject.class, Cache.class));
    }

    @Test
    public void testGetRealClass() {
        assert (arHelper.getRealClass(UncachedObject.class).equals(UncachedObject.class));
    }

    @Test
    public void testIsBufferedWrite() {
        assert (arHelper.isBufferedWrite(CachedObject.class));
    }

    @Test
    public void testGetAnnotationFromHierarchy() {
        assert (arHelper.getAnnotationFromHierarchy(UncachedObject.class, Entity.class) != null);
        assert (arHelper.getAnnotationFromHierarchy(new UncachedObject() {
            private String justASubclass;
        }.getClass(), Entity.class) != null);

    }

    @Test
    public void testHasAdditionalData() {
        assert (arHelper.hasAdditionalData(AdditionalDataTest.AddDat.class));
    }

    @Test
    public void testGetFieldName() {
        String fld = arHelper.getFieldName(UncachedObject.class, "counter");
        assert (fld != null);
        assert (fld.equals("counter"));
        fld = arHelper.getFieldName(UncachedObject.class, "intData");
        assert (fld != null);
        assert (fld.equals("int_data"));

    }

    @Test
    public void testCreateCamelCase() {
        assert (arHelper.createCamelCase("hello_world", false).equals("helloWorld"));
        assert (arHelper.createCamelCase("hello_world", true).equals("HelloWorld"));
    }

    @Test
    public void testConvertCamelCase() {
        assert (arHelper.convertCamelCase("hello_world").equals("hello_world"));
    }

    @Test
    public void testGetAllFields() throws Exception {
        List<Field> fld = arHelper.getAllFields(UncachedObject.class);
        assert (fld.size() == 10) : "wrong " + fld.size();
        assert (fld.contains(UncachedObject.class.getDeclaredField("counter")));
    }

    @Test
    public void testGetField() {
        Field fl = arHelper.getField(UncachedObject.class, "counter");
        assert (fl != null);
        assert (fl.getName().equals("counter"));
    }

    @Test
    public void testIsEntity() {
        assert (arHelper.isEntity(UncachedObject.class));
        assert (arHelper.isEntity(EmbeddedObject.class));
    }

    @Test
    public void testGetValue() {
        UncachedObject uc = new UncachedObject();
        uc.setCounter(123);
        Object c = arHelper.getValue(uc, "counter");
        assert (c != null);
        assert (c.equals(123));
    }

    @Test
    public void testSetValue() {
        UncachedObject uc = new UncachedObject();
        arHelper.setValue(uc, 123, "counter");
        assert (uc.getCounter() == 123);
    }

    @Test
    public void testGetId() {
        UncachedObject uc = new UncachedObject();
        uc.setMorphiumId(new MorphiumId());
        assert (arHelper.getId(uc).equals(uc.getMorphiumId()));
    }

    @Test
    public void testGetIdFieldName() {
        String idFld = arHelper.getIdFieldName(new UncachedObject());
        assert (idFld.equals("morphium_id")) : "wrong " + idFld;
    }

    @Test
    public void testGetIdField() throws Exception {
        assert (arHelper.getIdField(new UncachedObject()).equals(UncachedObject.class.getDeclaredField("morphiumId")));
    }

    @Test
    public void testGetFields() {
        assert (arHelper.getFields(UncachedObject.class, Id.class).size() == 1);
        assert (arHelper.getFields(UncachedObject.class, Index.class).size() == 2);
    }

    @Test
    public void testGetRealObject() {
        Morphium m = new Morphium();
        Object pr = m.createPartiallyUpdateableEntity(new UncachedObject());
        assert (arHelper.getRealObject(pr).getClass().equals(UncachedObject.class));
    }

    @Test
    public void testGetTypeOfField() {
        assert (Objects.equals(arHelper.getTypeOfField(UncachedObject.class, "value"), String.class));
    }

    @Test
    public void testStoresLastChange() {
        assert (!arHelper.storesLastChange(UncachedObject.class));
        assert (arHelper.storesLastChange(AutoVariableTest.LCTest.class));
    }

    @Test
    public void testStoresLastAccess() {
        assert (!arHelper.storesLastAccess(UncachedObject.class));
        assert (arHelper.storesLastAccess(AutoVariableTest.LATest.class));
    }

    @Test
    public void testStoresCreation() {
        assert (!arHelper.storesCreation(UncachedObject.class));
        assert (arHelper.storesCreation(AutoVariableTest.CTimeTest.class));
    }

    @Test
    public void testGetAllAnnotationsFromHierachy() {
        assert (arHelper.getAllAnnotationsFromHierachy(UncachedObject.class).size() == 4) : "wrong: " + arHelper.getAllAnnotationsFromHierachy(UncachedObject.class).size();
    }

    @Test
    public void testIgnorFieldsAnnotation() {
        assert (!arHelper.getFields(TestClass.class).contains("var1"));
        assert (arHelper.getFields(TestClass.class).contains("var2"));
    }

    @Test
    public void testLimitToFieldsAnnotationList() {
        assert (!arHelper.getFields(TestClass2.class).contains("var2") && !arHelper.getFields(TestClass2.class).contains("var3"));
        assert (arHelper.getFields(TestClass2.class).contains("var1"));
    }

    @Test
    public void testLimitToFieldsAnnotationType() {
        assert (arHelper.getFields(TestClass3.class).contains("var1"));
        assert (arHelper.getFields(TestClass3.class).contains("var2"));
        assert (arHelper.getFields(TestClass3.class).contains("var3"));
        assert (!arHelper.getFields(TestClass3.class).contains("not_valid"));
        assert (!arHelper.getFields(TestClass3.class).contains("notValid"));

    }


    @Test
    public void testLimitToFieldsContains() {
        assert (!arHelper.getFields(TestClass4.class).contains("var1"));
        assert (!arHelper.getFields(TestClass4.class).contains("var2"));
        assert (!arHelper.getFields(TestClass4.class).contains("var3"));
        assert (!arHelper.getFields(TestClass4.class).contains("id"));
        assert (!arHelper.getFields(TestClass4.class).contains("_id"));
        assert (arHelper.getFields(TestClass4.class).contains("something"));

    }

    @Test
    public void testLimitToFieldsRegex() {
        assert (!arHelper.getFields(TestClass5.class).contains("var1"));
        assert (!arHelper.getFields(TestClass5.class).contains("var2"));
        assert (arHelper.getFields(TestClass5.class).contains("var3"));
        assert (!arHelper.getFields(TestClass5.class).contains("id"));
        assert (!arHelper.getFields(TestClass5.class).contains("_id"));
        assert (arHelper.getFields(TestClass5.class).contains("something"));

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
}