package de.caluga.test.mongo.suite;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.driver.bson.MorphiumId;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Created by stephan on 26.11.15.
 */
public class AnnotationAndReflectionHelperTest {
    private AnnotationAndReflectionHelper arHelper = new AnnotationAndReflectionHelper(true);

    @Test
    public void testIsAnnotationPresentInHierarchy() throws Exception {
        assert (arHelper.isAnnotationPresentInHierarchy(UncachedObject.class, Entity.class));
        assert (arHelper.isAnnotationPresentInHierarchy(CachedObject.class, Cache.class));
    }

    @Test
    public void testGetRealClass() throws Exception {
        assert (arHelper.getRealClass(UncachedObject.class).equals(UncachedObject.class));
    }

    @Test
    public void testIsBufferedWrite() throws Exception {
        assert (arHelper.isBufferedWrite(CachedObject.class));
    }

    @Test
    public void testGetAnnotationFromHierarchy() throws Exception {
        assert (arHelper.getAnnotationFromHierarchy(UncachedObject.class, Entity.class) != null);
        assert (arHelper.getAnnotationFromHierarchy(new UncachedObject() {
            private String justASubclass;
        }.getClass(), Entity.class) != null);

    }

    @Test
    public void testHasAdditionalData() throws Exception {
        assert (arHelper.hasAdditionalData(AdditionalDataTest.AddDat.class));
    }

    @Test
    public void testGetFieldName() throws Exception {
        String fld = arHelper.getFieldName(UncachedObject.class, "counter");
        assert (fld != null);
        assert (fld.equals("counter"));
        fld = arHelper.getFieldName(UncachedObject.class, "intData");
        assert (fld != null);
        assert (fld.equals("int_data"));

    }

    @Test
    public void testCreateCamelCase() throws Exception {
        assert (arHelper.createCamelCase("hello_world", false).equals("helloWorld"));
        assert (arHelper.createCamelCase("hello_world", true).equals("HelloWorld"));
    }

    @Test
    public void testConvertCamelCase() throws Exception {
        assert (arHelper.convertCamelCase("hello_world").equals("hello_world"));
    }

    @Test
    public void testGetAllFields() throws Exception {
        List<Field> fld = arHelper.getAllFields(UncachedObject.class);
        assert (fld.size() == 10) : "wrong " + fld.size();
        assert (fld.contains(UncachedObject.class.getDeclaredField("counter")));
    }

    @Test
    public void testGetField() throws Exception {
        Field fl = arHelper.getField(UncachedObject.class, "counter");
        assert (fl != null);
        assert (fl.getName().equals("counter"));
    }

    @Test
    public void testIsEntity() throws Exception {
        assert (arHelper.isEntity(UncachedObject.class));
        assert (arHelper.isEntity(EmbeddedObject.class));
    }

    @Test
    public void testGetValue() throws Exception {
        UncachedObject uc = new UncachedObject();
        uc.setCounter(123);
        Object c = arHelper.getValue(uc, "counter");
        assert (c != null);
        assert (c.equals(123));
    }

    @Test
    public void testSetValue() throws Exception {
        UncachedObject uc = new UncachedObject();
        arHelper.setValue(uc, 123, "counter");
        assert (uc.getCounter() == 123);
    }

    @Test
    public void testGetId() throws Exception {
        UncachedObject uc = new UncachedObject();
        uc.setMorphiumId(new MorphiumId());
        assert (arHelper.getId(uc).equals(uc.getMorphiumId()));
    }

    @Test
    public void testGetIdFieldName() throws Exception {
        String idFld = arHelper.getIdFieldName(new UncachedObject());
        assert (idFld.equals("morphium_id")) : "wrong " + idFld;
    }

    @Test
    public void testGetIdField() throws Exception {
        assert (arHelper.getIdField(new UncachedObject()).equals(UncachedObject.class.getDeclaredField("morphiumId")));
    }

    @Test
    public void testGetFields() throws Exception {

    }

    @Test
    public void testGetRealObject() throws Exception {

    }

    @Test
    public void testGetTypeOfField() throws Exception {

    }

    @Test
    public void testStoresLastChange() throws Exception {

    }

    @Test
    public void testStoresLastAccess() throws Exception {

    }

    @Test
    public void testStoresCreation() throws Exception {

    }

    @Test
    public void testGetLongValue() throws Exception {

    }

    @Test
    public void testGetStringValue() throws Exception {

    }

    @Test
    public void testGetDateValue() throws Exception {

    }

    @Test
    public void testGetDoubleValue() throws Exception {

    }

    @Test
    public void testGetAllAnnotationsFromHierachy() throws Exception {

    }

    @Test
    public void testGetLastChangeField() throws Exception {

    }

    @Test
    public void testGetLastAccessField() throws Exception {

    }

    @Test
    public void testGetCreationTimeField() throws Exception {

    }

    @Test
    public void testCallLifecycleMethod() throws Exception {

    }

    @Test
    public void testIsAsyncWrite() throws Exception {

    }
}