package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.*;

@Tag("core")
public class ObjectMapperAnnotationHelperTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testCreateCamelCase(Morphium morphium) {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(true);
        assertEquals("thisIsATest", om.createCamelCase("this_is_a_test", false));
        assertEquals("ATestThisIs", om.createCamelCase("a_test_this_is", true));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testConvertCamelCase(Morphium morphium) {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(true);
        assert (om.convertCamelCase("thisIsATest").equals("this_is_a_test")) : "Conversion failed!";
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testDisableConvertCamelCase(Morphium morphium) {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(false);
        String fn = om.getMongoFieldName(UncachedObject.class, "intData");

        assertEquals("intData", fn);

        om = new AnnotationAndReflectionHelper(true);
        fn = om.getMongoFieldName(UncachedObject.class, "intData");

        assertEquals("int_data", fn);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testGetCollectionName(Morphium morphium) {
        MorphiumObjectMapper om = morphium.getMapper();
        assert (om.getCollectionName(CachedObject.class).equals("cached_object")) : "Cached object test failed";
        assert (om.getCollectionName(UncachedObject.class).equals("uncached_object")) : "Uncached object test failed";
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void massiveParallelGetCollectionNameTest(Morphium morphium) {

        for (int i = 0; i < 100; i++) {
            new Thread(() -> {
                MorphiumObjectMapper om = morphium.getMapper();
                assertEquals("cached_object", om.getCollectionName(CachedObject.class));
                Thread.yield();
                assertEquals("uncached_object", om.getCollectionName(UncachedObject.class));
                Thread.yield();
                assertEquals("ComplexObject", om.getCollectionName(ComplexObject.class));
            }).start();
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testGetId(Morphium morphium) {
        MorphiumObjectMapper om = morphium.getMapper();
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setStrValue("This \" is $ test");
        o.setMorphiumId(new MorphiumId());
        Object id = an.getId(o);
        assertEquals(id, o.getMorphiumId());
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testIsEntity(Morphium morphium) {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(true);
        assertTrue(om.isEntity(UncachedObject.class));
        assertTrue(om.isEntity(new UncachedObject()));
        assertTrue(!om.isEntity(""));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testGetValue(Morphium morphium) {
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setStrValue("This \" is $ test");
        assertEquals(12345, an.getValue(o, "counter"));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSetValue(Morphium morphium) {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(true);
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        om.setValue(o, "A test", "strValue");
        assertEquals("A test", o.getStrValue());
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void convertCamelCaseTest(Morphium morphium) {
        String n = morphium.getARHelper().convertCamelCase("thisIsATestTT");
        assertEquals("this_is_a_test_t_t", n);

    }
}
