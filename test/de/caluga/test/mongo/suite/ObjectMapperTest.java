package de.caluga.test.mongo.suite;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.ObjectMapper;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.Utils;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.bson.MorphiumId;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.MapListObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 14:04
 * <p/>
 */
public class ObjectMapperTest extends MongoTest {

    @Test
    public void customTypeMapperTest() throws Exception {
        ObjectMapper om = morphium.getMapper();
        BigInteger tst = new BigInteger("affedeadbeefaffedeadbeef42", 16);
        Map<String, Object> d = om.marshall(tst);

        BigInteger bi = om.unmarshall(BigInteger.class, d);
        assert (bi != null);
        assert (tst.equals(bi));

        BIObject bio = new BIObject();
        bio.biValue = tst;
        morphium.store(bio);

        BIObject bio2 = morphium.createQueryFor(BIObject.class).get();
        assert (bio2 != null);
        assert (bio2.biValue != null);
        assert (bio2.biValue.equals(tst));
    }


    @Test
    public void simpleParseFromStringTest() throws Exception {
        String json = "{ \"value\":\"test\",\"counter\":123}";
        ObjectMapper om = morphium.getMapper();
        UncachedObject uc = om.unmarshall(UncachedObject.class, json);
        assert (uc.getCounter() == 123);
    }

    @Test
    public void objectToStringParseTest() throws Exception {
        ObjectMapper om = morphium.getMapper();
        UncachedObject o = new UncachedObject();
        o.setValue("A test");
        o.setLongData(new long[]{1, 23, 4L, 5L});
        o.setCounter(1234);
        Map<String, Object> dbo = om.marshall(o);
        UncachedObject uc = om.unmarshall(UncachedObject.class, dbo);
        assert (uc.getCounter() == 1234);
        assert (uc.getLongData()[0] == 1);
    }


    @Test
    public void listContainerStringParseTest() throws Exception {
        ObjectMapper om = morphium.getMapper();
        ListContainer o = new ListContainer();
        o.addLong(1234);
        o.addString("string1");
        o.addString("string2");
        o.addString("string3");
        o.addString("string4");
        Map<String, Object> dbo = om.marshall(o);
        ListContainer uc = om.unmarshall(ListContainer.class, dbo);
        assert (uc.getStringList().size() == 4);
        assert (uc.getStringList().get(0).equals("string1"));
        assert (uc.getLongList().size() == 1);
    }

    @Test
    public void testCreateCamelCase() throws Exception {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(true);
        assert (om.createCamelCase("this_is_a_test", false).equals("thisIsATest")) : "Error camil case translation not working";
        assert (om.createCamelCase("a_test_this_is", true).equals("ATestThisIs")) : "Error - capitalized String wrong";


    }

    @Test
    public void testConvertCamelCase() throws Exception {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(true);
        assert (om.convertCamelCase("thisIsATest").equals("this_is_a_test")) : "Conversion failed!";
    }

    @Test
    public void testDisableConvertCamelCase() throws Exception {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(false);
        String fn = om.getFieldName(UncachedObject.class, "intData");

        assert (fn.equals("intData")) : "Conversion failed! " + fn;

        om = new AnnotationAndReflectionHelper(true);
        fn = om.getFieldName(UncachedObject.class, "intData");

        assert (fn.equals("int_data")) : "Conversion failed! " + fn;
    }

    @Test
    public void testGetCollectionName() throws Exception {
        ObjectMapperImpl om = (ObjectMapperImpl) morphium.getMapper();
        assert (om.getCollectionName(CachedObject.class).equals("cached_object")) : "Cached object test failed";
        assert (om.getCollectionName(UncachedObject.class).equals("uncached_object")) : "Uncached object test failed";

    }


    @Test
    public void massiveParallelGetCollectionNameTest() throws Exception {

        for (int i = 0; i < 100; i++) {
            new Thread() {
                @Override
                public void run() {
                    ObjectMapperImpl om = (ObjectMapperImpl) morphium.getMapper();
                    assert (om.getCollectionName(CachedObject.class).equals("cached_object")) : "Cached object test failed";
                    yield();
                    assert (om.getCollectionName(UncachedObject.class).equals("uncached_object")) : "Uncached object test failed";
                    yield();
                    assert (om.getCollectionName(ComplexObject.class).equals("ComplexObject")) : "complex object test failed";
                }
            }.start();
        }
    }

    @Test
    public void testMarshall() throws Exception {
        ObjectMapperImpl om = (ObjectMapperImpl) morphium.getMapper();
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setValue("This \" is $ test");
        Map<String, Object> dbo = om.marshall(o);

        String s = Utils.toJsonString(dbo);
        System.out.println("Marshalling was: " + s);
        assert (s.equals("{ \"dval\" : 0.0, \"counter\" : 12345, \"value\" : \"This \" is $ test\" } ")) : "String creation failed?" + s;
    }

    @Test
    public void testUnmarshall() throws Exception {
        ObjectMapperImpl om = (ObjectMapperImpl) morphium.getMapper();
        Map<String, Object> dbo = new HashMap<>();
        dbo.put("counter", 12345);
        dbo.put("value", "A test");
        om.unmarshall(UncachedObject.class, dbo);
    }

    @Test
    public void testGetId() throws Exception {
        ObjectMapperImpl om = (ObjectMapperImpl) morphium.getMapper();
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setValue("This \" is $ test");
        o.setMorphiumId(new MorphiumId());
        Object id = an.getId(o);
        assert (id.equals(o.getMorphiumId())) : "IDs not equal!";
    }


    @Test
    public void testIsEntity() throws Exception {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(true);
        assert (om.isEntity(UncachedObject.class)) : "Uncached Object no Entity?=!?=!?";
        assert (om.isEntity(new UncachedObject())) : "Uncached Object no Entity?=!?=!?";
        assert (!om.isEntity("")) : "String is an Entity?";
    }

    @Test
    public void testGetValue() throws Exception {
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setValue("This \" is $ test");
        assert (an.getValue(o, "counter").equals(12345)) : "Value not ok!";

    }

    @Test
    public void testSetValue() throws Exception {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(true);
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        om.setValue(o, "A test", "value");
        assert ("A test".equals(o.getValue())) : "Value not set";

    }


    @Test
    public void complexObjectTest() {
        ObjectMapperImpl om = (ObjectMapperImpl) morphium.getMapper();
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setValue("Embedded value");
        morphium.store(o);

        EmbeddedObject eo = new EmbeddedObject();
        eo.setName("Embedded only");
        eo.setValue("Value");
        eo.setTest(System.currentTimeMillis());

        ComplexObject co = new ComplexObject();
        co.setEinText("Ein text");
        co.setEmbed(eo);

        co.setEntityEmbeded(o);
        MorphiumId embedId = o.getMorphiumId();

        o = new UncachedObject();
        o.setCounter(12345);
        o.setValue("Referenced value");
        //        o.setMongoId(new MongoId(new Date()));
        morphium.store(o);

        co.setRef(o);
        co.setId(new MorphiumId());
        String st = Utils.toJsonString(co);
        System.out.println("Referenced object: " + Utils.toJsonString(om.marshall(o)));
        Map<String, Object> marshall = om.marshall(co);
        System.out.println("Complex object: " + Utils.toJsonString(marshall));


        //Unmarshalling stuff
        co = om.unmarshall(ComplexObject.class, marshall);
        assert (co.getEntityEmbeded().getMorphiumId() == null) : "Embeded entity got a mongoID?!?!?!";
        co.getEntityEmbeded().setMorphiumId(embedId);  //need to set ID manually, as it won't be stored!
        String st2 = Utils.toJsonString(co);
        assert (st.equals(st2)) : "Strings not equal?\n" + st + "\n" + st2;
        assert (co.getEmbed() != null) : "Embedded value not found!";

    }

    @Test
    public void nullValueTests() {
        ObjectMapperImpl om = (ObjectMapperImpl) morphium.getMapper();

        ComplexObject o = new ComplexObject();
        o.setTrans("TRANSIENT");
        Map<String, Object> obj = null;
        try {
            obj = om.marshall(o);
        } catch (IllegalArgumentException e) {
        }
        o.setEinText("Ein Text");
        obj = om.marshall(o);
        assert (!obj.containsKey("trans")) : "Transient field used?!?!?";
    }

    @Test
    public void listValueTest() {
        MapListObject o = new MapListObject();
        List lst = new ArrayList();
        lst.add("A Value");
        lst.add(27.0);
        lst.add(new UncachedObject());

        o.setListValue(lst);
        o.setName("Simple List");

        ObjectMapperImpl om = (ObjectMapperImpl) morphium.getMapper();
        Map<String, Object> marshall = om.marshall(o);
        String m = marshall.toString();

        assert (m.equals("{list_value=[A Value, 27.0, {dval=0.0, counter=0, class_name=de.caluga.test.mongo.suite.data.UncachedObject}], name=Simple List}")) : "Marshall not ok: " + m;

        MapListObject mo = om.unmarshall(MapListObject.class, marshall);
        System.out.println("Mo: " + mo.getName());
        System.out.println("lst: " + mo.getListValue());
        assert (mo.getName().equals(o.getName())) : "Names not equal?!?!?";
        for (int i = 0; i < lst.size(); i++) {
            Object listValueNew = mo.getListValue().get(i);
            Object listValueOrig = o.getListValue().get(i);
            assert (listValueNew.getClass().equals(listValueOrig.getClass())) : "Classes differ: " + listValueNew.getClass() + " - " + listValueOrig.getClass();
            assert (listValueNew.equals(listValueOrig)) : "Value not equals in list: " + listValueNew + " vs. " + listValueOrig;
        }
        System.out.println("test Passed!");

    }


    @Test
    public void mapValueTest() {
        MapListObject o = new MapListObject();
        Map<String, Object> map = new HashMap<>();
        map.put("a_string", "This is a string");
        map.put("a primitive value", 42);
        map.put("double", 42.0);
        map.put("null", null);
        map.put("Entity", new UncachedObject());
        o.setMapValue(map);
        o.setName("A map-value");

        ObjectMapperImpl om = (ObjectMapperImpl) morphium.getMapper();
        Map<String, Object> marshall = om.marshall(o);
        String m = Utils.toJsonString(marshall);
        System.out.println("Marshalled object: " + m);
        assert (m.equals("{ \"map_value\" : { \"Entity\" : { \"dval\" : 0.0, \"counter\" : 0, \"class_name\" : \"de.caluga.test.mongo.suite.data.UncachedObject\" } , \"a primitive value\" : 42, \"null\" :  null, \"double\" : 42.0, \"a_string\" : \"This is a string\" } , \"name\" : \"A map-value\" } ")) : "Value not marshalled coorectly";

        MapListObject mo = om.unmarshall(MapListObject.class, marshall);
        assert (mo.getName().equals("A map-value")) : "Name error";
        assert (mo.getMapValue() != null) : "map value is null????";
        for (String k : mo.getMapValue().keySet()) {
            Object v = mo.getMapValue().get(k);
            if (v == null) {
                assert (o.getMapValue().get(k) == null) : "v==null but original not?";
            } else {
                assert (o.getMapValue().get(k).getClass().equals(v.getClass())) : "Classes differ: " + o.getMapValue().get(k).getClass().getName() + " != " + v.getClass().getName();
                assert (o.getMapValue().get(k).equals(v)) : "Value not equal, key: " + k;
            }
        }

    }

    @Test
    public void objectMapperSpeedTest() {
        UncachedObject o = new UncachedObject();
        o.setCounter(42);
        o.setValue("The meaning of life");
        o.setMorphiumId(new MorphiumId());
        Map<String, Object> marshall = null;
        ObjectMapperImpl om = (ObjectMapperImpl) morphium.getMapper();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 25000; i++) {
            marshall = om.marshall(o);
        }
        long dur = System.currentTimeMillis() - start;

        log.info("Mapping of UncachedObject 25000 times took " + dur + "ms");
        assert (dur < 5000);
        start = System.currentTimeMillis();
        for (int i = 0; i < 25000; i++) {
            UncachedObject uc = om.unmarshall(UncachedObject.class, marshall);
        }
        dur = System.currentTimeMillis() - start;
        log.info("De-Marshalling of UncachedObject 25000 times took " + dur + "ms");
        assert (dur < 5000);

    }

    @Test
    public void objectMapperSpeedTestCachedObject() {
        CachedObject o = new CachedObject();
        o.setCounter(42);
        o.setValue("The meaning of life");
        o.setId(new MorphiumId());
        Map<String, Object> marshall = null;
        ObjectMapperImpl om = (ObjectMapperImpl) morphium.getMapper();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 25000; i++) {
            marshall = om.marshall(o);
        }
        long dur = System.currentTimeMillis() - start;

        log.info("Mapping of CachedObject 25000 times took " + dur + "ms");
        assert (dur < 1000);
        start = System.currentTimeMillis();
        for (int i = 0; i < 25000; i++) {
            CachedObject c = om.unmarshall(CachedObject.class, marshall);
        }
        dur = System.currentTimeMillis() - start;
        log.info("De-Marshalling of CachedObject 25000 times took " + dur + "ms");
        assert (dur < 1000);

    }


    @Test
    public void objectMapperSpeedTestComplexObjectNoRef() {
        ComplexObject o = new ComplexObject();
        EmbeddedObject em = new EmbeddedObject();
        em.setName("Embedded1");
        em.setValue("test");
        em.setTest(424242);
        o.setId(new MorphiumId());
        o.setEmbed(em);
        o.setChanged(System.currentTimeMillis());
        o.setCreated(System.currentTimeMillis());
        //        o.setcRef();
        o.setEinText("Text");
        o.setEntityEmbeded(new UncachedObject());
        o.setNullValue(23);
        //        o.setRef();
        o.setTrans("Trans");
        Map<String, Object> marshall = null;
        ObjectMapperImpl om = (ObjectMapperImpl) morphium.getMapper();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 25000; i++) {
            marshall = om.marshall(o);
        }
        long dur = System.currentTimeMillis() - start;

        log.info("Mapping of ComplexObject 25000 times took " + dur + "ms");
        assert (dur < 1000);
        start = System.currentTimeMillis();
        for (int i = 0; i < 25000; i++) {
            ComplexObject co = om.unmarshall(ComplexObject.class, marshall);
        }
        dur = System.currentTimeMillis() - start;
        log.info("De-Marshalling of ComplexObject 25000 times took " + dur + "ms");
        assert (dur < 1500);

    }


    @Test
    public void objectMapperSpeedTestComplexObjectNoCachedRef() {
        ComplexObject o = new ComplexObject();
        EmbeddedObject em = new EmbeddedObject();
        em.setName("Embedded1");
        em.setValue("test");
        em.setTest(424242);
        o.setId(new MorphiumId());
        o.setEmbed(em);
        o.setChanged(System.currentTimeMillis());
        o.setCreated(System.currentTimeMillis());
        //        o.setcRef();
        o.setEinText("Text");
        //        o.setEntityEmbeded(new UncachedObject());
        o.setNullValue(23);
        o.setTrans("Trans");
        UncachedObject uc = new UncachedObject();
        uc.setCounter(42);
        uc.setValue("test");
        morphium.store(uc);
        o.setRef(uc);
        Map<String, Object> marshall = null;
        ObjectMapperImpl om = (ObjectMapperImpl) morphium.getMapper();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 25000; i++) {
            marshall = om.marshall(o);
        }
        long dur = System.currentTimeMillis() - start;
        if (dur > 2000) {
            log.warn("Mapping of ComplexObject 25000 with uncached references times took " + dur + "ms");
        } else {
            log.info("Mapping of ComplexObject 25000 with uncached references times took " + dur + "ms");

        }
        assert (dur < 5000);
        start = System.currentTimeMillis();
        for (int i = 0; i < 25000; i++) {
            ComplexObject co = om.unmarshall(ComplexObject.class, marshall);
        }
        dur = System.currentTimeMillis() - start;
        log.info("De-Marshalling of ComplexObject with uncached references 25000 times took " + dur + "ms");
        assert (dur < 10000);

    }


    @Test
    public void objectMapperSpeedTestComplexObjectCachedRef() {
        ComplexObject o = new ComplexObject();
        EmbeddedObject em = new EmbeddedObject();
        em.setName("Embedded1");
        em.setValue("test");
        em.setTest(424242);
        o.setId(new MorphiumId());
        o.setEmbed(em);
        o.setChanged(System.currentTimeMillis());
        o.setCreated(System.currentTimeMillis());
        //        o.setcRef();
        o.setEinText("Text");
        //        o.setEntityEmbeded(new UncachedObject());
        o.setNullValue(23);
        o.setTrans("Trans");
        CachedObject cc = new CachedObject();
        cc.setCounter(42);
        cc.setValue("test");
        morphium.store(cc);
        waitForWrites();
        o.setcRef(cc);
        Map<String, Object> marshall = null;
        ObjectMapperImpl om = (ObjectMapperImpl) morphium.getMapper();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 25000; i++) {
            marshall = om.marshall(o);
        }
        long dur = System.currentTimeMillis() - start;
        if (dur > 2000) {
            log.warn("Mapping of ComplexObject 25000 with uncached references times took " + dur + "ms");
        } else {
            log.info("Mapping of ComplexObject 25000 with uncached references times took " + dur + "ms");
        }
        assert (dur < 5000);
        start = System.currentTimeMillis();
        for (int i = 0; i < 25000; i++) {
            ComplexObject co = om.unmarshall(ComplexObject.class, marshall);
        }
        dur = System.currentTimeMillis() - start;
        log.info("De-Marshalling of ComplexObject with cached references 25000 times took " + dur + "ms");
        assert (dur < 7500); //Mongo 2.6 is slower :(

    }


    @Test
    public void noDefaultConstructorTest() throws Exception {
        NoDefaultConstructorUncachedObject o = new NoDefaultConstructorUncachedObject("test", 15);
        String serialized = Utils.toJsonString(morphium.getMapper().marshall(o));
        log.info("Serialized... " + serialized);

        o = morphium.getMapper().unmarshall(NoDefaultConstructorUncachedObject.class, serialized);
        assert (o != null);
        assert (o.getCounter() == 15);
        assert (o.getValue().equals("test"));
    }

    @Test
    public void mapTest() throws Exception {
        ObjectMapper m = morphium.getMapper();


        MappedObject o = new MappedObject();
        o.aMap = new HashMap<>();
        o.aMap.put("test", "test");
        o.uc = new NoDefaultConstructorUncachedObject("v", 123);

        Map<String, Object> dbo = m.marshall(o);
        o = m.unmarshall(MappedObject.class, Utils.toJsonString(dbo));

        assert (o.aMap.get("test") != null);
    }

    public static class NoDefaultConstructorUncachedObject extends UncachedObject {
        public NoDefaultConstructorUncachedObject(String v, int c) {
            setCounter(c);
            setValue(v);
        }
    }

    @Entity
    public static class MappedObject {
        @Id
        public String id;
        public UncachedObject uc;
        public Map<String, String> aMap;

    }


    @Entity
    public static class BIObject {
        @Id
        public MorphiumId id;
        public String value;
        public BigInteger biValue;

    }
}
