package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.MorphiumReference;
import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.objectmapping.MorphiumTypeMapper;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.replicaset.ReplicaSetConf;
import de.caluga.test.mongo.suite.data.*;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ObjectMapperImplTest {

    static final Logger log = LoggerFactory.getLogger(ObjectMapperImpl.class);
    static final ObjectMapperImpl OM = new ObjectMapperImpl();

    @Test
    public void idTest() {
        UncachedObject o = new UncachedObject("test", 1234);
        o.setMorphiumId(new MorphiumId());
        Map<String, Object> m = OM.serialize(o);
        assert (m.get("_id") instanceof ObjectId);
        UncachedObject uc = OM.deserialize(UncachedObject.class, m);
        assertNotNull(uc.getMorphiumId());
        ;
    }


    @Test
    public void simpleParseFromStringTest() throws Exception {
        String json = "{ \"value\":\"test\",\"counter\":123}";
        UncachedObject uc = OM.deserialize(UncachedObject.class, json);
        assert (uc.getCounter() == 123);
    }

    @Test
    public void objectToStringParseTest() {
        UncachedObject o = new UncachedObject();
        o.setStrValue("A test");
        o.setLongData(new long[]{1, 23, 4L, 5L});
        o.setCounter(1234);
        Map<String, Object> dbo = OM.serialize(o);
        UncachedObject uc = OM.deserialize(UncachedObject.class, dbo);
        assert (uc.getCounter() == 1234);
        assert (uc.getLongData()[0] == 1);
    }


    @Test
    public void listContainerStringParseTest() {
        ListContainer o = new ListContainer();
        o.addLong(1234);
        o.addString("string1");
        o.addString("string2");
        o.addString("string3");
        o.addString("string4");
        Map<String, Object> dbo = OM.serialize(o);
        ListContainer uc = OM.deserialize(ListContainer.class, dbo);
        assert (uc.getStringList().size() == 4);
        assert (uc.getStringList().get(0).equals("string1"));
        assert (uc.getLongList().size() == 1);
    }

    @Test
    public void testCreateCamelCase() {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(true);
        assert (om.createCamelCase("this_is_a_test", false).equals("thisIsATest")) : "Error camel case translation not working";
        assert (om.createCamelCase("a_test_this_is", true).equals("ATestThisIs")) : "Error - capitalized String wrong";


    }

    @Test
    public void testConvertCamelCase() {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(true);
        assert (om.convertCamelCase("thisIsATest").equals("this_is_a_test")) : "Conversion failed!";
    }

    @Test
    public void testDisableConvertCamelCase() {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(false);
        String fn = om.getMongoFieldName(UncachedObject.class, "intData");

        assert (fn.equals("intData")) : "Conversion failed! " + fn;

        om = new AnnotationAndReflectionHelper(true);
        fn = om.getMongoFieldName(UncachedObject.class, "intData");

        assert (fn.equals("int_data")) : "Conversion failed! " + fn;
    }

    @Test
    public void testGetCollectionName() {
        assert (OM.getCollectionName(CachedObject.class).equals("cached_object")) : "Cached object test failed";
        assert (OM.getCollectionName(UncachedObject.class).equals("uncached_object")) : "Uncached object test failed";
    }

    @Test
    public void massiveParallelGetCollectionNameTest() {

        for (int i = 0; i < 2; i++) {
            new Thread(() -> {
                assert (OM.getCollectionName(CachedObject.class).equals("cached_object")) : "Cached object test failed";
                Thread.yield();
                assert (OM.getCollectionName(UncachedObject.class).equals("uncached_object")) : "Uncached object test failed";
                Thread.yield();
                assert (OM.getCollectionName(ComplexObject.class).equals("ComplexObject")) : "complex object test failed";
            }).start();
        }
        Thread.yield();
        Thread.yield();
    }

    @Test
    public void testMarshall() {
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setStrValue("This \" is $ test");
        Map<String, Object> dbo = OM.serialize(o);

        String s = Utils.toJsonString(dbo);
        System.out.println("Marshalling was: " + s);
        assert (MorphiumTestBase.stringWordCompare(s, "{ \"dval\" : 0.0, \"counter\" : 12345, \"str_value\" : \"This \" is $ test\" } ")) : "String creation failed?" + s;
        o = OM.deserialize(UncachedObject.class, dbo);
        log.info("Text is: " + o.getStrValue());
    }

    @Test
    public void testUnmarshall() {
        Map<String, Object> dbo = new HashMap<>();
        dbo.put("counter", 12345);
        dbo.put("value", "A test");
        OM.deserialize(UncachedObject.class, dbo);
    }

    @Test
    public void testGetId() {
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setStrValue("This \" is $ test");
        o.setMorphiumId(new MorphiumId());
        Object id = an.getId(o);
        assert (id.equals(o.getMorphiumId())) : "IDs not equal!";
    }


    @Test
    public void testIsEntity() {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(true);
        assert (om.isEntity(UncachedObject.class)) : "Uncached Object no Entity?=!?=!?";
        assert (om.isEntity(new UncachedObject())) : "Uncached Object no Entity?=!?=!?";
        assert (!om.isEntity("")) : "String is an Entity?";
    }

    @Test
    public void testGetValue() {
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setStrValue("This \" is $ test");
        assert (an.getValue(o, "counter").equals(12345)) : "Value not ok!";

    }

    @Test
    public void testSetValue() {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(true);
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        om.setValue(o, "A test", "str_value");
        assert ("A test".equals(o.getStrValue())) : "Value not set";

    }

    @Test
    public void complexObjectTest()  {
        UncachedObject ee = new UncachedObject();
        ee.setCounter(12345);
        ee.setStrValue("Embedded value");
        ee.setMorphiumId(new MorphiumId());

        EmbeddedObject eo = new EmbeddedObject();
        eo.setName("Embedded only");
        eo.setValue("Value");
        eo.setTest(System.currentTimeMillis());

        ComplexObject co = new ComplexObject();
        co.setEinText("Ein text");
        co.setEmbed(eo);

        co.setEntityEmbeded(ee);
        MorphiumId embedId = ee.getMorphiumId();

        co.setId(new MorphiumId());
        String st = Utils.toJsonString(co);
        Map<String, Object> marshall = OM.serialize(co);
        System.out.println("Complex object: " + Utils.toJsonString(marshall));

        // Unmarshalling stuff
        co = OM.deserialize(ComplexObject.class, marshall);
        assert (co.getEntityEmbeded().getMorphiumId() == null) : "Embeded entity got a mongoID?!?!?!";
        co.getEntityEmbeded().setMorphiumId(embedId); // need to set ID
                                                      // manually, as it won't
                                                      // be stored!
        String st2 = Utils.toJsonString(co);
        assert (MorphiumTestBase.stringWordCompare(st, st2)) : "Strings not equal?\n" + st + "\n" + st2;
        assertNotNull(co.getEmbed(), "Embedded value not found!");
    }

    @Test
    public void idSerializeDeserializeTest() {
        UncachedObject uc = new UncachedObject("bla", 1234);
        uc.setMorphiumId(new MorphiumId());

        Map<String, Object> tst = OM.serialize(uc);
        UncachedObject uc2 = OM.deserialize(UncachedObject.class, tst);
        assert (uc2.getMorphiumId().equals(uc.getMorphiumId()));
    }

    @Test
    public void nullValueTests() {
        ComplexObject o = new ComplexObject();
        o.setTrans("TRANSIENT");
        Map<String, Object> obj;
        try {
            obj = OM.serialize(o);
        } catch (IllegalArgumentException e) {
        }
        o.setEinText("Ein Text");
        obj = OM.serialize(o);
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

        Map<String, Object> marshall = OM.serialize(o);
        String m = marshall.toString();

        // assert (m.equals("{list_value=[A Value, 27.0, {dval=0.0, counter=0,
        // class_name=de.caluga.test.mongo.suite.data.UncachedObject}],
        // name=Simple List}")) : "Marshall not ok: " + m;
        assert (MorphiumTestBase.stringWordCompare(m, "{list_value=[A Value, 27.0, {dval=0.0, counter=0, class_name=uc}], name=Simple List}"));

        MapListObject mo = OM.deserialize(MapListObject.class, marshall);
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

        Map<String, Object> marshall = OM.serialize(o);
        String m = Utils.toJsonString(marshall);
        System.out.println("Marshalled object: " + m);
        // assert (m.equals("{ \"map_value\" : { \"Entity\" : { \"dval\" : 0.0,
        // \"counter\" : 0, \"class_name\" :
        // \"de.caluga.test.mongo.suite.data.UncachedObject\" } , \"a primitive
        // value\" : 42, \"null\" : null, \"double\" : 42.0, \"a_string\" :
        // \"This is a string\" } , \"name\" : \"A map-value\" } ")) : "Value
        // not marshalled corectly";
        assert (MorphiumTestBase.stringWordCompare(m, "{ \"map_value\" : { \"Entity\" : { \"dval\" : 0.0, \"counter\" : 0, \"class_name\" : \"uc\" } , \"a primitive value\" : 42, \"null\" :  null, \"double\" : 42.0, \"a_string\" : \"This is a string\" } , \"name\" : \"A map-value\" } ")) : "Value not marshalled corectly";

        MapListObject mo = OM.deserialize(MapListObject.class, marshall);
        assert (mo.getName().equals("A map-value")) : "Name error";
        assertNotNull(mo.getMapValue(), "map value is null????");
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
    @org.junit.jupiter.api.Disabled("Performance tests don't provide meaningful assertions for test coverage")
    public void objectMapperSpeedTest()  {
        //Thread.sleep(20000);
        UncachedObject o = new UncachedObject();
        o.setCounter(42);
        o.setStrValue("The meaning of life");
        o.setMorphiumId(new MorphiumId());
        o.setBoolData(new boolean[] { true, false, false, true });
        o.setDval(0.3333);
        Map<String, Object> marshall = null;
        log.info("--------------  Running with " + OM.getClass().getName());
        long start = System.currentTimeMillis();
        for (int i = 0; i < 25000; i++) {
            marshall = OM.serialize(o);
        }
        long dur = System.currentTimeMillis() - start;

        log.info("Mapping of UncachedObject 25000 times took " + dur + "ms");
        assert (dur < 5000);
        start = System.currentTimeMillis();
        for (int i = 0; i < 25000; i++) {
            UncachedObject uc = OM.deserialize(UncachedObject.class, marshall);
        }
        dur = System.currentTimeMillis() - start;
        log.info("De-Marshalling of UncachedObject 25000 times took " + dur + "ms");
        assert (dur < 5000);
    }

    @Test
    @org.junit.jupiter.api.Disabled("Performance tests don't provide meaningful assertions for test coverage")
    public void objectMapperSpeedTest2()  {
        UncachedObject o = new UncachedObject();
        o.setCounter(42);
        o.setStrValue("The meaning of life");
        o.setMorphiumId(new MorphiumId());
        o.setBoolData(new boolean[] { true, false, false, true });
        o.setDval(0.3333);
        Map<String, Object> marshall = null;
        log.info("--------------  Running with " + OM.getClass().getName());
        long start = System.currentTimeMillis();
        for (int i = 0; i < 25000; i++) {
            marshall = OM.serialize(o);
        }
        long dur = System.currentTimeMillis() - start;

        log.info("Mapping of UncachedObject 25000 times took " + dur + "ms");
        assert (dur < 5000);
        start = System.currentTimeMillis();
        for (int i = 0; i < 25000; i++) {
            UncachedObject uc = OM.deserialize(UncachedObject.class, marshall);
        }
        dur = System.currentTimeMillis() - start;
        log.info("De-Marshalling of UncachedObject 25000 times took " + dur + "ms");
        assert (dur < 5000);
    }

    @Test
    public void rsStatusTest() throws Exception {
        String json = "{ \"settings\" : { \"heartbeatTimeoutSecs\" : 10, \"catchUpTimeoutMillis\" : -1, \"catchUpTakeoverDelayMillis\" : 30000, \"getLastErrorModes\" : {  } , \"getLastErrorDefaults\" : { \"wtimeout\" : 0, \"w\" : 1 } , \"electionTimeoutMillis\" : 10000, \"chainingAllowed\" : true, \"replicaSetId\" : \"5adba61c986af770bb25454e\", \"heartbeatIntervalMillis\" : 2000 } , \"members\" :  [ { \"hidden\" : false, \"buildIndexes\" : true, \"arbiterOnly\" : false, \"host\" : \"localhost:27017\", \"slaveDelay\" : 0, \"votes\" : 1, \"_id\" : 0, \"priority\" : 10.0, \"tags\" : {  }  } , { \"hidden\" : false, \"buildIndexes\" : true, \"arbiterOnly\" : false, \"host\" : \"localhost:27018\", \"slaveDelay\" : 0, \"votes\" : 1, \"_id\" : 1, \"priority\" : 5.0, \"tags\" : {  }  } , { \"hidden\" : false, \"buildIndexes\" : true, \"arbiterOnly\" : true, \"host\" : \"localhost:27019\", \"slaveDelay\" : 0, \"votes\" : 1, \"_id\" : 2, \"priority\" : 0.0, \"tags\" : {  }  } ], \"protocolVersion\" : 1, \"_id\" : \"tst\", \"version\" : 1 } ";
        ReplicaSetConf c = OM.deserialize(ReplicaSetConf.class, json);
        assertNotNull(c);
        ;
        assert (c.getMembers().size() == 3);
    }

    @Test
    public void embeddedListTest() {
        ComplexObject co = new ComplexObject();
        co.setEmbeddedObjectList(new ArrayList<>());
        co.getEmbeddedObjectList().add(new EmbeddedObject("name", "test", System.currentTimeMillis()));
        co.getEmbeddedObjectList().add(new EmbeddedObject("name2", "test2", System.currentTimeMillis()));
        Map<String, Object> obj = OM.serialize(co);
        assertNotNull(obj.get("embeddedObjectList"));
        ;
        assert (((List) obj.get("embeddedObjectList")).size() == 2);
        ComplexObject co2 = OM.deserialize(ComplexObject.class, obj);
        assert (co2.getEmbeddedObjectList().size() == 2);
        assertNotNull(co2.getEmbeddedObjectList().get(0).getName());
        ;

    }

    @Test
    public void binaryDataTest() {
        UncachedObject o = new UncachedObject();
        o.setBinaryData(new byte[]{1, 2, 3, 4, 5, 5});

        Map<String, Object> obj = OM.serialize(o);
        assertNotNull(obj.get("binary_data"));
        ;
        assert (obj.get("binary_data").getClass().isArray());
        assert (obj.get("binary_data").getClass().getComponentType().equals(byte.class));
    }

    @Test
    public void noDefaultConstructorTest() throws Exception {
        NoDefaultConstructorUncachedObject o = new NoDefaultConstructorUncachedObject("test", 15);
        String serialized = Utils.toJsonString(OM.serialize(o));
        log.info("Serialized... " + serialized);

        o = OM.deserialize(NoDefaultConstructorUncachedObject.class, serialized);
        assertNotNull(o);
        ;
        assert (o.getCounter() == 15);
        assert (o.getStrValue().equals("test"));
    }

    @Test
    public void mapTest() throws Exception {
        MappedObject o = new MappedObject();
        o.aMap = new HashMap<>();
        o.aMap.put("test", "test");
        o.uc = new NoDefaultConstructorUncachedObject("v", 123);

        Map<String, Object> dbo = OM.serialize(o);
        o = OM.deserialize(MappedObject.class, Utils.toJsonString(dbo));

        assertNotNull(o.aMap.get("test"));
        ;
    }

    @Test
    public void objectMapperNGTest() {
        UncachedObject uc = new UncachedObject("value", 123);
        uc.setMorphiumId(new MorphiumId());
        uc.setLongData(new long[]{1L, 2L});
        Map<String, Object> obj = OM.serialize(uc);

        assertNotNull(obj.get("str_value"));
        ;
        assert (obj.get("str_value") instanceof String);
        assert (obj.get("counter") instanceof Integer);
        assert (obj.get("long_data") instanceof ArrayList);

        MappedObject mo = new MappedObject();
        mo.id = "test";
        mo.uc = uc;
        mo.aMap = new HashMap<>();
        mo.aMap.put("Test", "value1");
        mo.aMap.put("test2", "value2");
        obj = OM.serialize(mo);
        assertNotNull(obj.get("uc"));
        ;
        assert (((Map) obj.get("uc")).get("_id") == null);

        BIObject bo = new BIObject();
        bo.id = new MorphiumId();
        bo.value = "biVal";
        bo.biValue = new BigInteger("123afd33", 16);

        obj = OM.serialize(bo);
        assert (obj.get("_id") instanceof ObjectId || obj.get("_id") instanceof String || obj.get("_id") instanceof MorphiumId);
        assert (obj.get("bi_value") instanceof Map);

    }

    @Test
    public void setTest() {
        SetObject so = new SetObject();
        so.id = new MorphiumId();
        so.setOfStrings = new HashSet<>();
        so.setOfStrings.add("test");
        so.setOfStrings.add("test2");
        so.setOfStrings.add("test3");
        so.setOfUC = new HashSet<>();
        so.setOfUC.add(new UncachedObject("value", 1234));
        so.listOfSetOfStrings = new ArrayList<>();
        so.listOfSetOfStrings.add(new HashSet<>());
        so.listOfSetOfStrings.get(0).add("Test1");
        so.listOfSetOfStrings.get(0).add("Test2");
        so.listOfSetOfStrings.add(new HashSet<>());
        so.listOfSetOfStrings.get(1).add("Test3");
        so.listOfSetOfStrings.get(1).add("Test4");

        so.mapOfSetOfStrings = new HashMap<>();
        so.mapOfSetOfStrings.put("t1", new HashSet<>());
        so.mapOfSetOfStrings.get("t1").add("test1");
        so.mapOfSetOfStrings.get("t1").add("test11");
        so.mapOfSetOfStrings.put("t2", new HashSet<>());
        so.mapOfSetOfStrings.get("t2").add("test2");
        so.mapOfSetOfStrings.get("t2").add("test21");

        Map<String, Object> m = OM.serialize(so);
        assertNotNull(m.get("set_of_strings"));
        ;
        assert (m.get("set_of_strings") instanceof List);
        assert (((List) m.get("set_of_strings")).size() == 3);
        assert (((List) m.get("set_of_u_c")).size() == 1);

        SetObject setObject = OM.deserialize(SetObject.class, m);
        assertNotNull(setObject);
        ;
        setObject.setOfStrings.contains("test");
        setObject.setOfStrings.contains("test2");
        setObject.setOfStrings.contains("test3");
        assert (setObject.setOfUC.iterator().next() instanceof UncachedObject);

        assert (setObject.listOfSetOfStrings.size() == 2);
        Set<String> firstSetOfStrings = setObject.listOfSetOfStrings.get(0);
        assert (firstSetOfStrings.size() == 2);
        assert (firstSetOfStrings.contains("Test1"));
        assert (firstSetOfStrings.contains("Test2"));
        Set<String> secondSetOfStrings = setObject.listOfSetOfStrings.get(1);
        assert (secondSetOfStrings.size() == 2);
        assert (secondSetOfStrings.contains("Test3"));
        assert (secondSetOfStrings.contains("Test4"));

        Set<String> t1 = setObject.mapOfSetOfStrings.get("t1");
        assert (t1.contains("test1"));
        assert (t1.contains("test11"));
        Set<String> t2 = setObject.mapOfSetOfStrings.get("t2");
        assert (t2.contains("test2"));
        assert (t2.contains("test21"));
    }

    @Test
    public void setTestDeserializeLegacy() {
        HashMap<String, Object> legacyFromDb = new HashMap<>();
        legacyFromDb.put("_id", new ObjectId("5f9ad2e2a893502427d15b29"));
        legacyFromDb.put("set_of_strings", new ArrayList<>(Arrays.asList("test", "test2", "test3")));
        Map<String, Object> uc = new HashMap<>();
        uc.put("dval", Double.valueOf(0.0));
        uc.put("counter", Integer.valueOf(1234));
        uc.put("value", "value");
        uc.put("class_name", "uc");
        legacyFromDb.put("set_of_u_c", new ArrayList<>(Arrays.asList(uc)));
        Map<String, Object> set1 = new HashMap<>();
        set1.put("original_class_name", "java.util.HashSet");
        set1.put("_b64data", "rO0ABXNyABFqYXZhLnV0aWwuSGFzaFNldLpEhZWWuLc0AwAAeHB3DAAAABA/QAAAAAAAAnQABVRlc3QxdAAFVGVzdDJ4");
        Map<String, Object> set2 = new HashMap<>();
        set2.put("original_class_name", "java.util.HashSet");
        set2.put("_b64data", "rO0ABXNyABFqYXZhLnV0aWwuSGFzaFNldLpEhZWWuLc0AwAAeHB3DAAAABA/QAAAAAAAAnQABVRlc3Q0dAAFVGVzdDN4");
        legacyFromDb.put("list_of_set_of_strings", new ArrayList<>(Arrays.asList(set1, set2)));

        Map<String, Object> mapOfSetOfStrings = new HashMap<>();
        Map<String, Object> lt1 = new HashMap<>();
        lt1.put("original_class_name", "java.util.HashSet");
        lt1.put("_b64data", "rO0ABXNyABFqYXZhLnV0aWwuSGFzaFNldLpEhZWWuLc0AwAAeHB3DAAAABA/QAAAAAAAAnQABnRlc3QxMXQABXRlc3QxeA==");
        Map<String, Object> lt2 = new HashMap<>();
        lt2.put("original_class_name", "java.util.HashSet");
        lt2.put("_b64data", "rO0ABXNyABFqYXZhLnV0aWwuSGFzaFNldLpEhZWWuLc0AwAAeHB3DAAAABA/QAAAAAAAAnQABXRlc3QydAAGdGVzdDIxeA==");
        mapOfSetOfStrings.put("t1", lt1);
        mapOfSetOfStrings.put("t2", lt2);
        legacyFromDb.put("map_of_set_of_strings", mapOfSetOfStrings);

        SetObject setObject = OM.deserialize(SetObject.class, legacyFromDb);
        assertNotNull(setObject);
        ;
        setObject.setOfStrings.contains("test");
        setObject.setOfStrings.contains("test2");
        setObject.setOfStrings.contains("test3");
        assert (setObject.setOfUC.iterator().next() instanceof UncachedObject);

        assert (setObject.listOfSetOfStrings.size() == 2);
        Set<String> firstSetOfStrings = setObject.listOfSetOfStrings.get(0);
        assert (firstSetOfStrings.size() == 2);
        assert (firstSetOfStrings.contains("Test1"));
        assert (firstSetOfStrings.contains("Test2"));
        Set<String> secondSetOfStrings = setObject.listOfSetOfStrings.get(1);
        assert (secondSetOfStrings.size() == 2);
        assert (secondSetOfStrings.contains("Test3"));
        assert (secondSetOfStrings.contains("Test4"));

        Set<String> t1 = setObject.mapOfSetOfStrings.get("t1");
        assert (t1.contains("test1"));
        assert (t1.contains("test11"));
        Set<String> t2 = setObject.mapOfSetOfStrings.get("t2");
        assert (t2.contains("test2"));
        assert (t2.contains("test21"));
    }

    @Test
    public void testListOfEmbedded() {
        ListOfEmbedded lst = new ListOfEmbedded();
        lst.list = new ArrayList<>();
        lst.list.add(new EmbeddedObject("nam", "val", System.currentTimeMillis()));
        lst.list.add(new EmbeddedObject("nam", "val", System.currentTimeMillis()));
        lst.list.add(new EmbeddedObject("nam", "val", System.currentTimeMillis()));
        lst.list.add(new EmbeddedObject("nam", "val", System.currentTimeMillis()));

        Map<String, Object> obj = OM.serialize(lst);
        assertNotNull(obj.get("list"));
        ;
        assert (obj.get("list") instanceof List);
        assert (((List) obj.get("list")).get(0) instanceof Map);

        ListOfEmbedded lst2 = OM.deserialize(ListOfEmbedded.class, obj);
        assertNotNull(lst2.list);
        ;
        assert (lst2.list.size() == 4);
        assert (lst2.list.get(0).getName().equals("nam"));

        ((Map) ((List) obj.get("list")).get(0)).remove("class_name");

        lst2 = OM.deserialize(ListOfEmbedded.class, obj);
        assert (lst2.list.get(0) instanceof EmbeddedObject);

    }

    @Test
    public void objectMapperListOfListOfUncachedTest() {
        ListOfListOfListOfUncached lst3 = new ListOfListOfListOfUncached();
        lst3.list = new ArrayList<>();
        lst3.list.add(new ArrayList<>());
        lst3.list.add(new ArrayList<>());
        lst3.list.get(0).add(new ArrayList<>());
        lst3.list.get(0).get(0).add(new UncachedObject("test", 123));

        Map<String, Object> obj = OM.serialize(lst3);
        assert (obj.get("list") instanceof List);
        assert (((List) obj.get("list")).get(0) instanceof List);
        assert (((List) ((List) obj.get("list")).get(0)).get(0) instanceof List);
        assert (((List) ((List) ((List) obj.get("list")).get(0)).get(0)).get(0) instanceof Map);

        ListOfListOfListOfUncached lst4 = OM.deserialize(ListOfListOfListOfUncached.class, obj);
        assert (lst4.list.size() == 2);
        assert (lst4.list.get(0).size() == 1);
        assert (lst4.list.get(0).get(0).size() == 1);
        assert (lst4.list.get(0).get(0).get(0).getStrValue().equals("test"));
    }

    public static class NoDefaultConstructorUncachedObject extends UncachedObject {
        public NoDefaultConstructorUncachedObject(String v, int c) {
            setCounter(c);
            setStrValue(v);
        }
    }

    @Test
    public void objectMapperListOfMapOfListOfStringTest() {
        ListOfMapOfListOfString lst5 = new ListOfMapOfListOfString();
        lst5.list = new ArrayList<>();
        lst5.list.add(new HashMap<>());
        lst5.list.add(new HashMap<>());
        lst5.list.get(0).put("tst1", new ArrayList<>());
        lst5.list.get(0).get("tst1").add("test");
        Map<String, Object> obj = OM.serialize(lst5);
        assert (obj.get("list") instanceof List);
        assert (((List) obj.get("list")).get(0) instanceof Map);
        assert (((Map) ((List) obj.get("list")).get(0)).get("tst1") instanceof List);
        assert (((List) ((Map) ((List) obj.get("list")).get(0)).get("tst1")).get(0) instanceof String);

        ListOfMapOfListOfString lst6 = OM.deserialize(ListOfMapOfListOfString.class, obj);
        assert (lst6.list.size() == 2);
        assertNotNull(lst6.list.get(0));
        ;
        assertNotNull(lst6.list.get(0).get("tst1"));
        ;
    }

    @Test
    public void objectMapperListOfListOfStringTest() {
        ListOfListOfListOfString lst = new ListOfListOfListOfString();
        lst.list = new ArrayList<>();
        lst.list.add(new ArrayList<>());
        lst.list.add(new ArrayList<>());
        lst.list.get(0).add(new ArrayList<>());
        lst.list.get(0).get(0).add("TEst1");

        Map<String, Object> obj = OM.serialize(lst);
        assert (obj.get("list") instanceof List);
        assert (((List) obj.get("list")).get(0) instanceof List);
        assert (((List) ((List) obj.get("list")).get(0)).get(0) instanceof List);
        assert (((List) ((List) ((List) obj.get("list")).get(0)).get(0)).get(0) instanceof String);

        ListOfListOfListOfString lst2 = OM.deserialize(ListOfListOfListOfString.class, obj);
        assert (lst2.list.size() == 2);
        assert (lst2.list.get(0).size() == 1);
        assert (lst2.list.get(0).get(0).size() == 1);
        assert (lst2.list.get(0).get(0).get(0).equals("TEst1"));

    }

    @Test
    public void enumTest() {
        EnumTest e = new EnumTest();
        e.id = "enumTestId";
        e.anEnum = TestEnum.v1;
        e.set = EnumSet.of(TestEnum.v1, TestEnum.v4);

        e.aMap = new HashMap<>();
        e.aMap.put("test1", TestEnum.v2);
        e.aMap.put("test3", TestEnum.v3);

        e.enumKeyMap = new EnumMap<>(TestEnum.class);
        e.enumKeyMap.put(TestEnum.v1, "String-v1");
        e.enumKeyMap.put(TestEnum.v4, "String-v4");

        e.lst = new ArrayList<>();
        e.lst.add(TestEnum.v4);
        e.lst.add(TestEnum.v3);
        e.lst.add(TestEnum.v1);

        e.lstlst = new ArrayList<>();
        e.lstlst.add(EnumSet.noneOf(TestEnum.class));
        e.lstlst.add(EnumSet.of(TestEnum.v1));
        e.lstlst.add(EnumSet.of(TestEnum.v1, TestEnum.v2));
        e.lstlst.add(EnumSet.of(TestEnum.v1, TestEnum.v2, TestEnum.v3));
        e.lstlst.add(EnumSet.allOf(TestEnum.class));

        Map<String, Object> obj = OM.serialize(e);
        assertNotNull(obj.get("an_enum"));
        ;

        Map<String, Object> obj2 = OM.serialize(e);
        assertNotNull(obj2.get("an_enum"));
        ;

        EnumTest e2 = OM.deserialize(EnumTest.class, obj2);

        assertNotNull(e2);
        ;
        assert (e2.equals(e));
    }

    @Test
    public void enumWithClassBodyTest() {
        EnumWithClassBodyTest e = new EnumWithClassBodyTest();
        e.id = "EnumWithClassBodyTestId";
        e.timeUnit = TimeUnit.NANOSECONDS;
        e.timeUnitList = new ArrayList<>();
        e.timeUnitList.add(TimeUnit.MICROSECONDS);
        e.timeUnitList.add(TimeUnit.MILLISECONDS);
        e.timeUnitSet = new HashSet<>(EnumSet.of(TimeUnit.MILLISECONDS, TimeUnit.SECONDS));
        e.timeUnitEnumSet = EnumSet.of(TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        e.timeUnitValueMap = Collections.singletonMap("StringMINUTES", TimeUnit.MINUTES);
        e.timeUnitKeyMap = new EnumMap<>(TimeUnit.class);
        e.timeUnitKeyMap.put(TimeUnit.HOURS, "TestString1");
        e.timeUnitKeyMap.put(TimeUnit.DAYS, "TestString2");
        e.listOfTimeUnitMap = new ArrayList<>();
        e.listOfTimeUnitMap.add(new EnumMap<>(Collections.singletonMap(TimeUnit.NANOSECONDS, "TestString3")));
        e.listOfTimeUnitMap.add(new EnumMap<>(Collections.singletonMap(TimeUnit.MICROSECONDS, "TestString4")));

        Map<String, Object> obj = OM.serialize(e);
        EnumWithClassBodyTest e2 = OM.deserialize(EnumWithClassBodyTest.class, obj);

        assertNotNull(e2);
        ;
        assert (e2.equals(e));
    }

    @Test
    public void enumWithCustomToStringTest() {
        EnumWithCustomToStringTest e = new EnumWithCustomToStringTest();
        e.id = "EnumWithCustomToStringTestId";
        e.chronoUnit = ChronoUnit.NANOS;
        e.chronoUnitList = new ArrayList<>();
        e.chronoUnitList.add(ChronoUnit.MICROS);
        e.chronoUnitList.add(ChronoUnit.MILLIS);
        e.chronoUnitSet = new HashSet<>(EnumSet.of(ChronoUnit.MILLIS, ChronoUnit.SECONDS));
        e.chronoUnitEnumSet = EnumSet.of(ChronoUnit.MILLIS, ChronoUnit.SECONDS);
        e.chronoUnitValueMap = Collections.singletonMap("StringMINUTES", ChronoUnit.MINUTES);
        e.chronoUnitKeyMap = new EnumMap<>(ChronoUnit.class);
        e.chronoUnitKeyMap.put(ChronoUnit.HOURS, "TestString1");
        e.chronoUnitKeyMap.put(ChronoUnit.DAYS, "TestString2");
        e.listOfChronoUnitMap = new ArrayList<>();
        e.listOfChronoUnitMap.add(new EnumMap<>(Collections.singletonMap(ChronoUnit.WEEKS, "TestString3")));
        e.listOfChronoUnitMap.add(new EnumMap<>(Collections.singletonMap(ChronoUnit.MONTHS, "TestString4")));

        Map<String, Object> obj = OM.serialize(e);
        EnumWithCustomToStringTest e2 = OM.deserialize(EnumWithCustomToStringTest.class, obj);

        assertNotNull(e2);
        ;
        assert (e2.equals(e));
    }

    @Test
    public void enumInRawTest() {
        EnumInRaw e = new EnumInRaw();
        e.id = "EnumInRawTestId";
        e.StringToTimeUnit = Collections.singletonMap("StringMINUTES", TimeUnit.MINUTES);
        e.timeUnits = EnumSet.of(TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS);
        e.chronoUnits = EnumSet.of(ChronoUnit.CENTURIES, ChronoUnit.FOREVER);

        Map<String, Object> obj = OM.serialize(e);
        EnumInRaw e2 = OM.deserialize(EnumInRaw.class, obj);

        assertNotNull(e2);
        ;
        assert (e2.equals(e));
    }

    @Test
    public void referenceTest() {
        MorphiumReference r = new MorphiumReference("test", new MorphiumId());
        Map<String, Object> o = OM.serialize(r);
        assertNotNull(o.get("refid"));
        ;

        MorphiumReference r2 = OM.deserialize(MorphiumReference.class, o);
        assertNotNull(r2.getId());
        ;
    }

    @Test
    public void customMapperTest() {
        OM.registerCustomMapperFor(MyClass.class, new MorphiumTypeMapper<MyClass>() {
            @Override
            public Object marshall(MyClass o) {
                Map m = new HashMap();
                m.put("class", o.getClass().getName());
                m.put("value", "AMMENDED+" + o.theValue);
                return m;
            }

            @Override
            public MyClass unmarshall(Object d) {
                Map m = (Map) d;
                MyClass mc = new MyClass();
                mc.theValue = (String) m.get("value");
                mc.theValue = mc.theValue.substring(9);

                return mc;
            }
        });

        MyClass mc = new MyClass();
        mc.theValue = "a little Test";
        Map<String, Object> map = OM.serialize(mc);
        assert (map.get("class").equals(mc.getClass().getName()));
        assert (map.get("value").equals("AMMENDED+" + mc.theValue));

        MyClass mc2 = OM.deserialize(MyClass.class, map);
        assert (mc2.theValue.equals(mc.theValue));
    }

    @Test
    public void testStructure() throws Exception {
        Complex c = new Complex();
        c.id = new MorphiumId();
        c.structureK = new ArrayList<>();

        Map<String, Object> v1 = new HashMap<>();
        v1.put("String", "String");
        v1.put("Integer", 123);
        v1.put("List", Arrays.asList("l1", "l2"));
        c.structureK.add(v1);

        Map<String, Object> v2 = new HashMap<>();
        v2.put("String", "String");
        v2.put("Integer", 123);
        v2.put("List", Arrays.asList("l1", "l2"));
        v2.put("Map", UtilsMap.of("key", 123));
        c.structureK.add(v2);

        Map<String, Object> seralized = OM.serialize(c);
        log.info(Utils.toJsonString(seralized));

        Complex c2 = OM.deserialize(Complex.class, seralized);
        log.info("Deserialized!");
        assertNotNull(c2);
        ;
        assert (c2.id.equals(c.id));
        assert (c2.structureK.size() == c.structureK.size());
        assert (c2.structureK.get(0).get("String") instanceof String);
        assert (c2.structureK.get(0).get("Integer") instanceof Integer);
        assert (c2.structureK.get(0).get("List") instanceof List);
        assert (c2.structureK.get(0).get("Map") == null);
        assert (c2.structureK.get(1).get("String") instanceof String);
        assert (c2.structureK.get(1).get("Integer") instanceof Integer);
        assert (c2.structureK.get(1).get("List") instanceof List);
        assertNotNull(c2.structureK.get(1).get("Map"));
        ;
        assert (((Map) c2.structureK.get(1).get("Map")).get("key").equals(123));

        log.info("All fine!");
    }

    @Test
    public void testArray() {
        ArrayTestObj a = new ArrayTestObj();
        a.name = "ArrayTestObjId";
        a.intArr = new int[] { 1, 23, 456 };
        a.byteArr = a.name.getBytes(StandardCharsets.ISO_8859_1);
        a.stringArr = new String[] { "7", "abv" };
        a.listStringArr = Arrays.asList(new String[] { "7", "abv" }, new String[] { "8", "def" });
        a.arrListStringArr = new List[] {Arrays.asList(new String[] { "7", "abv" }, new String[] { "8", "def" }), Arrays.asList(new String[] { "9", "abv" }, new String[] { "10", "def" })};
        a.timeUnitArr = new TimeUnit[] { TimeUnit.DAYS, TimeUnit.SECONDS };
        a.listTimeUnitArr = Arrays.asList(new TimeUnit[] { TimeUnit.DAYS, TimeUnit.SECONDS }, new TimeUnit[] { TimeUnit.MICROSECONDS, TimeUnit.MILLISECONDS });
        a.mapByteArr = new HashMap<>();
        a.mapByteArr.put("firstByteArray", new byte[]{1, 12, 123});
        a.mapByteArr.put("secondByteArray", new byte[]{18, 127, -10});

        a.mapIntArr = Collections.singletonMap("someIntArray", new int[]{199999, 23, 456});
        a.arrOfMap = new Map[]{Collections.singletonMap("nanos", TimeUnit.NANOSECONDS), Collections.singletonMap("millis", TimeUnit.MILLISECONDS)};

        Map<String, Object> obj = OM.serialize(a);
        ArrayTestObj a2 = OM.deserialize(ArrayTestObj.class, obj);

        assert (Arrays.equals((byte[]) obj.get("byte_arr"), a.byteArr)) : "Byte array should be sento to mongo as is: " + obj.get("byteArr");
        assertNotNull(a2);
        ;
        assert (a2.equals(a));
    }

    @Embedded
    public static class MyClass {
        // does not need to be entity?
        String theValue;
    }

    public enum TestEnum {
        v1, v2, v3, v4,
    }

    @Entity
    public static class ListOfEmbedded {
        @Id
        public MorphiumId id;
        public List<EmbeddedObject> list;
    }

    @Entity
    public static class ListOfListOfListOfString {
        @Id
        public String id;

        public List<List<List<String>>> list;
    }

    @Entity
    public static class ListOfListOfListOfUncached {
        @Id
        public String id;

        public List<List<List<UncachedObject>>> list;
    }

    @Entity
    public static class ListOfMapOfListOfString {
        @Id
        public MorphiumId id;

        public List<Map<String, List<String>>> list;
    }

    @Entity
    public static class MappedObject {
        @Id
        public String id;
        public UncachedObject uc;
        public Map<String, String> aMap;

    }

    @Entity
    public static class SetObject {
        @Id
        public MorphiumId id;
        public Set<String> setOfStrings;
        public Set<UncachedObject> setOfUC;
        public List<Set<String>> listOfSetOfStrings;
        public Map<String, Set<String>> mapOfSetOfStrings;
    }

    @Entity
    public static class EnumTest {
        @Id
        public String id;
        public TestEnum anEnum;
        public Set<TestEnum> set;
        public Map<String, TestEnum> aMap;
        public Map<TestEnum, String> enumKeyMap;
        public List<TestEnum> lst;
        public List<Collection<TestEnum>> lstlst;

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            EnumTest other = (EnumTest) obj;
            if (aMap == null) {
                if (other.aMap != null)
                    return false;
            } else if (!aMap.equals(other.aMap))
                return false;
            if (anEnum != other.anEnum)
                return false;
            if (enumKeyMap == null) {
                if (other.enumKeyMap != null)
                    return false;
            } else if (!enumKeyMap.equals(other.enumKeyMap))
                return false;
            if (id == null) {
                if (other.id != null)
                    return false;
            } else if (!id.equals(other.id))
                return false;
            if (lst == null) {
                if (other.lst != null)
                    return false;
            } else if (!lst.equals(other.lst))
                return false;
            if (lstlst == null) {
                if (other.lstlst != null)
                    return false;
            } else if (!lstlst.equals(other.lstlst))
                return false;
            if (set == null) {
                return other.set == null;
            } else return set.equals(other.set);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((aMap == null) ? 0 : aMap.hashCode());
            result = prime * result + ((anEnum == null) ? 0 : anEnum.hashCode());
            result = prime * result + ((enumKeyMap == null) ? 0 : enumKeyMap.hashCode());
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((lst == null) ? 0 : lst.hashCode());
            result = prime * result + ((lstlst == null) ? 0 : lstlst.hashCode());
            result = prime * result + ((set == null) ? 0 : set.hashCode());
            return result;
        }
    }

    @Entity
    public static class EnumWithClassBodyTest {
        @Id
        public String id;
        public TimeUnit timeUnit;
        public List<TimeUnit> timeUnitList;
        public Set<TimeUnit> timeUnitSet;
        public EnumSet<TimeUnit> timeUnitEnumSet;
        public Map<String, TimeUnit> timeUnitValueMap;
        public EnumMap<TimeUnit, String> timeUnitKeyMap;
        public List<EnumMap<TimeUnit, String>> listOfTimeUnitMap;

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((listOfTimeUnitMap == null) ? 0 : listOfTimeUnitMap.hashCode());
            result = prime * result + ((timeUnit == null) ? 0 : timeUnit.hashCode());
            result = prime * result + ((timeUnitEnumSet == null) ? 0 : timeUnitEnumSet.hashCode());
            result = prime * result + ((timeUnitKeyMap == null) ? 0 : timeUnitKeyMap.hashCode());
            result = prime * result + ((timeUnitList == null) ? 0 : timeUnitList.hashCode());
            result = prime * result + ((timeUnitSet == null) ? 0 : timeUnitSet.hashCode());
            result = prime * result + ((timeUnitValueMap == null) ? 0 : timeUnitValueMap.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            EnumWithClassBodyTest other = (EnumWithClassBodyTest) obj;
            if (id == null) {
                if (other.id != null)
                    return false;
            } else if (!id.equals(other.id))
                return false;
            if (listOfTimeUnitMap == null) {
                if (other.listOfTimeUnitMap != null)
                    return false;
            } else if (!listOfTimeUnitMap.equals(other.listOfTimeUnitMap))
                return false;
            if (timeUnit != other.timeUnit)
                return false;
            if (timeUnitEnumSet == null) {
                if (other.timeUnitEnumSet != null)
                    return false;
            } else if (!timeUnitEnumSet.equals(other.timeUnitEnumSet))
                return false;
            if (timeUnitKeyMap == null) {
                if (other.timeUnitKeyMap != null)
                    return false;
            } else if (!timeUnitKeyMap.equals(other.timeUnitKeyMap))
                return false;
            if (timeUnitList == null) {
                if (other.timeUnitList != null)
                    return false;
            } else if (!timeUnitList.equals(other.timeUnitList))
                return false;
            if (timeUnitSet == null) {
                if (other.timeUnitSet != null)
                    return false;
            } else if (!timeUnitSet.equals(other.timeUnitSet))
                return false;
            if (timeUnitValueMap == null) {
                return other.timeUnitValueMap == null;
            } else return timeUnitValueMap.equals(other.timeUnitValueMap);
        }
    }

    @Entity
    public static class EnumWithCustomToStringTest {
        @Id
        public String id;
        public ChronoUnit chronoUnit;
        public List<ChronoUnit> chronoUnitList;
        public Set<ChronoUnit> chronoUnitSet;
        public EnumSet<ChronoUnit> chronoUnitEnumSet;
        public Map<String, ChronoUnit> chronoUnitValueMap;
        public EnumMap<ChronoUnit, String> chronoUnitKeyMap;
        public List<EnumMap<ChronoUnit, String>> listOfChronoUnitMap;

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((chronoUnit == null) ? 0 : chronoUnit.hashCode());
            result = prime * result + ((chronoUnitEnumSet == null) ? 0 : chronoUnitEnumSet.hashCode());
            result = prime * result + ((chronoUnitKeyMap == null) ? 0 : chronoUnitKeyMap.hashCode());
            result = prime * result + ((chronoUnitList == null) ? 0 : chronoUnitList.hashCode());
            result = prime * result + ((chronoUnitSet == null) ? 0 : chronoUnitSet.hashCode());
            result = prime * result + ((chronoUnitValueMap == null) ? 0 : chronoUnitValueMap.hashCode());
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((listOfChronoUnitMap == null) ? 0 : listOfChronoUnitMap.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            EnumWithCustomToStringTest other = (EnumWithCustomToStringTest) obj;
            if (chronoUnit != other.chronoUnit)
                return false;
            if (chronoUnitEnumSet == null) {
                if (other.chronoUnitEnumSet != null)
                    return false;
            } else if (!chronoUnitEnumSet.equals(other.chronoUnitEnumSet))
                return false;
            if (chronoUnitKeyMap == null) {
                if (other.chronoUnitKeyMap != null)
                    return false;
            } else if (!chronoUnitKeyMap.equals(other.chronoUnitKeyMap))
                return false;
            if (chronoUnitList == null) {
                if (other.chronoUnitList != null)
                    return false;
            } else if (!chronoUnitList.equals(other.chronoUnitList))
                return false;
            if (chronoUnitSet == null) {
                if (other.chronoUnitSet != null)
                    return false;
            } else if (!chronoUnitSet.equals(other.chronoUnitSet))
                return false;
            if (chronoUnitValueMap == null) {
                if (other.chronoUnitValueMap != null)
                    return false;
            } else if (!chronoUnitValueMap.equals(other.chronoUnitValueMap))
                return false;
            if (id == null) {
                if (other.id != null)
                    return false;
            } else if (!id.equals(other.id))
                return false;
            if (listOfChronoUnitMap == null) {
                return other.listOfChronoUnitMap == null;
            } else return listOfChronoUnitMap.equals(other.listOfChronoUnitMap);
        }
    }

    @Entity
    public static class EnumInRaw {
        @Id
        public String id;
        public Map StringToTimeUnit;
        public Set timeUnits;
        public Set chronoUnits;

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((StringToTimeUnit == null) ? 0 : StringToTimeUnit.hashCode());
            result = prime * result + ((chronoUnits == null) ? 0 : chronoUnits.hashCode());
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((timeUnits == null) ? 0 : timeUnits.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            EnumInRaw other = (EnumInRaw) obj;
            if (StringToTimeUnit == null) {
                if (other.StringToTimeUnit != null)
                    return false;
            } else if (!StringToTimeUnit.equals(other.StringToTimeUnit))
                return false;
            if (chronoUnits == null) {
                if (other.chronoUnits != null)
                    return false;
            } else if (!chronoUnits.equals(other.chronoUnits))
                return false;
            if (id == null) {
                if (other.id != null)
                    return false;
            } else if (!id.equals(other.id))
                return false;
            if (timeUnits == null) {
                return other.timeUnits == null;
            } else return timeUnits.equals(other.timeUnits);
        }

    }

    @Entity
    public static class BIObject {
        @Id
        public MorphiumId id;
        public String value;
        public BigInteger biValue;

    }

    @Entity
    public static class Complex {
        @Id
        public MorphiumId id;
        public List<Map<String, Object>> structureK;

    }
    @Embedded
    public static class Simple {
        public String test = "test_" + System.currentTimeMillis();
        public int value = (int) (System.currentTimeMillis() % 42);
    }

    @Entity
    @NoCache
    public static class ArrayTestObj {
        @Id
        public String name;
        public int[] intArr;
        public byte[] byteArr;
        public String[] stringArr;
        public List<String[]> listStringArr;
        public List<String[]>[] arrListStringArr;
        public TimeUnit[] timeUnitArr;
        public List<TimeUnit[]> listTimeUnitArr;
        public Map<String, byte[]> mapByteArr;
        public Map<String, int[]> mapIntArr;
        public Map<String, TimeUnit>[] arrOfMap;

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(arrOfMap);
            result = prime * result + Arrays.hashCode(intArr);
            result = prime * result + ((listStringArr == null) ? 0 : listStringArr.hashCode());
            result = prime * result + ((mapByteArr == null) ? 0 : mapByteArr.hashCode());
            result = prime * result + ((mapIntArr == null) ? 0 : mapIntArr.hashCode());
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            result = prime * result + Arrays.hashCode(stringArr);
            result = prime * result + Arrays.hashCode(timeUnitArr);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ArrayTestObj other = (ArrayTestObj) obj;
            if (!Arrays.equals(arrOfMap, other.arrOfMap))
                return false;
            if (!Arrays.equals(intArr, other.intArr))
                return false;
            if (!Arrays.equals(byteArr, other.byteArr))
                return false;
            if (listStringArr == null) {
                if (other.listStringArr != null)
                    return false;
            } else if (!listArrayEquals(listStringArr, other.listStringArr))
                return false;
            if (listTimeUnitArr == null) {
                if (other.listTimeUnitArr != null)
                    return false;
            } else if (!listArrayEquals(listTimeUnitArr, other.listTimeUnitArr))
                return false;
            if (mapByteArr == null) {
                if (other.mapByteArr != null)
                    return false;
            } else if (!mapArrayEquals(mapByteArr, other.mapByteArr))
                return false;
            if (mapIntArr == null) {
                if (other.mapIntArr != null)
                    return false;
            } else if (!mapArrayEquals(mapIntArr, other.mapIntArr))
                return false;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            if (!Arrays.equals(stringArr, other.stringArr))
                return false;
            return Arrays.equals(timeUnitArr, other.timeUnitArr);
        }

    }

    public static boolean listArrayEquals(List one, List other) {
        if (other == one)
            return true;

        ListIterator e1 = one.listIterator();
        ListIterator<?> e2 = ((List<?>) other).listIterator();
        while (e1.hasNext() && e2.hasNext()) {
            Object o1 = e1.next();
            Object o2 = e2.next();
            if (o1 != null && o2 != null) {
                if (o1.getClass().equals(o2.getClass())) {
                    if (o1 instanceof int[]) {
                        if (!Arrays.equals((int[]) o1, (int[]) o2)) {
                            return false;
                        }
                    } else if (o1 instanceof byte[]) {
                        if (!Arrays.equals((byte[]) o1, (byte[]) o2)) {
                            return false;
                        }
                    } else {
                        if (!Arrays.equals((Object[]) o1, (Object[]) o2)) {
                            return false;
                        }
                    }
                } else if (!o1.equals(o2)) {
                    return false;
                }
            } else if (!(o1 == null ? o2 == null : o1.equals(o2)))
                return false;
        }
        return !(e1.hasNext() || e2.hasNext());
    }

    public static <K, V> boolean mapArrayEquals(Map<K, V> one, Map<K, V> other) {
        if (other == one)
            return true;

        if (other.size() != one.size())
            return false;

        try {
            Iterator<Entry<K, V>> i = one.entrySet().iterator();
            while (i.hasNext()) {
                Entry<K, V> e = i.next();
                K key = e.getKey();
                V value = e.getValue();
                if (value == null) {
                    if (!(other.get(key) == null && other.containsKey(key)))
                        return false;
                }
                if (value.getClass().isArray()) {
                    V otherValue = other.get(key);
                    if (otherValue == null || !otherValue.getClass().equals(value.getClass())) {
                        return false;
                    } else {
                        if (otherValue instanceof int[]) {
                            if (!Arrays.equals((int[]) value, (int[]) otherValue)) {
                                return false;
                            }
                        } else if (otherValue instanceof byte[]) {
                            if (!Arrays.equals((byte[]) value, (byte[]) otherValue)) {
                                return false;
                            }
                        } else {
                            if (!Arrays.equals((Object[]) value, (Object[]) otherValue)) {
                                return false;
                            }
                        }
                     }

                } else {
                    if (!value.equals(other.get(key)))
                        return false;
                }
            }
        } catch (ClassCastException unused) {
            return false;
        } catch (NullPointerException unused) {
            return false;
        }

        return true;
    }

}
