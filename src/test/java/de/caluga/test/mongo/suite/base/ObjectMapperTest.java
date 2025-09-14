package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.MorphiumReference;
import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.morphium.objectmapping.MorphiumTypeMapper;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.replicaset.ReplicaSetConf;
import de.caluga.test.mongo.suite.data.*;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 14:04
 * <p/>
 */
@Tag("core")
public class ObjectMapperTest extends MorphiumTestBase {
    @Test
    public void mapSerializationTest() {
        ObjectMapperImpl om = (ObjectMapperImpl) morphium.getMapper();
        om.getMorphium().getConfig().setWarnOnNoEntitySerialization(true);
        Map<String, Object> map = om.serialize(new Simple());
        log.info("Got map");
        assert (map.get("test").toString().startsWith("test"));

        Simple s = om.deserialize(Simple.class, map);
        log.info("Got simple");

        Map<String, Object> m = new HashMap<>();
        m.put("test", "testvalue");
        m.put("simple", s);

        map = om.serializeMap(m, null);
        assert (map.get("test").equals("testvalue"));

        List<Simple> lst = new ArrayList<>();
        lst.add(new Simple());
        lst.add(new Simple());
        lst.add(new Simple());

        List serializedList = om.serializeIterable(lst, null, null);
        assert (serializedList.size() == 3);

        List<Simple> deserializedList = om.deserializeList(serializedList);
        log.info("Deserialized");
    }

    @Test
    public void customTypeMapperTest() throws InterruptedException {
        morphium.dropCollection(BIObject.class);
        MorphiumObjectMapper om = morphium.getMapper();
        BigInteger tst = new BigInteger("affedeadbeefaffedeadbeef42", 16);
        Map<String, Object> d = om.serialize(tst);

        BigInteger bi = om.deserialize(BigInteger.class, d);
        assertNotNull(bi);
        ;
        assertEquals(tst, bi);

        BIObject bio = new BIObject();
        bio.biValue = tst;
        morphium.store(bio);
        Thread.sleep(100);

        BIObject bio2 = morphium.createQueryFor(BIObject.class).get();
        assertNotNull(bio2);
        assertNotNull(bio2.biValue);
        assertEquals(bio2.biValue, tst);
    }

    @Test
    public void idTest() {
        UncachedObject o = new UncachedObject("test", 1234);
        o.setMorphiumId(new MorphiumId());
        Map<String, Object> m = morphium.getMapper().serialize(o);
        assertTrue(m.get("_id") instanceof ObjectId);
        UncachedObject uc = morphium.getMapper().deserialize(UncachedObject.class, m);
        assertNotNull(uc.getMorphiumId());
        ;
    }


    @Test
    public void simpleParseFromStringTest() throws Exception {
        String json = "{ \"value\":\"test\",\"counter\":123}";
        MorphiumObjectMapper om = morphium.getMapper();
        UncachedObject uc = om.deserialize(UncachedObject.class, json);
        assertEquals (123, uc.getCounter());
    }

    @Test
    public void objectToStringParseTest() {
        MorphiumObjectMapper om = morphium.getMapper();
        UncachedObject o = new UncachedObject();
        o.setStrValue("A test");
        o.setLongData(new long[] {1, 23, 4L, 5L});
        o.setCounter(1234);
        Map<String, Object> dbo = om.serialize(o);
        UncachedObject uc = om.deserialize(UncachedObject.class, dbo);
        assertEquals (1234, uc.getCounter());
        assertEquals (1, uc.getLongData()[0] );
    }


    @Test
    public void listContainerStringParseTest() {
        MorphiumObjectMapper om = morphium.getMapper();
        ListContainer o = new ListContainer();
        o.addLong(1234);
        o.addString("string1");
        o.addString("string2");
        o.addString("string3");
        o.addString("string4");
        Map<String, Object> dbo = om.serialize(o);
        ListContainer uc = om.deserialize(ListContainer.class, dbo);
        assertEquals (4, uc.getStringList().size());
        assertEquals("string1", uc.getStringList().get(0));
        assertEquals(1, uc.getLongList().size());
    }

    @Test
    public void testCreateCamelCase() {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(true);
        assertEquals("thisIsATest", om.createCamelCase("this_is_a_test", false));
        assertEquals("ATestThisIs", om.createCamelCase("a_test_this_is", true));


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

        assertEquals("intData", fn);

        om = new AnnotationAndReflectionHelper(true);
        fn = om.getMongoFieldName(UncachedObject.class, "intData");

        assertEquals("int_data", fn);
    }

    @Test
    public void testGetCollectionName() {
        MorphiumObjectMapper om = morphium.getMapper();
        assert (om.getCollectionName(CachedObject.class).equals("cached_object")) : "Cached object test failed";
        assert (om.getCollectionName(UncachedObject.class).equals("uncached_object")) : "Uncached object test failed";

    }


    @Test
    public void massiveParallelGetCollectionNameTest() {

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

    @Test
    public void testMarshall() {
        MorphiumObjectMapper om = morphium.getMapper();
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setStrValue("This \" is $ test");
        Map<String, Object> dbo = om.serialize(o);

        String s = Utils.toJsonString(dbo);
        System.out.println("Marshalling was: " + s);
        assertTrue (stringWordCompare(s, "{ \"dval\" : 0.0, \"counter\" : 12345, \"str_value\" : \"This \" is $ test\" } "));
        o = om.deserialize(UncachedObject.class, dbo);
        log.info("Text is: {}", o.getStrValue());
    }

    @Test
    public void testUnmarshall() {
        MorphiumObjectMapper om = morphium.getMapper();
        Map<String, Object> dbo = new HashMap<>();
        dbo.put("counter", 12345);
        dbo.put("value", "A test");
        om.deserialize(UncachedObject.class, dbo);
    }

    @Test
    public void testGetId() {
        MorphiumObjectMapper om = morphium.getMapper();
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setStrValue("This \" is $ test");
        o.setMorphiumId(new MorphiumId());
        Object id = an.getId(o);
        assertEquals(id, o.getMorphiumId());
    }


    @Test
    public void testIsEntity() {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(true);
        assertTrue (om.isEntity(UncachedObject.class));
        assertTrue (om.isEntity(new UncachedObject()));
        assertTrue (!om.isEntity("")) ;
    }

    @Test
    public void testGetValue() {
        AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(true);
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setStrValue("This \" is $ test");
        assertEquals(12345, an.getValue(o, "counter"));

    }

    @Test
    public void testSetValue() {
        AnnotationAndReflectionHelper om = new AnnotationAndReflectionHelper(true);
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        om.setValue(o, "A test", "strValue");
        assertEquals("A test", o.getStrValue());

    }


    @Test
    public void complexObjectTest() {
        MorphiumObjectMapper om = morphium.getMapper();
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setStrValue("Embedded value");
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
        o.setStrValue("Referenced value");
        //        o.setMongoId(new MongoId(new Date()));
        morphium.store(o);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }

        co.setRef(o);
        co.setId(new MorphiumId());
        String st = Utils.toJsonString(co);
        System.out.println("Referenced object: " + Utils.toJsonString(om.serialize(o)));
        Map<String, Object> marshall = om.serialize(co);
        System.out.println("Complex object: " + Utils.toJsonString(marshall));


        //Unmarshalling stuff
        co = om.deserialize(ComplexObject.class, marshall);
        assertNull(co.getEntityEmbeded().getMorphiumId());
        assertNotNull(co.getRef());
        ;
        co.getEntityEmbeded().setMorphiumId(embedId);  //need to set ID manually, as it won't be stored!
        co.getRef().setMorphiumId(o.getMorphiumId());
        //co.getcRef().setId(new MorphiumId());
        String st2 = Utils.toJsonString(co);
        assertTrue (stringWordCompare(st, st2));
        assertNotNull(co.getEmbed(), "Embedded value not found!");

    }

    @Test
    public void idSerializeDeserializeTest() {
        UncachedObject uc = new UncachedObject("bla", 1234);
        uc.setMorphiumId(new MorphiumId());

        Map<String, Object> tst = morphium.getMapper().serialize(uc);
        UncachedObject uc2 = morphium.getMapper().deserialize(UncachedObject.class, tst);
        assert (uc2.getMorphiumId().equals(uc.getMorphiumId()));
    }

    @Test
    public void serializeDeserializeTest() throws Exception {
        UncachedObject uc = new UncachedObject("value", 1234);
        uc.setMorphiumId(new MorphiumId());
        uc.setDval(123);

        Map<String, Object> map = morphium.getMapper().serialize(uc);
        assertNotNull(map.get("_id"));
        ;
        assertEquals("value", map.get("str_value"));
        assertEquals(1234, map.get("counter"));

        morphium.store(uc);

//        List<Map<String, Object>> res = morphium.getDriver().find(morphium.getConfig().getDatabase(), "uncached_object", UtilsMap.of("_id", uc.getMorphiumId()), null, null, 0, 0, 10000, null, null, null);
//        assert (res.size() == 1);
    }

    @Test
    public void nullValueTests() {
        MorphiumObjectMapper om = morphium.getMapper();

        ComplexObject o = new ComplexObject();
        o.setTrans("TRANSIENT");
        Map<String, Object> obj;
        try {
            obj = om.serialize(o);
        } catch (IllegalArgumentException e) {
        }
        o.setEinText("Ein Text");
        obj = om.serialize(o);
        assertFalse(obj.containsKey("trans"));
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

        MorphiumObjectMapper om = morphium.getMapper();
        Map<String, Object> marshall = om.serialize(o);
        String m = marshall.toString();

//        assert (m.equals("{list_value=[A Value, 27.0, {dval=0.0, counter=0, class_name=de.caluga.test.mongo.suite.data.UncachedObject}], name=Simple List}")) : "Marshall not ok: " + m;
        assertTrue (stringWordCompare(m, "{list_value=[A Value, 27.0, {dval=0.0, counter=0, class_name=uc}], name=Simple List}"));

        MapListObject mo = om.deserialize(MapListObject.class, marshall);
        System.out.println("Mo: " + mo.getName());
        System.out.println("lst: " + mo.getListValue());
        assert (mo.getName().equals(o.getName())) : "Names not equal?!?!?";
        for (int i = 0; i < lst.size(); i++) {
            Object listValueNew = mo.getListValue().get(i);
            Object listValueOrig = o.getListValue().get(i);
            assertEquals(listValueNew.getClass(), listValueOrig.getClass());
            assertEquals(listValueNew, listValueOrig);
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

        MorphiumObjectMapper om = morphium.getMapper();
        Map<String, Object> marshall = om.serialize(o);
        String m = Utils.toJsonString(marshall);
        System.out.println("Marshalled object: " + m);
//        assert (m.equals("{ \"map_value\" : { \"Entity\" : { \"dval\" : 0.0, \"counter\" : 0, \"class_name\" : \"de.caluga.test.mongo.suite.data.UncachedObject\" } , \"a primitive value\" : 42, \"null\" :  null, \"double\" : 42.0, \"a_string\" : \"This is a string\" } , \"name\" : \"A map-value\" } ")) : "Value not marshalled corectly";
        assertTrue (stringWordCompare(m, "{ \"map_value\" : { \"Entity\" : { \"dval\" : 0.0, \"counter\" : 0, \"class_name\" : \"uc\" } , \"a primitive value\" : 42, \"null\" :  null, \"double\" : 42.0, \"a_string\" : \"This is a string\" } , \"name\" : \"A map-value\" } "));

        MapListObject mo = om.deserialize(MapListObject.class, marshall);
        assertEquals("A map-value", mo.getName());
        assertNotNull(mo.getMapValue(), "map value is null????");
        for (String k : mo.getMapValue().keySet()) {
            Object v = mo.getMapValue().get(k);
            if (v == null) {
                assertNull(o.getMapValue().get(k));
            } else {
                assertEquals(o.getMapValue().get(k).getClass(), v.getClass());
                assertEquals(o.getMapValue().get(k), v);
            }
        }

    }

    @Test
    @org.junit.jupiter.api.Disabled("Performance tests don't provide meaningful assertions for test coverage")
    public void objectMapperSpeedTest() {
        UncachedObject o = new UncachedObject();
        o.setCounter(42);
        o.setStrValue("The meaning of life");
        o.setMorphiumId(new MorphiumId());
        Map<String, Object> marshall = null;
        MorphiumObjectMapper ob = morphium.getMapper();
        MorphiumObjectMapper o2 = new ObjectMapperImpl();
        o2.setMorphium(morphium);
        o2.setAnnotationHelper(morphium.getARHelper());

        for (MorphiumObjectMapper om : new MorphiumObjectMapper[] {o2, ob}) {
            log.info("--------------  Running with " + om.getClass().getName());
            long start = System.currentTimeMillis();
            for (int i = 0; i < 25000; i++) {
                marshall = om.serialize(o);
            }
            long dur = System.currentTimeMillis() - start;

            log.info("Mapping of UncachedObject 25000 times took " + dur + "ms");
            assertTrue (dur < 5000);
            start = System.currentTimeMillis();
            for (int i = 0; i < 25000; i++) {
                UncachedObject uc = om.deserialize(UncachedObject.class, marshall);
            }
            dur = System.currentTimeMillis() - start;
            log.info("De-Marshalling of UncachedObject 25000 times took " + dur + "ms");
            assertTrue (dur < 5000);
        }

    }

    @Test
    public void rsStatusTest() throws Exception {
        morphium.getConfig().setReplicasetMonitoring(false);
        String json = "{ \"settings\" : { \"heartbeatTimeoutSecs\" : 10, \"catchUpTimeoutMillis\" : -1, \"catchUpTakeoverDelayMillis\" : 30000, \"getLastErrorModes\" : {  } , \"getLastErrorDefaults\" : { \"wtimeout\" : 0, \"w\" : 1 } , \"electionTimeoutMillis\" : 10000, \"chainingAllowed\" : true, \"replicaSetId\" : \"5adba61c986af770bb25454e\", \"heartbeatIntervalMillis\" : 2000 } , \"members\" :  [ { \"hidden\" : false, \"buildIndexes\" : true, \"arbiterOnly\" : false, \"host\" : \"localhost:27017\", \"slaveDelay\" : 0, \"votes\" : 1, \"_id\" : 0, \"priority\" : 10.0, \"tags\" : {  }  } , { \"hidden\" : false, \"buildIndexes\" : true, \"arbiterOnly\" : false, \"host\" : \"localhost:27018\", \"slaveDelay\" : 0, \"votes\" : 1, \"_id\" : 1, \"priority\" : 5.0, \"tags\" : {  }  } , { \"hidden\" : false, \"buildIndexes\" : true, \"arbiterOnly\" : true, \"host\" : \"localhost:27019\", \"slaveDelay\" : 0, \"votes\" : 1, \"_id\" : 2, \"priority\" : 0.0, \"tags\" : {  }  } ], \"protocolVersion\" : 1, \"_id\" : \"tst\", \"version\" : 1 } ";
        ReplicaSetConf c = morphium.getMapper().deserialize(ReplicaSetConf.class, json);
        assertNotNull(c);
        ;
        assertEquals(3, c.getMembers().size());
    }

    @Test
    public void embeddedListTest() {
        ComplexObject co = new ComplexObject();
        co.setEmbeddedObjectList(new ArrayList<>());
        co.getEmbeddedObjectList().add(new EmbeddedObject("name", "test", System.currentTimeMillis()));
        co.getEmbeddedObjectList().add(new EmbeddedObject("name2", "test2", System.currentTimeMillis()));
        Map<String, Object> obj = morphium.getMapper().serialize(co);
        assertNotNull(obj.get("embeddedObjectList"));
        ;
        assertEquals(2, ((List) obj.get("embeddedObjectList")).size());
        ComplexObject co2 = morphium.getMapper().deserialize(ComplexObject.class, obj);
        assertEquals(2, co2.getEmbeddedObjectList().size());
        assertNotNull(co2.getEmbeddedObjectList().get(0).getName());
        ;

    }


    @Test
    public void binaryDataTest() {
        UncachedObject o = new UncachedObject();
        o.setBinaryData(new byte[] {1, 2, 3, 4, 5, 5});

        Map<String, Object> obj = morphium.getMapper().serialize(o);
        assertNotNull(obj.get("binary_data"));
        ;
        assertTrue (obj.get("binary_data").getClass().isArray());
        assertEquals(obj.get("binary_data").getClass().getComponentType(), byte.class);
    }


    @Test
    public void noDefaultConstructorTest() throws Exception {
        NoDefaultConstructorUncachedObject o = new NoDefaultConstructorUncachedObject("test", 15);
        String serialized = Utils.toJsonString(morphium.getMapper().serialize(o));
        log.info("Serialized... " + serialized);

        o = morphium.getMapper().deserialize(NoDefaultConstructorUncachedObject.class, serialized);
        assertNotNull(o);
        ;
        assertEquals(15, o.getCounter());
        assertEquals("test", o.getStrValue());
    }

    @Test
    public void mapTest() throws Exception {
        MorphiumObjectMapper m = morphium.getMapper();


        MappedObject o = new MappedObject();
        o.aMap = new HashMap<>();
        o.aMap.put("test", "test");
        o.uc = new NoDefaultConstructorUncachedObject("v", 123);

        Map<String, Object> dbo = m.serialize(o);
        o = m.deserialize(MappedObject.class, Utils.toJsonString(dbo));

        assertNotNull(o.aMap.get("test"));
        ;
    }

    @Test
    public void objectMapperNGTest() {
        MorphiumObjectMapper map = morphium.getMapper();
        UncachedObject uc = new UncachedObject("value", 123);
        uc.setMorphiumId(new MorphiumId());
        uc.setLongData(new long[] {1L, 2L});
        Map<String, Object> obj = map.serialize(uc);

        assertNotNull(obj.get("str_value"));
        ;
        assertTrue (obj.get("str_value") instanceof String);
        assertTrue (obj.get("counter") instanceof Integer);
        assertTrue (obj.get("long_data") instanceof ArrayList);

        MappedObject mo = new MappedObject();
        mo.id = "test";
        mo.uc = uc;
        mo.aMap = new HashMap<>();
        mo.aMap.put("Test", "value1");
        mo.aMap.put("test2", "value2");
        obj = map.serialize(mo);
        assertNotNull(obj.get("uc"));
        ;
        assertTrue (((Map) obj.get("uc")).get("_id") == null);

        BIObject bo = new BIObject();
        bo.id = new MorphiumId();
        bo.value = "biVal";
        bo.biValue = new BigInteger("123afd33", 16);

        obj = map.serialize(bo);
        assertTrue (obj.get("_id") instanceof ObjectId || obj.get("_id") instanceof String || obj.get("_id") instanceof MorphiumId);
        assertInstanceOf(Map.class, obj.get("bi_value"));


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

        Map<String, Object> m = morphium.getMapper().serialize(so);
        assertNotNull(m.get("set_of_strings"));
        ;
        assertInstanceOf(List.class, m.get("set_of_strings"));
        assertEquals(3, ((List) m.get("set_of_strings")).size());
        assertEquals(1, ((List) m.get("set_of_u_c")).size());

        SetObject so2 = morphium.getMapper().deserialize(SetObject.class, m);
        assertNotNull(so2);
        ;
        so2.setOfStrings.contains("test");
        so2.setOfStrings.contains("test2");
        so2.setOfStrings.contains("test3");
        assertInstanceOf(UncachedObject.class, so2.setOfUC.iterator().next());

        assertEquals(2, so2.listOfSetOfStrings.size());
        Set<String> firstSetOfStrings = so2.listOfSetOfStrings.get(0);
        assertEquals(2, firstSetOfStrings.size());
        assertTrue(firstSetOfStrings.contains("Test1"));
        assertTrue(firstSetOfStrings.contains("Test2"));
        Set<String> secondSetOfStrings = so2.listOfSetOfStrings.get(1);
        assertEquals(2, secondSetOfStrings.size());
        assertTrue(secondSetOfStrings.contains("Test3"));
        assertTrue(secondSetOfStrings.contains("Test4"));

        Set<String> t1 = so2.mapOfSetOfStrings.get("t1");
        assertTrue(t1.contains("test1"));
        assertTrue(t1.contains("test11"));
        Set<String> t2 = so2.mapOfSetOfStrings.get("t2");
        assertTrue(t2.contains("test2"));
        assertTrue(t2.contains("test21"));
    }

    @Test
    public void convertCamelCaseTest() {
        String n = morphium.getARHelper().convertCamelCase("thisIsATestTT");
        assertEquals("this_is_a_test_t_t", n);

    }


    @Test
    public void testListOfEmbedded() {
        MorphiumObjectMapper map = morphium.getMapper();
        log.info("--------------------- Running test with " + map.getClass().getName());
        ListOfEmbedded lst = new ListOfEmbedded();
        lst.list = new ArrayList<>();
        lst.list.add(new EmbeddedObject("nam", "val", System.currentTimeMillis()));
        lst.list.add(new EmbeddedObject("nam", "val", System.currentTimeMillis()));
        lst.list.add(new EmbeddedObject("nam", "val", System.currentTimeMillis()));
        lst.list.add(new EmbeddedObject("nam", "val", System.currentTimeMillis()));

        Map<String, Object> obj = map.serialize(lst);
        assertNotNull(obj.get("list"));
        ;
        assertInstanceOf(List.class, obj.get("list"));
        assertInstanceOf(Map.class, ((List) obj.get("list")).get(0));

        ListOfEmbedded lst2 = map.deserialize(ListOfEmbedded.class, obj);
        assertNotNull(lst2.list);
        ;
        assertEquals(4, lst2.list.size());
        assertEquals("nam", lst2.list.get(0).getName());

        ((Map) ((List) obj.get("list")).get(0)).remove("class_name");

        lst2 = map.deserialize(ListOfEmbedded.class, obj);
        assertInstanceOf(EmbeddedObject.class, lst2.list.get(0));

    }

    @Test
    public void objectMapperListOfListOfUncachedTest() {
        MorphiumObjectMapper map = morphium.getMapper();
        ListOfListOfListOfUncached lst3 = new ListOfListOfListOfUncached();
        lst3.list = new ArrayList<>();
        lst3.list.add(new ArrayList<>());
        lst3.list.add(new ArrayList<>());
        lst3.list.get(0).add(new ArrayList<>());
        lst3.list.get(0).get(0).add(new UncachedObject("test", 123));

        Map<String, Object> obj = map.serialize(lst3);
        assertInstanceOf(List.class, obj.get("list"));
        assertInstanceOf(List.class, ((List) obj.get("list")).get(0));
        assertInstanceOf(List.class, ((List) ((List) obj.get("list")).get(0)).get(0));
        assertInstanceOf(Map.class, ((List) ((List) ((List) obj.get("list")).get(0)).get(0)).get(0));

        ListOfListOfListOfUncached lst4 = map.deserialize(ListOfListOfListOfUncached.class, obj);
        assertEquals(2, lst4.list.size());
        assertEquals(1, lst4.list.get(0).size());
        assertEquals(1, lst4.list.get(0).get(0).size());
        assertEquals("test", lst4.list.get(0).get(0).get(0).getStrValue());
    }

    public static class NoDefaultConstructorUncachedObject extends UncachedObject {
        public NoDefaultConstructorUncachedObject(String v, int c) {
            setCounter(c);
            setStrValue(v);
        }
    }


    @Test
    public void objectMapperListOfMapOfListOfStringTest() {
        MorphiumObjectMapper map = morphium.getMapper();
        ListOfMapOfListOfString lst5 = new ListOfMapOfListOfString();
        lst5.list = new ArrayList<>();
        lst5.list.add(new HashMap<>());
        lst5.list.add(new HashMap<>());
        lst5.list.get(0).put("tst1", new ArrayList<>());
        lst5.list.get(0).get("tst1").add("test");
        Map<String, Object> obj = map.serialize(lst5);
        assertInstanceOf(List.class, obj.get("list"));
        assertInstanceOf(Map.class, ((List) obj.get("list")).get(0));
        assertInstanceOf(List.class, ((Map) ((List) obj.get("list")).get(0)).get("tst1"));
        assertInstanceOf(String.class, ((List) ((Map) ((List) obj.get("list")).get(0)).get("tst1")).get(0));

        ListOfMapOfListOfString lst6 = map.deserialize(ListOfMapOfListOfString.class, obj);
        assert (lst6.list.size() == 2);
        assertNotNull(lst6.list.get(0));
        ;
        assertNotNull(lst6.list.get(0).get("tst1"));
        ;
    }

    @Test
    public void objectMapperListOfListOfStringTest() {
        MorphiumObjectMapper map = morphium.getMapper();
        ListOfListOfListOfString lst = new ListOfListOfListOfString();
        lst.list = new ArrayList<>();
        lst.list.add(new ArrayList<>());
        lst.list.add(new ArrayList<>());
        lst.list.get(0).add(new ArrayList<>());
        lst.list.get(0).get(0).add("TEst1");

        Map<String, Object> obj = map.serialize(lst);
        assertInstanceOf(List.class, obj.get("list"));
        assertInstanceOf(List.class, ((List) obj.get("list")).get(0));
        assertInstanceOf(List.class, ((List) ((List) obj.get("list")).get(0)).get(0));
        assertInstanceOf(String.class, ((List) ((List) ((List) obj.get("list")).get(0)).get(0)).get(0));

        ListOfListOfListOfString lst2 = map.deserialize(ListOfListOfListOfString.class, obj);
        assertEquals(2, lst2.list.size());
        assertEquals(1, lst2.list.get(0).size());
        assertEquals(1, lst2.list.get(0).get(0).size());
        assertEquals("TEst1", lst2.list.get(0).get(0).get(0));

    }

    @Test
    public void enumTest() {
        EnumTest e = new EnumTest();
        e.anEnum = TestEnum.v1;
        e.aMap = new HashMap<>();
        e.aMap.put("test1", TestEnum.v2);
        e.aMap.put("test3", TestEnum.v3);

        e.lst = new ArrayList<>();
        e.lst.add(TestEnum.v4);
        e.lst.add(TestEnum.v3);
        e.lst.add(TestEnum.v1);


        Map<String, Object> obj = morphium.getMapper().serialize(e);
        assertNotNull(obj.get("an_enum"));
        ;

        MorphiumObjectMapper map = new ObjectMapperImpl();
        map.setMorphium(morphium);
        map.setAnnotationHelper(morphium.getARHelper());
        Map<String, Object> obj2 = map.serialize(e);
        assertNotNull(obj2.get("an_enum"));
        ;

        EnumTest e2 = map.deserialize(EnumTest.class, obj2);

        assertNotNull(e2);
        ;
        assertEquals(e2, e);
    }

    @Test
    public void referenceTest() {
        MorphiumReference r = new MorphiumReference("test", new MorphiumId());
        Map<String, Object> o = morphium.getMapper().serialize(r);
        assertNotNull(o.get("refid"));
        ;

        MorphiumReference r2 = morphium.getMapper().deserialize(MorphiumReference.class, o);
        assertNotNull(r2.getId());
        ;
    }

    @Test
    public void customMapperTest() {
        morphium.getMapper().registerCustomMapperFor(MyClass.class, new MorphiumTypeMapper<MyClass>() {
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
        Map<String, Object> map = morphium.getMapper().serialize(mc);
        assertEquals(map.get("class"), mc.getClass().getName());
        assertEquals(map.get("value"), "AMMENDED+" + mc.theValue);

        MyClass mc2 = morphium.getMapper().deserialize(MyClass.class, map);
        assertEquals(mc2.theValue, mc.theValue);

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

        Map<String, Object> seralized = new ObjectMapperImpl().serialize(c);
        log.info(Utils.toJsonString(seralized));

        Complex c2 = new ObjectMapperImpl().deserialize(Complex.class, seralized);
        log.info("Deserialized!");
        assertNotNull(c2);
        ;
        assertEquals(c2.id, c.id);
        assertEquals(c2.structureK.size(), c.structureK.size());
        assertInstanceOf(String.class, c2.structureK.get(0).get("String"));
        assertInstanceOf(Integer.class, c2.structureK.get(0).get("Integer"));
        assertInstanceOf(List.class, c2.structureK.get(0).get("List"));
        assertNull(c2.structureK.get(0).get("Map"));
        assertInstanceOf(String.class, c2.structureK.get(1).get("String"));
        assertInstanceOf(Integer.class, c2.structureK.get(1).get("Integer"));
        assertInstanceOf(List.class, c2.structureK.get(1).get("List"));
        assertNotNull(c2.structureK.get(1).get("Map"));
        ;
        assertEquals(123, ((Map) c2.structureK.get(1).get("Map")).get("key"));

        log.info("All fine!");
    }

    @Embedded
    public static class MyClass {
        //does not need to be entity?
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
        public Map<String, TestEnum> aMap;
        public List<TestEnum> lst;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof EnumTest)) return false;
            EnumTest enumTest = (EnumTest) o;
            return Objects.equals(id, enumTest.id) &&
                          anEnum == enumTest.anEnum &&
                          Objects.equals(aMap, enumTest.aMap) &&
                          Objects.equals(lst, enumTest.lst);
        }

        @Override
        public int hashCode() {

            return Objects.hash(id, anEnum, aMap, lst);
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


    public static class Simple {
        public String test = "test_" + System.currentTimeMillis();
        public int value = (int) (System.currentTimeMillis() % 42);
    }
}
