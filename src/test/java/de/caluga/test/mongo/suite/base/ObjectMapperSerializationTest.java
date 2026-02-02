package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.test.mongo.suite.data.ListContainer;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("core")
public class ObjectMapperSerializationTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void mapSerializationTest(Morphium morphium) {
        ObjectMapperImpl om = (ObjectMapperImpl) morphium.getMapper();
        om.getMorphium().getConfig().setWarnOnNoEntitySerialization(true);
        Map<String, Object> map = om.serialize(new Simple());
        log.info("Got map");
        assert (map.get("test").toString().startsWith("test"));

        Simple s = om.deserialize(Simple.class, map);
        log.info("Got simple");

        Map<String, Object> m = new java.util.HashMap<>();
        m.put("test", "testvalue");
        m.put("simple", s);

        map = om.serializeMap(m, null);
        assert (map.get("test").equals("testvalue"));

        java.util.List<Simple> lst = new java.util.ArrayList<>();
        lst.add(new Simple());
        lst.add(new Simple());
        lst.add(new Simple());

        List serializedList = om.serializeIterable(lst, null, null);
        assert (serializedList.size() == 3);

        java.util.List<Simple> deserializedList = om.deserializeList(serializedList);
        log.info("Deserialized");
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void idTest(Morphium morphium) {
        UncachedObject o = new UncachedObject("test", 1234);
        o.setMorphiumId(new MorphiumId());
        Map<String, Object> m = morphium.getMapper().serialize(o);
        assertTrue(m.get("_id") instanceof ObjectId);
        UncachedObject uc = morphium.getMapper().deserialize(UncachedObject.class, m);
        assertNotNull(uc.getMorphiumId());
        ;
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void simpleParseFromStringTest(Morphium morphium) throws Exception {
        String json = "{ \"value\":\"test\",\"counter\":123}";
        MorphiumObjectMapper om = morphium.getMapper();
        UncachedObject uc = om.deserialize(UncachedObject.class, json);
        assertEquals(123, uc.getCounter());
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void objectToStringParseTest(Morphium morphium) {
        MorphiumObjectMapper om = morphium.getMapper();
        UncachedObject o = new UncachedObject();
        o.setStrValue("A test");
        o.setLongData(new long[] {1, 23, 4L, 5L});
        o.setCounter(1234);
        Map<String, Object> dbo = om.serialize(o);
        UncachedObject uc = om.deserialize(UncachedObject.class, dbo);
        assertEquals(1234, uc.getCounter());
        assertEquals(1, uc.getLongData()[0]);
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void listContainerStringParseTest(Morphium morphium) {
        MorphiumObjectMapper om = morphium.getMapper();
        ListContainer o = new ListContainer();
        o.addLong(1234);
        o.addString("string1");
        o.addString("string2");
        o.addString("string3");
        o.addString("string4");
        Map<String, Object> dbo = om.serialize(o);
        ListContainer uc = om.deserialize(ListContainer.class, dbo);
        assertEquals(4, uc.getStringList().size());
        assertEquals("string1", uc.getStringList().get(0));
        assertEquals(1, uc.getLongList().size());
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testMarshall(Morphium morphium) {
        MorphiumObjectMapper om = morphium.getMapper();
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setStrValue("This \" is $ test");
        Map<String, Object> dbo = om.serialize(o);

        String s = Utils.toJsonString(dbo);
        System.out.println("Marshalling was: " + s);
        // With new behavior, null values are serialized as explicit nulls (not omitted)
        assertTrue(stringWordCompare(s, "{ \"float_data\" : null, \"dval\" : 0.0, \"double_data\" : null, \"str_value\" : \"This \" is $ test\", \"long_data\" : null, \"binary_data\" : null, \"counter\" : 12345, \"int_data\" : null } "));
        o = om.deserialize(UncachedObject.class, dbo);
        log.info("Text is: {}", o.getStrValue());
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testUnmarshall(Morphium morphium) {
        MorphiumObjectMapper om = morphium.getMapper();
        Map<String, Object> dbo = new java.util.HashMap<>();
        dbo.put("counter", 12345);
        dbo.put("value", "A test");
        om.deserialize(UncachedObject.class, dbo);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void idSerializeDeserializeTest(Morphium morphium) {
        UncachedObject uc = new UncachedObject("bla", 1234);
        uc.setMorphiumId(new MorphiumId());

        Map<String, Object> tst = morphium.getMapper().serialize(uc);
        UncachedObject uc2 = morphium.getMapper().deserialize(UncachedObject.class, tst);
        assert (uc2.getMorphiumId().equals(uc.getMorphiumId()));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void serializeDeserializeTest(Morphium morphium) throws Exception {
        UncachedObject uc = new UncachedObject("value", 1234);
        uc.setMorphiumId(new MorphiumId());
        uc.setDval(123);

        Map<String, Object> map = morphium.getMapper().serialize(uc);
        assertNotNull(map.get("_id"));
        ;
        assertEquals("value", map.get("str_value"));
        assertEquals(1234, map.get("counter"));

        morphium.store(uc);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void binaryDataTest(Morphium morphium) {
        UncachedObject o = new UncachedObject();
        o.setBinaryData(new byte[] {1, 2, 3, 4, 5, 5});

        Map<String, Object> obj = morphium.getMapper().serialize(o);
        assertNotNull(obj.get("binary_data"));
        ;
        assertTrue(obj.get("binary_data").getClass().isArray());
        assertEquals(obj.get("binary_data").getClass().getComponentType(), byte.class);
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void noDefaultConstructorTest(Morphium morphium) throws Exception {
        NoDefaultConstructorUncachedObject o = new NoDefaultConstructorUncachedObject("test", 15);
        String serialized = Utils.toJsonString(morphium.getMapper().serialize(o));
        log.info("Serialized... " + serialized);

        o = morphium.getMapper().deserialize(NoDefaultConstructorUncachedObject.class, serialized);
        assertNotNull(o);
        ;
        assertEquals(15, o.getCounter());
        assertEquals("test", o.getStrValue());
    }
}
