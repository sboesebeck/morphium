package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.MapListObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@Tag("core")
public class ObjectMapperCollectionsMappingTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void listValueTest(Morphium morphium) {
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

        // With new behavior, null values are serialized as explicit nulls (not omitted)
        assertTrue(stringWordCompare(m, "{list_value=[A Value, 27.0, {float_data=null, dval=0.0, double_data=null, str_value=null, long_data=null, binary_data=null, counter=0, class_name=uc, int_data=null}], map_value=null, name=Simple List, map_list_value=null}"));

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


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void mapValueTest(Morphium morphium) {
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
        // With new behavior, null values are serialized as explicit nulls (not omitted)
        assertTrue(stringWordCompare(m, "{ \"list_value\" : null, \"map_value\" : { \"Entity\" : { \"float_data\" : null, \"dval\" : 0.0, \"double_data\" : null, \"str_value\" : null, \"long_data\" : null, \"binary_data\" : null, \"counter\" : 0, \"class_name\" : \"uc\", \"int_data\" : null } , \"a primitive value\" : 42, \"null\" : null, \"double\" : 42.0, \"a_string\" : \"This is a string\" } , \"name\" : \"A map-value\", \"map_list_value\" : null }"));

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

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void embeddedListTest(Morphium morphium) {
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

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void setTest(Morphium morphium) {
        SetObject so = new SetObject();
        so.id = new de.caluga.morphium.driver.MorphiumId();
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


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testListOfEmbedded(Morphium morphium) {
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

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void objectMapperListOfListOfUncachedTest(Morphium morphium) {
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

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void objectMapperListOfMapOfListOfStringTest(Morphium morphium) {
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

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void objectMapperListOfListOfStringTest(Morphium morphium) {
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
}
