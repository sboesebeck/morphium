package de.caluga.test.mongo.suite;

import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.Utils;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.ObjectMapperTest.BIObject;
import de.caluga.test.mongo.suite.ObjectMapperTest.EnumTest;
import de.caluga.test.mongo.suite.ObjectMapperTest.ListOfEmbedded;
import de.caluga.test.mongo.suite.ObjectMapperTest.TestEnum;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.bson.types.ObjectId;
import org.junit.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ObjectMapperImplTest {

    public static final ObjectMapperImpl OM = new ObjectMapperImpl();

    @Test
    public void noDefaultConstructorTest() throws Exception {
        NoDefaultConstructorUncachedObject o = new NoDefaultConstructorUncachedObject("test", 15);
        String serialized = Utils.toJsonString(OM.serialize(o));
        System.out.println("Serialized... " + serialized);

        o = OM.deserialize(NoDefaultConstructorUncachedObject.class, serialized);
        assert (o != null);
        assert (o.getCounter() == 15);
        assert (o.getValue().equals("test"));
    }

    @Test
    public void mapTest() throws Exception {
        MappedObject o = new MappedObject();
        o.aMap = new HashMap<>();
        o.aMap.put("test", "test");
        o.uc = new NoDefaultConstructorUncachedObject("v", 123);

        Map<String, Object> dbo = OM.serialize(o);
        o = OM.deserialize(MappedObject.class, Utils.toJsonString(dbo));

        assert (o.aMap.get("test") != null);
    }

    public static class NoDefaultConstructorUncachedObject extends UncachedObject {
        public NoDefaultConstructorUncachedObject(String v, int c) {
            setCounter(c);
            setValue(v);
        }
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
        assert (m.get("set_of_strings") != null);
        assert (m.get("set_of_strings") instanceof List);
        assert (((List) m.get("set_of_strings")).size() == 3);
        assert (((List) m.get("set_of_u_c")).size() == 1);

        SetObject so2 = OM.deserialize(SetObject.class, m);
        assert (so2 != null);
        assert (so2.listOfSetOfStrings.size() == 2);

        Set<String> firstSetOfStrings = so2.listOfSetOfStrings.get(0);
        assert (firstSetOfStrings.size() == 2);

        Set<String> t1 = so2.mapOfSetOfStrings.get("t1");
        assert (t1 instanceof Set);
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
        assert (obj.get("list") != null);
        assert (obj.get("list") instanceof List);
        assert (((List) obj.get("list")).get(0) instanceof Map);

        ListOfEmbedded lst2 = OM.deserialize(ListOfEmbedded.class, obj);
        assert (lst2.list != null);
        assert (lst2.list.size() == 4);
        assert (lst2.list.get(0).getName().equals("nam"));

        ((Map) ((List) obj.get("list")).get(0)).remove("class_name");

        lst2 = OM.deserialize(ListOfEmbedded.class, obj);
        assert (lst2.list.get(0) instanceof EmbeddedObject);

    }

    @Test
    public void objectMapperNGTest() {
        UncachedObject uc = new UncachedObject("value", 123);
        uc.setMorphiumId(new MorphiumId());
        uc.setLongData(new long[] { 1L, 2L });
        Map<String, Object> obj = OM.serialize(uc);

        assert (obj.get("value") != null);
        assert (obj.get("value") instanceof String);
        assert (obj.get("counter") instanceof Integer);
        assert (obj.get("long_data") instanceof ArrayList);

        MappedObject mo = new MappedObject();
        mo.id = "test";
        mo.uc = uc;
        mo.aMap = new HashMap<>();
        mo.aMap.put("Test", "value1");
        mo.aMap.put("test2", "value2");
        obj = OM.serialize(mo);
        assert (obj.get("uc") != null);
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
        assert (lst4.list.get(0).get(0).get(0).getValue().equals("test"));
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
        assert (lst6.list.get(0) != null);
        assert (lst6.list.get(0).get("tst1") != null);
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
        e.anEnum = TestEnum.v1;
        e.aMap = new HashMap<>();
        e.aMap.put("test1", TestEnum.v2);
        e.aMap.put("test3", TestEnum.v3);

        e.lst = new ArrayList<>();
        e.lst.add(TestEnum.v4);
        e.lst.add(TestEnum.v3);
        e.lst.add(TestEnum.v1);

        Map<String, Object> obj = OM.serialize(e);
        assert (obj.get("an_enum") != null);

        Map<String, Object> obj2 = OM.serialize(e);
        assert (obj2.get("an_enum") != null);

        EnumTest e2 = OM.deserialize(EnumTest.class, obj2);

        assert (e2 != null);
        assert (e2.equals(e));
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

}
