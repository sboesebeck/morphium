package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.DefaultReadPreference;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.MapListObject;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * User: Stephan Bösebeck
 * Date: 12.07.12
 * Time: 16:20
 * <p/>
 */
@Tag("core")
public class MapListTest extends MorphiumTestBase {
    @Test
    public void mapListTest() throws InterruptedException {
        morphium.dropCollection(MapListObject.class);
        MapListObject o = new MapListObject();
        Map<String, Object> map = new HashMap<>();
        map.put("a_string", "This is a string");
        map.put("a primitive value", 42);
        map.put("double", 42.0);
        map.put("null", null);
        map.put("Entity", new UncachedObject());
        o.setMapValue(map);
        o.setName("A map-value");
        Map<String, List<Integer >> listMap = new HashMap<>();
        List<Integer> lst = new ArrayList<>();
        lst.add(1);
        lst.add(5);
        lst.add(3);
        listMap.put("eins-fuenf-drei", lst);
        lst = new ArrayList<>();
        lst.add(200);
        lst.add(300);
        lst.add(90);
        lst.add(1);
        listMap.put("zweihundert", lst);
        o.setMapListValue(listMap);
        morphium.store(o);
        Thread.sleep(100);
        MapListObject ml = morphium.findById(MapListObject.class, o.getId());
        assert(ml.getMapListValue().get("eins-fuenf-drei").size() == 3);
        assert(ml.getMapListValue().get("zweihundert").size() == 4);
    }

    @Test
    public void mapListEmbTest() throws InterruptedException {
        morphium.dropCollection(MapListObject.class);
        CMapListObject o = new CMapListObject();
        Map<String, Object> map = new HashMap<>();
        map.put("a_string", "This is a string");
        map.put("a primitive value", 42);
        map.put("double", 42.0);
        map.put("null", null);
        map.put("Entity", new UncachedObject());
        o.setMapValue(map);
        o.setName("A map-value");
        Map<String, List<Integer >> listMap = new HashMap<>();
        List<Integer> lst = new ArrayList<>();
        lst.add(1);
        lst.add(5);
        lst.add(3);
        listMap.put("eins-fuenf-drei", lst);
        lst = new ArrayList<>();
        lst.add(200);
        lst.add(300);
        lst.add(90);
        lst.add(1);
        listMap.put("zweihundert", lst);
        o.setMapListValue(listMap);
        Map<String, EmbObj> map2 = new HashMap<>();
        map2.put("test", new EmbObj("val", 22));
        map2.put("test2", new EmbObj("vvv", 42));
        o.setMap2(map2);
        Map<String, List<EmbObj >> map1 = new HashMap<>();
        List<EmbObj> objLst = new ArrayList<>();
        objLst.add(new EmbObj("in list 1", 7331));
        objLst.add(new EmbObj("in list 2", 57));
        objLst.add(new EmbObj("in list too", 42));
        map1.put("1st", objLst);
        objLst = new ArrayList<>();
        objLst.add(new EmbObj("in list again 1", 731));
        objLst.add(new EmbObj("in list again 2", 527));
        objLst.add(new EmbObj("in list again too", 421));
        map1.put("2nd", objLst);
        o.setMap1(map1);
        morphium.store(o);
        Thread.sleep(100);
        CMapListObject ml = morphium.findById(CMapListObject.class, o.getId());
        assertNotNull(ml, "Not Found?!?!?!?");
        assert(ml.getMapListValue().get("eins-fuenf-drei").size() == 3);
        assert(ml.getMapListValue().get("zweihundert").size() == 4);
        assertNotNull(ml.getMapListValue().get("zweihundert").get(0));
        ;
        assertNotNull(ml.getMap1().get("2nd").get(0).getTest());
        ;
        assert(ml.getMap2().get("test").getTest().equals("val"));
    }

    @Test
    public void testComplexList() throws InterruptedException {
        morphium.dropCollection(MapListObject.class);
        CMapListObject o = new CMapListObject();
        List<Map<String, String >> lst = new ArrayList<>();
        Map<String, String> strMap = new HashMap<>();
        strMap.put("tst1", "bla");
        strMap.put("tst2", "fasel");
        lst.add(strMap);
        strMap = new HashMap<>();
        strMap.put("tst2-1", "blubber");
        strMap.put("tst2-2", "blub");
        strMap.put("tst2-3", "peng");
        lst.add(strMap);
        o.setMap7(lst);
        morphium.store(o);
        Thread.sleep(100);
        CMapListObject ml = morphium.findById(CMapListObject.class, o.getId());
        assertNotNull(ml, "Not Found?!?!?!?");
        assert(ml.getMap7().get(0).get("tst1").equals("bla"));
        assert(ml.getMap7().get(1).get("tst2-2").equals("blub"));
    }

    @Test
    public void testMapOfListsString() throws InterruptedException {
        morphium.dropCollection(MapListObject.class);
        CMapListObject o = new CMapListObject();
        Map<String, List<String >> m = new HashMap<>();
        List<String> lst = new ArrayList<>();
        lst.add("bla");
        lst.add("fasel");
        m.put("m1", lst);
        lst = new ArrayList<>();
        lst.add("foo");
        lst.add("bar");
        lst.add("grin");
        m.put("m2", lst);
        o.setMap3(m);
        morphium.store(o);
        Thread.sleep(100);
        CMapListObject ml = morphium.findById(CMapListObject.class, o.getId());
        assert(ml.getMap3().get("m1").get(1).equals("fasel"));
        assert(ml.getMap3().get("m2").get(2).equals("grin"));
    }

    @Test
    public void testMapOfListsEmb() throws InterruptedException {
        morphium.dropCollection(MapListObject.class);
        CMapListObject o = new CMapListObject();
        Map<String, List<EmbObj >> m = new HashMap<>();
        List<EmbObj> lst = new ArrayList<>();
        lst.add(new EmbObj("bla", 12));
        lst.add(new EmbObj("fasel", 42));
        m.put("m1", lst);
        lst = new ArrayList<>();
        lst.add(new EmbObj("foo", 42452));
        lst.add(new EmbObj("bar", 2));
        lst.add(new EmbObj("grin", 7331));
        m.put("m2", lst);
        o.setMap4(m);
        morphium.store(o);
        Thread.sleep(100);
        Query<CMapListObject> q = morphium.createQueryFor(CMapListObject.class).f("id").eq(o.getId());
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        CMapListObject ml = q.get();
        assert(ml.getMap4().get("m1").get(1).getTest().equals("fasel"));
        assert(ml.getMap4().get("m1").get(1).getValue() == 42);
        assert(ml.getMap4().get("m2").get(2).getTest().equals("grin"));
        assert(ml.getMap4().get("m2").get(2).getValue() == 7331);
    }

    @Test
    public void testMapOfMaps() throws InterruptedException {
        morphium.dropCollection(MapListObject.class);
        CMapListObject o = new CMapListObject();
        Map<String, Map<String, String >> m = new HashMap<>();
        Map<String, String> mVal = new HashMap<>();
        mVal.put("bla", "fasel");
        mVal.put("foo", "bar");
        m.put("test", mVal);
        mVal = new HashMap<>();
        mVal.put("surname", "nachname");
        mVal.put("first name", "vorname");
        mVal.put("value", "wert");
        mVal.put("foo", "bla");
        mVal.put("bar", "blub");
        m.put("translate", mVal);
        o.setMap5(m);
        morphium.store(o);
        Thread.sleep(100);
        CMapListObject ml = morphium.findById(CMapListObject.class, o.getId());
        assert(ml.getMap5().get("test").get("bla").equals("fasel"));
        assert(ml.getMap5().get("translate").get("foo").equals("bla"));
    }

    @Test
    public void testMapOfMapEmbObj() throws InterruptedException {
        morphium.dropCollection(MapListObject.class);
        CMapListObject o = new CMapListObject();
        Map<String, Map<String, EmbObj >> m = new HashMap<>();
        Map<String, EmbObj> mVal = new HashMap<>();
        mVal.put("bla", new EmbObj("fasel", 1));
        mVal.put("foo", new EmbObj("bar", 2));
        m.put("test", mVal);
        mVal = new HashMap<>();
        mVal.put("surname", new EmbObj("nachname", 31));
        mVal.put("first name", new EmbObj("vorname", 42));
        mVal.put("value", new EmbObj("wert", 7331));
        mVal.put("foo", new EmbObj("bla", 123));
        mVal.put("bar", new EmbObj("blub", 1515));
        m.put("translate", mVal);
        o.setMap5a(m);
        morphium.store(o);
        Thread.sleep(100);
        CMapListObject ml = morphium.findById(CMapListObject.class, o.getId());
        assert(ml.getMap5a().get("test").get("bla").getTest().equals("fasel"));
        assert(ml.getMap5a().get("translate").get("foo").getTest().equals("bla"));
    }

    @Test
    public void testListOfListOfMap() throws InterruptedException {
        morphium.dropCollection(MapListObject.class);
        CMapListObject o = new CMapListObject();
        List<List<Map<String, String >>> lst = new ArrayList<>();
        List<Map<String, String >> l2 = new ArrayList<>();
        Map<String, String> map = new HashMap<>();
        map.put("k1", "v1");
        map.put("k2", "v2");
        l2.add(map);
        map = new HashMap<>();
        map.put("k11", "v11");
        map.put("k21", "v21");
        map.put("k31", "v31");
        l2.add(map);
        lst.add(l2);
        l2 = new ArrayList<>();
        map = new HashMap<>();
        map.put("k15", "v1");
        map.put("k25", "v2");
        l2.add(map);
        map = new HashMap<>();
        map.put("k51", "v11");
        map.put("k533", "v21");
        map.put("k513", "v31");
        l2.add(map);
        map = new HashMap<>();
        map.put("k512", "v11");
        map.put("k514", "v21");
        map.put("k513", "v31");
        l2.add(map);
        lst.add(l2);
        o.setMap7a(lst);
        morphium.store(o);
        Thread.sleep(100);
        CMapListObject ml = morphium.findById(CMapListObject.class, o.getId());
        assert(ml.getMap7a().get(1).get(0).get("k15").equals("v1"));
    }

    @Test
    public void testMapListMapEmb() throws InterruptedException {
        morphium.dropCollection(MapListObject.class);
        CMapListObject o = new CMapListObject();
        Map<String, List<Map<String, EmbObj >>> map = new HashMap<>();
        List<Map<String, EmbObj >> lst = new ArrayList<>();
        Map<String, EmbObj> map2 = new HashMap<>();
        map2.put("map2-v1", new EmbObj("test1", 741));
        map2.put("map2-v2", new EmbObj("test2", 742));
        map2.put("map2-v3", new EmbObj("test3", 744));
        lst.add(map2);
        map2 = new HashMap<>();
        map2.put("map3-v1", new EmbObj("test5", 1741));
        map2.put("map3-v2", new EmbObj("test6", 1742));
        map2.put("map3-v3", new EmbObj("test7", 1744));
        map2.put("map3-v4", new EmbObj("test8", 1754));
        lst.add(map2);
        map.put("list2", lst);
        lst = new ArrayList<>();
        map2 = new HashMap<>();
        map2.put("map1-v1", new EmbObj("test1", 741));
        map2.put("map1-v2", new EmbObj("test2", 742));
        map2.put("map1-v3", new EmbObj("test3", 744));
        map2.put("map1-v4", new EmbObj("test4", 784));
        map2.put("map1-v5", new EmbObj("test5", 724));
        lst.add(map2);
        map2 = new HashMap<>();
        map2.put("map2-v1", new EmbObj("test5", 1741));
        map2.put("map2-v2", new EmbObj("test6", 1742));
        map2.put("map2-v3", new EmbObj("test7", 1744));
        map2.put("map2-v4", new EmbObj("test8", 1754));
        lst.add(map2);
        map.put("list1", lst);
        o.setMap6a(map);
        morphium.store(o);
        Thread.sleep(100);
        CMapListObject ml = morphium.findById(CMapListObject.class, o.getId());
        //Map->List->Map->EmbObj
        assert(ml.getMap6a().get("list1").get(0).get("map1-v2").getTest().equals("test2"));
    }

    @Test
    public void complexMapTest() throws InterruptedException {
        MapListObject o = new MapListObject();
        o.setMapValue(UtilsMap.of("Testvalue", (Object) UtilsMap.of("$lte", "@123")));
        morphium.save(o);
        Thread.sleep(100);
        o = morphium.reread(o);
        assertTrue(o.getMapValue().containsKey("Testvalue"));
    }

    @Embedded
    public static class EmbObj {
        @Index
        private String test;
        private int value;

        public EmbObj() {
        }

        public EmbObj(String test, int value) {
            this.test = test;
            this.value = value;
        }

        public String getTest() {
            return test;
        }

        public void setTest(String test) {
            this.test = test;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }
    }

    @DefaultReadPreference(ReadPreferenceLevel.PRIMARY)
    public static class CMapListObject extends MapListObject {
        private Map<String, List<EmbObj >> map1;
        private Map<String, EmbObj> map2;
        private Map<String, List<String >> map3;
        private Map<String, List<EmbObj >> map4;

        private Map<String, Map<String, String >> map5;
        private Map<String, Map<String, EmbObj >> map5a;
        private Map<String, List<Map<String, EmbObj >>> map6a;

        private List<Map<String, String >> map7;
        private List<List<Map<String, String >>> map7a;

        public List<List<Map<String, String >>> getMap7a() {
            return map7a;
        }

        public void setMap7a(List<List<Map<String, String >>> map7a) {
            this.map7a = map7a;
        }

        public Map<String, List<Map<String, EmbObj >>> getMap6a() {
            return map6a;
        }

        public void setMap6a(Map<String, List<Map<String, EmbObj >>> map6a) {
            this.map6a = map6a;
        }

        public Map<String, Map<String, EmbObj >> getMap5a() {
            return map5a;
        }

        public void setMap5a(Map<String, Map<String, EmbObj >> map5a) {
            this.map5a = map5a;
        }

        public List<Map<String, String >> getMap7() {
            return map7;
        }

        public void setMap7(List<Map<String, String >> map7) {
            this.map7 = map7;
        }


        public Map<String, Map<String, String >> getMap5() {
            return map5;
        }

        public void setMap5(Map<String, Map<String, String >> map5) {
            this.map5 = map5;
        }

        public Map<String, List<EmbObj >> getMap4() {
            return map4;
        }

        public void setMap4(Map<String, List<EmbObj >> map4) {
            this.map4 = map4;
        }

        public Map<String, List<String >> getMap3() {
            return map3;
        }

        public void setMap3(Map<String, List<String >> map3) {
            this.map3 = map3;
        }

        public Map<String, List<EmbObj >> getMap1() {
            return map1;
        }

        public void setMap1(Map<String, List<EmbObj >> map1) {
            this.map1 = map1;
        }

        public Map<String, EmbObj> getMap2() {
            return map2;
        }

        public void setMap2(Map<String, EmbObj> map2) {
            this.map2 = map2;
        }
    }
}
