package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.Embedded;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 12.07.12
 * Time: 16:20
 * <p/>
 * TODO: Add documentation here
 */
public class MapListTest extends MongoTest {
    @Test
    public void mapListTest() {
        MorphiumSingleton.get().dropCollection(MapListObject.class);

        MapListObject o = new MapListObject();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("a_string", "This is a string");
        map.put("a primitive value", 42);
        map.put("double", 42.0);
        map.put("null", null);
        map.put("Entity", new UncachedObject());
        o.setMapValue(map);
        o.setName("A map-value");

        Map<String, List<Integer>> listMap = new HashMap<String, List<Integer>>();
        List<Integer> lst = new ArrayList<Integer>();
        lst.add(1);
        lst.add(5);
        lst.add(3);
        listMap.put("eins-fuenf-drei", lst);

        lst = new ArrayList<Integer>();
        lst.add(200);
        lst.add(300);
        lst.add(90);
        lst.add(1);
        listMap.put("zweihundert", lst);
        o.setMapListValue(listMap);
        MorphiumSingleton.get().store(o);

        MapListObject ml = MorphiumSingleton.get().findById(MapListObject.class, o.getId());
        assert (ml.getMapListValue().get("eins-fuenf-drei").size() == 3);
        assert (ml.getMapListValue().get("zweihundert").size() == 4);
    }

    @Test
    public void mapListEmbTest() throws Exception {
        MorphiumSingleton.get().dropCollection(MapListObject.class);

        CMapListObject o = new CMapListObject();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("a_string", "This is a string");
        map.put("a primitive value", 42);
        map.put("double", 42.0);
        map.put("null", null);
        map.put("Entity", new UncachedObject());
        o.setMapValue(map);
        o.setName("A map-value");

        Map<String, List<Integer>> listMap = new HashMap<String, List<Integer>>();
        List<Integer> lst = new ArrayList<Integer>();
        lst.add(1);
        lst.add(5);
        lst.add(3);
        listMap.put("eins-fuenf-drei", lst);

        lst = new ArrayList<Integer>();
        lst.add(200);
        lst.add(300);
        lst.add(90);
        lst.add(1);
        listMap.put("zweihundert", lst);
        o.setMapListValue(listMap);

        Map<String, EmbObj> map2 = new HashMap<String, EmbObj>();
        map2.put("test", new EmbObj("val", 22));
        map2.put("test2", new EmbObj("vvv", 42));
        o.setMap2(map2);

        Map<String, List<EmbObj>> map1 = new HashMap<String, List<EmbObj>>();
        List<EmbObj> objLst = new ArrayList<EmbObj>();
        objLst.add(new EmbObj("in list 1", 7331));
        objLst.add(new EmbObj("in list 2", 57));
        objLst.add(new EmbObj("in list too", 42));
        map1.put("1st", objLst);
        objLst = new ArrayList<EmbObj>();
        objLst.add(new EmbObj("in list again 1", 731));
        objLst.add(new EmbObj("in list again 2", 527));
        objLst.add(new EmbObj("in list again too", 421));
        map1.put("2nd", objLst);

        o.setMap1(map1);

        MorphiumSingleton.get().store(o);

        MapListObject ml = MorphiumSingleton.get().findById(CMapListObject.class, o.getId());
        assert (ml != null) : "Not Found?!?!?!?";
        assert (ml.getMapListValue().get("eins-fuenf-drei").size() == 3);
        assert (ml.getMapListValue().get("zweihundert").size() == 4);
    }


    @Embedded
    public static class EmbObj {
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

    public static class CMapListObject extends MapListObject {
        private Map<String, List<EmbObj>> map1;
        private Map<String, EmbObj> map2;

        public Map<String, List<EmbObj>> getMap1() {
            return map1;
        }

        public void setMap1(Map<String, List<EmbObj>> map1) {
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
