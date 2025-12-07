package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 29.06.12
 * Time: 15:09
 * <p>
 */
@Tag("core")
public class ListOfListTests extends MorphiumTestBase {
    @Test
    public void storeListOfLists() {
        morphium.clearCollection(LoLType.class);
        List<List<String>> val = new ArrayList<>();
        List<String> v1 = new ArrayList<>();
        v1.add("v1 - 1");
        v1.add("v1 - 2");
        v1.add("v1 - 3");
        val.add(v1);
        v1 = new ArrayList<>();
        v1.add("v2 - 1");
        v1.add("v2 - 2");
        v1.add("v2 - 3");
        v1.add("v2 - 4");
        val.add(v1);

        v1 = new ArrayList<>();
        v1.add("v3 - 1");
        v1.add("v3 - 2");
        val.add(v1);

        LoLType l = new LoLType();
        l.setLst(val);
        morphium.store(l);

        LoLType l2 = morphium.createQueryFor(LoLType.class).f("id").eq(l.id).get();
        assert (l2.lst.size() == l.lst.size()) : "Error in list sizes";
        assert (l2.lst.get(0).size() == l.lst.get(0).size()) : "error in sublist sizes";
        assert (l2.lst.get(1).get(0).equals(l.lst.get(1).get(0))) : "error in sublist values";
    }


    @Test
    public void jsonListTest() throws Exception {

        String s = "{ \"_id\" : \"5321be491c26b5f02eec7bd7\" , \"string_list\" : [ \"Hi\" , \"Ho\"] , \"uc_list\" : [ { \"counter\" : 5 } , { \"counter\" : 7} , { \"counter\" : 12 }]}";
        ListTypes l = morphium.getMapper().deserialize(ListTypes.class, s);
        System.out.println(l.getStringList().get(0));
        UncachedObject u;

        s = "{ \"_id\" : \"5321be491c26b5f02eec7bd7\" , \"string_list\" : [ \"Hi\" , \"Ho\"] , \"uc_list\" : [ { \"counter\" : 10 } , { \"counter\" : 12 } , { \"counter\" : 22 }],\"uc_lst_list\" : [[ { \"counter\" : 0 } , { \"counter\" : 1 } , { \"counter\" : 2 }]]}";
        l = morphium.getMapper().deserialize(ListTypes.class, s);
        System.out.println(l.getStringList().get(0));
        List<UncachedObject> lst = l.getUcLstList().get(0);
        u = lst.get(1);
        assert (u.getCounter() == 1);
        System.out.println("Done");
    }


    @Entity
    public static class ListTypes {
        @Id
        private MorphiumId id;
        private List<String> stringList;
        private List<UncachedObject> ucList;
        private List<List<UncachedObject>> ucLstList;

        public MorphiumId getId() {
            return id;
        }

        public List<List<UncachedObject>> getUcLstList() {
            return ucLstList;
        }

        public void setUcLstList(List<List<UncachedObject>> ucLstList) {
            this.ucLstList = ucLstList;
        }

        public void setId(MorphiumId id) {
            this.id = id;
        }

        public List<String> getStringList() {
            return stringList;
        }

        public void setStringList(List<String> stringList) {
            this.stringList = stringList;
        }

        public List<UncachedObject> getUcList() {
            return ucList;
        }

        public void setUcList(List<UncachedObject> ucList) {
            this.ucList = ucList;
        }
    }

    @Entity
    @WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
    @DefaultReadPreference(ReadPreferenceLevel.PRIMARY)
    public static class LoLType {
        @Id
        private MorphiumId id;

        private List<List<String>> lst;

        public List<List<String>> getLst() {
            return lst;
        }

        public void setLst(List<List<String>> lst) {
            this.lst = lst;
        }


    }
}
