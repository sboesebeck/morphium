package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.SafetyLevel;
import de.caluga.morphium.annotations.WriteSafety;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 29.06.12
 * Time: 15:09
 * <p/>
 * TODO: Add documentation here
 */
public class ListOfListTests extends MongoTest {
    @Test
    public void storeListOfLists() throws Exception {
        MorphiumSingleton.get().clearCollection(LoLType.class);
        List<List<String>> val = new ArrayList<List<String>>();
        List<String> v1 = new ArrayList<String>();
        v1.add("v1 - 1");
        v1.add("v1 - 2");
        v1.add("v1 - 3");
        val.add(v1);
        v1 = new ArrayList<String>();
        v1.add("v2 - 1");
        v1.add("v2 - 2");
        v1.add("v2 - 3");
        v1.add("v2 - 4");
        val.add(v1);

        v1 = new ArrayList<String>();
        v1.add("v3 - 1");
        v1.add("v3 - 2");
        val.add(v1);

        LoLType l = new LoLType();
        l.setLst(val);
        MorphiumSingleton.get().store(l);

        LoLType l2 = (LoLType) MorphiumSingleton.get().createQueryFor(LoLType.class).f("id").eq(l.id).get();
        assert (l2.lst.size() == l.lst.size()) : "Error in list sizes";
        assert (l2.lst.get(0).size() == l.lst.get(0).size()) : "error in sublist sizes";
        assert (l2.lst.get(1).get(0).equals(l.lst.get(1).get(0))) : "error in sublist values";
    }


    @Entity
    @WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
    public static class LoLType {
        @Id
        private ObjectId id;

        private List<List<String>> lst;

        public List<List<String>> getLst() {
            return lst;
        }

        public void setLst(List<List<String>> lst) {
            this.lst = lst;
        }

    }
}
