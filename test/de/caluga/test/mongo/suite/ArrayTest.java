package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.bson.MorphiumId;
import de.caluga.morphium.query.Query;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 01.06.12
 * Time: 15:27
 * <p/>
 */
public class ArrayTest extends MongoTest {

    @Entity
    @NoCache
    public static class ArrayTestObj {
        @Id
        private MorphiumId id;
        private String name;
        private int[] intArr;
        private String[] stringArr;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int[] getIntArr() {
            return intArr;
        }

        public void setIntArr(int[] intArr) {
            this.intArr = intArr;
        }

        public String[] getStringArr() {
            return stringArr;
        }

        public void setStringArr(String[] stringArr) {
            this.stringArr = stringArr;
        }
    }

    @Test
    public void testArrays() throws Exception {
        morphium.clearCollection(ArrayTestObj.class);
        ArrayTestObj obj = new ArrayTestObj();
        obj.setName("Name");
        obj.setIntArr(new int[]{1, 5, 3, 2});
        obj.setStringArr(new String[]{"test", "string", "array"});
        morphium.store(obj);

        Query<ArrayTestObj> q = morphium.createQueryFor(ArrayTestObj.class);
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        obj = q.get();
        assert (obj.getIntArr() != null && obj.getIntArr().length != 0) : "No ints found";
        assert (obj.getStringArr() != null && obj.getStringArr().length > 0) : "No strings found";
    }
}
