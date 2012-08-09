package de.caluga.test.mongo.suite;

import com.mongodb.DBObject;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.annotations.Property;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 20.06.12
 * Time: 11:18
 * <p/>
 */
public class IndexTest extends MongoTest {
    @Test
    public void indexOnNewCollTest() throws Exception {
        MorphiumSingleton.get().clearCollection(IndexedObject.class);
        IndexedObject obj = new IndexedObject("test", 101);
        MorphiumSingleton.get().store(obj);

        //index should now be available
        List<DBObject> idx = MorphiumSingleton.get().getDatabase().getCollection("indexed_object").getIndexInfo();
        boolean foundId = false;
        boolean foundTimerName = false;
        boolean foundTimerName2 = true;
        boolean foundTimer = false;
        boolean foundName = false;

        for (DBObject i : idx) {
            System.out.println(i.toString());
            DBObject key = (DBObject) i.get("key");
            if (key.get("_id") != null && key.get("_id").equals(1)) {
                foundId = true;
            } else if (key.get("name") != null && key.get("name").equals(1) && key.get("timer") == null) {
                foundName = true;
            } else if (key.get("timer") != null && key.get("timer").equals(-1) && key.get("name") == null) {
                foundTimer = true;
            } else if (key.get("timer") != null && key.get("timer").equals(-1) && key.get("name") != null && key.get("name").equals(-1)) {
                foundTimerName2 = true;
            } else if (key.get("timer") != null && key.get("timer").equals(1) && key.get("name") != null && key.get("name").equals(-1)) {
                foundTimerName = true;
            }
        }
        log.info("Found indices id:" + foundId + " timer: " + foundTimer + " TimerName: " + foundTimerName + " name: " + foundName + " TimerName2: " + foundTimerName2);
        assert (foundId && foundTimer && foundTimerName && foundName && foundTimerName2);
    }

    @Entity
    @Index({"-name, timer", "-name, -timer"})
    public static class IndexedObject {
        @Property
        @Index(decrement = true)
        private int timer;

        @Index
        private String name;

        @Id
        ObjectId id;

        public IndexedObject() {
        }

        public IndexedObject(String name, int timer) {
            this.name = name;
            this.timer = timer;
        }

        public int getTimer() {
            return timer;
        }

        public void setTimer(int timer) {
            this.timer = timer;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public ObjectId getId() {
            return id;
        }

        public void setId(ObjectId id) {
            this.id = id;
        }
    }
}
