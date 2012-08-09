package de.caluga.test.mongo.suite;

import com.mongodb.DBCollection;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.NameProvider;
import de.caluga.morphium.ObjectMapper;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.SafetyLevel;
import de.caluga.morphium.annotations.WriteSafety;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 19.06.12
 * Time: 11:53
 * <p/>
 * TODO: Add documentation here
 */
public class NameProviderTest extends MongoTest {
    @Test
    public void testNameProvider() throws Exception {
        String colName = MorphiumSingleton.get().getConfig().getMapper().getCollectionName(LogObject.class);
        assert (colName.endsWith("_Test"));
    }

    @Test
    public void testStoreWithNameProvider() throws Exception {
        MorphiumSingleton.get().clearCollection(LogObject.class);
        for (int i = 0; i < 100; i++) {
            LogObject lo = new LogObject();
            lo.setLevel(12);
            lo.setMsg("My Message " + i);
            lo.setTimestamp(System.currentTimeMillis());
            MorphiumSingleton.get().store(lo);
        }
        waitForWrites();
        String colName = MorphiumSingleton.get().getConfig().getMapper().getCollectionName(LogObject.class);
        assert (colName.endsWith("_Test"));
        DBCollection col = MorphiumSingleton.get().getDatabase().getCollection(colName);
        assert (col.getCount() == 100) : "Error - did not store??" + col.getCount();
    }


    @Test
    public void overrideNameProviderTest() throws Exception {
        MorphiumSingleton.get().clearCollection(UncachedObject.class);
        MorphiumSingleton.get().getConfig().getMapper().setNameProviderForClass(UncachedObject.class, new MyNp());
        String col = MorphiumSingleton.get().getConfig().getMapper().getCollectionName(UncachedObject.class);
        assert (col.equals("UncachedObject_Test")) : "Error - name is wrong: " + col;
    }

    public static class MyNp implements NameProvider {
        public MyNp() {
        }

        @Override
        public String getCollectionName(Class<?> type, ObjectMapper om, boolean translateCamelCase, boolean useFQN, String specifiedName, Morphium morphium) {
            return type.getSimpleName() + "_Test";
        }
    }

    @Entity(nameProvider = MyNp.class)
    @WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
    public static class LogObject {
        private String msg;
        private int level;
        private long timestamp;
        @Id
        private ObjectId id;


        public ObjectId getId() {
            return id;
        }

        public void setLog(ObjectId log) {
            this.id = log;
        }

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }

        public int getLevel() {
            return level;
        }

        public void setLevel(int level) {
            this.level = level;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }
}
