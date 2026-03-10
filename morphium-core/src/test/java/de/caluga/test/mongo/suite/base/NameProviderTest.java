package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.DefaultNameProvider;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.NameProvider;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.SafetyLevel;
import de.caluga.morphium.annotations.WriteSafety;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * User: Stephan Bösebeck
 * Date: 19.06.12
 * Time: 11:53
 * <p>
 */
@Tag("core")
public class NameProviderTest extends MultiDriverTestBase {
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testNameProvider(Morphium morphium) {
        String colName = morphium.getMapper().getCollectionName(LogObject.class);
        assert (colName.endsWith("_Test"));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testStoreWithNameProvider(Morphium morphium) {
        morphium.dropCollection(LogObject.class);
        morphium.dropCollection(LogObject.class, "LogObject_Test", null);
        for (int i = 0; i < 100; i++) {
            LogObject lo = new LogObject();
            lo.setLevel(12);
            lo.setMsg("My Message " + i);
            lo.setTimestamp(System.currentTimeMillis());
            morphium.store(lo);
        }
        waitForAsyncOperationsToStart(morphium, 1000);
        TestUtils.waitForWrites(morphium, log);
        String colName = morphium.getMapper().getCollectionName(LogObject.class);
        assert (colName.endsWith("_Test"));
        //        DBCollection col = morphium.getDatabase().getCollection(colName);
        long count = morphium.createQueryFor(LogObject.class, colName).countAll();
        assert (count == 100) : "Error - did not store?? " + count;
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void overrideNameProviderTest(Morphium morphium) {
        morphium.clearCollection(UncachedObject.class);
        morphium.getMapper().setNameProviderForClass(UncachedObject.class, new MyNp());
        String col = morphium.getMapper().getCollectionName(UncachedObject.class);
        assert (col.equals("UncachedObject_Test")) : "Error - name is wrong: " + col;
        morphium.getMapper().setNameProviderForClass(UncachedObject.class, new DefaultNameProvider());
    }

    public static class MyNp implements NameProvider {
        public MyNp() {
        }

        @Override
        public String getCollectionName(Class<?> type, MorphiumObjectMapper om, boolean translateCamelCase, boolean useFQN, String specifiedName, Morphium morphium) {
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
        private MorphiumId id;


        public MorphiumId getId() {
            return id;
        }

        public void setLog(MorphiumId log) {
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
