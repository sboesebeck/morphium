package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.query.Query;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 29.05.12
 * Time: 15:54
 * <p/>
 * TODO: Add documentation here
 */
public class LastAccessTest extends MongoTest {
    @Test
    public void createdTest() throws Exception {
        TstObjLA tst = new TstObjLA();
        tst.setValue("A value");
        MorphiumSingleton.get().store(tst);
        assert (tst.getCreationTime() > 0) : "No creation time set?!?!?!";

        tst.setValue("Annother value");
        MorphiumSingleton.get().store(tst);
        assert (tst.getLastChange() > 0) : "No last change set?";

        Query<TstObjLA> q = MorphiumSingleton.get().createQueryFor(TstObjLA.class);
        tst = q.get();
        assert (tst.getLastAccess() > 0) : "No last_access set?";

        tst = q.asList().get(0);
        assert (tst.getLastAccess() > 0) : "No last_access set?";
    }


    @Entity
    @NoCache
    @StoreLastAccess
    @StoreLastChange
    @StoreCreationTime
    public static class TstObjLA {
        @Id
        private ObjectId id;

        @LastAccess
        private long lastAccess;

        @LastChange
        private long lastChange;

        @CreationTime
        private long creationTime;

        private String value;

        public long getLastAccess() {
            return lastAccess;
        }

        public void setLastAccess(long lastAccess) {
            this.lastAccess = lastAccess;
        }

        public long getLastChange() {
            return lastChange;
        }

        public void setLastChange(long lastChange) {
            this.lastChange = lastChange;
        }

        public long getCreationTime() {
            return creationTime;
        }

        public void setCreationTime(long creationTime) {
            this.creationTime = creationTime;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
