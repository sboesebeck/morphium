package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.bson.MorphiumId;
import de.caluga.morphium.query.Query;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 29.05.12
 * Time: 15:54
 * <p/>
 */
public class LastAccessTest extends MongoTest {
    @Test
    public void createdTest() throws Exception {
        morphium.dropCollection(TstObjLA.class);
        TstObjLA tst = new TstObjLA();
        tst.setValue("A value");
        morphium.store(tst);
        assert (tst.getCreationTime() > 0) : "No creation time set?!?!?!";
        long creationTime = tst.getCreationTime();
        Thread.sleep(1000);


        tst.setValue("Annother value");
        morphium.store(tst);
        assert (tst.getLastChange() > 0) : "No last change set?";
        assert (tst.getLastChange() > creationTime) : "No last change set?";
        long lastChange = tst.getLastChange();
        assert (tst.getCreationTime() == creationTime) : "Creation time change?";

        Query<TstObjLA> q = morphium.createQueryFor(TstObjLA.class);
        Thread.sleep(1000);
        tst = q.get();
        assert (tst.getLastAccess() > 0) : "No last_access set?";
        long lastAccess = tst.getLastAccess();
        assert (tst.getCreationTime() == creationTime) : "Creation time change?";
        assert (tst.getLastAccess() != tst.getCreationTime()) : "Last access == creation time";

        tst = q.asList().get(0);
        assert (tst.getLastAccess() > 0) : "No last_access set?";

        q = morphium.createQueryFor(TstObjLA.class);
        Thread.sleep(1000);
        tst = q.get();
        assert (tst.getLastAccess() != lastAccess) : "Lat Access did not change?";
        assert (tst.getLastChange() == lastChange);
        assert (tst.getCreationTime() == creationTime);

    }


    @Test
    public void createdTestStringId() throws Exception {
        morphium.dropCollection(TstObjAutoValuesStringId.class);
        TstObjAutoValuesStringId tst = new TstObjAutoValuesStringId();
        tst.setId("test1");
        tst.setValue("A value");
        morphium.store(tst);
        assert (tst.getCreationTime() > 0) : "No creation time set?!?!?!";
        long creationTime = tst.getCreationTime();
        Thread.sleep(1000);


        tst.setValue("Annother value");
        morphium.store(tst);
        assert (tst.getLastChange() > 0) : "No last change set?";
        assert (tst.getLastChange() > creationTime) : "No last change set?";
        long lastChange = tst.getLastChange();
        assert (tst.getCreationTime() == creationTime) : "Creation time change?";

        Query<TstObjAutoValuesStringId> q = morphium.createQueryFor(TstObjAutoValuesStringId.class);
        Thread.sleep(1000);
        tst = q.get();
        assert (tst.getLastAccess() > 0) : "No last_access set?";
        long lastAccess = tst.getLastAccess();
        assert (tst.getCreationTime() == creationTime) : "Creation time change?";
        assert (tst.getLastAccess() != tst.getCreationTime()) : "Last access == creation time";

        tst = q.asList().get(0);
        assert (tst.getLastAccess() > 0) : "No last_access set?";

        q = morphium.createQueryFor(TstObjAutoValuesStringId.class);
        Thread.sleep(1000);
        tst = q.get();
        assert (tst.getLastAccess() != lastAccess) : "Lat Access did not change?";
        assert (tst.getLastChange() == lastChange);
        assert (tst.getCreationTime() == creationTime);

    }


    @Entity
    @NoCache
    @WriteSafety(level = SafetyLevel.MAJORITY)
    @LastAccess
    @LastChange
    @CreationTime(checkForNew = true)
    public static class TstObjAutoValuesStringId {
        @Id
        private String id;

        @LastAccess
        private long lastAccess;

        @LastChange
        private long lastChange;

        @CreationTime
        private long creationTime;

        private String value;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

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

    @Entity
    @NoCache
    @WriteSafety(level = SafetyLevel.MAJORITY)
    @LastAccess
    @LastChange
    @CreationTime
    public static class TstObjLA {
        @Id
        private MorphiumId id;

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
