package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 29.05.12
 * Time: 15:54
 * <p/>
 * TODO: Add documentation here
 */
public class LastAccessTest extends MongoTest{
    @Test
    public void createdTest() throws Exception {
        TstObjLA tst=new TstObjLA();
        tst.setValue("A value");
        MorphiumSingleton.get().store(tst);
        assert(tst.getCreationTime()>0):"No creation time set?!?!?!";

        tst.setValue("Annother value");
        MorphiumSingleton.get().store(tst);
        assert(tst.getLastChange()>0):"No last change set?";

        tst.getValue();
        assert(tst.getLastAccess()>0):"No last_access set?";
    }


    @Entity
    @NoCache
    @StoreLastAccess
    @StoreLastChange
    @StoreCreationTime
    public static class TstObjLA {
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
