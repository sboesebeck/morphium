package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Test;

/**
 * User: Stephan Bösebeck
 * Date: 29.05.12
 * Time: 15:54
 * <p/>
 */
public class LastAccessTest extends MorphiumTestBase {
    @Test
    public void createdTest() throws Exception {
        morphium.dropCollection(TstObjLA.class);
        TstObjLA tst = new TstObjLA();
        tst.setValue("A value");
        morphium.store(tst);
        assert(tst.getCreationTime() > 0) : "No creation time set?!?!?!";
        long creationTime = tst.getCreationTime();
        Thread.sleep(100);
        tst.setValue("Annother value");
        morphium.store(tst);
        assert(tst.getLastChange() > 0) : "No last change set?";
        assert(tst.getLastChange() > creationTime) : "No last change set?";
        long lastChange = tst.getLastChange();
        assert(tst.getCreationTime() == creationTime) : "Creation time change? was: " + creationTime + " is " + tst.getCreationTime();
        Query<TstObjLA> q = morphium.createQueryFor(TstObjLA.class);
        Thread.sleep(100);
        tst = q.get();
        assert(tst.getLastAccess() > 0) : "No last_access set?";
        long lastAccess = tst.getLastAccess();
        assert(tst.getCreationTime() == creationTime) : "Creation time change?";
        assert(tst.getLastAccess() != tst.getCreationTime()) : "Last access == creation time";
        tst = q.asList().get(0);
        assert(tst.getLastAccess() > 0) : "No last_access set?";
        q = morphium.createQueryFor(TstObjLA.class);
        Thread.sleep(100);
        tst = q.get();
        assert(tst.getLastAccess() != lastAccess) : "Lat Access did not change?";
        assert(tst.getLastChange() == lastChange);
        assert(tst.getCreationTime() == creationTime);
    }

    @Test
    public void createOnUpsert() throws Exception {
        morphium.dropCollection(TstObjLA.class);
        morphium.set(morphium.createQueryFor(TstObjLA.class).f("int_value").eq(12), "value", "a test", true, false, null);
        Thread.sleep(100);
        TstObjLA tst = morphium.createQueryFor(TstObjLA.class).get();
        assert(tst.getIntValue() == 12);
        assert(tst.getValue().equals("a test"));
        assert(tst.getCreationTime() != 0);
    }

    @Test
    public void createdTestStringId() throws Exception {
        morphium.dropCollection(TstObjAutoValuesStringId.class);
        Thread.sleep(500);
        TstObjAutoValuesStringId tst = new TstObjAutoValuesStringId();
        tst.setId("test1");
        tst.setValue("A value");
        morphium.store(tst);
        assert(tst.getCreationTime() > 0) : "No creation time set?!?!?!";
        long creationTime = tst.getCreationTime();
        Thread.sleep(100);
        tst.setValue("Annother value");
        morphium.store(tst);
        assert(tst.getLastChange() > 0) : "No last change set?";
        assert(tst.getLastChange() > creationTime) : "No last change set?";
        long lastChange = tst.getLastChange();
        assert(tst.getCreationTime() == creationTime) : "Creation time change?";
        Query<TstObjAutoValuesStringId> q = morphium.createQueryFor(TstObjAutoValuesStringId.class);
        Thread.sleep(100);
        tst = q.get();
        assert(tst.getLastAccess() > 0) : "No last_access set?";
        long lastAccess = tst.getLastAccess();
        assert(tst.getCreationTime() == creationTime) : "Creation time change?";
        assert(tst.getLastAccess() != tst.getCreationTime()) : "Last access == creation time";
        tst = q.asList().get(0);
        assert(tst.getLastAccess() > 0) : "No last_access set?";
        q = morphium.createQueryFor(TstObjAutoValuesStringId.class);
        Thread.sleep(100);
        tst = q.get();
        assert(tst.getLastAccess() != lastAccess) : "Lat Access did not change?";
        assert(tst.getLastChange() == lastChange);
        assert(tst.getCreationTime() == creationTime);
    }

    @Test
    public void testLastAccessInc() throws Exception {
        Thread.sleep(200);
        TstObjLA la = new TstObjLA();
        la.setValue("value");
        morphium.store(la);
        long s = System.currentTimeMillis();

        while (morphium.findById(TstObjLA.class, la.id) == null) {
            Thread.sleep(100);
            assert(System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
        }

        morphium.reread(la);
        assert(la.creationTime != 0);
        assert(la.lastChange != 0);
        la.setValue("new Value");
        morphium.store(la);
        s = System.currentTimeMillis();

        while (morphium.findById(TstObjLA.class, la.id).getLastChange() == la.getCreationTime()) {
            Thread.sleep(100);
            assert(System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
        }

        morphium.reread(la);
        long lc = la.getLastChange();
        assert(la.getCreationTime() != la.getLastChange());
        morphium.set(la, "value", "set");
        morphium.reread(la);
        assert(lc != la.getLastChange());
        lc = la.getLastChange();
        la.setIntValue(41);
        Thread.sleep(100); //forcing last change to be later!
        morphium.store(la);
        s = System.currentTimeMillis();

        while (morphium.findById(TstObjLA.class, la.id).getLastChange() == lc) {
            Thread.sleep(100);
            assert(System.currentTimeMillis() - s < 15000);
        }

        morphium.reread(la);
        assert(lc != la.getLastChange());
        lc = la.getLastChange();
        morphium.inc(la, "int_value", 1);
        s = System.currentTimeMillis();

        while (morphium.findById(TstObjLA.class, la.id).getLastChange() == lc) {
            Thread.sleep(100);
            assert(System.currentTimeMillis() - s < 15000);
        }

        morphium.reread(la);
        assert(la.getIntValue() == 42);
        assert(lc != la.getLastChange());
        //now using ID
        lc = la.getLastChange();
        Query<TstObjLA> q = morphium.createQueryFor(TstObjLA.class).f("_id").eq(la.getId());
        morphium.set(q, "int_value", 1);
        s = System.currentTimeMillis();

        while (morphium.findById(TstObjLA.class, la.id).getLastChange() == lc) {
            Thread.sleep(100);
            assert(System.currentTimeMillis() - s < 15000);
        }

        morphium.reread(la);
        assert(la.getIntValue() == 1);
        assert(lc != la.getLastChange());
        lc = la.getLastChange();
        morphium.inc(q, "int_value", 41);
        s = System.currentTimeMillis();

        while (morphium.findById(TstObjLA.class, la.id).getLastChange() == lc) {
            Thread.sleep(100);
            assert(System.currentTimeMillis() - s < 15000);
        }

        morphium.reread(la);
        assert(la.getIntValue() == 42);
        assert(lc != la.getLastChange());
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
        private int intValue;

        public MorphiumId getId() {
            return id;
        }

        public void setId(MorphiumId id) {
            this.id = id;
        }

        public int getIntValue() {
            return intValue;
        }

        public void setIntValue(int intValue) {
            this.intValue = intValue;
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
}
