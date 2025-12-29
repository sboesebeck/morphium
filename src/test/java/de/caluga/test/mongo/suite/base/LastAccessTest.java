package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static de.caluga.test.mongo.suite.base.TestUtils.waitForConditionToBecomeTrue;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

/**
 * User: Stephan Bösebeck
 * Date: 29.05.12
 * Time: 15:54
 * <p>
 */
@Tag("core")
public class LastAccessTest extends MultiDriverTestBase {
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void createdTest(Morphium morphium) throws Exception  {
        morphium.dropCollection(TstObjLA.class);
        TstObjLA tst = new TstObjLA();
        tst.setValue("A value");
        morphium.store(tst);
        assert(tst.getCreationTime() > 0) : "No creation time set?!?!?!";
        long creationTime = tst.getCreationTime();

        // Wait until we can verify the object exists and enough time has passed
        waitForConditionToBecomeTrue(5000, "Object not found after store",
            () -> morphium.createQueryFor(TstObjLA.class).countAll() > 0);
        Thread.sleep(50); // Small delay to ensure timestamp difference

        tst.setValue("Annother value");
        morphium.store(tst);
        assert(tst.getLastChange() > 0) : "No last change set?";
        assert(tst.getLastChange() > creationTime) : "No last change set?";
        long lastChange = tst.getLastChange();
        assert(tst.getCreationTime() == creationTime) : "Creation time change? was: " + creationTime + " is " + tst.getCreationTime();

        Query<TstObjLA> q = morphium.createQueryFor(TstObjLA.class);
        // Wait for lastAccess to be set (happens on read)
        final long prevLastAccess = tst.getLastAccess();
        waitForConditionToBecomeTrue(5000, "lastAccess not updated on query",
            () -> {
                TstObjLA found = q.get();
                return found != null && found.getLastAccess() > 0 && found.getLastAccess() != prevLastAccess;
            });

        tst = q.get();
        assert(tst.getLastAccess() > 0) : "No last_access set?";
        long lastAccess = tst.getLastAccess();
        assert(tst.getCreationTime() == creationTime) : "Creation time change?";
        assert(tst.getLastAccess() != tst.getCreationTime()) : "Last access == creation time";
        tst = q.asList().get(0);
        assert(tst.getLastAccess() > 0) : "No last_access set?";

        Query<TstObjLA> q2 = morphium.createQueryFor(TstObjLA.class);
        // Wait for lastAccess to change again
        final long currentLastAccess = lastAccess;
        waitForConditionToBecomeTrue(5000, "lastAccess did not change on second query",
            () -> {
                TstObjLA found = q2.get();
                return found != null && found.getLastAccess() != currentLastAccess;
            });

        tst = q2.get();
        assert(tst.getLastAccess() != lastAccess) : "Last Access did not change?";
        assert(tst.getLastChange() == lastChange);
        assert(tst.getCreationTime() == creationTime);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void createOnUpsert(Morphium morphium) throws Exception  {
        morphium.dropCollection(TstObjLA.class);
        morphium.set(morphium.createQueryFor(TstObjLA.class).f("int_value").eq(12), "value", "a test", true, false, null);

        // Wait for the upserted object to be available
        waitForConditionToBecomeTrue(5000, "Upserted object not found",
            () -> morphium.createQueryFor(TstObjLA.class).countAll() > 0);

        TstObjLA tst = morphium.createQueryFor(TstObjLA.class).get();
        assert(tst.getIntValue() == 12);
        assert(tst.getValue().equals("a test"));
        assert(tst.getCreationTime() != 0);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void createdTestStringId(Morphium morphium) throws Exception  {
        morphium.dropCollection(TstObjAutoValuesStringId.class);

        // Wait for collection to be dropped
        waitForConditionToBecomeTrue(5000, "Collection not dropped",
            () -> !morphium.exists(TstObjAutoValuesStringId.class));

        TstObjAutoValuesStringId tst = new TstObjAutoValuesStringId();
        tst.setId("test1");
        tst.setValue("A value");
        morphium.store(tst);
        assert(tst.getCreationTime() > 0) : "No creation time set?!?!?!";
        long creationTime = tst.getCreationTime();

        // Wait until we can verify the object exists and enough time has passed
        waitForConditionToBecomeTrue(5000, "Object not found after store",
            () -> morphium.createQueryFor(TstObjAutoValuesStringId.class).countAll() > 0);
        Thread.sleep(50); // Small delay to ensure timestamp difference

        tst.setValue("Annother value");
        morphium.store(tst);
        assert(tst.getLastChange() > 0) : "No last change set?";
        assert(tst.getLastChange() > creationTime) : "No last change set?";
        long lastChange = tst.getLastChange();
        assert(tst.getCreationTime() == creationTime) : "Creation time change?";

        Query<TstObjAutoValuesStringId> q = morphium.createQueryFor(TstObjAutoValuesStringId.class);
        // Wait for lastAccess to be set (happens on read)
        final long prevLastAccess = tst.getLastAccess();
        waitForConditionToBecomeTrue(5000, "lastAccess not updated on query",
            () -> {
                TstObjAutoValuesStringId found = q.get();
                return found != null && found.getLastAccess() > 0 && found.getLastAccess() != prevLastAccess;
            });

        tst = q.get();
        assert(tst.getLastAccess() > 0) : "No last_access set?";
        long lastAccess = tst.getLastAccess();
        assert(tst.getCreationTime() == creationTime) : "Creation time change?";
        assert(tst.getLastAccess() != tst.getCreationTime()) : "Last access == creation time";
        tst = q.asList().get(0);
        assert(tst.getLastAccess() > 0) : "No last_access set?";

        Query<TstObjAutoValuesStringId> q2 = morphium.createQueryFor(TstObjAutoValuesStringId.class);
        // Wait for lastAccess to change again
        final long currentLastAccess = lastAccess;
        waitForConditionToBecomeTrue(5000, "lastAccess did not change on second query",
            () -> {
                TstObjAutoValuesStringId found = q2.get();
                return found != null && found.getLastAccess() != currentLastAccess;
            });

        tst = q2.get();
        assert(tst.getLastAccess() != lastAccess) : "Last Access did not change?";
        assert(tst.getLastChange() == lastChange);
        assert(tst.getCreationTime() == creationTime);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testLastAccessInc(Morphium morphium) throws Exception  {
        TstObjLA la = new TstObjLA();
        la.setValue("value");
        morphium.store(la);

        // Wait for object to be stored
        final MorphiumId laId = la.id;
        waitForConditionToBecomeTrue(10000, "Object not found after store",
            () -> morphium.findById(TstObjLA.class, laId) != null);

        morphium.reread(la);
        assert(la.creationTime != 0);
        assert(la.lastChange != 0);
        la.setValue("new Value");
        morphium.store(la);

        // Wait for lastChange to be updated
        final long creationTime = la.getCreationTime();
        waitForConditionToBecomeTrue(10000, "lastChange not updated after store",
            () -> {
                TstObjLA found = morphium.findById(TstObjLA.class, laId);
                return found != null && found.getLastChange() != creationTime;
            });

        morphium.reread(la);
        long lc = la.getLastChange();
        assert(la.getCreationTime() != la.getLastChange());
        morphium.setInEntity(la, "value", "set");
        morphium.reread(la);
        assert(lc != la.getLastChange());
        lc = la.getLastChange();
        la.setIntValue(41);
        Thread.sleep(50); // Small delay to ensure timestamp difference
        morphium.store(la);

        // Wait for lastChange to update
        final long lc1 = lc;
        waitForConditionToBecomeTrue(15000, "lastChange not updated after setIntValue",
            () -> {
                TstObjLA found = morphium.findById(TstObjLA.class, laId);
                return found != null && found.getLastChange() != lc1;
            });

        morphium.reread(la);
        assert(lc != la.getLastChange());
        lc = la.getLastChange();
        morphium.inc(la, "int_value", 1);

        // Wait for inc to complete
        final long lc2 = lc;
        waitForConditionToBecomeTrue(15000, "lastChange not updated after inc",
            () -> {
                TstObjLA found = morphium.findById(TstObjLA.class, laId);
                return found != null && found.getLastChange() != lc2;
            });

        morphium.reread(la);
        assert(la.getIntValue() == 42);
        assert(lc != la.getLastChange());

        // Now using ID query
        lc = la.getLastChange();
        Query<TstObjLA> q = morphium.createQueryFor(TstObjLA.class).f("_id").eq(la.getId());
        morphium.set(q, "int_value", 1);

        final long lc3 = lc;
        waitForConditionToBecomeTrue(15000, "lastChange not updated after set via query",
            () -> {
                TstObjLA found = morphium.findById(TstObjLA.class, laId);
                return found != null && found.getLastChange() != lc3;
            });

        morphium.reread(la);
        assert(la.getIntValue() == 1);
        assert(lc != la.getLastChange());
        lc = la.getLastChange();
        morphium.inc(q, "int_value", 41);

        final long lc4 = lc;
        waitForConditionToBecomeTrue(15000, "lastChange not updated after inc via query",
            () -> {
                TstObjLA found = morphium.findById(TstObjLA.class, laId);
                return found != null && found.getLastChange() != lc4;
            });

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
