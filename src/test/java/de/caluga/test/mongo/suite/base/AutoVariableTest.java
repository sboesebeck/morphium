package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 22.11.13
 * Time: 09:15
 * To change this template use File | Settings | File Templates.
 */
@Tag("core")
public class AutoVariableTest extends MorphiumTestBase {

    @Test
    public void disableAutoVariablesThreadded() throws Exception {
        //side Thread
        Thread t = new Thread() {
            public void run() {
                morphium.disableAutoValuesForThread();
                CTimeTest ct = new CTimeTest();
                ct.value = "should not work";
                morphium.store(ct);
                assert(ct.created == null);
                assert(ct.timestamp == 0);
                morphium.reread(ct);
                assert(ct.created == null);
                assert(ct.timestamp == 0);
                LCTest lc = new LCTest();
                lc.value = "a test";
                morphium.store(lc);
                assert(lc.lastChange == 0);
                assert(lc.lastChangeDate == null);
                assert(lc.lastChangeString == null);
                lc.value = "updated";
                morphium.store(lc);
                assert(lc.lastChange == 0);
                assert(lc.lastChangeDate == null);
                assert(lc.lastChangeString == null);
                morphium.setInEntity(lc, "value", "set", false, null);

                TestUtils.waitForConditionToBecomeTrue(2000, "SetInEntity not persisted",
                    () -> {
                        try {
                            var obj = morphium.findById(LCTest.class, lc.morphiumId);
                            return obj != null && "set".equals(obj.value);
                        } catch (Exception e) {
                            return false;
                        }
                    });

                morphium.reread(lc);
                assert(lc.lastChange == 0);
                assert(lc.lastChangeDate == null);
                assert(lc.lastChangeString == null);
                assert(lc.value.equals("set"));
                morphium.createQueryFor(LCTest.class).f("_id").eq(lc.morphiumId).set("value", "set");

                TestUtils.waitForConditionToBecomeTrue(2000, "Query set not persisted",
                    () -> {
                        try {
                            var obj = morphium.findById(LCTest.class, lc.morphiumId);
                            return obj != null && "set".equals(obj.value);
                        } catch (Exception e) {
                            return false;
                        }
                    });

                morphium.reread(lc);
                assert(lc.lastChange == 0);
                assert(lc.lastChangeDate == null);
                assert(lc.lastChangeString == null);
                LATest la = new LATest();
                la.value = "last access";
                morphium.store(la);

                final MorphiumId laId = la.morphiumId;
                TestUtils.waitForConditionToBecomeTrue(2000, "LATest not persisted",
                    () -> morphium.findById(LATest.class, laId) != null);

                la = morphium.findById(LATest.class, la.morphiumId);
                assert(la.lastAccess == 0);
            }
        };
        t.start();
        CTimeTest ct = new CTimeTest();
        ct.value = "should not work";
        morphium.store(ct);
        TestUtils.waitForConditionToBecomeTrue(2000, "CTimeTest not persisted",
            () -> morphium.findById(CTimeTest.class, ct.morphiumId) != null);
        assertNotNull(ct.created);
        ;
        assert(ct.timestamp != 0);
        morphium.reread(ct);
        assertNotNull(ct.created);
        ;
        assert(ct.timestamp != 0);
        LCTest lc = new LCTest();
        lc.value = "a test";
        morphium.store(lc);
        TestUtils.waitForConditionToBecomeTrue(2000, "LCTest not persisted",
            () -> morphium.findById(LCTest.class, lc.morphiumId) != null);
        assert(lc.lastChange != 0);
        assertNotNull(lc.lastChangeDate);
        ;
        assertNotNull(lc.lastChangeString);
        ;
        lc.value = "updated";
        morphium.store(lc);
        TestUtils.waitForConditionToBecomeTrue(2000, "LCTest update not persisted",
            () -> {
                var obj = morphium.findById(LCTest.class, lc.morphiumId);
                return obj != null && "updated".equals(obj.value);
            });
        assert(lc.lastChange != 0);
        assertNotNull(lc.lastChangeDate);
        ;
        assertNotNull(lc.lastChangeString);
        ;
        morphium.setInEntity(lc, "value", "set", false, null);
        TestUtils.waitForConditionToBecomeTrue(2000, "SetInEntity not persisted",
            () -> {
                var obj = morphium.findById(LCTest.class, lc.morphiumId);
                return obj != null && "set".equals(obj.value);
            });
        morphium.reread(lc);
        assert(lc.lastChange != 0);
        assertNotNull(lc.lastChangeDate);
        ;
        assertNotNull(lc.lastChangeString);
        ;
        assert(lc.value.equals("set"));
        morphium.createQueryFor(LCTest.class).f("_id").eq(lc.morphiumId).set("value", "set");
        TestUtils.waitForConditionToBecomeTrue(2000, "Query set not persisted",
            () -> {
                var obj = morphium.findById(LCTest.class, lc.morphiumId);
                return obj != null && "set".equals(obj.value);
            });
        morphium.reread(lc);
        assert(lc.lastChange != 0);
        assertNotNull(lc.lastChangeDate);
        ;
        assertNotNull(lc.lastChangeString);
        ;
        LATest la = new LATest();
        la.value = "last access";
        morphium.store(la);
        final MorphiumId laId = la.morphiumId;
        TestUtils.waitForConditionToBecomeTrue(2000, "LATest not persisted",
            () -> morphium.findById(LATest.class, laId) != null);
        long stored = System.currentTimeMillis();
        la = morphium.findById(LATest.class, la.morphiumId);
        assert(la.lastAccess != 0);
        assert(la.lastAccess > stored);

        while (t.isAlive()) {
            Thread.yield();
        }
    }

    @Test
    public void disableAutoValues() throws Exception {
        morphium.getConfig().objectMappingSettings().disableAutoValues();
        CTimeTest ct = new CTimeTest();
        ct.value = "should not work";
        morphium.store(ct);
        assert(ct.created == null);
        assert(ct.timestamp == 0);
        morphium.reread(ct);
        assert(ct.created == null);
        assert(ct.timestamp == 0);
        LCTest lc = new LCTest();
        lc.value = "a test";
        morphium.store(lc);
        assert(lc.lastChange == 0);
        assert(lc.lastChangeDate == null);
        assert(lc.lastChangeString == null);
        lc.value = "updated";
        morphium.store(lc);
        assert(lc.lastChange == 0);
        assert(lc.lastChangeDate == null);
        assert(lc.lastChangeString == null);
        morphium.setInEntity(lc, "value", "set", false, null);
        morphium.reread(lc);
        assert(lc.lastChange == 0);
        assert(lc.lastChangeDate == null);
        assert(lc.lastChangeString == null);
        assert(lc.value.equals("set"));
        morphium.createQueryFor(LCTest.class).f("_id").eq(lc.morphiumId).set("value", "set");
        morphium.reread(lc);
        assert(lc.lastChange == 0);
        assert(lc.lastChangeDate == null);
        assert(lc.lastChangeString == null);
        LATest la = new LATest();
        la.value = "last access";
        morphium.store(la);
        final MorphiumId laId2 = la.morphiumId;
        TestUtils.waitForConditionToBecomeTrue(2000, "LATest not persisted",
            () -> morphium.findById(LATest.class, laId2) != null);
        la = morphium.findById(LATest.class, la.morphiumId);
        assert(la.lastAccess == 0);
        morphium.getConfig().objectMappingSettings().enableAutoValues();
    }

    @Test
    public void testCreationTime() throws Exception {
        morphium.dropCollection(CTimeTest.class);
        TestUtils.waitForCollectionToBeDeleted(morphium, CTimeTest.class);
        CTimeTest ct = new CTimeTest();
        ct.value = "A test";
        morphium.store(ct);
        TestUtils.waitForConditionToBecomeTrue(2000, "CTimeTest not persisted",
            () -> morphium.createQueryFor(CTimeTest.class).countAll() == 1);
        assertNotNull(ct.created);
        assert(ct.timestamp != 0);
        Query<CTimeTest> q = morphium.createQueryFor(CTimeTest.class).f("value").eq("annother test");
        q.set("additional", "value", true, true, null);
        TestUtils.waitForConditionToBecomeTrue(2000, "Query upsert not persisted",
            () -> morphium.createQueryFor(CTimeTest.class).f("value").eq("annother test").countAll() == 1);
        assert(q.countAll() == 1) : "Count wrong: " + q.countAll();
        assert(q.get().timestamp != 0);
        assertNotNull(q.get().created);
        ;
        assert(q.get().value.equals("annother test"));
        q = morphium.createQueryFor(CTimeTest.class).f("value").eq("additional test");
        morphium.push(q, "lst", "value", true, true);
        assert(q.countAll() == 1) : "Count wrong: " + q.countAll();
        assert(q.get().timestamp != 0);
        assertNotNull(q.get().created);
        ;
        assert(q.get().value.equals("additional test"));
        assert(q.get().lst.size() == 1);
        List<CTimeTest> lst = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            ct = new CTimeTest();
            ct.value = "value" + i;
            ct.additional = "auch";
            lst.add(ct);
        }

        morphium.storeList(lst);

        for (CTimeTest tst : q.q().asIterable()) {
            assert(tst.timestamp != 0);
            assertNotNull(tst.created);
            ;
            assertNotNull(tst.createdString);
            ;
        }
    }

    @Test
    public void testLastAccess() throws Exception {
        morphium.dropCollection(LATest.class);
        LATest la = new LATest();
        la.value = "value1";
        morphium.store(la);
        la = new LATest();
        la.value = "value2";
        morphium.store(la);
        TestUtils.waitForConditionToBecomeTrue(2000, "LATest objects not persisted",
            () -> morphium.createQueryFor(LATest.class).countAll() == 2);
        la = morphium.createQueryFor(LATest.class).f("value").eq("value1").get();
        assert(la.lastAccess != 0);
        assertNotNull(la.lastAccessDate);
        long lastAcc = la.lastAccess;
        la = morphium.createQueryFor(LATest.class).f("value").eq("value1").get();
        assertNotEquals(lastAcc, la.lastAccess);
        assertNotNull(la.lastAccessString);
    }

    @Test
    public void testAutoVariablesBulkWrite() throws Exception {
        morphium.dropCollection(CTimeTest.class);
        List<CTimeTest> lst = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            CTimeTest ct = new CTimeTest();
            ct.additional = "tst";
            ct.value = "value" + i;
            lst.add(ct);
        }

        morphium.storeList(lst);
        TestUtils.waitForConditionToBecomeTrue(2000, "Bulk write not persisted",
            () -> morphium.createQueryFor(CTimeTest.class).countAll() == 100);
        lst = morphium.createQueryFor(CTimeTest.class).asList();

        for (CTimeTest t : lst) {
            assertNotNull(t.created);
            assertNotEquals(0, t.timestamp);
        }

        log.info("Done");
    }

    @Test
    public void testLastChange() throws Exception {
        morphium.dropCollection(LCTest.class);
        LCTest lc = new LCTest();
        lc.value = "value1";
        morphium.store(lc);
        lc = new LCTest();
        lc.value = "value2";
        morphium.store(lc);
        lc = new LCTest();
        lc.value = "value3";
        morphium.store(lc);
        TestUtils.waitForConditionToBecomeTrue(2000, "LCTest objects not persisted",
            () -> morphium.createQueryFor(LCTest.class).countAll() == 3);
        lc = morphium.createQueryFor(LCTest.class).f("value").eq("value1").get();
        long created = lc.lastChange;
        lc.value = "different";
        morphium.store(lc);
        TestUtils.waitForConditionToBecomeTrue(2000, "LCTest update not persisted",
            () -> {
                var obj = morphium.createQueryFor(LCTest.class).f("value").eq("different").get();
                return obj != null;
            });
        assert(lc.lastChange != 0);
        assertNotNull(lc.lastChangeDate);
        assert(lc.lastChange > created);
        Query<LCTest> q = morphium.createQueryFor(LCTest.class);
        q.set("value", "all_same", false, true);
        long cmp = 0;

        for (LCTest tst : q.asIterable()) {
            if (cmp == 0) {
                cmp = tst.lastChange;
            }

            assert(tst.lastChange != 0);
            assert(tst.lastChange == cmp) : "Last change wrong cmp: " + cmp + " but is: " + tst.lastChange;
            assertNotNull(tst.lastChangeDate);
            ;
            assertNotNull(tst.lastChangeString);
            ;
        }
    }

    @Test
    public void testCTNonOjbectId() throws Exception {
        morphium.getConfig().objectMappingSettings().setCheckForNew(true);
        morphium.dropCollection(CTimeTestStringId.class);
        TestUtils.waitForCollectionToBeDeleted(morphium, CTimeTestStringId.class);
        //        while (morphium.getDriver().exists(morphium.getConfig().getDatabase(), morphium.getMapper().getCollectionName(CTimeTestStringId.class))) {
        //            Thread.sleep(100);
        //            log.info("... waiting...");
        //        }
        //        Thread.sleep(1000);
        CTimeTestStringId record = new CTimeTestStringId();
        record.mongoId = "12345";
        record.value = "v1";
        morphium.store(record);
        record = new CTimeTestStringId();
        record.mongoId = "12346";
        record.value = "v2";
        morphium.store(record);
        record = new CTimeTestStringId();
        record.mongoId = "12347";
        record.value = "v3";
        morphium.store(record);
        TestUtils.waitForConditionToBecomeTrue(2000, "CTimeTestStringId objects not persisted",
            () -> morphium.createQueryFor(CTimeTestStringId.class).countAll() == 3);
        Query<CTimeTestStringId> q = morphium.createQueryFor(CTimeTestStringId.class);
        q = q.f("value").eq("v1");
        record = q.get();
        assertNotNull(record.created);
        ;
        assert(record.timestamp != 0);
        long created = record.timestamp;
        record.value = "v1*";
        morphium.store(record);
        TestUtils.waitForConditionToBecomeTrue(2000, "CTimeTestStringId update not persisted",
            () -> morphium.createQueryFor(CTimeTestStringId.class).f("value").eq("v1*").get() != null);
        record = q.q().f("value").eq("v1*").get();
        assertNotNull(record);
        assert(record.timestamp == created) : "Record timestamp " + record.timestamp;
        q = q.q().f("value").eq("new");
        q.set("additional", "1111", true, true);
        TestUtils.waitForConditionToBecomeTrue(2000, "Query upsert not persisted",
            () -> morphium.createQueryFor(CTimeTestStringId.class).f("value").eq("new").get() != null);
        record = q.get();
        assert(record.timestamp != 0);
        ArrayList<CTimeTestStringId> lst = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            CTimeTestStringId ct = new CTimeTestStringId();
            ct.mongoId = "" + i;
            ct.value = "v" + i;
            ct.additional = "add";
            lst.add(ct);
        }

        morphium.storeList(lst);
        Thread.sleep(150);

        for (CTimeTestStringId ct : q.q().asIterable()) {
            assert(ct.timestamp != 0);
            assertNotNull(ct.created);
            ;
        }
    }


    @Entity
    @NoCache
    @LastChange
    @WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
    public static class LCTest {
        @Id
        private MorphiumId morphiumId;
        private String value;
        @LastChange
        private long lastChange;
        @LastChange
        private Date lastChangeDate;
        @LastChange
        private String lastChangeString;
    }

    @Entity
    @NoCache
    @LastAccess
    public static class LATest {
        @Id
        private MorphiumId morphiumId;
        private String value;
        @LastAccess
        private long lastAccess;
        @LastAccess
        private Date lastAccessDate;
        @LastAccess
        private String lastAccessString;
    }

    @Entity
    @NoCache
    @CreationTime()
    public static class CTimeTest {
        @Id
        private MorphiumId morphiumId;
        private String value;
        private String additional;
        private List<String> lst;

        @CreationTime
        private Date created;

        @CreationTime
        private long timestamp;

        @CreationTime
        private String createdString;

    }


    @Entity
    @NoCache
    @CreationTime(checkForNew = true)
    public static class CTimeTestStringId {
        @Id
        private String mongoId;
        private String value;
        private String additional;
        private List<String> lst;

        @CreationTime
        private Date created;

        @CreationTime
        private long timestamp;


    }
}
