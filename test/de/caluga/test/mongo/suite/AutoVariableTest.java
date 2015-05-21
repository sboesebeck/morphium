package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.query.Query;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 22.11.13
 * Time: 09:15
 * To change this template use File | Settings | File Templates.
 */
public class AutoVariableTest extends MongoTest {

    @Test
    public void testCreationTime() throws Exception {
        MorphiumSingleton.get().dropCollection(CTimeTest.class);
        CTimeTest ct = new CTimeTest();
        ct.value = "A test";

        MorphiumSingleton.get().store(ct);

        assert (ct.created != null);
        assert (ct.timestamp != 0);

        Query<CTimeTest> q = MorphiumSingleton.get().createQueryFor(CTimeTest.class).f("value").eq("annother test");
        MorphiumSingleton.get().set(q, "additional", "value", true, true);
        assert (q.countAll() == 1) : "Count wrong: " + q.countAll();
        assert (q.get().timestamp != 0);
        assert (q.get().created != null);
        assert (q.get().value.equals("annother test"));

        q = MorphiumSingleton.get().createQueryFor(CTimeTest.class).f("value").eq("additional test");
        MorphiumSingleton.get().push(q, "lst", "value", true, true);
        assert (q.countAll() == 1) : "Count wrong: " + q.countAll();
        assert (q.get().timestamp != 0);
        assert (q.get().created != null);
        assert (q.get().value.equals("additional test"));
        assert (q.get().lst.size() == 1);


        List<CTimeTest> lst = new ArrayList<CTimeTest>();
        for (int i = 0; i < 100; i++) {
            ct = new CTimeTest();
            ct.value = "value" + i;
            ct.additional = "auch";
            lst.add(ct);
        }
        MorphiumSingleton.get().storeList(lst);

        for (CTimeTest tst : q.q().asIterable()) {
            assert (tst.timestamp != 0);
            assert (tst.created != null);
            assert (tst.createdString != null);
        }
    }

    @Test
    public void testLastAccess() throws Exception {
        MorphiumSingleton.get().dropCollection(LATest.class);

        LATest la = new LATest();
        la.value = "value1";
        MorphiumSingleton.get().store(la);

        la = new LATest();
        la.value = "value2";
        MorphiumSingleton.get().store(la);

        la = MorphiumSingleton.get().createQueryFor(LATest.class).f("value").eq("value1").get();
        assert (la.lastAccess != 0);
        assert (la.lastAccessDate != null);
        long lastAcc = la.lastAccess;
        Thread.sleep(1); //just to be sure
        la = MorphiumSingleton.get().createQueryFor(LATest.class).f("value").eq("value1").get();
        assert (la.lastAccess != lastAcc);
        assert (la.lastAccessString != null);

    }

    @Test
    public void testAutoVariablesBulkWrite() throws Exception {
        MorphiumSingleton.get().dropCollection(CTimeTest.class);
        List<CTimeTest> lst = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            CTimeTest ct = new CTimeTest();
            ct.additional = "tst";
            ct.value = "value" + i;
            lst.add(ct);
        }

        MorphiumSingleton.get().storeList(lst);

        lst = MorphiumSingleton.get().createQueryFor(CTimeTest.class).asList();
        for (CTimeTest t : lst) {
            assert (t.created != null);
            assert (t.timestamp != 0);
        }
        log.info("Done");

    }

    @Test
    public void testLastChange() throws Exception {
        MorphiumSingleton.get().dropCollection(LCTest.class);
        LCTest lc = new LCTest();
        lc.value = "value1";
        MorphiumSingleton.get().store(lc);

        lc = new LCTest();
        lc.value = "value2";
        MorphiumSingleton.get().store(lc);

        lc = new LCTest();
        lc.value = "value3";
        MorphiumSingleton.get().store(lc);

        lc = MorphiumSingleton.get().createQueryFor(LCTest.class).f("value").eq("value1").get();
        long created = lc.lastChange;
        Thread.sleep(10);
        lc.value = "different";
        MorphiumSingleton.get().store(lc);
        assert (lc.lastChange != 0);
        assert (lc.lastChangeDate != null);
        assert (lc.lastChange > created);

        Query<LCTest> q = MorphiumSingleton.get().createQueryFor(LCTest.class);
        MorphiumSingleton.get().set(q, "value", "all_same", false, true);
        long cmp = 0;
        for (LCTest tst : q.asIterable()) {
            if (cmp == 0) cmp = tst.lastChange;
            assert (tst.lastChange != 0);
            assert (tst.lastChange == cmp);
            assert (tst.lastChangeDate != null);
            assert (tst.lastChangeString != null);
        }


    }

    @Test
    public void testCTNonOjbectId() throws Exception {
        MorphiumSingleton.get().dropCollection(CTimeTestStringId.class);
        CTimeTestStringId record = new CTimeTestStringId();
        record.mongoId = "12345";
        record.value = "v1";
        MorphiumSingleton.get().store(record);


        record = new CTimeTestStringId();
        record.mongoId = "12346";
        record.value = "v2";
        MorphiumSingleton.get().store(record);

        record = new CTimeTestStringId();
        record.mongoId = "12346";
        record.value = "v3";
        MorphiumSingleton.get().store(record);

        Query<CTimeTestStringId> q = MorphiumSingleton.get().createQueryFor(CTimeTestStringId.class);
        q = q.f("value").eq("v1");
        record = q.get();
        assert (record.created != null);
        assert (record.timestamp != 0);
        long created = record.timestamp;

        record.value = "v1*";
        MorphiumSingleton.get().store(record);
        record = q.q().f("value").eq("v1*").get();
        assert (record.timestamp == created);

        q = q.q().f("value").eq("new");
        MorphiumSingleton.get().set(q, "additional", "1111", true, true);
        record = q.get();
        assert (record.timestamp != 0);


        ArrayList<CTimeTestStringId> lst = new ArrayList<CTimeTestStringId>();
        for (int i = 0; i < 100; i++) {
            CTimeTestStringId ct = new CTimeTestStringId();
            ct.mongoId = "" + i;
            ct.value = "v" + i;
            ct.additional = "add";
            lst.add(ct);
        }
        MorphiumSingleton.get().storeList(lst);

        for (CTimeTestStringId ct : q.q().asIterable()) {
            assert (ct.timestamp != 0);
            assert (ct.created != null);
        }
    }


    @Entity
    @NoCache
    @LastChange
    public static class LCTest {
        @Id
        private ObjectId mongoId;
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
        private ObjectId mongoId;
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
    @CreationTime(checkForNew = false)
    public static class CTimeTest {
        @Id
        private ObjectId mongoId;
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
