package de.caluga.test.mongo.suite;

import de.caluga.morphium.Collation;
import de.caluga.morphium.Utils;
import de.caluga.morphium.async.AsyncCallbackAdapter;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.MongoField;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class QueryTest extends MorphiumTestBase {


    @Test
    public void testWhere() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.where("this.value=5");

        assert (q.toQueryObject().get("$where").equals("this.value=5"));
    }

    @Test
    public void testF() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        MongoField<UncachedObject> f = q.f("_id");
        assert (f.getFieldString().equals("_id"));

        f = q.q().f(UncachedObject.Fields.morphiumId);
        assert (f.getFieldString().equals("_id"));

        MongoField<ComplexObject> f2 = morphium.createQueryFor(ComplexObject.class).f(ComplexObject.Fields.entityEmbeded, UncachedObject.Fields.counter);
        assert (f2.getFieldString().equals("entityEmbeded.counter"));
        f2 = morphium.createQueryFor(ComplexObject.class).f(ComplexObject.Fields.entityEmbeded, UncachedObject.Fields.morphiumId);
        assert (f2.getFieldString().equals("entityEmbeded._id"));

        f2 = morphium.createQueryFor(ComplexObject.class).f("entity_embeded", "counter");
        assert (f2.getFieldString().equals("entityEmbeded.counter"));
    }

    public void testGetServer() {
    }

    @Test
    public void testOverrideDB() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        assert (q.getDB().equals(morphium.getConfig().getDatabase()));
        q.overrideDB("testDB");
        assert (q.getDB().equals("testDB"));
    }

    @Test
    public void testGetCollation() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.setCollation(new Collation());
        assert (q.getCollation() != null);
    }

    @Test
    public void testOr() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        Query<UncachedObject> o1 = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gt(10);
        Query<UncachedObject> o2 = morphium.createQueryFor(UncachedObject.class).f("value").eq("test");
        q.or(o1, o2);

        Map<String, Object> qo = q.toQueryObject();
        assert (qo.get("$or") != null);
        assert (((List) qo.get("$or")).size() == 2);
        assert (((List<Map>) qo.get("$or")).get(0).get("counter") != null);
        assert (((List<Map>) qo.get("$or")).get(1).get("value") != null);
    }


    @Test
    public void testNor() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        Query<UncachedObject> o1 = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gt(10);
        Query<UncachedObject> o2 = morphium.createQueryFor(UncachedObject.class).f("value").eq("test");
        q.nor(o1, o2);

        Map<String, Object> qo = q.toQueryObject();
        assert (qo.get("$nor") != null);
        assert (((List) qo.get("$nor")).size() == 2);
        assert (((List<Map>) qo.get("$nor")).get(0).get("counter") != null);
        assert (((List<Map>) qo.get("$nor")).get(1).get("value") != null);
    }

    @Test
    public void testLimit() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.limit(10);
        assert (q.getLimit() == 10);
    }

    @Test
    public void testSkip() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.skip(10);
        assert (q.getSkip() == 10);
    }

    @Test
    public void testSort() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.sort(UncachedObject.Fields.counter, UncachedObject.Fields.value);

        assert (q.getSort() != null);
        assert (q.getSort().get("counter").equals(Integer.valueOf(1)));
        assert (q.getSort().get("value").equals(Integer.valueOf(1)));

        int cnt = 0;
        for (String s : q.getSort().keySet()) {
            assert (cnt < 2);
            assert cnt != 0 || (s.equals("counter"));
            assert cnt != 1 || (s.equals("value"));
            cnt++;
        }
    }

    @Test
    public void testSortEnum() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.sortEnum(Utils.getMap((Enum) UncachedObject.Fields.counter, -1).add(UncachedObject.Fields.value, 1));

        assert (q.getSort() != null);
        assert (q.getSort().get("counter").equals(Integer.valueOf(-1)));
        assert (q.getSort().get("value").equals(Integer.valueOf(1)));

        int cnt = 0;
        for (String s : q.getSort().keySet()) {
            assert (cnt < 2);
            assert cnt != 0 || (s.equals("counter"));
            assert cnt != 1 || (s.equals("value"));
            cnt++;
        }
    }

    @Test
    public void testCountAll() throws Exception {
        createUncachedObjects(10);
        Thread.sleep(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.f(UncachedObject.Fields.counter).lt(100);
        q.limit(1);
        assert (q.countAll() == 10) : "Wrong amount: " + q.countAll();
    }

    @Test
    public void testAsyncCountAll() throws Exception {
        createUncachedObjects(10);
        Thread.sleep(100);
        final AtomicLong c = new AtomicLong(0);

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.f(UncachedObject.Fields.counter).lt(100);
        q.limit(1);

        q.countAll(new AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                c.set((Long) param[0]);
            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {

            }
        });
        Thread.sleep(500);
        assert (c.get() == 10);
    }

    public void testGet() {
    }

    public void testTestGet() {
    }

    public void testIdList() {
    }

    public void testTestIdList() {
    }

    @Test
    public void testQ() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.f("counter").eq(10);
        q.where("this.test=5");
        q.limit(12);
        q.sort("value");

        assert (q.q().getSort() == null || q.q().getSort().isEmpty());
        assert (q.q().getWhere() == null);
        assert (q.q().toQueryObject().size() == 0);
        assert (q.q().getLimit() == 0);
    }

    @Test
    public void testComplexQuery() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(100);

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        List<UncachedObject> lst = q.complexQuery(Utils.getMap("counter", 12));
        assert (lst.size() == 1);

    }

    public void testGetARHelper() {
    }

    public void testSetARHelpter() {
    }

    public void testTestComplexQuery() {
    }

    public void testTestComplexQuery1() {
    }

    public void testComplexQueryCount() {
    }

    public void testComplexQueryOne() {
    }

    public void testTestComplexQueryOne() {
    }

    public void testTestComplexQueryOne1() {
    }

    public void testGetLimit() {
    }

    public void testGetSkip() {
    }

    public void testGetSort() {
    }

    public void testTestClone() {
    }

    public void testGetReadPreferenceLevel() {
    }

    public void testSetReadPreferenceLevel() {
    }

    public void testGetWhere() {
    }

    public void testGetMorphium() {
    }

    public void testSetMorphium() {
    }

    public void testSetExecutor() {
    }

    public void testGetById() {
    }

    public void testTestGetById() {
    }

    public void testSet() {
    }

    public void testTestSet() {
    }

    public void testSetEnum() {
    }

    public void testTestSet1() {
    }

    public void testTestSet2() {
    }

    public void testTestSet3() {
    }

    public void testTestSetEnum() {
    }

    public void testTestSet4() {
    }

    public void testTestSet5() {
    }

    public void testTestSet6() {
    }

    public void testTestSetEnum1() {
    }

    public void testTestSet7() {
    }

    public void testTestSet8() {
    }

    public void testTestSet9() {
    }

    public void testTestSetEnum2() {
    }

    public void testTestSet10() {
    }

    public void testPush() {
    }

    public void testTestPush() {
    }

    public void testTestPush1() {
    }

    public void testTestPush2() {
    }

    public void testPushAll() {
    }

    public void testTestPushAll() {
    }

    public void testTestPushAll1() {
    }

    public void testTestPushAll2() {
    }

    public void testPullAll() {
    }

    public void testTestPullAll() {
    }

    public void testTestPullAll1() {
    }

    public void testTestPullAll2() {
    }

    public void testPull() {
    }

    public void testTestPull() {
    }

    public void testTestPull1() {
    }

    public void testTestPull2() {
    }

    public void testInc() {
    }

    public void testTestInc() {
    }

    public void testTestInc1() {
    }

    public void testTestInc2() {
    }

    public void testTestInc3() {
    }

    public void testTestInc4() {
    }

    public void testTestInc5() {
    }

    public void testTestInc6() {
    }

    public void testTestInc7() {
    }

    public void testTestInc8() {
    }

    public void testTestInc9() {
    }

    public void testTestInc10() {
    }

    public void testTestInc11() {
    }

    public void testTestInc12() {
    }

    public void testTestInc13() {
    }

    public void testTestInc14() {
    }

    public void testTestInc15() {
    }

    public void testTestInc16() {
    }

    public void testTestInc17() {
    }

    public void testTestInc18() {
    }

    public void testDec() {
    }

    public void testTestDec() {
    }

    public void testTestDec1() {
    }

    public void testTestDec2() {
    }

    public void testTestDec3() {
    }

    public void testTestDec4() {
    }

    public void testTestDec5() {
    }

    public void testTestDec6() {
    }

    public void testTestDec7() {
    }

    public void testTestDec8() {
    }

    public void testTestDec9() {
    }

    public void testTestDec10() {
    }

    public void testTestDec11() {
    }

    public void testTestDec12() {
    }

    public void testTestDec13() {
    }

    public void testTestDec14() {
    }

    public void testGetNumberOfPendingRequests() {
    }

    public void testGetCollectionName() {
    }

    public void testSetCollectionName() {
    }

    public void testTextSearch() {
    }

    public void testTestTextSearch() {
    }

    public void testTestF1() {
    }

    public void testTestF2() {
    }

    public void testDelete() {
    }

    public void testFindOneAndDelete() {
    }

    public void testFindOneAndUpdate() {
    }

    public void testFindOneAndUpdateEnums() {
    }

    public void testIsAutoValuesEnabled() {
    }

    public void testSetAutoValuesEnabled() {
    }

    public void testGetTags() {
    }

    public void testAddTag() {
    }

    public void testDisableAutoValues() {
    }

    public void testEnableAutoValues() {
    }

    public void testText() {
    }

    public void testTestText() {
    }

    public void testTestText1() {
    }

    public void testTestText2() {
    }

    public void testTestText3() {
    }

    public void testSetProjection() {
    }

    public void testAddProjection() {
    }

    public void testTestAddProjection() {
    }

    public void testTestAddProjection1() {
    }

    public void testTestSetProjection() {
    }

    public void testTestAddProjection2() {
    }

    public void testHideFieldInProjection() {
    }

    public void testTestHideFieldInProjection() {
    }

    public void testGetFieldListForQuery() {
    }

    public void testDistinct() {
    }
}