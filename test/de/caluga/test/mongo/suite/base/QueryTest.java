package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Collation;
import de.caluga.morphium.Utils;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.async.AsyncCallbackAdapter;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.MongoField;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.*;
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
        Query<UncachedObject> o2 = morphium.createQueryFor(UncachedObject.class).f("strValue").eq("test");
        q.or(o1, o2);

        Map<String, Object> qo = q.toQueryObject();
        assert (qo.get("$or") != null);
        assert (((List) qo.get("$or")).size() == 2);
        assert (((List<Map>) qo.get("$or")).get(0).get("counter") != null);
        assert (((List<Map>) qo.get("$or")).get(1).get("str_value") != null);
    }


    @Test
    public void testNor() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        Query<UncachedObject> o1 = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gt(10);
        Query<UncachedObject> o2 = morphium.createQueryFor(UncachedObject.class).f("strValue").eq("test");
        q.nor(o1, o2);

        Map<String, Object> qo = q.toQueryObject();
        assert (qo.get("$nor") != null);
        assert (((List) qo.get("$nor")).size() == 2);
        assert (((List<Map>) qo.get("$nor")).get(0).get("counter") != null);
        assert (((List<Map>) qo.get("$nor")).get(1).get("str_value") != null);
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
        q.sort(UncachedObject.Fields.counter, UncachedObject.Fields.strValue);

        assert (q.getSort() != null);
        assert (q.getSort().get("counter").equals(Integer.valueOf(1)));
        assert (q.getSort().get("str_value").equals(Integer.valueOf(1)));

        int cnt = 0;
        for (String s : q.getSort().keySet()) {
            assert (cnt < 2);
            assert cnt != 0 || (s.equals("counter"));
            assert cnt != 1 || (s.equals("str_value"));
            cnt++;
        }
    }

    @Test
    public void testSortEnum() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.sortEnum(Utils.getMap((Enum) UncachedObject.Fields.counter, -1).add(UncachedObject.Fields.strValue, 1));

        assert (q.getSort() != null);
        assert (q.getSort().get("counter").equals(Integer.valueOf(-1)));
        assert (q.getSort().get("str_value").equals(Integer.valueOf(1)));

        int cnt = 0;
        for (String s : q.getSort().keySet()) {
            assert (cnt < 2);
            assert cnt == 0 || (s.equals("counter"));
            assert cnt == 1 || (s.equals("str_value"));
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
    public void testCountAllWhere() throws Exception {
        createUncachedObjects(10);
        Thread.sleep(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.where("this.counter<100");
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
        Thread.sleep(1500);
        assert (c.get() == 10);
    }


    @Test
    public void testQ() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.f("counter").eq(10);
        q.where("this.test=5");
        q.limit(12);
        q.sort("strValue");

        assert (q.q().getSort() == null || q.q().getSort().isEmpty());
        assert (q.q().getWhere() == null);
        assert (q.q().toQueryObject().size() == 0);
        assert (q.q().getLimit() == 0);
    }

    @Test
    public void testRawQuery() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(100);

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        List<UncachedObject> lst = q.rawQuery(Utils.getMap("counter", 12)).asList();
        assert (lst.size() == 1);

    }

    @Test
    public void testSet() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(100);
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(42).set(UncachedObject.Fields.strValue, "changed", false, false, null);
        Thread.sleep(50);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("changed").asList();
        assert (lst.size() == 1);
    }

    @Test
    public void testTestSet() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(100);
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).lt(3).set(UncachedObject.Fields.strValue, "changed", true, true, null);
        Thread.sleep(550);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("changed").asList();
        assert (lst.size() == 2);
    }

    @Test
    public void testSetEnum() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(100);
        Map<Enum, Object> m = new HashMap<>();
        m.put(UncachedObject.Fields.strValue, "changed");
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(42).setEnum(m, false, false);
        Thread.sleep(50);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("changed").asList();
        assert (lst.size() == 1);
    }

    @Test
    public void testSetEnum2() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(100);
        Map<Enum, Object> m = new HashMap<>();
        m.put(UncachedObject.Fields.strValue, "changed");
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).lt(3).setEnum(m, false, true);
        Thread.sleep(50);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("changed").asList();
        assert (lst.size() == 2);
    }

    @Test
    public void testSetEnum3() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(200);
        Map<Enum, Object> m = new HashMap<>();
        m.put(UncachedObject.Fields.strValue, "changed");
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gt(1000).f(UncachedObject.Fields.counter).lt(1002).setEnum(m, true, true);
        Thread.sleep(50);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("changed").asList();
        assert (lst.size() == 1);
    }


    @Test
    public void testSetEnumAsync() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(500);
        Map<Enum, Object> m = new HashMap<>();
        m.put(UncachedObject.Fields.strValue, "changed");
        AtomicLong cnt = new AtomicLong(0);
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).lt(2).setEnum(m, new AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                log.info("success!");
                log.info(Utils.toJsonString(param));
                cnt.incrementAndGet();
            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {

            }
        });
        while (cnt.get() == 0) {
            Thread.yield();
        }
        Thread.sleep(500);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("changed").asList();
        assert (lst.size() == 1);
    }


    @Test
    public void testSetUpsert() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(200);
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(10002).set(UncachedObject.Fields.strValue, "new", true, true, null);
        Thread.sleep(50);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("new").asList();
        assert (lst.size() == 1);
        assert (lst.get(0).getCounter() == 10002);
    }

    @Test
    public void testTestSet2() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(200);
        Map<String, Object> m = new HashMap<>();
        m.put(UncachedObject.Fields.strValue.name(), "changed");
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).lt(2).set(m);
        Thread.sleep(250);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("changed").asList();
        assert (lst.size() == 1);
    }

    @Test
    public void testTestSet3() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(100);
        Map<String, Object> m = new HashMap<>();
        m.put(UncachedObject.Fields.strValue.name(), "new");
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(10002).set(m, true, true, null);
        Thread.sleep(250);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("new").asList();
        assert (lst.size() == 1);
        assert (lst.get(0).getCounter() == 10002);
    }


    @Test
    public void testPush() throws Exception {
        UncachedObject uc = new UncachedObject("value", 10055);
        morphium.store(uc);
        long s = System.currentTimeMillis();
        while (morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).countAll() == 0) {
            Thread.sleep(100);
            assert (System.currentTimeMillis() - s < 5000);

        }
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.morphiumId).eq(uc.getMorphiumId())
                .push(UncachedObject.Fields.intData, 42);
        morphium.reread(uc);
        assert (uc.getIntData() != null);
        assert (uc.getIntData()[0] == 42);
    }

    @Test
    public void testTestPush() throws Exception {
        UncachedObject uc = new UncachedObject("value", 10055);
        morphium.store(uc);
        long s = System.currentTimeMillis();
        while (morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).countAll() == 0) {
            Thread.sleep(100);
            assert (System.currentTimeMillis() - s < 5000);
        }
        morphium.push(uc, UncachedObject.Fields.intData, 42, false);
        assert (uc.getIntData() != null);
        assert (uc.getIntData()[0] == 42);
        morphium.push(uc, UncachedObject.Fields.intData, 123, false);
        assert (uc.getIntData() != null);
        assert (uc.getIntData()[0] == 42);
        assert (uc.getIntData()[1] == 123);
    }

    @Test
    public void testPushAll() throws Exception {
        UncachedObject uc = new UncachedObject("value", 10055);
        morphium.store(uc);
        long s = System.currentTimeMillis();
        while (morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).countAll() == 0) {
            Thread.sleep(100);
            assert (System.currentTimeMillis() - s < 5000);

        }
        List<Integer> lst = Arrays.asList(42, 123);

        morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).pushAll(UncachedObject.Fields.intData, lst);
        Thread.sleep(500);
        morphium.reread(uc);
        assert (uc.getIntData() != null);
        assert (uc.getIntData()[0] == 42);
        assert (uc.getIntData()[1] == 123);
    }

    @Test
    public void testPull() throws Exception {
        UncachedObject uc = new UncachedObject("value", 1021);
        uc.setIntData(new int[]{12, 23, 52, 42});
        morphium.store(uc);
        long s = System.currentTimeMillis();
        while (morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).countAll() == 0) {
            Thread.sleep(100);
            assert (System.currentTimeMillis() - s < 5000);

        }
        morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).pull(UncachedObject.Fields.intData, 12, false, false, null);
        morphium.reread(uc);
        assert (uc.getIntData().length == 3);
        assert (uc.getIntData()[0] != 12);
    }

    @Test
    public void testTestPull() throws Exception {
        UncachedObject uc = new UncachedObject("value", 1021);
        uc.setIntData(new int[]{12, 23, 52, 42});
        morphium.store(uc);
        long s = System.currentTimeMillis();
        while (morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).countAll() == 0) {
            Thread.sleep(100);
            assert (System.currentTimeMillis() - s < 5000);

        }
        morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).pull(UncachedObject.Fields.intData, Expr.lte(Expr.intExpr(20)), false, false, null);
        morphium.reread(uc);
        assert (uc.getIntData().length == 3);
        assert (uc.getIntData()[0] != 12);

    }

    @Test
    public void testInc() throws Exception {
        createUncachedObjects(10);
        Thread.sleep(50);
        morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.counter).lt(5)
                .inc(UncachedObject.Fields.counter, 100);
        Thread.sleep(50);
        long cnt = morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.counter).gte(100).countAll();
        assert (cnt != 0);
        assert (cnt == 1);
    }

    @Test
    public void testInc2() throws Exception {
        createUncachedObjects(10);
        Thread.sleep(50);
        morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.counter).lt(5)
                .inc(UncachedObject.Fields.counter, 100, false, true);
        Thread.sleep(50);
        long cnt = morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.counter).gte(100).countAll();
        assert (cnt != 0);
        assert (cnt == 4);
    }


    @Test
    public void testInc3() throws Exception {
        createUncachedObjects(10);
        Thread.sleep(250);
        morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.counter).lt(5)
                .inc(UncachedObject.Fields.dval, 0.2, false, true);
        Thread.sleep(550);
        long cnt = morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.dval).eq(0.2).countAll();
        assert (cnt != 0);
        assert (cnt == 4);
    }

    @Test
    public void testIncAsync() throws Exception {
        createUncachedObjects(10);
        Thread.sleep(250);
        AtomicInteger ai = new AtomicInteger(0);
        morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.counter).lt(5)
                .inc(UncachedObject.Fields.dval, 0.2, false, true, new AsyncCallbackAdapter<UncachedObject>() {
                    @Override
                    public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                        ai.incrementAndGet();
                    }
                });
        Thread.sleep(550);
        assert (ai.get() == 1);
        long cnt = morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.dval).eq(0.2).countAll();
        assert (cnt != 0);
        assert (cnt == 4);
    }


    @Test
    public void testDec() throws Exception {
        createUncachedObjects(10);
        Thread.sleep(50);
        morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.counter).lt(5)
                .dec(UncachedObject.Fields.counter, 100);
        Thread.sleep(550);
        long cnt = morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.counter).lt(0).countAll();
        assert (cnt != 0);
        assert (cnt == 1);
    }


    @Test
    public void testDec2() throws Exception {
        createUncachedObjects(10);
        Thread.sleep(150);
        morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.counter).lt(5)
                .dec(UncachedObject.Fields.counter, 100, false, true);
        Thread.sleep(150);
        long cnt = morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.counter).lt(0).countAll();
        assert (cnt != 0);
        assert (cnt == 4);
    }


    @Test
    public void testDec3() throws Exception {
        createUncachedObjects(10);
        Thread.sleep(50);
        morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.counter).lt(5)
                .dec(UncachedObject.Fields.dval, 0.2, false, true);
        long cnt = 0;
        long s = System.currentTimeMillis();
        while (cnt < 4) {
            cnt = morphium.createQueryFor(UncachedObject.class)
                    .f(UncachedObject.Fields.dval).eq(-0.2).countAll();
            Thread.sleep(100);
            assert (System.currentTimeMillis() - s < 5000);
        }
        assert (cnt == 4);
    }

    @Test
    public void testDecAsync() throws Exception {
        createUncachedObjects(10);
        long s = System.currentTimeMillis();
        while (morphium.createQueryFor(UncachedObject.class).countAll() < 10) {
            Thread.sleep(50);
            assert (System.currentTimeMillis() - s < 5000);
        }
        AtomicInteger ai = new AtomicInteger(0);
        morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.counter).lt(5)
                .dec(UncachedObject.Fields.dval, 0.2, false, true, new AsyncCallbackAdapter<UncachedObject>() {
                    @Override
                    public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                        ai.incrementAndGet();
                    }
                });
        Thread.sleep(150);
        assert (ai.get() == 1);
        long cnt = morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.dval).eq(-0.2).countAll();
        assert (cnt != 0);
        assert (cnt == 4);
    }


    @Test
    public void testTestSetProjection() throws Exception {
        UncachedObject uc = new UncachedObject("test", 2);
        uc.setDval(3.14152);
        morphium.store(uc);
        long s = System.currentTimeMillis();
        while (morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).countAll() == 0) {
            Thread.sleep(100);
            assert (System.currentTimeMillis() - s < 5000);
        }

        Thread.sleep(150);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(2).
                setProjection(UncachedObject.Fields.counter, UncachedObject.Fields.dval).asList();
        assert (lst.size() == 1);
        assert (lst.get(0).getStrValue() == null);
        assert (lst.get(0).getDval() != 0);
        assert (lst.get(0).getCounter() != 0);
    }

    @Test
    public void testTestAddProjection2() throws Exception {
        UncachedObject uc = new UncachedObject("test", 22);
        uc.setDval(3.14152);
        morphium.store(uc);
        long s = System.currentTimeMillis();
        while (morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(22).countAll() == 0) {
            Thread.sleep(100);
            assert (System.currentTimeMillis() - s < 5000);

        }

        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(22).addProjection(UncachedObject.Fields.counter)
                .addProjection(UncachedObject.Fields.dval).asList();
        assert (lst.size() == 1) : "Count wrong: " + lst.size();
        assert (lst.get(0).getStrValue() == null);
        assert (lst.get(0).getDval() != 0);
        assert (lst.get(0).getCounter() != 0);
    }

    @Test
    public void testHideFieldInProjection() throws Exception {
        createUncachedObjects(10);
        //Thread.sleep(550);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(2).hideFieldInProjection(UncachedObject.Fields.strValue).asList();
        long s = System.currentTimeMillis();
        while (lst.size() < 1) {
            lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(2).hideFieldInProjection(UncachedObject.Fields.strValue).asList();
            Thread.sleep(50);
            assert (System.currentTimeMillis() - s < 5000);
        }
        assert (lst.size() == 1);
        assert (lst.get(0).getStrValue() == null);
    }


    @Test
    public void testSubDocs() throws Exception {
        SubDocTest sd = new SubDocTest();
        morphium.store(sd);

        Query<SubDocTest> q = morphium.createQueryFor(SubDocTest.class).f(SubDocTest.Fields.id).eq(sd.getId());

        q.push("sub_docs.test.subtest", "this value added");
        q.push("sub_docs.test.subtest", "this value added2");
        assert (q.get().subDocs.size() != 0);

    }

    @Entity
    public static class SubDocTest {
        @Id
        private MorphiumId id;
        private Map<String, Map<String, List<String>>> subDocs;

        public Map<String, Map<String, List<String>>> getSubDocs() {
            return subDocs;
        }

        public void setSubDocs(Map<String, Map<String, List<String>>> subDocs) {
            this.subDocs = subDocs;
        }

        public MorphiumId getId() {
            return id;
        }

        public void setId(MorphiumId id) {
            this.id = id;
        }

        public enum Fields {subDocs, id}
    }


}