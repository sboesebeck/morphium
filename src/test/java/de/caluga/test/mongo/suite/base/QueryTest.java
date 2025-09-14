package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Collation;
import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.async.AsyncCallbackAdapter;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.MongoField;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.ListContainer;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("core")
public class QueryTest extends MorphiumTestBase {

    @Test
    public void testQuery() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.or(q.q().f(UncachedObject.Fields.counter).lte(15),
             q.q().f("counter").gte(10),
             q.q().f("counter").lt(15).f("counter").gt(10).f("str_value").eq("hallo").f("strValue").ne("test")
            );
        Map<String, Object> dbObject = q.toQueryObject();
        assertNotNull(dbObject, "Map<String,Object> created is null?");
        String str = Utils.toJsonString(dbObject);
        assertNotNull(str, "ToString is NULL?!?!?");
        System.out.println("Query: " + str);
        assert(str.trim().equals("{ \"$or\" :  [ { \"counter\" : { \"$lte\" : 15 }  } , { \"counter\" : { \"$gte\" : 10 }  } , { \"$and\" :  [ { \"counter\" : { \"$lt\" : 15 }  } , { \"counter\" : { \"$gt\" : 10 }  } , { \"str_value\" : \"hallo\" } , { \"str_value\" : { \"$ne\" : \"test\" }  } ] } ] }"))
            : "Query-Object wrong";
        q = q.q();
        q.f("counter").gt(0).f("counter").lt(10);
        dbObject = q.toQueryObject();
        str = Utils.toJsonString(dbObject);
        System.out.println("Query: " + str);
        q = q.q(); //new query
        q = q.f("counter").mod(10, 5);
        dbObject = q.toQueryObject();
        str = Utils.toJsonString(dbObject);
        assertNotNull(str, "ToString is NULL?!?!?");
        System.out.println("Query: " + str);
        assert(str.trim().equals("{ \"counter\" : { \"$mod\" :  [ 10, 5] }  }")) : "Query wrong";
        q = q.q(); //new query
        q = q.f("counter").gte(5).f("counter").lte(10);
        q.or(q.q().f("counter").eq(15), q.q().f(UncachedObject.Fields.counter).eq(22));
        dbObject = q.toQueryObject();
        str = Utils.toJsonString(dbObject);
        assertNotNull(str, "ToString is NULL?!?!?");
        log.info("Query: " + str);
    }

    @Test
    public void testComplexAndOr() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").lt(100).or(q.q().f("counter").eq(50), q.q().f(UncachedObject.Fields.counter).eq(101));
        String s = Utils.toJsonString(q.toQueryObject());
        log.info("Query: " + s);
        assert(s.trim().equals("{ \"$and\" :  [ { \"counter\" : { \"$lt\" : 100 }  } , { \"$or\" :  [ { \"counter\" : 50 } , { \"counter\" : 101 } ] } ] }"));
    }

    @Test
    public void testOrder() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").lt(1000).f("str_value").eq("test");
        String str = Utils.toJsonString(q.toQueryObject());
        log.info("Query1: " + str);
        q = q.q();
        q = q.f("strValue").eq("test").f("counter").lt(1000);
        String str2 = Utils.toJsonString(q.toQueryObject());
        log.info("Query2: " + str2);
        assert(!str.equals(str2));
        q = q.q();
        q = q.f("str_value").eq("test").f("counter").lt(1000).f("counter").gt(10);
        str = Utils.toJsonString(q.toQueryObject());
        log.info("2nd Query1: " + str);
        q = q.q();
        q = q.f("counter").gt(10).f("strValue").eq("test").f("counter").lt(1000);
        str = Utils.toJsonString(q.toQueryObject());
        log.info("2nd Query2: " + str);
        assert(!str.equals(str2));
    }

    @Test
    public void testToString() {
        Query<ListContainer> q = morphium.createQueryFor(ListContainer.class);
        q = q.f(ListContainer.Fields.longList).size(10);
        String qStr = q.toString();
        log.info("ToString: " + qStr);
        log.info("query: " + Utils.toJsonString(q.toQueryObject()));
        assert(Utils.toJsonString(q.toQueryObject()).trim().equals("{ \"long_list\" : { \"$size\" : 10 }  }"));
    }

    @Test
    public void distinctTest() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            morphium.store(new UncachedObject("uc", i % 3));
        }

        Thread.sleep(100);
        List lt = morphium.createQueryFor(UncachedObject.class).distinct("counter");
        assert(lt.size() == 3);
    }

    @Test
    public void testSize() throws InterruptedException {
        ListContainer lc = new ListContainer();

        for (int i = 0; i < 10; i++) {
            lc.addLong(i);
        }

        lc.setName("A test");
        morphium.store(lc);
        lc = new ListContainer();

        for (int i = 0; i < 5; i++) {
            lc.addLong(i);
        }

        lc.setName("A test2");
        morphium.store(lc);
        Thread.sleep(100);
        Query<ListContainer> q = morphium.createQueryFor(ListContainer.class);
        q = q.f(ListContainer.Fields.longList).size(10);
        lc = q.get();
        assert(lc.getLongList().size() == 10);
        assert(lc.getName().equals("A test"));
    }

    @Test
    public void speedTest() throws Exception {
        int numThr = 100;
        List<Thread> threads = new ArrayList<>();
        long start = System.currentTimeMillis();

        for (int t = 0; t < numThr; t++) {
            Thread thread = new Thread(()-> {
                for (int i = 0; i < 100000; i++) {
                    Query q = morphium.createQueryFor(CachedObject.class);
                }
            });
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        threads.clear();
        long dur = System.currentTimeMillis() - start;
        log.info("Creating the query with " + numThr + " threads took " + dur + "ms");
        start = System.currentTimeMillis();

        for (int t = 0; t < numThr; t++) {
            Thread thread = new Thread(()-> {
                for (int i = 0; i < 100000; i++) {
                    morphium.createQueryFor(CachedObject.class).f("counter");
                }
            });
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        threads.clear();
        dur = System.currentTimeMillis() - start;
        log.info("Creating the query+field with " + numThr + " threads took " + dur + "ms");
        start = System.currentTimeMillis();

        for (int t = 0; t < numThr; t++) {
            Thread thread = new Thread(()-> {
                for (int i = 0; i < 100000; i++) {
                    morphium.createQueryFor(CachedObject.class).f("counter").eq(109);
                }
            });
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        threads.clear();
        dur = System.currentTimeMillis() - start;
        log.info("Creating the query+field+op with " + numThr + " threads took " + dur + "ms");
    }

    @Test
    public void testWhere() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.where("this.value=5");
        assert(q.toQueryObject().get("$where").equals("this.value=5"));
    }

    @Test
    public void testF() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        MongoField<UncachedObject> f = q.f("_id");
        assert(f.getFieldString().equals("_id"));
        f = q.q().f(UncachedObject.Fields.morphiumId);
        assert(f.getFieldString().equals("_id"));
        MongoField<ComplexObject> f2 = morphium.createQueryFor(ComplexObject.class).f(ComplexObject.Fields.entityEmbeded, UncachedObject.Fields.counter);
        assert(f2.getFieldString().equals("entityEmbeded.counter"));
        f2 = morphium.createQueryFor(ComplexObject.class).f(ComplexObject.Fields.entityEmbeded, UncachedObject.Fields.morphiumId);
        assert(f2.getFieldString().equals("entityEmbeded._id"));
        f2 = morphium.createQueryFor(ComplexObject.class).f("entity_embeded", "counter");
        assert(f2.getFieldString().equals("entityEmbeded.counter"));
    }

    public void testGetServer() {
    }

    @Test
    public void testOverrideDB() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        assert(q.getDB().equals(morphium.getConfig().getDatabase()));
        q.overrideDB("testDB");
        assert(q.getDB().equals("testDB"));
    }

    @Test
    public void testGetCollation() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.setCollation(new Collation());
        assertNotNull(q.getCollation());
        ;
    }

    @Test
    public void testOr() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        Query<UncachedObject> o1 = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gt(10);
        Query<UncachedObject> o2 = morphium.createQueryFor(UncachedObject.class).f("strValue").eq("test");
        q.or(o1, o2);
        Map<String, Object> qo = q.toQueryObject();
        assertNotNull(qo.get("$or"));
        ;
        assert(((List) qo.get("$or")).size() == 2);
        assertNotNull(((List<Map>) qo.get("$or")).get(0).get("counter"));
        ;
        assertNotNull(((List<Map>) qo.get("$or")).get(1).get("str_value"));
        ;
    }

    @Test
    public void testNor() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        Query<UncachedObject> o1 = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gt(10);
        Query<UncachedObject> o2 = morphium.createQueryFor(UncachedObject.class).f("strValue").eq("test");
        q.nor(o1, o2);
        Map<String, Object> qo = q.toQueryObject();
        assertNotNull(qo.get("$nor"));
        ;
        assert(((List) qo.get("$nor")).size() == 2);
        assertNotNull(((List<Map>) qo.get("$nor")).get(0).get("counter"));
        ;
        assertNotNull(((List<Map>) qo.get("$nor")).get(1).get("str_value"));
        ;
    }

    @Test
    public void testLimit() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.limit(10);
        assert(q.getLimit() == 10);
    }

    @Test
    public void testSkip() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.skip(10);
        assert(q.getSkip() == 10);
    }

    @Test
    public void testSort() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.sort(UncachedObject.Fields.counter, UncachedObject.Fields.strValue);
        assertNotNull(q.getSort());
        ;
        assert(q.getSort().get("counter").equals(Integer.valueOf(1)));
        assert(q.getSort().get("str_value").equals(Integer.valueOf(1)));
        int cnt = 0;

        for (String s : q.getSort().keySet()) {
            assert(cnt < 2);
            assert cnt != 0 || (s.equals("counter"));
            assert cnt != 1 || (s.equals("str_value"));
            cnt++;
        }
    }

    @Test
    public void testSortEnum() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.sortEnum(UtilsMap.of((Enum) UncachedObject.Fields.counter, -1, UncachedObject.Fields.strValue, 1));
        assertNotNull(q.getSort());
        ;
        assert(q.getSort().get("counter").equals(Integer.valueOf(-1)));
        assert(q.getSort().get("str_value").equals(Integer.valueOf(1)));
        int cnt = 0;

        for (String s : q.getSort().keySet()) {
            assert(cnt < 2);
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
        assert(q.countAll() == 10) : "Wrong amount: " + q.countAll();
    }

    @Test
    public void testCountAllWhere() throws Exception {
        createUncachedObjects(10);
        Thread.sleep(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.where("this.counter<100");
        q.limit(1);
        assert(q.countAll() == 10) : "Wrong amount: " + q.countAll();
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
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object ... param) {
                c.set((Long) param[0]);
            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object ... param) {
            }
        });

        long s = System.currentTimeMillis();

        while (c.get() != 10) {
            Thread.sleep(100);
            assert(System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
        }

        assert(c.get() == 10);
    }

    @Test
    public void testQ() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.f("counter").eq(10);
        q.where("this.test=5");
        q.limit(12);
        q.sort("strValue");
        assert(q.q().getSort() == null || q.q().getSort().isEmpty());
        assert(q.q().getWhere() == null);
        assert(q.q().toQueryObject().size() == 0);
        assert(q.q().getLimit() == 0);
    }

    @Test
    public void testRawQuery() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        List<UncachedObject> lst = q.rawQuery(UtilsMap.of("counter", 12)).asList();
        assert(lst.size() == 1);
    }

    @Test
    public void testSet() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(100);
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(42).set(UncachedObject.Fields.strValue, "changed", false, false, null);
        Thread.sleep(50);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("changed").asList();
        assert(lst.size() == 1);
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
        assert(lst.size() == 1);
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
        assert(lst.size() == 2);
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
        assert(lst.size() == 1);
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
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object ... param) {
                log.info("success!");
                log.info(Utils.toJsonString(param));
                cnt.incrementAndGet();
            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object ... param) {
            }
        });

        while (cnt.get() == 0) {
            Thread.yield();
        }

        Thread.sleep(100);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("changed").asList();
        assert(lst.size() == 1);
    }

    @Test
    public void testSetUpsert() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(50);
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(10002).set(UncachedObject.Fields.strValue, "new", true, true, null);
        Thread.sleep(50);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("new").asList();
        assert(lst.size() == 1);
        assert(lst.get(0).getCounter() == 10002);
    }

    @Test
    public void testSet2() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(50);
        Map<String, Object> m = new HashMap<>();
        m.put(UncachedObject.Fields.strValue.name(), "changed");
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).lt(2).set(m);
        Thread.sleep(150);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("changed").asList();
        assert(lst.size() == 1);
    }

    @Test
    public void testSet3() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(100);
        Map<String, Object> m = new HashMap<>();
        m.put(UncachedObject.Fields.strValue.name(), "new");
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(10002).set(m, true, true, null);
        Thread.sleep(250);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("new").asList();
        assertEquals(1, lst.size());
        assertEquals(10002, lst.get(0).getCounter());
    }

    @Test
    public void testPush() throws Exception {
        UncachedObject uc = new UncachedObject("value", 10055);
        morphium.store(uc);
        TestUtils.waitForConditionToBecomeTrue(morphium.getConfig().getMaxWaitTime(), "Did not store?", ()->morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).countAll() == 1);
        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.morphiumId).eq(uc.getMorphiumId())
                .push(UncachedObject.Fields.intData, 42);
        Thread.sleep(200);
        morphium.reread(uc);
        assertNotNull(uc.getIntData());
        assertEquals(42, uc.getIntData()[0]);
    }

    @Test
    public void testPushAll() throws Exception {
        UncachedObject uc = new UncachedObject("value", 10055);
        morphium.store(uc);
        TestUtils.waitForConditionToBecomeTrue(morphium.getConfig().getMaxWaitTime(), "Did not store?", ()->morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).countAll() == 1);
        List<Integer> lst = Arrays.asList(42, 123);
        morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).pushAll(UncachedObject.Fields.intData, lst);
        Thread.sleep(200);
        morphium.reread(uc);
        assertNotNull(uc.getIntData());
        assertEquals(42, uc.getIntData()[0]);
        assertEquals(123, uc.getIntData()[1]);
    }

    @Test
    public void testPull() throws Exception {
        UncachedObject uc = new UncachedObject("value", 1021);
        uc.setIntData(new int[] {12, 23, 52, 42});
        morphium.store(uc);
        // long s = System.currentTimeMillis();
        TestUtils.waitForConditionToBecomeTrue(morphium.getConfig().getMaxWaitTime(), "Did not store?", ()->morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).countAll() == 1);
        // while (morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).countAll() == 0) {
        //     Thread.sleep(100);
        //     assert (System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
        //
        // }
        morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).pull(UncachedObject.Fields.intData, 12, false, false, null);
        Thread.sleep(100);
        morphium.reread(uc);
        assertEquals(3, uc.getIntData().length);
        assertEquals(23, uc.getIntData()[0]);
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
        assert(cnt != 0);
        assert(cnt == 1);
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
        long s = System.currentTimeMillis();

        while (cnt == 0) {
            cnt = morphium.createQueryFor(UncachedObject.class)
                          .f(UncachedObject.Fields.counter).gte(100).countAll();
            assert(System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
        }

        assert(cnt != 0);
        assert(cnt == 4);
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
        assert(cnt != 0);
        assert(cnt == 4);
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
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object ... param) {
                ai.incrementAndGet();
            }
        });

        Thread.sleep(250);
        assert(ai.get() == 1);
        long cnt = morphium.createQueryFor(UncachedObject.class)
                   .f(UncachedObject.Fields.dval).eq(0.2).countAll();
        assert(cnt != 0);
        assert(cnt == 4);
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
        assert(cnt != 0);
        assert(cnt == 1);
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
        assert(cnt != 0);
        assert(cnt == 4);
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
            assert(System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
        }

        assert(cnt == 4);
    }

    @Test
    public void testDecAsync() throws Exception {
        createUncachedObjects(10);
        long s = System.currentTimeMillis();

        while (TestUtils.countUC(morphium) < 10) {
            Thread.sleep(50);
            assert(System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
        }

        AtomicInteger ai = new AtomicInteger(0);
        morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.counter).lt(5)
        .dec(UncachedObject.Fields.dval, 0.2, false, true, new AsyncCallbackAdapter<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object ... param) {
                ai.incrementAndGet();
            }
        });

        Thread.sleep(150);
        assert(ai.get() == 1);
        long cnt = morphium.createQueryFor(UncachedObject.class)
                   .f(UncachedObject.Fields.dval).eq(-0.2).countAll();
        assert(cnt != 0);
        assert(cnt == 4);
    }

    @Test
    public void testSetProjection() throws Exception {
        UncachedObject uc = new UncachedObject("test", 2);
        uc.setDval(3.14152);
        morphium.store(uc);
        long s = System.currentTimeMillis();

        while (morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).countAll() == 0) {
            Thread.sleep(100);
            assert(System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
        }

        Thread.sleep(150);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(2).
                                           setProjection(UncachedObject.Fields.counter, UncachedObject.Fields.dval).asList();
        assert(lst.size() == 1);
        assert(lst.get(0).getStrValue() == null);
        assert(lst.get(0).getDval() != 0);
        assert(lst.get(0).getCounter() != 0);
    }

    @Test
    public void testAddProjection2() throws Exception {
        UncachedObject uc = new UncachedObject("test", 22);
        uc.setDval(3.14152);
        morphium.store(uc);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(22).addProjection(UncachedObject.Fields.counter)
                                          .addProjection(UncachedObject.Fields.dval);
        long s = System.currentTimeMillis();
        List<UncachedObject> lst = q.asList();

        while (lst.size() == 0) {
            Thread.sleep(100);
            assert(System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
            lst = q.asList();
        }

        assert(lst.size() == 1) : "Count wrong: " + lst.size() + " count is:" + q.countAll();
        assert(lst.get(0).getStrValue() == null);
        assert(lst.get(0).getDval() != 0);
        assert(lst.get(0).getCounter() != 0);
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
            assert(System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
        }

        assert(lst.size() == 1);
        assert(lst.get(0).getStrValue() == null);
    }

    @Test
    public void testSubDocs() throws Exception {
        SubDocTest sd = new SubDocTest();
        morphium.store(sd);
        Thread.sleep(100);
        Query<SubDocTest> q = morphium.createQueryFor(SubDocTest.class).f(SubDocTest.Fields.id).eq(sd.getId());
        q.push("sub_docs.test.subtest", "this value added");
        q.push("sub_docs.test.subtest", "this value added2");
        assert(q.get().subDocs.size() != 0);
    }

    @Entity
    public static class SubDocTest {
        @Id
        private MorphiumId id;
        private Map<String, Map<String, List<String >>> subDocs;

        public Map<String, Map<String, List<String >>> getSubDocs() {
            return subDocs;
        }

        public void setSubDocs(Map<String, Map<String, List<String >>> subDocs) {
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
