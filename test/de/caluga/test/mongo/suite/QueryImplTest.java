package de.caluga.test.mongo.suite;

import de.caluga.morphium.Utils;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 16:18
 * <p/>
 */
public class QueryImplTest extends MongoTest {

    @Test
    public void testQuery() {

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);


        q.or(q.q().f("counter").lte(15),
                q.q().f("counter").gte(10),
                q.q().f("counter").lt(15).f("counter").gt(10).f("value").eq("hallo").f("value").ne("test")
        );
        Map<String, Object> dbObject = q.toQueryObject();
        assert (dbObject != null) : "Map<String,Object> created is null?";

        String str = Utils.toJsonString(dbObject);
        assert (str != null) : "ToString is NULL?!?!?";

        System.out.println("Query: " + str);
        assert (str.trim().equals("{ \"$or\" :  [ { \"counter\" : { \"$lte\" : 15 }  } , { \"counter\" : { \"$gte\" : 10 }  } , { \"$and\" :  [ { \"counter\" : { \"$lt\" : 15 }  } , { \"counter\" : { \"$gt\" : 10 }  } , { \"value\" : \"hallo\" } , { \"value\" : { \"$ne\" : \"test\" }  } ] } ] }")) : "Query-Object wrong";

        q = q.q();
        q.f("counter").gt(0).f("counter").lt(10);
        dbObject = q.toQueryObject();
        str = Utils.toJsonString(dbObject);
        System.out.println("Query: " + str);

        q = q.q(); //new query
        q = q.f("counter").mod(10, 5);
        dbObject = q.toQueryObject();
        str = Utils.toJsonString(dbObject);
        assert (str != null) : "ToString is NULL?!?!?";

        System.out.println("Query: " + str);
        assert (str.trim().equals("{ \"counter\" : { \"$mod\" :  [ 10, 5] }  }")) : "Query wrong";

        q = q.q(); //new query
        q = q.f("counter").gte(5).f("counter").lte(10);
        q.or(q.q().f("counter").eq(15), q.q().f("counter").eq(22));
        dbObject = q.toQueryObject();
        str = Utils.toJsonString(dbObject);
        assert (str != null) : "ToString is NULL?!?!?";

        System.out.println("Query: " + str);
    }


    @Test
    public void testComplexAndOr() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").lt(100).or(q.q().f("counter").eq(50), q.q().f("counter").eq(101));
        String s = Utils.toJsonString(q.toQueryObject());
        log.info("Query: " + s);
        assert (s.trim().equals("{ \"$and\" :  [ { \"counter\" : { \"$lt\" : 100 }  } , { \"$or\" :  [ { \"counter\" : 50 } , { \"counter\" : 101 } ] } ] }"));
    }

    @Test
    public void testOrder() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").lt(1000).f("value").eq("test");
        String str = Utils.toJsonString(q.toQueryObject());
        log.info("Query1: " + str);
        q = q.q();
        q = q.f("value").eq("test").f("counter").lt(1000);
        String str2 = Utils.toJsonString(q.toQueryObject());
        log.info("Query2: " + str2);
        assert (!str.equals(str2));

        q = q.q();
        q = q.f("value").eq("test").f("counter").lt(1000).f("counter").gt(10);
        str = Utils.toJsonString(q.toQueryObject());
        log.info("2nd Query1: " + str);

        q = q.q();
        q = q.f("counter").gt(10).f("value").eq("test").f("counter").lt(1000);
        str = Utils.toJsonString(q.toQueryObject());
        log.info("2nd Query2: " + str);

        assert (!str.equals(str2));


    }

    @Test
    public void testToString() {
        Query<ListContainer> q = morphium.createQueryFor(ListContainer.class);
        q = q.f(ListContainer.Fields.longList).size(10);
        String qStr = q.toString();
        log.info("ToString: " + qStr);
        log.info("query: " + Utils.toJsonString(q.toQueryObject()));

        assert (Utils.toJsonString(q.toQueryObject()).trim().equals("{ \"long_list\" : { \"$size\" : 10 }  }"));
    }

    @Test
    public void testSize() {

        ListContainer lc = new ListContainer();
        for (int i = 0; i < 10; i++) {
            lc.addLong((long) i);
        }
        lc.setName("A test");
        morphium.store(lc);

        lc = new ListContainer();
        for (int i = 0; i < 5; i++) {
            lc.addLong((long) i);
        }
        lc.setName("A test2");
        morphium.store(lc);


        Query<ListContainer> q = morphium.createQueryFor(ListContainer.class);
        q = q.f(ListContainer.Fields.longList).size(10);
        lc = q.get();
        assert (lc.getLongList().size() == 10);
        assert (lc.getName().equals("A test"));

    }

    @Test
    public void speedTest() throws Exception {
        int numThr = 100;

        List<Thread> threads = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (int t = 0; t < numThr; t++) {
            Thread thread = new Thread(() -> {
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
            Thread thread = new Thread(() -> {
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
            Thread thread = new Thread(() -> {
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
}
