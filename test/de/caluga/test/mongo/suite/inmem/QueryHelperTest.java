package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.inmem.QueryHelper;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.Map;

public class QueryHelperTest extends MorphiumInMemTestBase {


    @Test
    public void simpleMatchTest() throws Exception {
        Map<String, Object> doc = Utils.getMap("counter", (Object) 12).add("str_value", "hello");

        Map<String, Object> query = morphium.createQueryFor(UncachedObject.class).f("counter").eq(12)
                .f("str_value").eq("not hello").toQueryObject();
        assert (!QueryHelper.matchesQuery(query, doc));


        query = morphium.createQueryFor(UncachedObject.class).f("counter").eq(12)
                .f("strValue").eq("hello").toQueryObject();

        assert (QueryHelper.matchesQuery(query, doc));


    }

    @Test
    public void orMatchTest() throws Exception {
        Map<String, Object> doc = Utils.getMap("counter", (Object) 12).add("str_value", "hello");

        Query<UncachedObject> query = morphium.createQueryFor(UncachedObject.class);
        query.or(query.q().f("counter").eq(12), query.q().f("strValue").eq("not hello"));
        assert (QueryHelper.matchesQuery(query.toQueryObject(), doc));

        query = morphium.createQueryFor(UncachedObject.class);
        query.or(query.q().f("strValue").eq("not hello"), query.q().f("counter").eq(12));
        assert (QueryHelper.matchesQuery(query.toQueryObject(), doc));

        query = morphium.createQueryFor(UncachedObject.class);
        query.or(query.q().f("str_value").eq("not hello"), query.q().f("counter").eq(22));
        assert (!QueryHelper.matchesQuery(query.toQueryObject(), doc));
//        query=morphium.createQueryFor(UncachedObject.class).f("counter").eq(12)
//                .f("value").eq("hello").toQueryObject();
//
//        assert (QueryHelper.matchesQuery(query,doc));

    }


}
