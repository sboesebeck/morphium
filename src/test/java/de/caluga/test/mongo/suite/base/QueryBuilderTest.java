package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Collation;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.query.MongoField;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.ListContainer;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SuppressWarnings("unchecked")
@Tag("core")
public class QueryBuilderTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testQuery(Morphium morphium) {
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
        assert (str.trim().equals("{ \"$or\" :  [ { \"counter\" : { \"$lte\" : 15 }  } , { \"counter\" : { \"$gte\" : 10 }  } , { \"$and\" :  [ { \"counter\" : { \"$lt\" : 15 }  } , { \"counter\" : { \"$gt\" : 10 }  } , { \"str_value\" : \"hallo\" } , { \"str_value\" : { \"$ne\" : \"test\" }  } ] } ] }"))
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
        assert (str.trim().equals("{ \"counter\" : { \"$mod\" :  [ 10, 5] }  }")) : "Query wrong";
        q = q.q(); //new query
        q = q.f("counter").gte(5).f("counter").lte(10);
        q.or(q.q().f("counter").eq(15), q.q().f(UncachedObject.Fields.counter).eq(22));
        dbObject = q.toQueryObject();
        str = Utils.toJsonString(dbObject);
        assertNotNull(str, "ToString is NULL?!?!?");
        log.info("Query: " + str);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testComplexAndOr(Morphium morphium) {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").lt(100).or(q.q().f("counter").eq(50), q.q().f(UncachedObject.Fields.counter).eq(101));
        String s = Utils.toJsonString(q.toQueryObject());
        log.info("Query: " + s);
        assert (s.trim().equals("{ \"$and\" :  [ { \"counter\" : { \"$lt\" : 100 }  } , { \"$or\" :  [ { \"counter\" : 50 } , { \"counter\" : 101 } ] } ] }"));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testOrder(Morphium morphium) {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").lt(1000).f("str_value").eq("test");
        String str = Utils.toJsonString(q.toQueryObject());
        log.info("Query1: " + str);
        q = q.q();
        q = q.f("strValue").eq("test").f("counter").lt(1000);
        String str2 = Utils.toJsonString(q.toQueryObject());
        log.info("Query2: " + str2);
        assert (!str.equals(str2));
        q = q.q();
        q = q.f("str_value").eq("test").f("counter").lt(1000).f("counter").gt(10);
        str = Utils.toJsonString(q.toQueryObject());
        log.info("2nd Query1: " + str);
        q = q.q();
        q = q.f("counter").gt(10).f("strValue").eq("test").f("counter").lt(1000);
        str = Utils.toJsonString(q.toQueryObject());
        log.info("2nd Query2: " + str);
        assert (!str.equals(str2));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testToString(Morphium morphium) {
        Query<ListContainer> q = morphium.createQueryFor(ListContainer.class);
        q = q.f(ListContainer.Fields.longList).size(10);
        String qStr = q.toString();
        log.info("ToString: " + qStr);
        log.info("query: " + Utils.toJsonString(q.toQueryObject()));
        assert (Utils.toJsonString(q.toQueryObject()).trim().equals("{ \"long_list\" : { \"$size\" : 10 }  }"));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testWhere(Morphium morphium) {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.where("this.value=5");
        assert (q.toQueryObject().get("$where").equals("this.value=5"));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testF(Morphium morphium) {
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

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testOverrideDB(Morphium morphium) {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        assert (q.getDB().equals(morphium.getConfig().connectionSettings().getDatabase()));
        q.overrideDB("testDB");
        assert (q.getDB().equals("testDB"));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testGetCollation(Morphium morphium) {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.setCollation(new Collation());
        assertNotNull(q.getCollation());
        ;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testOr(Morphium morphium) {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        Query<UncachedObject> o1 = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gt(10);
        Query<UncachedObject> o2 = morphium.createQueryFor(UncachedObject.class).f("strValue").eq("test");
        q.or(o1, o2);
        Map<String, Object> qo = q.toQueryObject();
        assertNotNull(qo.get("$or"));
        ;
        assert (((java.util.List<?>) qo.get("$or")).size() == 2);
        assertNotNull(((java.util.List<Map<String, Object>>) qo.get("$or")).get(0).get("counter"));
        ;
        assertNotNull(((java.util.List<Map<String, Object>>) qo.get("$or")).get(1).get("str_value"));
        ;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testNor(Morphium morphium) {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        Query<UncachedObject> o1 = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gt(10);
        Query<UncachedObject> o2 = morphium.createQueryFor(UncachedObject.class).f("strValue").eq("test");
        q.nor(o1, o2);
        Map<String, Object> qo = q.toQueryObject();
        assertNotNull(qo.get("$nor"));
        ;
        assert (((java.util.List<?>) qo.get("$nor")).size() == 2);
        assertNotNull(((java.util.List<Map<String, Object>>) qo.get("$nor")).get(0).get("counter"));
        ;
        assertNotNull(((java.util.List<Map<String, Object>>) qo.get("$nor")).get(1).get("str_value"));
        ;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testQ(Morphium morphium) {
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
}
