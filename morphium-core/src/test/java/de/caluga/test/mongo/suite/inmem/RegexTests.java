package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("inmemory")
public class RegexTests extends MorphiumInMemTestBase {

    @Test
    public void simpleRegexTests() throws Exception {
        createTestData();
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        assert (q.f(UncachedObject.Fields.strValue).matches("VALUE.*").countAll() == 50);
        List<UncachedObject> lst = q.q().f(UncachedObject.Fields.strValue).matches("9$").asList();
        assert (lst.size() > 1);
        for (UncachedObject o : lst) {
            assert (o.getStrValue().endsWith("9"));
        }

    }

    @Test
    public void patternRegexTest() throws Exception {
        createTestData();
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        assert (q.f(UncachedObject.Fields.strValue).matches(Pattern.compile("VALUE.*")).countAll() == 50);
        List<UncachedObject> lst = q.q().f(UncachedObject.Fields.strValue).matches(Pattern.compile("9$")).asList();
        assert (lst.size() > 1);
        for (UncachedObject o : lst) {
            assert (o.getStrValue().endsWith("9"));
        }

    }

    @Test
    public void regexOptionTest() throws Exception {
        createTestData();
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        assert (q.f(UncachedObject.Fields.strValue).matches("value 9$", "i").countAll() == 1);
        List<UncachedObject> lst = q.q().f(UncachedObject.Fields.strValue).matches("value .*9$", "i").asList();
        assert (lst.size() > 1);
        for (UncachedObject o : lst) {
            assert (o.getStrValue().endsWith("9"));
        }

    }

    @Test
    public void patternOptionTest() throws Exception {
        createTestData();
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        assert (q.f(UncachedObject.Fields.strValue).matches(Pattern.compile("value 9$", Pattern.CASE_INSENSITIVE)).countAll() == 1);
        List<UncachedObject> lst = q.q().f(UncachedObject.Fields.strValue).matches(Pattern.compile("value .*9$", Pattern.CASE_INSENSITIVE)).asList();
        assert (lst.size() > 1);
        for (UncachedObject o : lst) {
            assert (o.getStrValue().endsWith("9"));
        }

    }

    @Test
    public void startEndTest() throws Exception {
        createTestData();
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        assert (q.f(UncachedObject.Fields.strValue).matches(Pattern.compile("^value [12]3$", Pattern.CASE_INSENSITIVE)).countAll() == 2);
        q = morphium.createQueryFor(UncachedObject.class);
        assert (q.f(UncachedObject.Fields.strValue).matches("^value [12]3$", "i").countAll() == 2);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void fluentNotMatchesQueryStructureTest() {
        // Verify not() returns MongoField for fluent chaining and produces correct query structure
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);

        // Fluent chaining: not() returns MongoField<T>, so .matches() can be called directly
        q.f(UncachedObject.Fields.strValue).not().matches("VALUE.*");

        Map<String, Object> queryObj = q.toQueryObject();
        // Expected: {str_value: {$not: {$regex: "VALUE.*"}}}
        assert (queryObj.containsKey("str_value")) : "Query should contain str_value field";
        Map<String, Object> fieldExpr = (Map<String, Object>) queryObj.get("str_value");
        assert (fieldExpr.containsKey("$not")) : "Should contain $not operator, got: " + fieldExpr.keySet();
        Object notValue = fieldExpr.get("$not");
        assert (notValue instanceof Map) : "$not value should be a Map with operators";
        Map<String, Object> innerExpr = (Map<String, Object>) notValue;
        assert (innerExpr.containsKey("$regex")) : "Should contain $regex inside $not";
        assertEquals("VALUE.*", innerExpr.get("$regex"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void fluentNotGtQueryStructureTest() {
        // Verify not() with gt produces {field: {$not: {$gt: value}}}
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.f(UncachedObject.Fields.counter).not().gt(5);

        Map<String, Object> queryObj = q.toQueryObject();
        assert (queryObj.containsKey("counter")) : "Query should contain counter field";
        Map<String, Object> fieldExpr = (Map<String, Object>) queryObj.get("counter");
        assert (fieldExpr.containsKey("$not")) : "Should contain $not operator";
        Object notValue = fieldExpr.get("$not");
        assert (notValue instanceof Map) : "$not value should be a Map with operators";
        Map<String, Object> innerExpr = (Map<String, Object>) notValue;
        assert (innerExpr.containsKey("$gt")) : "Should contain $gt inside $not";
        assertEquals(5, innerExpr.get("$gt"));
    }

    @Test
    public void fluentNotMatchesExecutionTest() throws Exception {
        createTestData();
        // not().matches("VALUE.*") should return entries NOT starting with uppercase VALUE
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        long matchCount = q.f(UncachedObject.Fields.strValue).matches("VALUE.*").countAll();
        assertEquals(50, matchCount);

        q = morphium.createQueryFor(UncachedObject.class);
        long notMatchCount = q.f(UncachedObject.Fields.strValue).not().matches("VALUE.*").countAll();
        assertEquals(50, notMatchCount, "NOT matching uppercase should return lowercase entries");
    }

    @Test
    public void fluentNotGtExecutionTest() throws Exception {
        for (int i = 1; i <= 10; i++) {
            morphium.store(new UncachedObject("item", i));
        }

        // not().gt(5) → counter <= 5
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        List<UncachedObject> result = q.f(UncachedObject.Fields.counter).not().gt(5).asList();
        assertEquals(5, result.size());
        for (UncachedObject o : result) {
            assert (o.getCounter() <= 5) : "Counter should be <= 5 but was " + o.getCounter();
        }
    }

    public void createTestData() {
        for (int i = 0; i < 100; i++) {
            String v;
            if (i % 2 == 0) {
                v = "VALUE " + (i + 1);
            } else {
                v = "value " + (i + 1);
            }
            UncachedObject o = new UncachedObject(v, i);
            morphium.store(o);
        }
    }
}
