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
        // Expected: {str_value: {$regex: {$not: "VALUE.*"}}}
        assert (queryObj.containsKey("str_value")) : "Query should contain str_value field";
        Map<String, Object> fieldExpr = (Map<String, Object>) queryObj.get("str_value");
        assert (fieldExpr.containsKey("$regex")) : "Should contain $regex operator";
        Object regexValue = fieldExpr.get("$regex");
        // The $not wraps the regex value
        assert (regexValue instanceof Map) : "$regex value should be a Map with $not, got: " + regexValue.getClass();
        Map<String, Object> notExpr = (Map<String, Object>) regexValue;
        assert (notExpr.containsKey("$not")) : "Should contain $not inside $regex";
        assertEquals("VALUE.*", notExpr.get("$not"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void fluentNotGtQueryStructureTest() {
        // Verify not() with gt produces {field: {$gt: {$not: value}}}
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.f(UncachedObject.Fields.counter).not().gt(5);

        Map<String, Object> queryObj = q.toQueryObject();
        assert (queryObj.containsKey("counter")) : "Query should contain counter field";
        Map<String, Object> fieldExpr = (Map<String, Object>) queryObj.get("counter");
        assert (fieldExpr.containsKey("$gt")) : "Should contain $gt operator";
        Object gtValue = fieldExpr.get("$gt");
        assert (gtValue instanceof Map) : "$gt value should be a Map with $not";
        Map<String, Object> notExpr = (Map<String, Object>) gtValue;
        assert (notExpr.containsKey("$not")) : "Should contain $not inside $gt";
        assertEquals(5, notExpr.get("$not"));
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
