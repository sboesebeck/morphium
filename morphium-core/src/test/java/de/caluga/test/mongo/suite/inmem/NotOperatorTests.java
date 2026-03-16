package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.QueryHelper;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

@Tag("inmemory")
public class NotOperatorTests extends MorphiumInMemTestBase {

    // --- Query structure tests ---

    @Test
    public void notEqProducesNe() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.strValue).not().eq("hello");
        Map<String, Object> qo = q.toQueryObject();
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldExpr = (Map<String, Object>) qo.get("str_value");
        assertNotNull(fieldExpr, "Field expression should not be null");
        assertEquals("hello", fieldExpr.get("$ne"), "not().eq() should produce $ne");
        assertNull(fieldExpr.get("$not"), "Should NOT have $not wrapper for equality");
    }

    @Test
    public void notGtProducesNotGt() {
        var field = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter);
        field.not();
        Query<UncachedObject> q = field.gt(10);
        Map<String, Object> qo = q.toQueryObject();
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldExpr = (Map<String, Object>) qo.get("counter");
        assertNotNull(fieldExpr, "Field expression should not be null");
        @SuppressWarnings("unchecked")
        Map<String, Object> notExpr = (Map<String, Object>) fieldExpr.get("$not");
        assertNotNull(notExpr, "$not should be present");
        assertEquals(10, notExpr.get("$gt"), "$gt value should be 10");
    }

    @Test
    public void notLtProducesNotLt() {
        var field = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter);
        field.not();
        Query<UncachedObject> q = field.lt(5);
        Map<String, Object> qo = q.toQueryObject();
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldExpr = (Map<String, Object>) qo.get("counter");
        @SuppressWarnings("unchecked")
        Map<String, Object> notExpr = (Map<String, Object>) fieldExpr.get("$not");
        assertNotNull(notExpr, "$not should be present");
        assertEquals(5, notExpr.get("$lt"));
    }

    @Test
    public void notMatchesPatternProducesNotRegex() {
        var field = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue);
        field.not();
        Query<UncachedObject> q = field.matches(Pattern.compile("OPEN.*"));
        Map<String, Object> qo = q.toQueryObject();
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldExpr = (Map<String, Object>) qo.get("str_value");
        assertNotNull(fieldExpr);
        @SuppressWarnings("unchecked")
        Map<String, Object> notExpr = (Map<String, Object>) fieldExpr.get("$not");
        assertNotNull(notExpr, "$not should be present");
        assertEquals("OPEN.*", notExpr.get("$regex"));
    }

    @Test
    public void notMatchesPatternWithFlagsPreservesOptions() {
        var field = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue);
        field.not();
        Query<UncachedObject> q = field.matches(Pattern.compile("open.*", Pattern.CASE_INSENSITIVE));
        Map<String, Object> qo = q.toQueryObject();
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldExpr = (Map<String, Object>) qo.get("str_value");
        @SuppressWarnings("unchecked")
        Map<String, Object> notExpr = (Map<String, Object>) fieldExpr.get("$not");
        assertNotNull(notExpr, "$not should be present");
        assertEquals("open.*", notExpr.get("$regex"), "$regex should be inside $not");
        assertEquals("i", notExpr.get("$options"), "$options should be inside $not alongside $regex");
    }

    @Test
    public void notMatchesStringProducesNotRegex() {
        var field = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue);
        field.not();
        Query<UncachedObject> q = field.matches("^CLOSED.*", "i");
        Map<String, Object> qo = q.toQueryObject();
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldExpr = (Map<String, Object>) qo.get("str_value");
        @SuppressWarnings("unchecked")
        Map<String, Object> notExpr = (Map<String, Object>) fieldExpr.get("$not");
        assertNotNull(notExpr);
        assertEquals("^CLOSED.*", notExpr.get("$regex"));
        assertEquals("i", notExpr.get("$options"));
    }

    @Test
    public void notInProducesNotIn() {
        var field = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter);
        field.not();
        Query<UncachedObject> q = field.in(List.of(1, 2, 3));
        Map<String, Object> qo = q.toQueryObject();
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldExpr = (Map<String, Object>) qo.get("counter");
        @SuppressWarnings("unchecked")
        Map<String, Object> notExpr = (Map<String, Object>) fieldExpr.get("$not");
        assertNotNull(notExpr, "$not should wrap $in");
        assertEquals(List.of(1, 2, 3), notExpr.get("$in"));
    }

    // --- Fluent API test ---

    @Test
    public void fluentNotChaining() {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.strValue).not().eq("hello");
        Map<String, Object> qo = q.toQueryObject();
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldExpr = (Map<String, Object>) qo.get("str_value");
        assertNotNull(fieldExpr);
        assertEquals("hello", fieldExpr.get("$ne"));
    }

    // --- InMemory driver execution tests ---

    @Test
    public void notEqQueryExecution() {
        morphium.store(new UncachedObject("hello", 1));
        morphium.store(new UncachedObject("world", 2));
        morphium.store(new UncachedObject("hello", 3));

        List<UncachedObject> results = morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.strValue).not().eq("hello").asList();
        assertEquals(1, results.size(), "not().eq('hello') should return only 'world'");
        assertEquals("world", results.get(0).getStrValue());
    }

    @Test
    public void notGtQueryExecution() {
        morphium.store(new UncachedObject("a", 5));
        morphium.store(new UncachedObject("b", 15));
        morphium.store(new UncachedObject("c", 25));

        var field = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter);
        field.not();
        List<UncachedObject> results = field.gt(10).asList();
        assertEquals(1, results.size());
        assertEquals(5, results.get(0).getCounter());
    }

    @Test
    public void notMatchesPatternQueryExecution() {
        morphium.store(new UncachedObject("OPEN_order", 1));
        morphium.store(new UncachedObject("OPEN_other", 2));
        morphium.store(new UncachedObject("CLOSED_order", 3));
        morphium.store(new UncachedObject("CANCELLED", 4));

        var field = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue);
        field.not();
        List<UncachedObject> results = field.matches(Pattern.compile("OPEN.*")).asList();
        assertEquals(2, results.size(), "NOT regex should exclude matching docs");
    }

    @Test
    public void notMatchesCaseInsensitiveQueryExecution() {
        morphium.store(new UncachedObject("Open_order", 1));
        morphium.store(new UncachedObject("open_other", 2));
        morphium.store(new UncachedObject("CLOSED_order", 3));

        var field = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue);
        field.not();
        List<UncachedObject> results = field.matches(Pattern.compile("open.*", Pattern.CASE_INSENSITIVE)).asList();
        assertEquals(1, results.size(), "Case-insensitive NOT regex should exclude both open variants");
        assertEquals("CLOSED_order", results.get(0).getStrValue());
    }

    @Test
    public void patternMultilineOnlyDoesNotAddCaseInsensitive() {
        // Regression for Bug 3: MULTILINE-only should NOT add case-insensitive
        morphium.store(new UncachedObject("VALUE 1", 1));
        morphium.store(new UncachedObject("value 2", 2));

        // MULTILINE only — should match "^value" literally (lowercase), not case-insensitively
        long count = morphium.createQueryFor(UncachedObject.class)
                .f(UncachedObject.Fields.strValue)
                .matches(Pattern.compile("^value", Pattern.MULTILINE)).countAll();
        assertEquals(1, count,
                "MULTILINE-only should match lowercase 'value' but not uppercase 'VALUE'");
    }
}
