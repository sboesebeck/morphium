package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.UtilsMap;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


@Tag("inmemory")
public class RawQueryTests extends MorphiumInMemTestBase {

    @Test
    public void rawQueryTest() throws Exception {
        UncachedObject o = new UncachedObject("Value 123", 123);
        morphium.store(o);

        o = new UncachedObject("no value", 1233);
        morphium.store(o);

        Map<String, Object> rawQuery = UtilsMap.of("str_value", "no value");
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).rawQuery(rawQuery).asList();
        assert (lst.size() == 1);
    }

    @Test
    public void rawRegexQueryTest() throws Exception {
        UncachedObject o = new UncachedObject("Value 123", 123);
        morphium.store(o);

        o = new UncachedObject("no value", 1233);
        morphium.store(o);

        o = new UncachedObject("no value", 42);
        morphium.store(o);

        Map<String, Object> rawQuery = UtilsMap.of("str_value", UtilsMap.of("$regex", ".*[0-9]+.*", "$options", "i"));
        List<UncachedObject> lst;
        lst = morphium.createQueryFor(UncachedObject.class).rawQuery(rawQuery).asList();
        assertEquals(1, lst.size());
        rawQuery = UtilsMap.of("str_value", UtilsMap.of("$regex", ".*value.*", "$options", "i"));
        lst = morphium.createQueryFor(UncachedObject.class).rawQuery(rawQuery).asList();
        assertEquals(3, lst.size());

        lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).matches(Pattern.compile(".*value.*", Pattern.CASE_INSENSITIVE)).asList();
        assertEquals(3, lst.size());
    }

    @Test
    public void notRegexQueryTest() throws Exception {
        morphium.store(new UncachedObject("OPEN_order", 1));
        morphium.store(new UncachedObject("OPEN_other", 2));
        morphium.store(new UncachedObject("CLOSED_order", 3));
        morphium.store(new UncachedObject("CANCELLED", 4));

        // NOT LIKE "OPEN.*" → should exclude the two OPEN entries, returning 2
        var query = morphium.createQueryFor(UncachedObject.class);
        var field = query.f(UncachedObject.Fields.strValue);
        field.not();
        field.matches(Pattern.compile("OPEN.*"));
        List<UncachedObject> lst = query.asList();
        assertEquals(2, lst.size(), "NOT regex should exclude matching docs");

        // Verify the query document structure: {str_value: {$not: {$regex: "OPEN.*"}}}
        Map<String, Object> queryObj = query.toQueryObject();
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldExpr = (Map<String, Object>) queryObj.get("str_value");
        assertNotNull(fieldExpr, "Field expression should not be null");
        assertNotNull(fieldExpr.get("$not"), "$not operator should be present");
        @SuppressWarnings("unchecked")
        Map<String, Object> notExpr = (Map<String, Object>) fieldExpr.get("$not");
        assertNotNull(notExpr.get("$regex"), "$regex should be inside $not");
    }
}
