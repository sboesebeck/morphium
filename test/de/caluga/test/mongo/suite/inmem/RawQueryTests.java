package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.Utils;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class RawQueryTests extends MorphiumInMemTestBase {

    @Test
    public void rawQueryTest() throws Exception {
        UncachedObject o = new UncachedObject("Value 123", 123);
        morphium.store(o);

        o = new UncachedObject("no value", 1233);
        morphium.store(o);

        Map<String, Object> rawQuery = Map.of("str_value", "no value");
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).rawQuery(rawQuery).asList();
        assert (lst.size() == 1);
    }

    @Test
    public void rawRegexQueryTest() throws Exception {
        UncachedObject o = new UncachedObject("Value 123", 123);
        morphium.store(o);

        o = new UncachedObject("no value", 1233);
        morphium.store(o);

        Map<String, Object> rawQuery = Map.of("str_value", Map.of("$regex", ".*[0-9]+.*", "$options", "i"));
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).rawQuery(rawQuery).asList();
        assert (lst.size() == 1);

        rawQuery = Map.of("str_value", Map.of("$regex", ".*value.*", "$options", "i"));
        lst = morphium.createQueryFor(UncachedObject.class).rawQuery(rawQuery).asList();
        assert (lst.size() == 2);

        lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).matches(Pattern.compile(".*value.*", Pattern.CASE_INSENSITIVE)).asList();
        assert (lst.size() == 2);

    }
}
