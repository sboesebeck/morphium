package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.UtilsMap;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;


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
}
