package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(lst.size()).isEqualTo(1);
        rawQuery = UtilsMap.of("str_value", UtilsMap.of("$regex", ".*value.*", "$options", "i"));
        lst = morphium.createQueryFor(UncachedObject.class).rawQuery(rawQuery).asList();
        assertThat(lst.size()).isEqualTo(3);

        lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).matches(Pattern.compile(".*value.*", Pattern.CASE_INSENSITIVE)).asList();
        assertThat(lst.size()).isEqualTo(3);
    }
}
