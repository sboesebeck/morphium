package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("core")
public class QuerySortPagingTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testLimit(Morphium morphium) {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.limit(10);
        assert (q.getLimit() == 10);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSkip(Morphium morphium) {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.skip(10);
        assert (q.getSkip() == 10);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSort(Morphium morphium) {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.sort(UncachedObject.Fields.counter, UncachedObject.Fields.strValue);
        assertNotNull(q.getSort());
        ;
        assert (q.getSort().get("counter").equals(Integer.valueOf(1)));
        assert (q.getSort().get("str_value").equals(Integer.valueOf(1)));
        int cnt = 0;

        for (String s : q.getSort().keySet()) {
            assert (cnt < 2);
            assert cnt != 0 || (s.equals("counter"));
            assert cnt != 1 || (s.equals("str_value"));
            cnt++;
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSortEnum(Morphium morphium) {
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.sortEnum(UtilsMap.of((Enum) UncachedObject.Fields.counter, -1, UncachedObject.Fields.strValue, 1));
        assertNotNull(q.getSort());
        ;
        assert (q.getSort().get("counter").equals(Integer.valueOf(-1)));
        assert (q.getSort().get("str_value").equals(Integer.valueOf(1)));
        int cnt = 0;

        for (String s : q.getSort().keySet()) {
            assert (cnt < 2);
            assert cnt == 0 || (s.equals("counter"));
            assert cnt == 1 || (s.equals("str_value"));
            cnt++;
        }
    }
}
