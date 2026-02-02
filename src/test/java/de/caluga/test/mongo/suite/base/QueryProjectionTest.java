package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("core")
public class QueryProjectionTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSetProjection(Morphium morphium) throws Exception {
        UncachedObject uc = new UncachedObject("test", 2);
        uc.setDval(3.14152);
        morphium.store(uc);
        long s = System.currentTimeMillis();

        while (morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).countAll() == 0) {
            Thread.sleep(100);
            assert (System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
        }

        Thread.sleep(150);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(2)
            .setProjection(UncachedObject.Fields.counter, UncachedObject.Fields.dval).asList();
        assertEquals(lst.size(), 1);
        assert (lst.get(0).getStrValue() == null);
        assert (lst.get(0).getDval() != 0);
        assert (lst.get(0).getCounter() != 0);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testAddProjection2(Morphium morphium) throws Exception {
        UncachedObject uc = new UncachedObject("test", 22);
        uc.setDval(3.14152);
        morphium.store(uc);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(22).addProjection(UncachedObject.Fields.counter)
            .addProjection(UncachedObject.Fields.dval);
        long s = System.currentTimeMillis();
        List<UncachedObject> lst = q.asList();

        while (lst.size() == 0) {
            Thread.sleep(100);
            assert (System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
            lst = q.asList();
        }

        assertEquals(lst.size(), 1, "Count wrong: " + lst.size() + " count is:" + q.countAll());
        assert (lst.get(0).getStrValue() == null);
        assert (lst.get(0).getDval() != 0);
        assert (lst.get(0).getCounter() != 0);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testHideFieldInProjection(Morphium morphium) throws Exception {
        createUncachedObjects(morphium, 10);
        List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(2).hideFieldInProjection(UncachedObject.Fields.strValue).asList();
        long s = System.currentTimeMillis();

        while (lst.size() < 1) {
            lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(2).hideFieldInProjection(UncachedObject.Fields.strValue).asList();
            Thread.sleep(50);
            assert (System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
        }

        assertEquals(lst.size(), 1);
        assert (lst.get(0).getStrValue() == null);
    }
}
