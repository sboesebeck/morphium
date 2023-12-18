package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 30.06.12
 * Time: 13:58
 * <p/>
 */
@SuppressWarnings("AssertWithSideEffects")
public class IdCacheTest extends MorphiumTestBase {

    @Test
    public void idTest() throws Exception {
        for (int i = 1; i < 100; i++) {
            CachedObject u = new CachedObject();
            u.setCounter(i);
            u.setValue("Counter = " + i);
            morphium.store(u);
        }

        TestUtils.waitForWrites(morphium,log);
        long s = System.currentTimeMillis();
        Query<CachedObject> q = morphium.createQueryFor(CachedObject.class);
        while (q.countAll() != 99) {
            log.info("Count is still: " + q.countAll());
            Thread.sleep(100);
            assert (System.currentTimeMillis() - s < morphium.getConfig().getMaxWaitTime());
        }
        q = q.f("counter").lt(30);
        List<CachedObject> lst = q.asList();

        String k = morphium.getCache().getCacheKey(q);
        assert (lst.size() == 29) : "Size matters! " + lst.size();
        Thread.sleep(1100);
        Map<String, Integer> sizes = morphium.getCache().getSizes();
        MorphiumId id = lst.get(0).getId();

        CachedObject c = morphium.findById(CachedObject.class, id);
        assert (lst.get(0) == c) : "Object differ?";

        c.setCounter(1009);
        assert (lst.get(0).getCounter() == 1009) : "changes not work?";

        morphium.reread(c);
        assert (c.getCounter() != 1009) : "reread did not work?";

        assert (lst.get(0) == c) : "Object changed?!?!?";
    }
}
