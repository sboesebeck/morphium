package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

/**
 * User: Stephan Bösebeck
 * Date: 30.06.12
 * Time: 13:58
 * <p>
 */
@SuppressWarnings("AssertWithSideEffects")
@Tag("core")
@Tag("cache")
public class IdCacheTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void idTest(Morphium morphium) throws Exception  {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        if (morphium.getConfig().driverSettings().getDriverName().equals(InMemoryDriver.driverName)) {
            log.info("Skipping test %s for InMemoryDriver", tstName);
            return;
        }
        // Skip for MorphiumServer - cache sync doesn't work over network
        if (morphium.getDriver().isInMemoryBackend()) {
            log.info("Skipping cache test for MorphiumServer - cache sync not supported over network");
            morphium.close();
            return;
        }
        // Ensure clean state - drop collection first
        morphium.dropCollection(CachedObject.class);
        Thread.sleep(100);
        for (int i = 1; i < 100; i++) {
            CachedObject u = new CachedObject();
            u.setCounter(i);
            u.setValue("Counter = " + i);
            morphium.store(u);
        }

        TestUtils.waitForWrites(morphium, log);
        // Wait for all objects to be replicated and queryable in replica set
        TestUtils.waitForConditionToBecomeTrue(30000, "Objects not replicated",
            () -> {
                long count = morphium.createQueryFor(CachedObject.class).countAll();
                if (count != 99) {
                    log.info("Count is still: " + count);
                    return false;
                }
                return true;
            });
        Query<CachedObject> q = morphium.createQueryFor(CachedObject.class).f("counter").lt(30);
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
