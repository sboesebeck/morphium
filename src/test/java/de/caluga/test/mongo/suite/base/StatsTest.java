package de.caluga.test.mongo.suite.base;

import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;


@Tag("core")
public class StatsTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testDbStats(Morphium morphium) throws Exception  {
        createUncachedObjects(morphium, 100);
        Thread.sleep(100);
        Map<String, Object> stats = morphium.getDbStats();
        assert (!stats.isEmpty());
        for (String k : stats.keySet()) {
            log.info("Stat: " + k + "   : " + stats.get(k));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testCollStats(Morphium morphium) throws Exception  {
        createUncachedObjects(morphium, 100);
        Thread.sleep(100);
        Map<String, Object> stats = morphium.getCollStats(UncachedObject.class);
        assert (!stats.isEmpty());
        for (String k : stats.keySet()) {
            log.info("Stat: " + k + "   : " + stats.get(k));
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void existsTest(Morphium morphium) throws Exception  {
        createUncachedObjects(morphium, 1);
        Thread.sleep(100);
        assertTrue(morphium.exists(morphium.getDatabase(), "uncached_object"));
        assertFalse(morphium.exists(morphium.getDatabase(), "uncached_object_none"));
    }
}


