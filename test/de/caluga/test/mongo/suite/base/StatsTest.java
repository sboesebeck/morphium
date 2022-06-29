package de.caluga.test.mongo.suite.base;

import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class StatsTest extends MorphiumTestBase {

    @Test
    public void testDbStats() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(100);
        Map<String, Object> stats = morphium.getDbStats();
        assert (!stats.isEmpty());
        for (String k : stats.keySet()) {
            log.info("Stat: " + k + "   : " + stats.get(k));
        }
    }

    @Test
    public void testCollStats() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(100);
        Map<String, Object> stats = morphium.getCollStats(UncachedObject.class);
        assert (!stats.isEmpty());
        for (String k : stats.keySet()) {
            log.info("Stat: " + k + "   : " + stats.get(k));
        }
    }


    @Test
    public void existsTest() throws Exception {
        createUncachedObjects(1);
        Thread.sleep(100);
        assertThat(morphium.exists(morphium.getDatabase(), "uncached_object")).isTrue();
        assertThat(morphium.exists(morphium.getDatabase(), "uncached_object_none")).isFalse();
    }
}


