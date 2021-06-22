package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Utils;
import org.junit.Test;

public class StatisticsTest extends MorphiumTestBase {

    @Test(expected = RuntimeException.class)
    public void statisticsTest() {
        morphium.getStatistics().put("test", 0.2);
    }

    @Test(expected = RuntimeException.class)
    public void putAll() {
        morphium.getStatistics().putAll(Utils.getMap("test", 0.2));
    }

    @Test()
    public void equalsTest() {
        assert (!morphium.getStatistics().equals(Utils.getMap("test", 0.2)));
    }

    @Test()
    public void hashcodeTest() {
        assert (morphium.getStatistics().hashCode() != 0);
    }

    @Test(expected = RuntimeException.class)
    public void remove() {
        morphium.getStatistics().remove("test");
    }


}
