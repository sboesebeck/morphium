package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.UtilsMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Tag;

@Tag("core")
public class StatisticsTest extends MorphiumTestBase {

    @Test
    public void statisticsTest() {
        assertThrows(RuntimeException.class, ()-> {
            morphium.getStatistics().put("test", 0.2);
        });

    }

    @Test
    public void putAll() {
        assertThrows(RuntimeException.class, ()-> {
            morphium.getStatistics().putAll(UtilsMap.of("test", 0.2));
        });
    }

    @Test()
    public void equalsTest() {
        assert (!morphium.getStatistics().equals(UtilsMap.of("test", 0.2)));
    }

    @Test()
    public void hashcodeTest() {
        assert (morphium.getStatistics().hashCode() != 0);
    }

    @Test
    public void remove() {
        assertThrows(RuntimeException.class, ()-> {
            morphium.getStatistics().remove("test");
        });
    }


}
