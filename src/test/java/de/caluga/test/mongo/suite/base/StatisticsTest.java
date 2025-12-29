package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.UtilsMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

@Tag("core")
public class StatisticsTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void statisticsTest(Morphium morphium) {
        assertThrows(RuntimeException.class, ()-> {
            morphium.getStatistics().put("test", 0.2);
        });

    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void putAll(Morphium morphium) {
        assertThrows(RuntimeException.class, ()-> {
            morphium.getStatistics().putAll(UtilsMap.of("test", 0.2));
        });
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void equalsTest(Morphium morphium) {
        assert (!morphium.getStatistics().equals(UtilsMap.of("test", 0.2)));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void hashcodeTest(Morphium morphium) {
        assert (morphium.getStatistics().hashCode() != 0);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void remove(Morphium morphium) {
        assertThrows(RuntimeException.class, ()-> {
            morphium.getStatistics().remove("test");
        });
    }


}
