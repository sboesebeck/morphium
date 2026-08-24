package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.UtilsMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.StatisticKeys;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("core")
public class StatisticsTest extends MultiDriverTestBase {

    /**
     * Before any cached read has happened, CHITS and CMISS are both 0, so the
     * naive ratio CHITS/(CHITS+CMISS) is 0.0/0.0 = NaN. Prometheus/OTel
     * exporters silently drop NaN samples, so a fresh application shows the
     * cache-hit-ratio metric as entirely missing instead of a real "no data
     * yet" 0% -- found while wiring the quarkus-morphium observability
     * module (this PR) against a live otel-collector/Prometheus stack.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void cacheHitRatioIsZeroNotNaNBeforeAnyCachedRead(Morphium morphium) {
        Double hitPerc = morphium.getStatistics().get(StatisticKeys.CHITSPERC.name());
        Double missPerc = morphium.getStatistics().get(StatisticKeys.CMISSPERC.name());
        assertFalse(hitPerc.isNaN(), "CHITSPERC must not be NaN before any cached read");
        assertFalse(missPerc.isNaN(), "CMISSPERC must not be NaN before any cached read");
        assertEquals(0.0, hitPerc);
        assertEquals(0.0, missPerc);
    }

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
        assertTrue((!morphium.getStatistics().equals(UtilsMap.of("test", 0.2))));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void hashcodeTest(Morphium morphium) {
        assertTrue((morphium.getStatistics().hashCode() != 0));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void remove(Morphium morphium) {
        assertThrows(RuntimeException.class, ()-> {
            morphium.getStatistics().remove("test");
        });
    }


}
