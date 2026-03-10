package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.aggregation.MorphiumAggregationIterator;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


@Tag("core")
public class AggregationIteratorTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aggregatorIteratorTest(Morphium morphium) throws Exception {
        try (morphium) {

            for (int i = 0; i < 1000; i++) {
                morphium.store(new UncachedObject("Value", (int) (1000 * Math.random())));
                if (i % 100 == 0) {
                    log.info("Stored " + i);
                }
            }
            log.info("objects stored");
            Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);

            agg.group(Expr.field(UncachedObject.Fields.counter)).sum("number", 1).end();
            agg.sort("-number");

            MorphiumAggregationIterator<UncachedObject, Map> maps = agg.aggregateIterable();
            int count = 0;
            for (Map m : maps) {
                count++;
                log.info(m.toString());
                assertNotNull(m.get("number"));
                assertTrue(((Number) m.get("number")).intValue() > 0);
                assertTrue(count <= 1000);
            }

            //checking other methods

            assertEquals(0, maps.available());


            Aggregator<UncachedObject, AggRes> agg2 = morphium.createAggregator(UncachedObject.class, AggRes.class);

            agg2.group(Expr.field(UncachedObject.Fields.counter)).sum("number", 1).end();
            agg2.sort("-number");

            for (AggRes m : agg2.aggregateIterable()) {
                log.info(m.toString());
                assert (m.number != null && m.number.intValue() > 0);
            }
        }

    }


    @Entity
    public static class AggRes {
        @Id
        public Integer counter;
        public Integer number;

        @Override
        public String toString() {
            return "AggRes{" +
                   "counter=" + counter +
                   ", number=" + number +
                   '}';
        }
    }
}
