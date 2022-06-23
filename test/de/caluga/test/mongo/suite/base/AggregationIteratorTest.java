package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.aggregation.MorphiumAggregationIterator;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.Map;

public class AggregationIteratorTest extends MorphiumTestBase {

    @Test
    public void aggregatorIteratorTest() {
        morphium.dropCollection(UncachedObject.class);
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
        for (Map m : maps) {
            log.info(m.toString());
            assert (m.get("number") != null);
            assert (((Integer) m.get("number")).intValue() > 0);
        }

        //checking other methods

        assert (maps.getCurrentBuffer() == null);
        assert (maps.available() == 0);


        Aggregator<UncachedObject, AggRes> agg2 = morphium.createAggregator(UncachedObject.class, AggRes.class);

        agg2.group(Expr.field(UncachedObject.Fields.counter)).sum("number", 1).end();
        agg2.sort("-number");

        for (AggRes m : agg2.aggregateIterable()) {
            log.info(m.toString());
            assert (m.number != null && m.number.intValue() > 0);
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
