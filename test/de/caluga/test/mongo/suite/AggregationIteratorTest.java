package de.caluga.test.mongo.suite;

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
        for (int i = 0; i < 10000; i++) {
            morphium.store(new UncachedObject("Value", (int) (1000 * Math.random())));
        }

        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);

        agg.group(Expr.field(UncachedObject.Fields.counter)).sum("number", 1).end();
        agg.sort("-number");

        MorphiumAggregationIterator<UncachedObject, Map> maps = agg.aggregateIterable();
        long count = maps.getCount();
        log.info("Got count: " + count);
        for (Map m : maps) {
            log.info(m.toString());
            assert (m.get("number") != null);
            assert (((Integer) m.get("number")).intValue() > 0);
            count--;
        }
        assert (count == 0) : "Count is not null now: " + count;


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
