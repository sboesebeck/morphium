package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Utils;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class AggregationCountTest extends MorphiumTestBase {

    @Test
    public void testCount() throws Exception {
        createUncachedObjects(1000);

        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        agg.match(morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gt(10))
                .project("cnt", Expr.sum(Expr.intExpr(1)));
        List<Map> res = agg.aggregate();
        log.info(Utils.toJsonString(res));
        assertThat(agg.getCount()).isEqualTo(990);
    }


    @Test
    public void testCountEmpty() throws Exception {
        createUncachedObjects(1000);

        Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
        agg.match(morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gt(1000))
                .project("cnt", Expr.sum(Expr.intExpr(1)));
        List<Map> res = agg.aggregate();
        log.info(Utils.toJsonString(res));
        assertThat(agg.getCount()).isEqualTo(0);
    }
}
