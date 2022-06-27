package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Utils;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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
//
//    @Test
//    public void aggregationSpeedCompare() throws Exception {
//        morphium.getDriver().setMaxWaitTime(10000);
//        log.info("Creating a ton of documents... this may take a while");
//        int amount = 1500000;
//        createUncachedObjects(amount);
//        List<Integer> toSearch = new ArrayList<>();
//        for (int i = 0; i < 10; i++) {
//            toSearch.add((int) (Math.random() * amount));
//        }
//        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f("counter").in(toSearch).f(UncachedObject.Fields.strValue).matches(Pattern.compile("str"));
//        long start = System.currentTimeMillis();
//        long count = q.countAll();
//        long dur = System.currentTimeMillis() - start;
//        log.info(String.format("Standard count took %d ms", dur));
//        toSearch.clear();
//        for (int i = 0; i < 10; i++) {
//            toSearch.add((int) (Math.random() * amount));
//        }
//        var agg = morphium.createAggregator(UncachedObject.class, Map.class);
//        agg.match(morphium.createQueryFor(UncachedObject.class).f("counter").in(Arrays.asList(1, 5, 23, 4, 12, 29)))
//                .match(morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).matches(Pattern.compile("str"))
//        );
//        agg.count("cnt");
//
//        start = System.currentTimeMillis();
//        var res = agg.aggregate();
//        dur = System.currentTimeMillis() - start;
//        log.info(String.format("Aggregation count took %d ms", dur));
//
//
//    }
}
