package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Tag("core")
public class AggregationCountTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testCount(Morphium morphium) throws Exception {
        try (morphium) {

            createUncachedObjects(morphium, 1000);

            Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
            agg.match(morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gt(10))
               .project("cnt", Expr.sum(Expr.intExpr(1)));
            List<Map> res = agg.aggregate();
            log.info(Utils.toJsonString(res));
            assertEquals(990, agg.getCount());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testCountEmpty(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 1000);

            Aggregator<UncachedObject, Map> agg = morphium.createAggregator(UncachedObject.class, Map.class);
            agg.match(morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gt(1000))
               .project("cnt", Expr.sum(Expr.intExpr(1)));
            List<Map> res = agg.aggregate();
            log.info(Utils.toJsonString(res));
            assertEquals(0, agg.getCount());
        }
    }
//
//    @Test
//    public void aggregationSpeedCompare() throws Exception {
//        morphium.getDriver().setMaxWaitTime(10000);
//        log.info("Creating a ton of documents... this may take a while");
//        int amount = 1500000;
//        createUncachedObjects(morphium, amount);
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
