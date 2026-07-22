package de.caluga.test.mongo.suite.aggregationStages;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * #254: the $setWindowFields stage - the common core: partitionBy, sortBy, documents-windows and
 * the window functions $sum/$avg/$min/$max/$count/$push/$first/$last/$rank/$denseRank/
 * $documentNumber/$shift. Everything beyond that (range/time windows, $derivative & friends)
 * must fail loudly, never return wrong results.
 */
@Tag("inmemory")
public class SetWindowFieldsStageTest extends MultiDriverTestBase {

    private static final String COLL = "swf_stage_test";

    private InMemoryDriver drv(Morphium m) {
        return (InMemoryDriver) m.getDriver();
    }

    private String db(Morphium m) {
        return m.getConfig().connectionSettings().getDatabase();
    }

    @SafeVarargs
    private void seed(Morphium m, Map<String, Object>... docs) throws Exception {
        List<Map<String, Object>> l = new ArrayList<>();

        for (Map<String, Object> d : docs) {
            d.putIfAbsent("_id", new MorphiumId());
            l.add(d);
        }

        drv(m).store(db(m), COLL, l, null);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Aggregator<Object, Map> agg(Morphium m, Map<String, Object> spec) {
        Aggregator<Object, Map> agg = (Aggregator<Object, Map>) drv(m).createAggregator(m, Object.class, Map.class);
        agg.setCollectionName(COLL);
        agg.addOperator(UtilsMap.of("$setWindowFields", spec));
        return agg;
    }

    private Map<String, Object> byX(List<Map<String, Object>> docs, int x) {
        return docs.stream().filter(d -> Integer.valueOf(x).equals(d.get("x"))).findFirst()
               .orElseThrow(() -> new AssertionError("no doc with x=" + x));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void runningTotal_documentsUnboundedToCurrent(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 10), Doc.of("x", 2, "v", 20), Doc.of("x", 3, "v", 30));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("total", Doc.of("$sum", "$v",
                        "window", Doc.of("documents", List.of("unbounded", "current"))))
            )).aggregateMap();

            assertEquals(10.0, ((Number) byX(res, 1).get("total")).doubleValue(), 1e-9);
            assertEquals(30.0, ((Number) byX(res, 2).get("total")).doubleValue(), 1e-9);
            assertEquals(60.0, ((Number) byX(res, 3).get("total")).doubleValue(), 1e-9);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void defaultWindow_isWholePartition(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium,
                 Doc.of("p", "a", "x", 1, "v", 1), Doc.of("p", "a", "x", 2, "v", 2),
                 Doc.of("p", "b", "x", 1, "v", 100));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "partitionBy", "$p",
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("partTotal", Doc.of("$sum", "$v"))
            )).aggregateMap();

            for (Map<String, Object> d : res) {
                if ("a".equals(d.get("p"))) {
                    assertEquals(3.0, ((Number) d.get("partTotal")).doubleValue(), 1e-9);
                } else {
                    assertEquals(100.0, ((Number) d.get("partTotal")).doubleValue(), 1e-9);
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void movingAverage_documentsMinusOneToCurrent(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 10), Doc.of("x", 2, "v", 20), Doc.of("x", 3, "v", 60));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("mavg", Doc.of("$avg", "$v",
                        "window", Doc.of("documents", List.of(-1, 0))))
            )).aggregateMap();

            assertEquals(10.0, ((Number) byX(res, 1).get("mavg")).doubleValue(), 1e-9);
            assertEquals(15.0, ((Number) byX(res, 2).get("mavg")).doubleValue(), 1e-9);
            assertEquals(40.0, ((Number) byX(res, 3).get("mavg")).doubleValue(), 1e-9);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void minMaxCountPushFirstLast(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 5), Doc.of("x", 2, "v", 1), Doc.of("x", 3, "v", 9));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of(
                    "mn", Doc.of("$min", "$v"),
                    "mx", Doc.of("$max", "$v"),
                    "cnt", Doc.of("$count", new java.util.HashMap<>()),
                    "all", Doc.of("$push", "$v"),
                    "fst", Doc.of("$first", "$v"),
                    "lst", Doc.of("$last", "$v"))
            )).aggregateMap();

            Map<String, Object> d = byX(res, 2);
            assertEquals(1.0, ((Number) d.get("mn")).doubleValue(), 1e-9);
            assertEquals(9.0, ((Number) d.get("mx")).doubleValue(), 1e-9);
            assertEquals(3, ((Number) d.get("cnt")).intValue());
            assertEquals(List.of(5, 1, 9), d.get("all"));
            assertEquals(5, d.get("fst"));
            assertEquals(9, d.get("lst"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void rankDenseRankDocumentNumber_withTies(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium,
                 Doc.of("x", 1, "score", 10),
                 Doc.of("x", 2, "score", 10),
                 Doc.of("x", 3, "score", 20));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("score", 1),
                "output", Doc.of(
                    "rnk", Doc.of("$rank", new java.util.HashMap<>()),
                    "drnk", Doc.of("$denseRank", new java.util.HashMap<>()),
                    "docNum", Doc.of("$documentNumber", new java.util.HashMap<>()))
            )).aggregateMap();

            // ties share the rank; the next rank skips ($rank) or does not skip ($denseRank)
            assertEquals(1, ((Number) byX(res, 1).get("rnk")).intValue());
            assertEquals(1, ((Number) byX(res, 2).get("rnk")).intValue());
            assertEquals(3, ((Number) byX(res, 3).get("rnk")).intValue());
            assertEquals(1, ((Number) byX(res, 3).get("drnk")).intValue() - ((Number) byX(res, 2).get("drnk")).intValue());
            assertEquals(2, ((Number) byX(res, 3).get("drnk")).intValue());
            assertEquals(List.of(1, 2, 3),
                         List.of(((Number) byX(res, 1).get("docNum")).intValue(),
                                 ((Number) byX(res, 2).get("docNum")).intValue(),
                                 ((Number) byX(res, 3).get("docNum")).intValue()));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void shift_byOneWithDefault(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 10), Doc.of("x", 2, "v", 20), Doc.of("x", 3, "v", 30));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("nextV", Doc.of("$shift", Doc.of("output", "$v", "by", 1, "default", -1)))
            )).aggregateMap();

            assertEquals(20, byX(res, 1).get("nextV"));
            assertEquals(30, byX(res, 2).get("nextV"));
            assertEquals(-1, byX(res, 3).get("nextV"), "past the partition end -> default");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void rank_doesNotLeakAcrossPartitions(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium,
                 Doc.of("p", "a", "x", 1, "score", 5), Doc.of("p", "a", "x", 2, "score", 7),
                 Doc.of("p", "b", "x", 3, "score", 9));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "partitionBy", "$p",
                "sortBy", Doc.of("score", 1),
                "output", Doc.of("rnk", Doc.of("$rank", new java.util.HashMap<>()))
            )).aggregateMap();

            assertEquals(1, ((Number) byX(res, 3).get("rnk")).intValue(), "partition b restarts at rank 1");
        }
    }

    // ---- statistical window accumulators (#255) ------------------------------------------

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void stdDevPopAndSamp_wholePartition(Morphium morphium) throws Exception {
        try (morphium) {
            // classic example set: mean 5, sum of squared deviations 32
            int[] vals = {2, 4, 4, 4, 5, 5, 7, 9};

            for (int i = 0; i < vals.length; i++) {
                seed(morphium, Doc.of("x", i + 1, "v", vals[i]));
            }

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of(
                    "sdPop", Doc.of("$stdDevPop", "$v"),
                    "sdSamp", Doc.of("$stdDevSamp", "$v"))
            )).aggregateMap();

            Map<String, Object> d = byX(res, 1);
            assertEquals(2.0, ((Number) d.get("sdPop")).doubleValue(), 1e-9);
            assertEquals(Math.sqrt(32.0 / 7.0), ((Number) d.get("sdSamp")).doubleValue(), 1e-9);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void stdDevSamp_singleDocWindow_isNull(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 10), Doc.of("x", 2, "v", 20));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of(
                    "sdSamp", Doc.of("$stdDevSamp", "$v",
                        "window", Doc.of("documents", List.of("current", "current"))),
                    "sdPop", Doc.of("$stdDevPop", "$v",
                        "window", Doc.of("documents", List.of("current", "current"))))
            )).aggregateMap();

            assertNull(byX(res, 1).get("sdSamp"), "sample stddev of a single value is undefined");
            assertEquals(0.0, ((Number) byX(res, 1).get("sdPop")).doubleValue(), 1e-9);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void covariancePopAndSamp_wholePartition(Morphium morphium) throws Exception {
        try (morphium) {
            // pairs (1,2), (2,4), (3,6): covPop = 4/3, covSamp = 2
            seed(morphium,
                 Doc.of("x", 1, "a", 1, "b", 2),
                 Doc.of("x", 2, "a", 2, "b", 4),
                 Doc.of("x", 3, "a", 3, "b", 6));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of(
                    "covPop", Doc.of("$covariancePop", List.of("$a", "$b")),
                    "covSamp", Doc.of("$covarianceSamp", List.of("$a", "$b")))
            )).aggregateMap();

            Map<String, Object> d = byX(res, 2);
            assertEquals(4.0 / 3.0, ((Number) d.get("covPop")).doubleValue(), 1e-9);
            assertEquals(2.0, ((Number) d.get("covSamp")).doubleValue(), 1e-9);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void covarianceSamp_singleDocWindow_isNull(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "a", 1, "b", 2), Doc.of("x", 2, "a", 2, "b", 4));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of(
                    "covSamp", Doc.of("$covarianceSamp", List.of("$a", "$b"),
                        "window", Doc.of("documents", List.of("current", "current"))),
                    "covPop", Doc.of("$covariancePop", List.of("$a", "$b"),
                        "window", Doc.of("documents", List.of("current", "current"))))
            )).aggregateMap();

            assertNull(byX(res, 1).get("covSamp"), "sample covariance of a single pair is undefined");
            assertEquals(0.0, ((Number) byX(res, 1).get("covPop")).doubleValue(), 1e-9);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void covariance_missingSecondExpression_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "a", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("cov", Doc.of("$covariancePop", List.of("$a")))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5397900, ex.getMongoCode());
        }
    }

    // ---- N-accumulators and $top/$bottom family (#255) -----------------------------------

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void firstNlastNminNmaxN_wholePartition(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 5), Doc.of("x", 2, "v", 1), Doc.of("x", 3, "v", 9));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of(
                    "f2", Doc.of("$firstN", Doc.of("n", 2, "input", "$v")),
                    "l2", Doc.of("$lastN", Doc.of("n", 2, "input", "$v")),
                    "mn2", Doc.of("$minN", Doc.of("n", 2, "input", "$v")),
                    "mx2", Doc.of("$maxN", Doc.of("n", 2, "input", "$v")))
            )).aggregateMap();

            Map<String, Object> d = byX(res, 1);
            assertEquals(List.of(5, 1), d.get("f2"));
            assertEquals(List.of(1, 9), d.get("l2"));
            assertEquals(List.of(1, 5), d.get("mn2"));
            assertEquals(List.of(9, 5), d.get("mx2"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void firstN_nLargerThanWindow_returnsAll(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 5), Doc.of("x", 2, "v", 1));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("f9", Doc.of("$firstN", Doc.of("n", 9, "input", "$v")))
            )).aggregateMap();

            assertEquals(List.of(5, 1), byX(res, 1).get("f9"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void firstN_invalidN_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 5));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("f", Doc.of("$firstN", Doc.of("n", 0, "input", "$v")))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5787908, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void topBottomTopNBottomN_ownSortBy(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium,
                 Doc.of("x", 1, "score", 10),
                 Doc.of("x", 2, "score", 30),
                 Doc.of("x", 3, "score", 20));

            // the operator's own sortBy (score desc) differs from the stage sortBy (x asc)
            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of(
                    "best", Doc.of("$top", Doc.of("sortBy", Doc.of("score", -1), "output", "$x")),
                    "worst", Doc.of("$bottom", Doc.of("sortBy", Doc.of("score", -1), "output", "$x")),
                    "best2", Doc.of("$topN", Doc.of("n", 2, "sortBy", Doc.of("score", -1), "output", "$x")),
                    "worst2", Doc.of("$bottomN", Doc.of("n", 2, "sortBy", Doc.of("score", -1), "output", "$x")))
            )).aggregateMap();

            Map<String, Object> d = byX(res, 1);
            assertEquals(2, d.get("best"));
            assertEquals(1, d.get("worst"));
            assertEquals(List.of(2, 3), d.get("best2"));
            assertEquals(List.of(3, 1), d.get("worst2"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void topN_invalidN_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "score", 10));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("t", Doc.of("$topN",
                        Doc.of("n", -1, "sortBy", Doc.of("score", -1), "output", "$x")))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5787908, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void top_missingSortBy_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "score", 10));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("t", Doc.of("$top", Doc.of("output", "$x")))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5397900, ex.getMongoCode());
        }
    }

    // ---- $expMovingAvg (#255) ------------------------------------------------------------

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void expMovingAvg_withN(Morphium morphium) throws Exception {
        try (morphium) {
            // N=2 -> alpha = 2/3: r1=10, r2=20*2/3+10*1/3=50/3, r3=30*2/3+(50/3)*1/3=230/9
            seed(morphium, Doc.of("x", 1, "v", 10), Doc.of("x", 2, "v", 20), Doc.of("x", 3, "v", 30));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("ema", Doc.of("$expMovingAvg", Doc.of("input", "$v", "N", 2)))
            )).aggregateMap();

            assertEquals(10.0, ((Number) byX(res, 1).get("ema")).doubleValue(), 1e-9);
            assertEquals(50.0 / 3.0, ((Number) byX(res, 2).get("ema")).doubleValue(), 1e-9);
            assertEquals(230.0 / 9.0, ((Number) byX(res, 3).get("ema")).doubleValue(), 1e-9);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void expMovingAvg_withAlpha_nonNumericKeepsState(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium,
                 Doc.of("x", 1, "v", 10),
                 Doc.of("x", 2, "v", "not a number"),
                 Doc.of("x", 3, "v", 20));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("ema", Doc.of("$expMovingAvg", Doc.of("input", "$v", "alpha", 0.5)))
            )).aggregateMap();

            assertEquals(10.0, ((Number) byX(res, 1).get("ema")).doubleValue(), 1e-9);
            assertNull(byX(res, 2).get("ema"), "non-numeric input yields null and keeps the state");
            assertEquals(15.0, ((Number) byX(res, 3).get("ema")).doubleValue(), 1e-9);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void expMovingAvg_withWindow_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 10));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("ema", Doc.of("$expMovingAvg", Doc.of("input", "$v", "N", 2),
                        "window", Doc.of("documents", List.of(-1, 0))))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5371601, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void expMovingAvg_bothNAndAlpha_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 10));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("ema", Doc.of("$expMovingAvg", Doc.of("input", "$v", "N", 2, "alpha", 0.5)))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5397900, ex.getMongoCode());
        }
    }

    // ---- $linearFill / $locf (#255) ------------------------------------------------------

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void linearFill_interpolatesProportionally(Morphium morphium) throws Exception {
        try (morphium) {
            // anchors at x=0 (v=0) and x=3 (v=30); x=4 has no right anchor -> stays null
            seed(morphium,
                 Doc.of("x", 0, "v", 0),
                 Doc.of("x", 1),
                 Doc.of("x", 2),
                 Doc.of("x", 3, "v", 30),
                 Doc.of("x", 4));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("filled", Doc.of("$linearFill", "$v"))
            )).aggregateMap();

            assertEquals(0.0, ((Number) byX(res, 0).get("filled")).doubleValue(), 1e-9);
            assertEquals(10.0, ((Number) byX(res, 1).get("filled")).doubleValue(), 1e-9);
            assertEquals(20.0, ((Number) byX(res, 2).get("filled")).doubleValue(), 1e-9);
            assertEquals(30.0, ((Number) byX(res, 3).get("filled")).doubleValue(), 1e-9);
            assertNull(byX(res, 4).get("filled"), "no right anchor -> stays null");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void linearFill_nonMonotonicSortBy_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 1), Doc.of("x", 1, "v", 2));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("filled", Doc.of("$linearFill", "$v"))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(605001, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void linearFill_withWindow_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("filled", Doc.of("$linearFill", "$v",
                        "window", Doc.of("documents", List.of(-1, 0))))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5371601, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void locf_carriesLastObservationForward(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium,
                 Doc.of("x", 1),
                 Doc.of("x", 2, "v", 5),
                 Doc.of("x", 3),
                 Doc.of("x", 4, "v", 7),
                 Doc.of("x", 5));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("filled", Doc.of("$locf", "$v"))
            )).aggregateMap();

            assertNull(byX(res, 1).get("filled"), "leading nulls stay null");
            assertEquals(5, byX(res, 2).get("filled"));
            assertEquals(5, byX(res, 3).get("filled"));
            assertEquals(7, byX(res, 4).get("filled"));
            assertEquals(7, byX(res, 5).get("filled"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void locf_withWindow_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("filled", Doc.of("$locf", "$v",
                        "window", Doc.of("documents", List.of(-1, 0))))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5371601, ex.getMongoCode());
        }
    }

    // ---- loud failure for the unsupported rest -------------------------------------------

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void unsupportedWindowFunction_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("d", Doc.of("$percentile", Doc.of("input", "$v", "p", List.of(0.5), "method", "approximate")))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5397901, ex.getMongoCode());
            assertTrue(ex.getMessage().contains("$percentile"));
        }
    }

    // ---- $derivative / $integral (#255) --------------------------------------------------

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void derivative_numericSortBy(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 0, "v", 0), Doc.of("x", 10, "v", 100), Doc.of("x", 30, "v", 140));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("dv", Doc.of("$derivative", Doc.of("input", "$v"),
                        "window", Doc.of("documents", List.of(-1, 0))))
            )).aggregateMap();

            assertNull(byX(res, 0).get("dv"), "single-document window has no derivative");
            assertEquals(10.0, ((Number) byX(res, 10).get("dv")).doubleValue(), 1e-9);
            assertEquals(2.0, ((Number) byX(res, 30).get("dv")).doubleValue(), 1e-9);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void derivative_unitHour_dateSortBy(Morphium morphium) throws Exception {
        try (morphium) {
            // 30 minutes apart, +30 in value -> 60 per hour
            seed(morphium,
                 Doc.of("x", 1, "t", new Date(0L), "v", 0),
                 Doc.of("x", 2, "t", new Date(1800000L), "v", 30));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("t", 1),
                "output", Doc.of("dv", Doc.of("$derivative", Doc.of("input", "$v", "unit", "hour"),
                        "window", Doc.of("documents", List.of(-1, 0))))
            )).aggregateMap();

            assertEquals(60.0, ((Number) byX(res, 2).get("dv")).doubleValue(), 1e-9);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void integral_trapezoid_numericSortBy(Morphium morphium) throws Exception {
        try (morphium) {
            // trapezoids: (0+10)/2*2 + (10+20)/2*2 = 10 + 30 = 40
            seed(morphium, Doc.of("x", 0, "v", 0), Doc.of("x", 2, "v", 10), Doc.of("x", 4, "v", 20));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("area", Doc.of("$integral", Doc.of("input", "$v")))
            )).aggregateMap();

            assertEquals(40.0, ((Number) byX(res, 0).get("area")).doubleValue(), 1e-9);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void integral_unitSecond_dateSortBy(Morphium morphium) throws Exception {
        try (morphium) {
            // constant 10 over 2 seconds -> 20
            seed(morphium,
                 Doc.of("x", 1, "t", new Date(0L), "v", 10),
                 Doc.of("x", 2, "t", new Date(2000L), "v", 10));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("t", 1),
                "output", Doc.of("area", Doc.of("$integral", Doc.of("input", "$v", "unit", "second")))
            )).aggregateMap();

            assertEquals(20.0, ((Number) byX(res, 1).get("area")).doubleValue(), 1e-9);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void derivative_withoutSortBy_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "output", Doc.of("dv", Doc.of("$derivative", Doc.of("input", "$v")))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5371801, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void derivative_monthUnit_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "t", new Date(0L), "v", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("t", 1),
                "output", Doc.of("dv", Doc.of("$derivative", Doc.of("input", "$v", "unit", "month")))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5397904, ex.getMongoCode());
        }
    }

    // ---- range windows (#255) ------------------------------------------------------------

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void rangeWindow_numericBounds(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium,
                 Doc.of("x", 0, "v", 1), Doc.of("x", 5, "v", 2),
                 Doc.of("x", 10, "v", 3), Doc.of("x", 15, "v", 4));

            // window = all docs whose x lies in [cur - 5, cur + 5]
            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("s", Doc.of("$sum", "$v", "window", Doc.of("range", List.of(-5, 5))))
            )).aggregateMap();

            assertEquals(3.0, ((Number) byX(res, 0).get("s")).doubleValue(), 1e-9);
            assertEquals(6.0, ((Number) byX(res, 5).get("s")).doubleValue(), 1e-9);
            assertEquals(9.0, ((Number) byX(res, 10).get("s")).doubleValue(), 1e-9);
            assertEquals(7.0, ((Number) byX(res, 15).get("s")).doubleValue(), 1e-9);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void rangeWindow_unboundedToCurrent(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 1), Doc.of("x", 2, "v", 2), Doc.of("x", 3, "v", 3), Doc.of("x", 4, "v", 4));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("s", Doc.of("$sum", "$v",
                        "window", Doc.of("range", List.of("unbounded", "current"))))
            )).aggregateMap();

            assertEquals(1.0, ((Number) byX(res, 1).get("s")).doubleValue(), 1e-9);
            assertEquals(3.0, ((Number) byX(res, 2).get("s")).doubleValue(), 1e-9);
            assertEquals(6.0, ((Number) byX(res, 3).get("s")).doubleValue(), 1e-9);
            assertEquals(10.0, ((Number) byX(res, 4).get("s")).doubleValue(), 1e-9);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void rangeWindow_unitDay_dateSortBy(Morphium morphium) throws Exception {
        try (morphium) {
            long day = 86400000L;
            seed(morphium,
                 Doc.of("x", 1, "t", new Date(1 * day), "v", 10),
                 Doc.of("x", 2, "t", new Date(2 * day), "v", 20),
                 Doc.of("x", 3, "t", new Date(5 * day), "v", 40));

            // [-1, 0] days: day 2 sees day 1 + day 2; day 5 only itself
            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("t", 1),
                "output", Doc.of("s", Doc.of("$sum", "$v",
                        "window", Doc.of("range", List.of(-1, 0), "unit", "day")))
            )).aggregateMap();

            assertEquals(10.0, ((Number) byX(res, 1).get("s")).doubleValue(), 1e-9);
            assertEquals(30.0, ((Number) byX(res, 2).get("s")).doubleValue(), 1e-9);
            assertEquals(40.0, ((Number) byX(res, 3).get("s")).doubleValue(), 1e-9);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void rangeWindow_descendingSortBy_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", -1),
                "output", Doc.of("s", Doc.of("$sum", "$v", "window", Doc.of("range", List.of(-1, 1))))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5339902, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void rangeWindow_withoutSortBy_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "output", Doc.of("s", Doc.of("$sum", "$v", "window", Doc.of("range", List.of(-1, 1))))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5339902, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void rangeWindow_nonNumericSortValue_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 1), Doc.of("x", "not a number", "v", 2));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("s", Doc.of("$sum", "$v", "window", Doc.of("range", List.of(-1, 1))))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5397905, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void rankWithoutSortBy_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "output", Doc.of("rnk", Doc.of("$rank", new java.util.HashMap<>()))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5371602, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void rankWithWindow_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("rnk", Doc.of("$rank", new java.util.HashMap<>(),
                        "window", Doc.of("documents", List.of(-1, 0))))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5371601, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void missingOutput_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of("sortBy", Doc.of("x", 1)));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5397900, ex.getMongoCode());
        }
    }
}
