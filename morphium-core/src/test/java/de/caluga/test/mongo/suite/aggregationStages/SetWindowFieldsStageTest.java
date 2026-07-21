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

    // ---- loud failure for the unsupported rest -------------------------------------------

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void unsupportedWindowFunction_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("d", Doc.of("$derivative", Doc.of("input", "$v")))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5397901, ex.getMongoCode());
            assertTrue(ex.getMessage().contains("$derivative"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void rangeWindow_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1, "v", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("s", Doc.of("$sum", "$v", "window", Doc.of("range", List.of(-1, 1))))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5397902, ex.getMongoCode());
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
