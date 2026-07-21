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
 * #254: the $fill stage - filling null/missing values with a constant expression ("value") or by
 * method "locf" (last observed carried forward) / "linear" (interpolation along the sortBy field),
 * optionally per partition.
 */
@Tag("inmemory")
public class FillStageTest extends MultiDriverTestBase {

    private static final String COLL = "fill_stage_test";

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
    private Aggregator<Object, Map> agg(Morphium m, Map<String, Object> fillSpec) {
        Aggregator<Object, Map> agg = (Aggregator<Object, Map>) drv(m).createAggregator(m, Object.class, Map.class);
        agg.setCollectionName(COLL);
        agg.addOperator(UtilsMap.of("$fill", fillSpec));
        return agg;
    }

    private Object valueAt(List<Map<String, Object>> docs, String sortField, Object sortValue, String field) {
        for (Map<String, Object> d : docs) {
            if (sortValue.equals(d.get(sortField))) {
                return d.get(field);
            }
        }

        fail("no doc with " + sortField + "=" + sortValue);
        return null;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void valueFill_replacesNullAndMissing_keepsExisting(Morphium morphium) throws Exception {
        try (morphium) {
            Map<String, Object> withNull = Doc.of("x", 1);
            withNull.put("v", null);
            seed(morphium, withNull, Doc.of("x", 2, "v", 7), Doc.of("x", 3));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "output", Doc.of("v", Doc.of("value", 0))
            )).aggregateMap();

            assertEquals(3, res.size());
            assertEquals(0, valueAt(res, "x", 1, "v"));
            assertEquals(7, valueAt(res, "x", 2, "v"));
            assertEquals(0, valueAt(res, "x", 3, "v"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void locf_carriesLastObservedForward_leadingNullsStayNull(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium,
                 Doc.of("x", 1), // leading missing - nothing observed yet
                 Doc.of("x", 2, "v", 5),
                 Doc.of("x", 3),
                 Doc.of("x", 4, "v", 9),
                 Doc.of("x", 5));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("v", Doc.of("method", "locf"))
            )).aggregateMap();

            assertNull(valueAt(res, "x", 1, "v"), "nothing observed before the first value stays null");
            assertEquals(5, valueAt(res, "x", 2, "v"));
            assertEquals(5, valueAt(res, "x", 3, "v"));
            assertEquals(9, valueAt(res, "x", 4, "v"));
            assertEquals(9, valueAt(res, "x", 5, "v"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void linear_interpolatesBetweenKnownPoints_edgesStayNull(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium,
                 Doc.of("x", 0, "v", 0),
                 Doc.of("x", 2),
                 Doc.of("x", 4, "v", 10),
                 Doc.of("x", 6)); // after the last known point - must stay null

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("v", Doc.of("method", "linear"))
            )).aggregateMap();

            assertEquals(5.0, ((Number) valueAt(res, "x", 2, "v")).doubleValue(), 1e-9);
            assertNull(valueAt(res, "x", 6, "v"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void locf_respectsPartitionBy(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium,
                 Doc.of("p", "a", "x", 1, "v", 1),
                 Doc.of("p", "a", "x", 2),
                 Doc.of("p", "b", "x", 1), // partition b has no prior observation
                 Doc.of("p", "b", "x", 2, "v", 99));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "partitionBy", "$p",
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("v", Doc.of("method", "locf"))
            )).aggregateMap();

            Object aFilled = res.stream().filter(d -> "a".equals(d.get("p")) && Integer.valueOf(2).equals(d.get("x")))
                .findFirst().get().get("v");
            Object bLeading = res.stream().filter(d -> "b".equals(d.get("p")) && Integer.valueOf(1).equals(d.get("x")))
                .findFirst().get().get("v");
            assertEquals(1, aFilled, "locf must not leak across partitions");
            assertNull(bLeading, "value from partition a must not leak into partition b");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void methodWithoutSortBy_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "output", Doc.of("v", Doc.of("method", "locf"))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(6050201, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void unknownMethod_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("v", Doc.of("method", "bogus"))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(6050200, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void missingOutput_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("x", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of("sortBy", Doc.of("x", 1)));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(6050200, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void linear_nonNumericValue_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium,
                 Doc.of("x", 0, "v", "notANumber"),
                 Doc.of("x", 1),
                 Doc.of("x", 2, "v", 5));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "sortBy", Doc.of("x", 1),
                "output", Doc.of("v", Doc.of("method", "linear"))));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(6050202, ex.getMongoCode());
        }
    }
}
