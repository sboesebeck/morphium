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
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * #254: the $densify stage - gap filling for numeric and date sequences. Covers explicit array
 * bounds ([lower, upper) - lower inclusive, upper exclusive), "partition" and "full" bounds,
 * partitionByFields, date steps with a unit, and the spec validation errors.
 */
@Tag("inmemory")
public class DensifyStageTest extends MultiDriverTestBase {

    private static final String COLL = "densify_stage_test";

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
    private Aggregator<Object, Map> agg(Morphium m, Map<String, Object> densifySpec) {
        Aggregator<Object, Map> agg = (Aggregator<Object, Map>) drv(m).createAggregator(m, Object.class, Map.class);
        agg.setCollectionName(COLL);
        agg.addOperator(UtilsMap.of("$densify", densifySpec));
        return agg;
    }

    private Set<Double> numericValues(List<Map<String, Object>> docs, String field) {
        return docs.stream().filter(d -> d.get(field) != null)
               .map(d -> ((Number) d.get(field)).doubleValue()).collect(Collectors.toSet());
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void explicitBounds_lowerInclusiveUpperExclusive_skipsExisting(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("v", 4));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "field", "v",
                "range", Doc.of("step", 2, "bounds", List.of(0, 10))
            )).aggregateMap();

            // generated: 0, 2, 6, 8 (4 exists, 10 is excluded) + the existing doc
            assertEquals(5, res.size());
            assertEquals(Set.of(0.0, 2.0, 4.0, 6.0, 8.0), numericValues(res, "v"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void fullBounds_spansMinToMax(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("v", 0), Doc.of("v", 10));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "field", "v",
                "range", Doc.of("step", 2.5, "bounds", "full")
            )).aggregateMap();

            assertEquals(Set.of(0.0, 2.5, 5.0, 7.5, 10.0), numericValues(res, "v"));
            assertEquals(5, res.size());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void partitionBounds_densifiesEachPartitionSeparately(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium,
                 Doc.of("p", "a", "v", 0), Doc.of("p", "a", "v", 3),
                 Doc.of("p", "b", "v", 10), Doc.of("p", "b", "v", 12));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "field", "v",
                "partitionByFields", List.of("p"),
                "range", Doc.of("step", 1, "bounds", "partition")
            )).aggregateMap();

            List<Map<String, Object>> pa = res.stream().filter(d -> "a".equals(d.get("p"))).collect(Collectors.toList());
            List<Map<String, Object>> pb = res.stream().filter(d -> "b".equals(d.get("p"))).collect(Collectors.toList());
            assertEquals(Set.of(0.0, 1.0, 2.0, 3.0), numericValues(pa, "v"));
            assertEquals(Set.of(10.0, 11.0, 12.0), numericValues(pb, "v"));
            // generated docs must carry their partition fields
            for (Map<String, Object> d : res) {
                assertNotNull(d.get("p"), "generated documents must carry the partition fields");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void fullBounds_withPartitions_eachPartitionSpansGlobalRange(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium,
                 Doc.of("p", "a", "v", 0), Doc.of("p", "a", "v", 2),
                 Doc.of("p", "b", "v", 4));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "field", "v",
                "partitionByFields", List.of("p"),
                "range", Doc.of("step", 1, "bounds", "full")
            )).aggregateMap();

            List<Map<String, Object>> pa = res.stream().filter(d -> "a".equals(d.get("p"))).collect(Collectors.toList());
            List<Map<String, Object>> pb = res.stream().filter(d -> "b".equals(d.get("p"))).collect(Collectors.toList());
            // full range is the global [0, 4] for both partitions
            assertEquals(Set.of(0.0, 1.0, 2.0, 3.0, 4.0), numericValues(pa, "v"));
            assertEquals(Set.of(0.0, 1.0, 2.0, 3.0, 4.0), numericValues(pb, "v"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void dateDensify_withUnitHour(Morphium morphium) throws Exception {
        try (morphium) {
            long base = 1700000000000L - (1700000000000L % 3600000L); // on a full hour
            seed(morphium, Doc.of("ts", new Date(base)), Doc.of("ts", new Date(base + 4 * 3600000L)));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "field", "ts",
                "range", Doc.of("step", 1, "unit", "hour", "bounds", "full")
            )).aggregateMap();

            assertEquals(5, res.size(), "hours 0..4 -> 2 existing + 3 generated");
            Set<Long> times = res.stream().map(d -> ((Date) d.get("ts")).getTime()).collect(Collectors.toSet());

            for (int i = 0; i <= 4; i++) {
                assertTrue(times.contains(base + i * 3600000L), "missing hour offset " + i);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void dateDensify_withUnitMonth(Morphium morphium) throws Exception {
        try (morphium) {
            java.time.ZonedDateTime start = java.time.ZonedDateTime.of(2023, 1, 1, 0, 0, 0, 0, java.time.ZoneOffset.UTC);
            seed(morphium,
                 Doc.of("ts", Date.from(start.toInstant())),
                 Doc.of("ts", Date.from(start.plusMonths(3).toInstant())));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "field", "ts",
                "range", Doc.of("step", 1, "unit", "month", "bounds", "full")
            )).aggregateMap();

            assertEquals(4, res.size(), "Jan, Feb, Mar, Apr");
            Set<Long> times = res.stream().map(d -> ((Date) d.get("ts")).getTime()).collect(Collectors.toSet());
            assertTrue(times.contains(Date.from(start.plusMonths(1).toInstant()).getTime()));
            assertTrue(times.contains(Date.from(start.plusMonths(2).toInstant()).getTime()));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void docsWithoutDensifyField_areKept(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("v", 0), Doc.of("v", 2), Doc.of("other", "x"));

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "field", "v",
                "range", Doc.of("step", 1, "bounds", "full")
            )).aggregateMap();

            assertEquals(4, res.size(), "0,1,2 plus the field-less doc");
            assertTrue(res.stream().anyMatch(d -> "x".equals(d.get("other"))));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void nonPositiveStep_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("v", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "field", "v", "range", Doc.of("step", 0, "bounds", "full")));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5733303, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void missingRange_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("v", 1));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of("field", "v"));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5733201, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void unitOnNumericField_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("v", 1), Doc.of("v", 5));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "field", "v", "range", Doc.of("step", 1, "unit", "hour", "bounds", "full")));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5733201, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void dateFieldWithoutUnit_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, Doc.of("ts", new Date(0)), Doc.of("ts", new Date(7200000)));

            Aggregator<Object, Map> agg = agg(morphium, Doc.of(
                "field", "ts", "range", Doc.of("step", 1, "bounds", "full")));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5733201, ex.getMongoCode());
        }
    }
}
