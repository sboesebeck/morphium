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
 * #254: the $collStats pipeline stage. Counts are real; byte-size fields carry the real BSON
 * data size (and an estimated index size) since dbStats/collStats learned to compute them.
 */
@Tag("inmemory")
public class CollStatsStageTest extends MultiDriverTestBase {

    private static final String COLL = "collstats_stage_test";

    private InMemoryDriver drv(Morphium m) {
        return (InMemoryDriver) m.getDriver();
    }

    private String db(Morphium m) {
        return m.getConfig().connectionSettings().getDatabase();
    }

    private void seed(Morphium m, int n) throws Exception {
        List<Map<String, Object>> l = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            l.add(Doc.of("_id", new MorphiumId(), "i", i));
        }

        drv(m).store(db(m), COLL, l, null);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Aggregator<Object, Map> agg(Morphium m, Map<String, Object> spec) {
        Aggregator<Object, Map> agg = (Aggregator<Object, Map>) drv(m).createAggregator(m, Object.class, Map.class);
        agg.setCollectionName(COLL);
        agg.addOperator(UtilsMap.of("$collStats", spec));
        return agg;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void countRequested_reportsRealCount(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, 3);

            List<Map<String, Object>> res = agg(morphium, Doc.of("count", new java.util.HashMap<>())).aggregateMap();
            assertEquals(1, res.size());
            Map<String, Object> stats = res.get(0);
            assertEquals(3, ((Number) stats.get("count")).intValue());
            assertEquals(db(morphium) + "." + COLL, stats.get("ns"));
            assertTrue(stats.get("localTime") instanceof Date);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void storageStats_countAndByteSizesReal(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, 2);

            List<Map<String, Object>> res = agg(morphium, Doc.of("storageStats", new java.util.HashMap<>())).aggregateMap();
            assertEquals(1, res.size());
            @SuppressWarnings("unchecked")
            Map<String, Object> storage = (Map<String, Object>) res.get(0).get("storageStats");
            assertNotNull(storage);
            assertEquals(2, ((Number) storage.get("count")).intValue());
            long size = ((Number) storage.get("size")).longValue();
            assertTrue(size > 0, "size must be the real BSON data size: " + storage);
            assertEquals(size, ((Number) storage.get("storageSize")).longValue(),
                "no padding/compression in memory - storageSize equals size");
            assertEquals(size / 2.0, ((Number) storage.get("avgObjSize")).doubleValue(), 0.001);
            assertTrue(((Number) storage.get("totalIndexSize")).longValue() > 0,
                "estimated index size for the _id index: " + storage);
            assertEquals(size + ((Number) storage.get("totalIndexSize")).longValue(),
                ((Number) storage.get("totalSize")).longValue());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void latencyAndQueryExecStats_presentWithZeroedCounters(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, 1);

            List<Map<String, Object>> res = agg(morphium, Doc.of(
                "latencyStats", Doc.of("histograms", false),
                "queryExecStats", new java.util.HashMap<>())).aggregateMap();
            assertEquals(1, res.size());
            assertNotNull(res.get(0).get("latencyStats"));
            assertNotNull(res.get(0).get("queryExecStats"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void emptySpec_returnsSingleMetaDoc(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, 1);

            List<Map<String, Object>> res = agg(morphium, new java.util.HashMap<>()).aggregateMap();
            assertEquals(1, res.size());
            assertEquals(db(morphium) + "." + COLL, res.get(0).get("ns"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void unknownField_throws(Morphium morphium) throws Exception {
        try (morphium) {
            seed(morphium, 1);

            Aggregator<Object, Map> agg = agg(morphium, Doc.of("bogusOption", new java.util.HashMap<>()));
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(40415, ex.getMongoCode());
            assertTrue(ex.getMessage().contains("bogusOption"));
        }
    }
}
