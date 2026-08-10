package de.caluga.test.morphium.driver;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Task 6 (B2g): $stdDevPop / $stdDevSamp group accumulators + Expr evaluate, the async
 * aggregateMap(AsyncOperationCallback) implementation, and the aggregation swallow-site audit
 * (unrecognized pipeline stage / unknown group operator now surface as command errors instead
 * of an empty result).
 *
 * Hand-computed reference values (no reachable MongoDB in this environment):
 *   values = [2,4,4,4,5,5,7,9], n=8, mean=5
 *   sumSquaredDiff = 9+1+1+1+0+0+4+16 = 32
 *   popVariance   = 32/8 = 4        -> stdDevPop  = 2.0
 *   sampVariance  = 32/7 = 4.571... -> stdDevSamp = sqrt(32/7) = 2.13808993529939...
 */
@Tag("inmemory")
public class StdDevAggregationTest {

    private Morphium morphium;
    private InMemoryDriver drv;
    private final String db = "stddev_test";
    private final String coll = "stddev_items";

    @Entity(collectionName = "stddev_items")
    public static class StdDevItem {
        @Id
        public MorphiumId id;
        public String grp;
        public Object value;
    }

    @BeforeEach
    public void setup() throws Exception {
        MorphiumConfig cfg = new MorphiumConfig(db, 10, 10000, 1000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        drv = (InMemoryDriver) morphium.getDriver();
        new ClearCollectionCommand(drv).setDb(db).setColl(coll).doClear();
    }

    @AfterEach
    public void tearDown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    private void seed(Object... values) throws Exception {
        List<Map<String, Object>> docs = new java.util.ArrayList<>();

        for (Object v : values) {
            docs.add(Doc.of("_id", new MorphiumId(), "grp", "A", "value", v));
        }

        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();
    }

    @SuppressWarnings("unchecked")
    private Aggregator<StdDevItem, Map> aggregator() {
        Aggregator<StdDevItem, Map> agg = (Aggregator<StdDevItem, Map>) drv.createAggregator(morphium, StdDevItem.class, Map.class);
        agg.setCollectionName(coll);
        return agg;
    }

    // ---- group accumulator: hand-computed values ----------------------------------------

    @Test
    public void groupStdDevPopAndSamp_handComputedValues() throws Exception {
        seed(2, 4, 4, 4, 5, 5, 7, 9);

        Aggregator<StdDevItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$group", Doc.of(
            "_id", "$grp",
            "pop", Doc.of("$stdDevPop", "$value"),
            "samp", Doc.of("$stdDevSamp", "$value")
        )));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).hasSize(1);
        Map<String, Object> group = res.get(0);
        assertEquals(2.0, ((Number) group.get("pop")).doubleValue(), 1e-9);
        assertEquals(Math.sqrt(32.0 / 7.0), ((Number) group.get("samp")).doubleValue(), 1e-9);
    }

    @Test
    public void groupStdDevSamp_singleValue_isNull_popIsZero() throws Exception {
        seed(42);

        Aggregator<StdDevItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$group", Doc.of(
            "_id", "$grp",
            "pop", Doc.of("$stdDevPop", "$value"),
            "samp", Doc.of("$stdDevSamp", "$value")
        )));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).hasSize(1);
        Map<String, Object> group = res.get(0);
        assertEquals(0.0, ((Number) group.get("pop")).doubleValue(), 1e-9);
        assertNull(group.get("samp"), "sample stddev of a single value is undefined (n-1=0) -> null");
    }

    @Test
    public void groupStdDevPop_mixedTypes_ignoresNonNumeric() throws Exception {
        // Same numeric set as the hand-computed test, interspersed with non-numeric junk that
        // MongoDB's $stdDevPop/$stdDevSamp silently ignore.
        seed(2, "not-a-number", 4, 4, 4, 5, 5, null, 7, 9);

        Aggregator<StdDevItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$group", Doc.of(
            "_id", "$grp",
            "pop", Doc.of("$stdDevPop", "$value"),
            "samp", Doc.of("$stdDevSamp", "$value")
        )));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).hasSize(1);
        Map<String, Object> group = res.get(0);
        assertEquals(2.0, ((Number) group.get("pop")).doubleValue(), 1e-9);
        assertEquals(Math.sqrt(32.0 / 7.0), ((Number) group.get("samp")).doubleValue(), 1e-9);
    }

    // ---- Expr-level evaluate (array argument) --------------------------------------------

    @Test
    public void exprStdDevPop_overArrayField_handComputedValue() {
        Map<String, Object> context = Map.of("scores", List.of(2, 4, 4, 4, 5, 5, 7, 9));
        Object result = Expr.stdDevPop(Expr.field("scores")).evaluate(context);
        assertEquals(2.0, ((Number) result).doubleValue(), 1e-9);
    }

    @Test
    public void exprStdDevSamp_overArrayField_handComputedValue() {
        Map<String, Object> context = Map.of("scores", List.of(2, 4, 4, 4, 5, 5, 7, 9));
        Object result = Expr.stdDevSamp(Expr.field("scores")).evaluate(context);
        assertEquals(Math.sqrt(32.0 / 7.0), ((Number) result).doubleValue(), 1e-9);
    }

    @Test
    public void exprStdDevSamp_overArrayField_singleElement_isNull() {
        Map<String, Object> context = Map.of("scores", List.of(42));
        Object result = Expr.stdDevSamp(Expr.field("scores")).evaluate(context);
        assertNull(result);
    }

    @Test
    public void exprStdDevPop_overArray_ignoresNonNumeric() {
        Map<String, Object> context = Map.of("scores", Arrays.asList(2, "junk", 4, 4, 4, 5, 5, 7, 9));
        Object result = Expr.stdDevPop(Expr.field("scores")).evaluate(context);
        assertEquals(2.0, ((Number) result).doubleValue(), 1e-9);
    }

    @Test
    public void exprStdDevPop_variadicForm_handComputedValue() {
        // { $stdDevPop: [2,4,4,4,5,5,7,9] } parses to the varargs Expr[] form (Expr.parse spreads
        // array literals into individual expressions), mirroring $sum/$avg/$min/$max.
        Object result = Expr.stdDevPop(
            Expr.intExpr(2), Expr.intExpr(4), Expr.intExpr(4), Expr.intExpr(4),
            Expr.intExpr(5), Expr.intExpr(5), Expr.intExpr(7), Expr.intExpr(9)
        ).evaluate(Map.of());
        assertEquals(2.0, ((Number) result).doubleValue(), 1e-9);
    }

    // ---- async aggregateMap(AsyncOperationCallback) ---------------------------------------

    @Test
    public void asyncAggregateMap_callsBackWithResult() throws Exception {
        seed(2, 4, 4, 4, 5, 5, 7, 9);

        Aggregator<StdDevItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$group", Doc.of(
            "_id", "$grp",
            "pop", Doc.of("$stdDevPop", "$value")
        )));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<List<Map<String, Object>>> resultRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        agg.aggregateMap(new AsyncOperationCallback<Map<String, Object>>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<Map<String, Object>> q, long duration, List<Map<String, Object>> result, Map<String, Object> entity, Object... param) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<Map<String, Object>> q, long duration, String error, Throwable t, Map<String, Object> entity, Object... param) {
                errorRef.set(t);
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS), "aggregateMap callback must be invoked");
        assertNull(errorRef.get(), "must not have failed");
        assertNotNull(resultRef.get());
        assertEquals(1, resultRef.get().size());
        assertEquals(2.0, ((Number) resultRef.get().get(0).get("pop")).doubleValue(), 1e-9);
    }

    @Test
    public void asyncAggregateMap_callsBackWithError_onBadPipeline() throws Exception {
        seed(1);

        Aggregator<StdDevItem, Map> agg = aggregator();
        // $notARealStage does not exist -> must surface as a command error via onOperationError,
        // never hang and never silently succeed with an empty/partial result.
        agg.addOperator(UtilsMap.of("$notARealStage", Doc.of("x", 1)));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<List<Map<String, Object>>> resultRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        agg.aggregateMap(new AsyncOperationCallback<Map<String, Object>>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<Map<String, Object>> q, long duration, List<Map<String, Object>> result, Map<String, Object> entity, Object... param) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<Map<String, Object>> q, long duration, String error, Throwable t, Map<String, Object> entity, Object... param) {
                errorRef.set(t);
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS), "aggregateMap callback must be invoked even on failure");
        assertNull(resultRef.get(), "must not have silently succeeded with an (empty) result");
        assertNotNull(errorRef.get(), "onOperationError must be called with the failure");
        assertInstanceOf(MorphiumDriverException.class, errorRef.get());
        assertEquals(40324, ((MorphiumDriverException) errorRef.get()).getMongoCode());
    }

    // ---- swallow-site audit: command errors instead of empty results ---------------------

    @Test
    public void unrecognizedPipelineStage_throwsCommandError_notEmptyResult() throws Exception {
        seed(1, 2, 3);

        Aggregator<StdDevItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$totallyBogusStage", Doc.of("x", 1)));

        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
        assertEquals(40324, ex.getMongoCode());
        assertTrue(ex.getMessage().contains("$totallyBogusStage"));
    }

    @Test
    public void unknownGroupOperator_throwsCommandError_notEmptyResult() throws Exception {
        seed(1, 2, 3);

        Aggregator<StdDevItem, Map> agg = aggregator();
        // $bogusAccumulator is not a real accumulator and does not parse as a generic expression
        // either (a bare number is not itself an operator map) -> must be reported, not dropped.
        agg.addOperator(UtilsMap.of("$group", Doc.of(
            "_id", "$grp",
            "broken", Doc.of("$bogusAccumulator", "$value")
        )));

        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
        assertEquals(15952, ex.getMongoCode());
        assertTrue(ex.getMessage().contains("$bogusAccumulator"));
    }

    // ---- #237: mis-grouped $bucket case labels no longer silently run $bucket -------------

    @Test
    public void redactStage_throwsCommandError_notSilentlyRunningBucket() throws Exception {
        seed(1, 2, 3);

        Aggregator<StdDevItem, Map> agg = aggregator();
        // $redact used to share the $bucket switch body and silently returned an empty/nonsense
        // result instead of a real implementation or a proper "unsupported stage" error.
        agg.addOperator(UtilsMap.of("$redact", "$$KEEP"));

        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
        assertEquals(40324, ex.getMongoCode());
        assertTrue(ex.getMessage().contains("$redact"), "error must name the offending stage: " + ex.getMessage());
    }

    @Test
    public void unionWithStage_throwsCommandError_notSilentlyRunningBucket() throws Exception {
        seed(1, 2, 3);

        Aggregator<StdDevItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$unionWith", "someOtherCollection"));

        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
        assertEquals(40324, ex.getMongoCode());
        assertTrue(ex.getMessage().contains("$unionWith"), "error must name the offending stage: " + ex.getMessage());
    }

    @Test
    public void bucketOutputUnknownAccumulator_throwsCommandError_notNull() throws Exception {
        seed(1, 2, 3);

        Aggregator<StdDevItem, Map> agg = aggregator();
        // #237 sibling: the $bucket output accumulator helper (computeGroupValue) used to return
        // null for an unrecognized accumulator operator instead of reporting it.
        agg.addOperator(UtilsMap.of("$bucket", Doc.of(
            "groupBy", "$value",
            "boundaries", java.util.List.of(0, 2, 10),
            "default", "other",
            "output", Doc.of("bad", Doc.of("$bogusAccumulator", "$value"))
        )));

        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
        assertEquals(15952, ex.getMongoCode());
        assertTrue(ex.getMessage().contains("$bogusAccumulator"), "error must name the offending accumulator: " + ex.getMessage());
    }

    // ---- #238: $avg must not leak its internal $_calc_ bookkeeping key into group output --

    @Test
    public void groupAvg_doesNotLeakCalcBookkeepingKey() throws Exception {
        seed(2, 4, 6);

        Aggregator<StdDevItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$group", Doc.of(
            "_id", "$grp",
            "average", Doc.of("$avg", "$value")
        )));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).hasSize(1);
        Map<String, Object> group = res.get(0);
        assertEquals(4.0, ((Number) group.get("average")).doubleValue(), 1e-9);

        for (String key : group.keySet()) {
            assertFalse(key.startsWith("$_calc_"),
                "internal $_calc_ bookkeeping key must not leak into $avg group output, found: " + key);
        }
    }

    // ---- #243: $indexStats must not silently run $geoNear's body ---------------------------

    @Test
    public void indexStatsStage_throwsCommandError_notSilentlyRunningGeoNear() throws Exception {
        seed(1, 2, 3);

        Aggregator<StdDevItem, Map> agg = aggregator();
        // $indexStats used to share $geoNear's case body (distance calc/sort) - same mis-grouped
        // case-label bug class as #237. It is not implemented, so it must error, not run geoNear.
        agg.addOperator(UtilsMap.of("$indexStats", new java.util.HashMap<>()));

        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
        assertEquals(40324, ex.getMongoCode());
        assertTrue(ex.getMessage().contains("$indexStats"), "error must name the stage: " + ex.getMessage());
    }
}
