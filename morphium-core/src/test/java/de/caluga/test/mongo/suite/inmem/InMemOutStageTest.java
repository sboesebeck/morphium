package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * #254: the $out stage - it used to be a stub: the fluent builder accepted .out(...) but execution
 * always failed with "unrecognized stage". $out REPLACES the target collection with the pipeline
 * result (clear + write through the driver, so index/watcher bookkeeping stays intact), is
 * terminal and returns no documents.
 */
@Tag("inmemory")
public class InMemOutStageTest extends MorphiumInMemTestBase {

    private static final String SRC = "out_src";
    private static final String TARGET = "out_target";

    private InMemoryDriver drv() {
        return (InMemoryDriver) morphium.getDriver();
    }

    private String db() {
        return morphium.getConfig().connectionSettings().getDatabase();
    }

    @SafeVarargs
    private void seed(String coll, Map<String, Object>... docs) throws Exception {
        List<Map<String, Object>> l = new ArrayList<>();

        for (Map<String, Object> d : docs) {
            d.putIfAbsent("_id", new MorphiumId());
            l.add(d);
        }

        drv().store(db(), coll, l, null);
    }

    private List<Map<String, Object>> read(String db, String coll) throws Exception {
        return drv().find(db, coll, Doc.of(), null, null, 0, 0);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Aggregator<Object, Map> srcAgg() {
        Aggregator<Object, Map> agg = (Aggregator<Object, Map>) drv().createAggregator(morphium, Object.class, Map.class);
        agg.setCollectionName(SRC);
        return agg;
    }

    @Test
    public void outWritesResults_andIsTerminal() throws Exception {
        seed(SRC, Doc.of("k", "a"), Doc.of("k", "b"));

        Aggregator<Object, Map> agg = srcAgg();
        agg.addOperator(UtilsMap.of("$out", TARGET));
        List<Map<String, Object>> res = agg.aggregateMap();

        assertTrue(res.isEmpty(), "$out is terminal and returns no documents");
        assertEquals(2, read(db(), TARGET).size());
    }

    @Test
    public void outReplacesTargetEntirely() throws Exception {
        seed(TARGET, Doc.of("stale", 1), Doc.of("stale", 2), Doc.of("stale", 3));
        seed(SRC, Doc.of("fresh", 1));

        Aggregator<Object, Map> agg = srcAgg();
        agg.addOperator(UtilsMap.of("$out", TARGET));
        agg.aggregateMap();

        List<Map<String, Object>> target = read(db(), TARGET);
        assertEquals(1, target.size(), "old target content must be gone");
        assertEquals(1, target.get(0).get("fresh"));
        assertNull(target.get(0).get("stale"));
    }

    @Test
    public void outWithEmptyPipelineResult_clearsTarget() throws Exception {
        seed(TARGET, Doc.of("stale", 1));
        seed(SRC, Doc.of("k", "a"));

        Aggregator<Object, Map> agg = srcAgg();
        agg.addOperator(UtilsMap.of("$match", Doc.of("k", "no-such-value")));
        agg.addOperator(UtilsMap.of("$out", TARGET));
        agg.aggregateMap();

        assertEquals(0, read(db(), TARGET).size(), "replace semantics: empty result -> empty target");
    }

    @Test
    public void outLeavesSourceUntouched() throws Exception {
        seed(SRC, Doc.of("k", "a"), Doc.of("k", "b"));

        Aggregator<Object, Map> agg = srcAgg();
        agg.addOperator(UtilsMap.of("$out", TARGET));
        agg.aggregateMap();

        assertEquals(2, read(db(), SRC).size());
    }

    @Test
    public void outIntoOtherDb_viaDbCollMap() throws Exception {
        seed(SRC, Doc.of("k", "a"));

        Aggregator<Object, Map> agg = srcAgg();
        agg.addOperator(UtilsMap.of("$out", Doc.of("db", "out_other_db", "coll", TARGET)));
        agg.aggregateMap();

        assertEquals(1, read("out_other_db", TARGET).size());
        assertEquals(0, read(db(), TARGET).size(), "must not have written to the source db");
    }

    @Test
    public void outViaFluentBuilder_worksEndToEnd() throws Exception {
        // regression for the stub: .out(...) built a stage that always failed at execution
        seed(SRC, Doc.of("k", "a"));

        Aggregator<Object, Map> agg = srcAgg();
        agg.out(TARGET);
        List<Map<String, Object>> res = agg.aggregateMap();

        assertTrue(res.isEmpty());
        assertEquals(1, read(db(), TARGET).size());
    }

    @Test
    public void outAfterGroup_writesGroupResult() throws Exception {
        seed(SRC, Doc.of("k", "a", "v", 1), Doc.of("k", "a", "v", 2), Doc.of("k", "b", "v", 5));

        Aggregator<Object, Map> agg = srcAgg();
        agg.addOperator(UtilsMap.of("$group", Doc.of("_id", "$k", "sum", Doc.of("$sum", "$v"))));
        agg.addOperator(UtilsMap.of("$out", TARGET));
        agg.aggregateMap();

        List<Map<String, Object>> target = read(db(), TARGET);
        assertEquals(2, target.size());
    }

    @Test
    public void outWithoutCollection_throws() throws Exception {
        seed(SRC, Doc.of("k", "a"));

        Aggregator<Object, Map> agg = srcAgg();
        agg.addOperator(UtilsMap.of("$out", Doc.of("db", "somewhere")));
        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
        assertEquals(16994, ex.getMongoCode());
    }
}
