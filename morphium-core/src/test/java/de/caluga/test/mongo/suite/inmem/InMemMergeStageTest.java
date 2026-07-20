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
 * #241: the $merge stage. It used to report success and write nothing at all; this covers the real
 * implementation - whenMatched (merge/replace/keepExisting/fail), whenNotMatched
 * (insert/discard/fail), the `on` default of _id, ambiguous matches, and $merge being terminal.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@Tag("inmemory")
public class InMemMergeStageTest extends MorphiumInMemTestBase {

    private static final String SRC = "merge_src";
    private static final String TARGET = "merge_target";

    private InMemoryDriver drv() {
        return (InMemoryDriver) morphium.getDriver();
    }

    private String db() {
        return morphium.getConfig().connectionSettings().getDatabase();
    }

    private void seed(String coll, Map<String, Object>... docs) throws Exception {
        List<Map<String, Object>> l = new ArrayList<>();
        for (Map<String, Object> d : docs) {
            d.putIfAbsent("_id", new MorphiumId());
            l.add(d);
        }
        drv().store(db(), coll, l, null);
    }

    private List<Map<String, Object>> target() throws Exception {
        return drv().find(db(), TARGET, Doc.of(), null, null, 0, 0);
    }

    /** Aggregator over SRC that ends in the given $merge spec. */
    private Aggregator<Object, Map> mergeAgg(Map<String, Object> mergeSpec) {
        Aggregator<Object, Map> agg = (Aggregator<Object, Map>) drv().createAggregator(morphium, Object.class, Map.class);
        agg.setCollectionName(SRC);
        agg.addOperator(UtilsMap.of("$merge", mergeSpec));
        return agg;
    }

    // ---- whenNotMatched ------------------------------------------------------------------

    @Test
    public void mergeInsertsWhenNotMatched_andIsTerminal() throws Exception {
        seed(SRC, Doc.of("k", "a", "v", 1), Doc.of("k", "b", "v", 2));

        List<Map<String, Object>> out = mergeAgg(Doc.of("into", TARGET, "on", List.of("k"))).aggregateMap();

        assertTrue(out.isEmpty(), "$merge is a terminal stage and returns no documents");
        assertEquals(2, target().size(), "both source documents must be written to the target");
    }

    @Test
    public void whenNotMatchedDiscard_writesNothing() throws Exception {
        seed(SRC, Doc.of("k", "a", "v", 1));

        mergeAgg(Doc.of("into", TARGET, "on", List.of("k"), "whenNotMatched", "discard")).aggregateMap();

        assertEquals(0, target().size(), "discard must not write unmatched documents");
    }

    @Test
    public void whenNotMatchedFail_throws() throws Exception {
        seed(SRC, Doc.of("k", "a", "v", 1));

        Aggregator<Object, Map> agg = mergeAgg(Doc.of("into", TARGET, "on", List.of("k"), "whenNotMatched", "fail"));
        var ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
        assertFalse(ex.getMessage().contains("not supported"),
            "must fail because of whenNotMatched:fail, not because $merge is unimplemented: " + ex.getMessage());
    }

    // ---- whenMatched ---------------------------------------------------------------------

    @Test
    public void whenMatchedMerge_isDefault_andNewFieldsWin() throws Exception {
        MorphiumId existingId = new MorphiumId();
        seed(TARGET, Doc.of("_id", existingId, "k", "a", "v", 99, "keepMe", "yes"));
        seed(SRC, Doc.of("k", "a", "v", 1));

        mergeAgg(Doc.of("into", TARGET, "on", List.of("k"))).aggregateMap();

        assertEquals(1, target().size(), "must update in place, not add a second document");
        Map<String, Object> doc = target().get(0);
        assertEquals(1, ((Number) doc.get("v")).intValue(), "the incoming value must win over the existing one");
        assertEquals("yes", doc.get("keepMe"), "fields only present in the target are kept");
        assertEquals(existingId, doc.get("_id"), "the target's _id must be preserved");
    }

    @Test
    public void whenMatchedReplace_dropsFieldsNotInTheNewDocument() throws Exception {
        MorphiumId existingId = new MorphiumId();
        seed(TARGET, Doc.of("_id", existingId, "k", "a", "v", 99, "goneAfterReplace", "x"));
        seed(SRC, Doc.of("k", "a", "v", 1));

        mergeAgg(Doc.of("into", TARGET, "on", List.of("k"), "whenMatched", "replace")).aggregateMap();

        assertEquals(1, target().size());
        Map<String, Object> doc = target().get(0);
        assertEquals(1, ((Number) doc.get("v")).intValue());
        assertFalse(doc.containsKey("goneAfterReplace"), "replace must not keep target-only fields");
        assertEquals(existingId, doc.get("_id"), "the target's _id must be preserved");
    }

    @Test
    public void whenMatchedKeepExisting_leavesTargetUntouched() throws Exception {
        seed(TARGET, Doc.of("k", "a", "v", 99));
        seed(SRC, Doc.of("k", "a", "v", 1));

        mergeAgg(Doc.of("into", TARGET, "on", List.of("k"), "whenMatched", "keepExisting")).aggregateMap();

        assertEquals(1, target().size());
        assertEquals(99, ((Number) target().get(0).get("v")).intValue(), "existing value must survive");
    }

    @Test
    public void whenMatchedFail_throws() throws Exception {
        seed(TARGET, Doc.of("k", "a", "v", 99));
        seed(SRC, Doc.of("k", "a", "v", 1));

        Aggregator<Object, Map> agg = mergeAgg(Doc.of("into", TARGET, "on", List.of("k"), "whenMatched", "fail"));
        var ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
        assertFalse(ex.getMessage().contains("not supported"),
            "must fail because of whenMatched:fail, not because $merge is unimplemented: " + ex.getMessage());
    }

    // ---- `on` semantics ------------------------------------------------------------------

    @Test
    public void onDefaultsToId() throws Exception {
        MorphiumId shared = new MorphiumId();
        seed(TARGET, Doc.of("_id", shared, "v", 99));
        seed(SRC, Doc.of("_id", shared, "v", 1));

        mergeAgg(Doc.of("into", TARGET)).aggregateMap();

        assertEquals(1, target().size(), "matching on the default _id must update in place");
        assertEquals(1, ((Number) target().get(0).get("v")).intValue());
    }

    @Test
    public void onAsPlainStringIsAccepted() throws Exception {
        seed(TARGET, Doc.of("k", "a", "v", 99));
        seed(SRC, Doc.of("k", "a", "v", 1));

        mergeAgg(Doc.of("into", TARGET, "on", "k")).aggregateMap();

        assertEquals(1, target().size());
        assertEquals(1, ((Number) target().get(0).get("v")).intValue());
    }

    @Test
    public void ambiguousOnMatch_throws() throws Exception {
        seed(TARGET, Doc.of("k", "a", "v", 1), Doc.of("k", "a", "v", 2));
        seed(SRC, Doc.of("k", "a", "v", 3));

        Aggregator<Object, Map> agg = mergeAgg(Doc.of("into", TARGET, "on", List.of("k")));
        var ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap,
            "`on` must identify at most one target document");
        assertFalse(ex.getMessage().contains("not supported"),
            "must fail because the match is ambiguous, not because $merge is unimplemented: " + ex.getMessage());
    }

    @Test
    public void intoAsDbAndCollDocument_isSupported() throws Exception {
        seed(SRC, Doc.of("k", "a", "v", 1));

        mergeAgg(Doc.of("into", Doc.of("db", db(), "coll", TARGET), "on", List.of("k"))).aggregateMap();

        assertEquals(1, target().size(), "into may be given as {db, coll}");
    }
}
