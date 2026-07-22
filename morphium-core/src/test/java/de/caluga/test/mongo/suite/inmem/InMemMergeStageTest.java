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
    @SuppressWarnings({"rawtypes", "unchecked"})
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

    // ---- whenMatched as a pipeline (#241) ------------------------------------------------

    @Test
    public void pipelineWhenMatched_setWithNewVariable_combinesExistingAndIncoming() throws Exception {
        MorphiumId existingId = new MorphiumId();
        seed(TARGET, Doc.of("_id", existingId, "k", "a", "counter", 10, "keepMe", "yes"));
        seed(SRC, Doc.of("k", "a", "counter", 5));

        List<Map<String, Object>> pipeline = List.of(
            Doc.of("$set", Doc.of("counter", Doc.of("$add", List.of("$counter", "$$new.counter")))));
        mergeAgg(Doc.of("into", TARGET, "on", List.of("k"), "whenMatched", pipeline)).aggregateMap();

        assertEquals(1, target().size(), "must update in place, not add a second document");
        Map<String, Object> doc = target().get(0);
        assertEquals(15, ((Number) doc.get("counter")).intValue(),
            "$counter is the existing value, $$new.counter the incoming one - their sum must be stored");
        assertEquals("yes", doc.get("keepMe"), "$set operates on the existing document, target-only fields survive");
        assertEquals(existingId, doc.get("_id"), "the target's _id must be preserved");
    }

    @Test
    public void pipelineWhenMatched_replaceRootWithNew_behavesLikeReplace() throws Exception {
        MorphiumId existingId = new MorphiumId();
        seed(TARGET, Doc.of("_id", existingId, "k", "a", "v", 99, "goneAfterReplace", "x"));
        seed(SRC, Doc.of("k", "a", "v", 1));

        List<Map<String, Object>> pipeline = List.of(Doc.of("$replaceRoot", Doc.of("newRoot", "$$new")));
        mergeAgg(Doc.of("into", TARGET, "on", List.of("k"), "whenMatched", pipeline)).aggregateMap();

        assertEquals(1, target().size());
        Map<String, Object> doc = target().get(0);
        assertEquals(1, ((Number) doc.get("v")).intValue(), "the incoming document must replace the target");
        assertFalse(doc.containsKey("goneAfterReplace"), "replaceRoot with $$new must not keep target-only fields");
        assertEquals(existingId, doc.get("_id"), "the target's _id must be preserved");
    }

    @Test
    public void pipelineWhenMatched_replaceWithNew_isEquivalentToReplaceRoot() throws Exception {
        seed(TARGET, Doc.of("k", "a", "v", 99, "goneAfterReplace", "x"));
        seed(SRC, Doc.of("k", "a", "v", 1));

        List<Map<String, Object>> pipeline = List.of(Doc.of("$replaceWith", "$$new"));
        mergeAgg(Doc.of("into", TARGET, "on", List.of("k"), "whenMatched", pipeline)).aggregateMap();

        assertEquals(1, target().size());
        Map<String, Object> doc = target().get(0);
        assertEquals(1, ((Number) doc.get("v")).intValue());
        assertFalse(doc.containsKey("goneAfterReplace"));
    }

    @Test
    public void pipelineWhenMatched_unsetAndSet_areApplied() throws Exception {
        seed(TARGET, Doc.of("k", "a", "v", 99, "tmp", "dropMe"));
        seed(SRC, Doc.of("k", "a", "v", 1));

        List<Map<String, Object>> pipeline = List.of(
            Doc.of("$unset", "tmp"),
            Doc.of("$set", Doc.of("merged", true, "incomingV", "$$new.v")));
        mergeAgg(Doc.of("into", TARGET, "on", List.of("k"), "whenMatched", pipeline)).aggregateMap();

        assertEquals(1, target().size());
        Map<String, Object> doc = target().get(0);
        assertFalse(doc.containsKey("tmp"), "$unset must remove the field");
        assertEquals(Boolean.TRUE, doc.get("merged"));
        assertEquals(1, ((Number) doc.get("incomingV")).intValue());
        assertEquals(99, ((Number) doc.get("v")).intValue(), "fields not touched by the pipeline stay as in the target");
    }

    @Test
    public void pipelineWhenMatched_projectInclusion_keepsOnlyListedFields() throws Exception {
        seed(TARGET, Doc.of("k", "a", "v", 99, "dropped", "x"));
        seed(SRC, Doc.of("k", "a", "v", 1));

        List<Map<String, Object>> pipeline = List.of(Doc.of("$project", Doc.of("k", 1, "v", 1)));
        mergeAgg(Doc.of("into", TARGET, "on", List.of("k"), "whenMatched", pipeline)).aggregateMap();

        assertEquals(1, target().size());
        Map<String, Object> doc = target().get(0);
        assertEquals("a", doc.get("k"));
        assertEquals(99, ((Number) doc.get("v")).intValue(), "$project reads from the existing document");
        assertFalse(doc.containsKey("dropped"), "inclusion projection must drop unlisted fields");
        assertNotNull(doc.get("_id"), "_id is kept by an inclusion projection");
    }

    @Test
    public void pipelineWhenMatched_customLet_bindsVariable() throws Exception {
        seed(TARGET, Doc.of("k", "a", "v", 99));
        seed(SRC, Doc.of("k", "a", "v", 1));

        List<Map<String, Object>> pipeline = List.of(
            Doc.of("$set", Doc.of("v", Doc.of("$add", List.of("$v", "$$inc")))));
        mergeAgg(Doc.of("into", TARGET, "on", List.of("k"),
                        "let", Doc.of("inc", "$v"),
                        "whenMatched", pipeline)).aggregateMap();

        assertEquals(1, target().size());
        assertEquals(100, ((Number) target().get(0).get("v")).intValue(),
            "$$inc is the incoming v (1), added to the existing v (99)");
    }

    @Test
    public void pipelineWhenMatched_customLetWithRootExpression_seesIncomingDocument() throws Exception {
        seed(TARGET, Doc.of("k", "a", "v", 99));
        seed(SRC, Doc.of("k", "a", "v", 1));

        List<Map<String, Object>> pipeline = List.of(Doc.of("$replaceRoot", Doc.of("newRoot", "$$incoming")));
        mergeAgg(Doc.of("into", TARGET, "on", List.of("k"),
                        "let", Doc.of("incoming", "$$ROOT"),
                        "whenMatched", pipeline)).aggregateMap();

        assertEquals(1, target().size());
        assertEquals(1, ((Number) target().get(0).get("v")).intValue(),
            "$$ROOT in a let expression is the incoming document");
    }

    @Test
    public void pipelineWhenMatched_customLet_newVariableIsUnavailable() throws Exception {
        seed(TARGET, Doc.of("k", "a", "v", 99));
        seed(SRC, Doc.of("k", "a", "v", 1));

        List<Map<String, Object>> pipeline = List.of(Doc.of("$set", Doc.of("x", "$$new.v")));
        Aggregator<Object, Map> agg = mergeAgg(Doc.of("into", TARGET, "on", List.of("k"),
                                               "let", Doc.of("inc", "$v"),
                                               "whenMatched", pipeline));

        var ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap,
            "a custom let replaces the default {new: $$ROOT}, so $$new must be undefined");
        assertTrue(ex.getMessage().contains("new"),
            "error must name the undefined variable: " + ex.getMessage());
    }

    @Test
    public void pipelineWhenMatched_unknownStage_throws() throws Exception {
        seed(TARGET, Doc.of("k", "a", "v", 99));
        seed(SRC, Doc.of("k", "a", "v", 1));

        List<Map<String, Object>> pipeline = List.of(Doc.of("$match", Doc.of("v", 1)));
        Aggregator<Object, Map> agg = mergeAgg(Doc.of("into", TARGET, "on", List.of("k"), "whenMatched", pipeline));

        var ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap,
            "only $addFields/$set, $project/$unset and $replaceRoot/$replaceWith are allowed");
        assertTrue(ex.getMessage().contains("$match"),
            "error must name the offending stage: " + ex.getMessage());
    }

    @Test
    public void letWithoutPipelineWhenMatched_throws() throws Exception {
        seed(SRC, Doc.of("k", "a", "v", 1));

        Aggregator<Object, Map> agg = mergeAgg(Doc.of("into", TARGET, "on", List.of("k"),
                                               "let", Doc.of("inc", "$v"),
                                               "whenMatched", "merge"));

        var ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
        assertTrue(ex.getMessage().contains("let"),
            "let is only allowed when whenMatched is a pipeline: " + ex.getMessage());
    }

    @Test
    public void pipelineWhenMatched_unmatchedDocumentsAreInsertedUnchanged() throws Exception {
        seed(TARGET, Doc.of("k", "a", "v", 10));
        seed(SRC, Doc.of("k", "a", "v", 1), Doc.of("k", "b", "v", 2));

        List<Map<String, Object>> pipeline = List.of(
            Doc.of("$set", Doc.of("v", Doc.of("$add", List.of("$v", "$$new.v")))));
        mergeAgg(Doc.of("into", TARGET, "on", List.of("k"), "whenMatched", pipeline)).aggregateMap();

        assertEquals(2, target().size(), "the unmatched document must be inserted");

        for (Map<String, Object> doc : target()) {
            if ("a".equals(doc.get("k"))) {
                assertEquals(11, ((Number) doc.get("v")).intValue(), "matched doc goes through the pipeline");
            } else {
                assertEquals("b", doc.get("k"));
                assertEquals(2, ((Number) doc.get("v")).intValue(), "unmatched doc is inserted as-is, not piped");
            }
        }
    }
}
