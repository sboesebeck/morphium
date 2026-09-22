package de.caluga.test.morphium.driver;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the InMemAggregator stage-correctness cluster (#395): $skip, $unwind,
 * $lookup, $graphLookup, $sort and $search.
 */
@Tag("inmemory")
public class InMemAggregationStageFixesTest {

    private Morphium morphium;
    private InMemoryDriver drv;
    private final String db = "agg_stage_fixes";
    private final String coll = "stage_items";

    @BeforeEach
    public void setup() throws Exception {
        MorphiumConfig cfg = new MorphiumConfig(db, 10, 10000, 1000);
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        drv = (InMemoryDriver) morphium.getDriver();
        new ClearCollectionCommand(drv).setDb(db).setColl(coll).doClear();
    }

    @AfterEach
    public void cleanup() {
        if (morphium != null) {
            morphium.close();
        }
    }

    @Entity(collectionName = "stage_items")
    public static class StageItem {
        @Id
        public MorphiumId id;
        public String name;
        public int n;
    }

    @SuppressWarnings("unchecked")
    private Aggregator<StageItem, Map> aggregator() {
        Aggregator<StageItem, Map> agg = (Aggregator<StageItem, Map>) drv.createAggregator(morphium, StageItem.class, Map.class);
        agg.setCollectionName(coll);
        return agg;
    }

    private void insert(Map<String, Object>... docs) throws Exception {
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(Arrays.asList(docs))
                .execute();
    }

    // ---------------------------------------------------------------- $skip

    @Test
    public void skipReturnsTheRemainingDocuments() throws Exception {
        insert(Doc.of("_id", new MorphiumId(), "n", 0),
               Doc.of("_id", new MorphiumId(), "n", 1),
               Doc.of("_id", new MorphiumId(), "n", 2),
               Doc.of("_id", new MorphiumId(), "n", 3),
               Doc.of("_id", new MorphiumId(), "n", 4));

        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$sort", Doc.of("n", 1)));
        agg.addOperator(UtilsMap.of("$skip", 2));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).hasSize(3);
        assertThat(res.stream().map(m -> ((Number) m.get("n")).intValue()).collect(Collectors.toList()))
                .containsExactly(2, 3, 4);
    }

    @Test
    public void skipPastTheEndReturnsEmptyWithoutThrowing() throws Exception {
        insert(Doc.of("_id", new MorphiumId(), "n", 0),
               Doc.of("_id", new MorphiumId(), "n", 1),
               Doc.of("_id", new MorphiumId(), "n", 2));

        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$skip", 10));

        assertThat(agg.aggregateMap()).isEmpty();
    }

    @Test
    public void skipMoreThanHalfPreviouslyThrew() throws Exception {
        // 4 docs, $skip 3: the old subList(3, 4-3=1) threw IllegalArgumentException
        insert(Doc.of("_id", new MorphiumId(), "n", 0),
               Doc.of("_id", new MorphiumId(), "n", 1),
               Doc.of("_id", new MorphiumId(), "n", 2),
               Doc.of("_id", new MorphiumId(), "n", 3));

        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$sort", Doc.of("n", 1)));
        agg.addOperator(UtilsMap.of("$skip", 3));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).hasSize(1);
        assertThat(((Number) res.get(0).get("n")).intValue()).isEqualTo(3);
    }

    // ---------------------------------------------------------------- $unwind

    @Test
    public void unwindExpandsArraysAndKeepsDocumentsAfterAMissingOne() throws Exception {
        // The missing-tags document comes FIRST on purpose: the old code `break`ed out of the
        // whole input loop, dropping every document after it (#395).
        insert(Doc.of("_id", new MorphiumId(), "name", "no-tags"),
               Doc.of("_id", new MorphiumId(), "name", "a", "tags", List.of("x", "y")),
               Doc.of("_id", new MorphiumId(), "name", "b", "tags", List.of("z")));

        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$unwind", "$tags"));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).extracting(m -> (String) m.get("name")).containsExactly("a", "a", "b");
        assertThat(res).extracting(m -> m.get("tags")).containsExactly("x", "y", "z");
    }

    @Test
    public void unwindDropsOnlyTheDocumentWithAMissingPath() throws Exception {
        insert(Doc.of("_id", new MorphiumId(), "name", "a", "tags", List.of("x")),
               Doc.of("_id", new MorphiumId(), "name", "missing"),
               Doc.of("_id", new MorphiumId(), "name", "b", "tags", List.of("z")));

        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$unwind", "$tags"));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).extracting(m -> (String) m.get("name")).containsExactly("a", "b");
    }

    @Test
    public void unwindPreserveNullAndEmptyArraysKeepsTheDocument() throws Exception {
        insert(Doc.of("_id", new MorphiumId(), "name", "a", "tags", List.of("x")),
               Doc.of("_id", new MorphiumId(), "name", "missing"),
               Doc.of("_id", new MorphiumId(), "name", "empty", "tags", List.of()));

        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$unwind", Doc.of(
                "path", "$tags",
                "preserveNullAndEmptyArrays", true)));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).extracting(m -> (String) m.get("name"))
                .containsExactly("a", "missing", "empty");
        // the preserved documents do not gain the unwound field value
        assertThat(res.stream().filter(m -> "missing".equals(m.get("name"))).findFirst().get())
                .doesNotContainKey("tags");
    }

    @Test
    public void unwindWithIncludeArrayIndexRecordsTheIndex() throws Exception {
        insert(Doc.of("_id", new MorphiumId(), "name", "a", "tags", List.of("x", "y", "z")));

        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$unwind", Doc.of(
                "path", "$tags",
                "includeArrayIndex", "pos")));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).extracting(m -> ((Number) m.get("pos")).longValue())
                .containsExactly(0L, 1L, 2L);
        assertThat(res).extracting(m -> m.get("tags")).containsExactly("x", "y", "z");
    }

    @Test
    public void unwindScalarValueEmitsTheDocumentOnce() throws Exception {
        insert(Doc.of("_id", new MorphiumId(), "name", "a", "tags", "single"));

        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$unwind", "$tags"));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).hasSize(1);
        assertThat(res.get(0).get("tags")).isEqualTo("single");
    }

    @Test
    public void unwindDottedPathReplacesTheNestedValueWithoutMutatingTheSource() throws Exception {
        Map<String, Object> nested = Doc.of("tags", List.of("x", "y"));
        Doc source = Doc.of("_id", new MorphiumId(), "name", "a", "inner", nested);
        insert(source);

        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$unwind", "$inner.tags"));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).hasSize(2);
        assertThat(res).extracting(m -> ((Map) m.get("inner")).get("tags")).containsExactly("x", "y");
        // the stored source document must not have been touched
        assertThat(nested.get("tags")).isEqualTo(List.of("x", "y"));
    }

    @Test
    public void unwindWithoutPathThrowsACommandError() throws Exception {
        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$unwind", Doc.of("preserveNullAndEmptyArrays", true)));

        assertThatThrownBy(agg::aggregateMap).isNotNull();
    }

    // ---------------------------------------------------------------- $sort

    @Test
    public void sortMissingFieldDoesNotThrowAndSortsItFirst() throws Exception {
        insert(Doc.of("_id", new MorphiumId(), "n", 2),
               Doc.of("_id", new MorphiumId(), "name", "no-n"),
               Doc.of("_id", new MorphiumId(), "n", 1));

        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$sort", Doc.of("n", 1)));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).hasSize(3);
        assertThat(res.get(0).get("n")).isNull();
        assertThat(((Number) res.get(1).get("n")).intValue()).isEqualTo(1);
        assertThat(((Number) res.get(2).get("n")).intValue()).isEqualTo(2);
    }

    @Test
    public void sortResolvesDottedPaths() throws Exception {
        insert(Doc.of("_id", new MorphiumId(), "name", "a", "inner", Doc.of("k", 2)),
               Doc.of("_id", new MorphiumId(), "name", "b", "inner", Doc.of("k", 1)));

        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$sort", Doc.of("inner.k", 1)));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).extracting(m -> (String) m.get("name")).containsExactly("b", "a");
    }

    @Test
    public void sortDescending() throws Exception {
        insert(Doc.of("_id", new MorphiumId(), "n", 1),
               Doc.of("_id", new MorphiumId(), "n", 3),
               Doc.of("_id", new MorphiumId(), "n", 2));

        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$sort", Doc.of("n", -1)));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).extracting(m -> ((Number) m.get("n")).intValue()).containsExactly(3, 2, 1);
    }

    // ---------------------------------------------------------------- $search

    @Test
    public void searchFailsLoudlyInsteadOfDroppingEveryDocument() throws Exception {
        insert(Doc.of("_id", new MorphiumId(), "name", "a"));

        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$search", Doc.of("text", Doc.of("query", "x"))));

        assertThatThrownBy(agg::aggregateMap).isNotNull();
    }

    // ---------------------------------------------------------------- $lookup

    private static final String FOREIGN = "stage_foreign";

    private void insertForeign(Map<String, Object>... docs) throws Exception {
        new InsertMongoCommand(drv).setDb(db).setColl(FOREIGN)
                .setDocuments(Arrays.asList(docs))
                .execute();
    }

    @Test
    public void lookupMatchesDottedLocalPath() throws Exception {
        insertForeign(Doc.of("_id", "c1", "name", "one"),
                      Doc.of("_id", "c2", "name", "two"));
        insert(Doc.of("_id", new MorphiumId(), "name", "a", "inner", Doc.of("key", "c1")));

        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$lookup", Doc.of(
                "from", FOREIGN,
                "localField", "inner.key",
                "foreignField", "_id",
                "as", "joined")));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).hasSize(1);
        List<Map<String, Object>> joined = (List<Map<String, Object>>) res.get(0).get("joined");
        assertThat(joined).hasSize(1);
        assertThat(joined.get(0).get("name")).isEqualTo("one");
    }

    @Test
    public void lookupMatchesArrayValuedJoinKey() throws Exception {
        insertForeign(Doc.of("_id", "c2", "name", "two"));
        insert(Doc.of("_id", new MorphiumId(), "name", "a", "refs", List.of("c1", "c2")));

        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$lookup", Doc.of(
                "from", FOREIGN,
                "localField", "refs",
                "foreignField", "_id",
                "as", "joined")));

        List<Map<String, Object>> res = agg.aggregateMap();
        List<Map<String, Object>> joined = (List<Map<String, Object>>) res.get(0).get("joined");
        assertThat(joined).hasSize(1);
        assertThat(joined.get(0).get("name")).isEqualTo("two");
    }

    @Test
    public void lookupLetWithoutPipelineThrowsInsteadOfCrossJoining() throws Exception {
        insertForeign(Doc.of("_id", "c1"), Doc.of("_id", "c2"));
        insert(Doc.of("_id", new MorphiumId(), "name", "a", "key", "c1"));

        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$lookup", Doc.of(
                "from", FOREIGN,
                "let", Doc.of("k", "$key"),
                "as", "joined")));

        assertThatThrownBy(agg::aggregateMap).isNotNull();
    }

    // ---------------------------------------------------------------- $graphLookup

    private void insertHierarchy() throws Exception {
        insert(Doc.of("_id", new MorphiumId(), "name", "Dev", "reports_to", null),
               Doc.of("_id", new MorphiumId(), "name", "Eliot", "reports_to", "Dev"),
               Doc.of("_id", new MorphiumId(), "name", "Ron", "reports_to", "Eliot"),
               Doc.of("_id", new MorphiumId(), "name", "Andrew", "reports_to", "Eliot"));
    }

    private Aggregator<StageItem, Map> andrewHierarchy(Integer maxDepth) {
        Aggregator<StageItem, Map> agg = aggregator();
        agg.addOperator(UtilsMap.of("$match", Doc.of("name", "Andrew")));
        Map<String, Object> spec = Doc.of(
                "from", coll,
                "startWith", "$reports_to",
                "connectFromField", "reports_to",
                "connectToField", "name",
                "as", "chain",
                "depthField", "depth");

        if (maxDepth != null) {
            spec.put("maxDepth", maxDepth);
        }

        agg.addOperator(UtilsMap.of("$graphLookup", spec));
        return agg;
    }

    @Test
    public void graphLookupFollowsTheReportingHierarchy() throws Exception {
        insertHierarchy();

        List<Map<String, Object>> res = andrewHierarchy(null).aggregateMap();
        assertThat(res).hasSize(1);
        List<Map<String, Object>> chain = (List<Map<String, Object>>) res.get(0).get("chain");
        Map<String, Map<String, Object>> byName = chain.stream()
                .collect(Collectors.toMap(m -> (String) m.get("name"), m -> m));

        assertThat(byName).containsOnlyKeys("Eliot", "Dev");
        assertThat(((Number) byName.get("Eliot").get("depth")).intValue()).isEqualTo(0);
        assertThat(((Number) byName.get("Dev").get("depth")).intValue()).isEqualTo(1);
    }

    @Test
    public void graphLookupMaxDepthZeroReturnsTheImmediateMatch() throws Exception {
        insertHierarchy();

        List<Map<String, Object>> res = andrewHierarchy(0).aggregateMap();
        List<Map<String, Object>> chain = (List<Map<String, Object>>) res.get(0).get("chain");
        assertThat(chain).hasSize(1);
        assertThat(chain.get(0).get("name")).isEqualTo("Eliot");
    }
}
