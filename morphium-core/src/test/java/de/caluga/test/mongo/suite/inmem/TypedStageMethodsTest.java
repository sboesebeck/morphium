package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.AggregatorImpl;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Typed builder methods for the stages implemented in #254 ($documents, $densify, $fill,
 * $setWindowFields) - before these existed, the stages were only reachable via genericStage().
 * Field names in the specs are translated like every other typed stage method (see
 * AggregatorFieldNameTranslationTest for the general mechanism and why AggregatorImpl has to
 * be tested explicitly next to the InMemAggregator the test-driver returns).
 */
@SuppressWarnings("rawtypes")
@Tag("inmemory")
public class TypedStageMethodsTest extends MorphiumInMemTestBase {

    @NoCache
    @Entity(translateCamelCase = true)
    public static class StageEntity {
        @Id
        private MorphiumId id;
        private String groupName;
        private int itemCount;

        public StageEntity() {}

        public StageEntity(String groupName, int itemCount) {
            this.groupName = groupName;
            this.itemCount = itemCount;
        }

        public enum Fields { groupName, itemCount }
    }

    private List<Aggregator<StageEntity, Map>> bothImplementations() {
        return List.of(
                   morphium.createAggregator(StageEntity.class, Map.class),
                   new AggregatorImpl<>(morphium, StageEntity.class, Map.class));
    }

    private Map<String, Object> firstStage(Aggregator<StageEntity, Map> agg) {
        List<Map<String, Object>> pipeline = agg.getPipeline();
        assertFalse(pipeline.isEmpty(), "pipeline should have at least one stage");
        return pipeline.get(0);
    }

    @Test
    public void documentsBuildsStage() {
        List<Map<String, Object>> docs = List.of(Doc.of("a", 1), Doc.of("a", 2));

        for (Aggregator<StageEntity, Map> agg : bothImplementations()) {
            Map<String, Object> stage = firstStage(agg.documents(docs));
            assertEquals(docs, stage.get("$documents"), "in " + agg.getClass().getSimpleName());
        }
    }

    @Test
    public void densifyBuildsStageWithTranslatedField() {
        for (Aggregator<StageEntity, Map> agg : bothImplementations()) {
            Map<String, Object> stage = firstStage(agg.densify("itemCount", 2, List.of(0, 10)));
            @SuppressWarnings("unchecked")
            Map<String, Object> spec = (Map<String, Object>) stage.get("$densify");
            assertNotNull(spec, "in " + agg.getClass().getSimpleName());
            assertEquals("item_count", spec.get("field"));
            @SuppressWarnings("unchecked")
            Map<String, Object> range = (Map<String, Object>) spec.get("range");
            assertEquals(2, range.get("step"));
            assertEquals(List.of(0, 10), range.get("bounds"));
        }
    }

    @Test
    public void densifyDefaultsToFullBounds() {
        for (Aggregator<StageEntity, Map> agg : bothImplementations()) {
            Map<String, Object> stage = firstStage(agg.densify("itemCount", 1));
            @SuppressWarnings("unchecked")
            Map<String, Object> spec = (Map<String, Object>) stage.get("$densify");
            @SuppressWarnings("unchecked")
            Map<String, Object> range = (Map<String, Object>) spec.get("range");
            assertEquals("full", range.get("bounds"), "in " + agg.getClass().getSimpleName());
        }
    }

    @Test
    public void densifyWithUnitAndPartitions() {
        for (Aggregator<StageEntity, Map> agg : bothImplementations()) {
            Map<String, Object> stage = firstStage(
                agg.densify("itemCount", 1, "partition", "hour", List.of("groupName")));
            @SuppressWarnings("unchecked")
            Map<String, Object> spec = (Map<String, Object>) stage.get("$densify");
            @SuppressWarnings("unchecked")
            Map<String, Object> range = (Map<String, Object>) spec.get("range");
            assertEquals("hour", range.get("unit"), "in " + agg.getClass().getSimpleName());
            assertEquals(List.of("group_name"), spec.get("partitionByFields"),
                "partition fields must be translated");
        }
    }

    @Test
    public void fillBuildsStageWithTranslatedFields() {
        for (Aggregator<StageEntity, Map> agg : bothImplementations()) {
            Map<String, Object> stage = firstStage(agg.fill(
                    Doc.of("itemCount", 1),
                    Doc.of("groupName", Doc.of("method", "locf"))));
            @SuppressWarnings("unchecked")
            Map<String, Object> spec = (Map<String, Object>) stage.get("$fill");
            assertNotNull(spec, "in " + agg.getClass().getSimpleName());
            assertEquals(Doc.of("item_count", 1), spec.get("sortBy"));
            assertEquals(Doc.of("group_name", Doc.of("method", "locf")), spec.get("output"));
        }
    }

    @Test
    public void setWindowFieldsBuildsStage() {
        // $-refs are only translated with the (opt-in) aggregation field name translation,
        // exactly like project() - keys are always translated
        for (Aggregator<StageEntity, Map> agg : bothImplementations()) {
            agg.setTranslateAggregationFieldNames(true);
            Map<String, Object> stage = firstStage(agg.setWindowFields(
                    "$groupName",
                    Doc.of("itemCount", 1),
                    Doc.of("total", Doc.of("$sum", "$itemCount"))));
            @SuppressWarnings("unchecked")
            Map<String, Object> spec = (Map<String, Object>) stage.get("$setWindowFields");
            assertNotNull(spec, "in " + agg.getClass().getSimpleName());
            assertEquals("$group_name", spec.get("partitionBy"), "partitionBy field ref must be translated");
            assertEquals(Doc.of("item_count", 1), spec.get("sortBy"));
            @SuppressWarnings("unchecked")
            Map<String, Object> output = (Map<String, Object>) spec.get("output");
            assertEquals(Doc.of("$sum", "$item_count"), output.get("total"),
                "field refs inside output expressions must be translated");
        }
    }

    @Test
    public void typedStagesExecuteOnInMem() throws Exception {
        morphium.store(new StageEntity("a", 1));
        morphium.store(new StageEntity("a", 4));
        TestUtilsShim.waitForCount(morphium, StageEntity.class, 2);

        // $densify via typed method: fill the gap 2..3
        List<Map> densified = morphium.createAggregator(StageEntity.class, Map.class)
            .densify("itemCount", 1, List.of(1, 5))
            .aggregate();
        assertEquals(4, densified.size(), "densify must generate the missing values 2 and 3");

        // $setWindowFields via typed method: document-wide $sum per partition; ref translation
        // is opt-in, so enable it to use Java property names in the window expressions
        List<Map> windowed = morphium.createAggregator(StageEntity.class, Map.class)
            .setTranslateAggregationFieldNames(true)
            .setWindowFields("$groupName", Doc.of("itemCount", 1), Doc.of("total", Doc.of("$sum", "$itemCount")))
            .aggregate();
        assertEquals(2, windowed.size());
        assertEquals(5, ((Number) windowed.get(0).get("total")).intValue(),
            "window output field must hold the partition-wide sum");

        // $documents via typed method
        List<Map> docs = morphium.createAggregator(StageEntity.class, Map.class)
            .documents(List.of(Doc.of("x", 42)))
            .aggregate();
        assertEquals(1, docs.size());
        assertEquals(42, ((Number) docs.get(0).get("x")).intValue());
    }

    /** small local helper to avoid a fixed sleep after store() */
    private static class TestUtilsShim {
        static void waitForCount(de.caluga.morphium.Morphium m, Class<?> cls, long expected) throws Exception {
            de.caluga.test.mongo.suite.base.TestUtils.waitForConditionToBecomeTrue(5000,
                "expected " + expected + " docs", () -> m.createQueryFor(cls).countAll() == expected);
        }
    }
}
