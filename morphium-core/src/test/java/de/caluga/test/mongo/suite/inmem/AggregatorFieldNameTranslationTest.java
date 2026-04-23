package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.MorphiumId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that AggregatorImpl translates Java camelCase field names to their
 * MongoDB equivalents when building the pipeline
 */
@SuppressWarnings("rawtypes")
@Tag("inmemory")
public class AggregatorFieldNameTranslationTest extends MorphiumInMemTestBase {

    @NoCache
    @Entity(translateCamelCase = true)
    public static class AggEntity {
        @Id
        private MorphiumId id;
        private String firstName;
        private String lastName;
        private int itemCount;

        public AggEntity() {}
    }

    private Map<String, Object> firstStage(Aggregator<AggEntity, Map> agg) {
        List<Map<String, Object>> pipeline = agg.getPipeline();
        assertFalse(pipeline.isEmpty(), "pipeline should have at least one stage");
        return pipeline.get(0);
    }

    @Test
    public void projectTranslatesCamelCaseFieldNames() {
        Aggregator<AggEntity, Map> agg = morphium.createAggregator(AggEntity.class, Map.class);
        agg.project("firstName", "lastName");

        Map<String, Object> stage = firstStage(agg);
        assertTrue(stage.containsKey("$project"), "stage should be $project");
        @SuppressWarnings("unchecked")
        Map<String, Object> projection = (Map<String, Object>) stage.get("$project");

        assertTrue(projection.containsKey("first_name"), "firstName should be translated to first_name in pipeline");
        assertTrue(projection.containsKey("last_name"), "lastName should be translated to last_name in pipeline");
        assertFalse(projection.containsKey("firstName"), "untranslated firstName should not appear in pipeline");
    }

    @Test
    public void projectWithExprTranslatesCamelCaseFieldName() {
        Aggregator<AggEntity, Map> agg = morphium.createAggregator(AggEntity.class, Map.class);
        agg.project("itemCount", Expr.field("item_count"));

        Map<String, Object> stage = firstStage(agg);
        @SuppressWarnings("unchecked")
        Map<String, Object> projection = (Map<String, Object>) stage.get("$project");

        assertTrue(projection.containsKey("item_count"), "itemCount should be translated to item_count in pipeline");
        assertFalse(projection.containsKey("itemCount"), "untranslated itemCount should not appear in pipeline");
    }

    @Test
    public void unwindTranslatesCamelCaseFieldName() {
        Aggregator<AggEntity, Map> agg = morphium.createAggregator(AggEntity.class, Map.class);
        agg.unwind("firstName");

        Map<String, Object> stage = firstStage(agg);
        assertTrue(stage.containsKey("$unwind"), "stage should be $unwind");
        assertEquals("first_name", stage.get("$unwind"), "firstName should be translated to first_name in pipeline");
    }

    @Test
    public void unsetTranslatesCamelCaseFieldNames() {
        Aggregator<AggEntity, Map> agg = morphium.createAggregator(AggEntity.class, Map.class);
        agg.unset("firstName", "lastName");

        Map<String, Object> stage = firstStage(agg);
        assertTrue(stage.containsKey("$unset"), "stage should be $unset");
        @SuppressWarnings("unchecked")
        List<String> fields = (List<String>) stage.get("$unset");

        assertTrue(fields.contains("first_name"), "firstName should be translated to first_name in pipeline");
        assertTrue(fields.contains("last_name"), "lastName should be translated to last_name in pipeline");
        assertFalse(fields.contains("firstName"), "untranslated firstName should not appear in pipeline");
    }
}