package de.caluga.test.mongo.suite.inmem;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.AggregatorImpl;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.MorphiumId;
import org.slf4j.LoggerFactory;
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

        public enum Fields { firstName, lastName, itemCount }
    }

    @NoCache
    @Entity(translateCamelCase = true)
    public static class LookupEntity {
        @Id
        private MorphiumId id;
        private String ownerName;

        public LookupEntity() {}

        public enum Fields { ownerName }
    }

    private Map<String, Object> firstStage(Aggregator<AggEntity, Map> agg) {
        List<Map<String, Object>> pipeline = agg.getPipeline();
        assertFalse(pipeline.isEmpty(), "pipeline should have at least one stage");
        return pipeline.get(0);
    }

    /** the InMem driver returns an InMemAggregator - AggregatorImpl has to be tested explicitly */
    private List<Aggregator<AggEntity, Map>> bothImplementations() {
        return List.of(
                   morphium.createAggregator(AggEntity.class, Map.class),
                   new AggregatorImpl<>(morphium, AggEntity.class, Map.class));
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

    @Test
    public void unsetEnumTranslatesCamelCaseFieldNames() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.unset(AggEntity.Fields.firstName, AggEntity.Fields.lastName);

            Map<String, Object> stage = firstStage(agg);
            assertTrue(stage.containsKey("$unset"), impl + ": stage should be $unset");
            @SuppressWarnings("unchecked")
            List<String> fields = (List<String>) stage.get("$unset");

            assertTrue(fields.contains("first_name"), impl + ": firstName should be translated to first_name in pipeline");
            assertTrue(fields.contains("last_name"), impl + ": lastName should be translated to last_name in pipeline");
            assertFalse(fields.contains("firstName"), impl + ": untranslated firstName should not appear in pipeline");
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> groupParams(Aggregator<AggEntity, Map> agg) {
        Map<String, Object> stage = firstStage(agg);
        assertTrue(stage.containsKey("$group"), "stage should be $group");
        return (Map<String, Object>) stage.get("$group");
    }

    @SuppressWarnings("unchecked")
    private Object opRef(Map<String, Object> group, String name, String op) {
        assertTrue(group.containsKey(name), "group should contain output field " + name);
        Map<String, Object> opMap = (Map<String, Object>) group.get(name);
        assertTrue(opMap.containsKey(op), name + " should use operator " + op);
        return opMap.get(op);
    }

    @Test
    public void stdDevSampStringOverloadUsesDollarOperator() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.group("$lastName").stdDevSamp("s1", "$item_count").end();

            Map<String, Object> group = groupParams(agg);
            assertEquals("$item_count", opRef(group, "s1", "$stdDevSamp"),
                         impl + ": stdDevSamp must emit the $stdDevSamp operator");
        }
    }

    private List<ILoggingEvent> runAndCaptureWarns(Aggregator<AggEntity, Map> agg, Runnable pipelineBuilder) {
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(agg.getClass());
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            pipelineBuilder.run();
            return appender.list.stream()
                   .filter(ev -> ev.getLevel() == Level.WARN)
                   .collect(java.util.stream.Collectors.toList());
        } finally {
            logger.detachAppender(appender);
        }
    }

    private ILoggingEvent runAndCaptureWarn(Aggregator<AggEntity, Map> agg, Runnable pipelineBuilder) {
        List<ILoggingEvent> warns = runAndCaptureWarns(agg, pipelineBuilder);
        return warns.isEmpty() ? null : warns.get(0);
    }

    @Test
    public void warnsWhenTranslatedProjectKeyIsReferencedUntranslated() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            ILoggingEvent warn = runAndCaptureWarn(agg, () -> {
                agg.project(Map.of("itemCount", "$item_count"));
                agg.group("$lastName").sum("total", "$itemCount").end();
            });
            assertNotNull(warn, impl + ": referencing $itemCount after project translated the key to item_count should WARN");
            assertTrue(warn.getFormattedMessage().contains("itemCount"),
                       impl + ": warning should name the reference the user wrote");
            assertTrue(warn.getFormattedMessage().contains("item_count"),
                       impl + ": warning should name the translated key");
        }
    }

    @Test
    public void noWarnWhenTranslatedKeyIsReferencedCorrectly() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            ILoggingEvent warn = runAndCaptureWarn(agg, () -> {
                agg.project(Map.of("itemCount", "$item_count"));
                agg.group("$lastName").sum("total", "$item_count").end();
            });
            assertNull(warn, impl + ": referencing the translated name must not WARN");
        }
    }

    @Test
    public void warnsOutsideGroupStagesToo() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            ILoggingEvent warn = runAndCaptureWarn(agg, () -> {
                agg.project(Map.of("itemCount", "$item_count"));
                agg.addFields(Map.of("doubled", "$itemCount"));
            });
            assertNotNull(warn, impl + ": a non-group stage referencing the renamed key must WARN too");
        }
    }

    @Test
    public void warnIsEmittedOnlyOncePerReference() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            List<ILoggingEvent> warns = runAndCaptureWarns(agg, () -> {
                agg.project(Map.of("itemCount", "$item_count"));
                agg.group("$lastName").sum("a", "$itemCount").max("b", "$itemCount").end();
            });
            assertEquals(1, warns.size(), impl + ": the same untranslated reference must only be warned about once");
        }
    }

    @Test
    public void noWarnForLiteralContent() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            ILoggingEvent warn = runAndCaptureWarn(agg, () -> {
                agg.project(Map.of("itemCount", "$item_count"));
                agg.addFields(Map.of("label", Map.of("$literal", "$itemCount")));
            });
            assertNull(warn, impl + ": $literal content is data, not a reference - no WARN");
        }
    }

    @Test
    public void lookupEnumTranslatesLocalAndForeignFieldNames() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.lookup(LookupEntity.class, AggEntity.Fields.firstName, LookupEntity.Fields.ownerName, "joined", null, null);

            Map<String, Object> stage = firstStage(agg);
            assertTrue(stage.containsKey("$lookup"), impl + ": stage should be $lookup");
            @SuppressWarnings("unchecked")
            Map<String, Object> lookup = (Map<String, Object>) stage.get("$lookup");

            assertEquals("first_name", lookup.get("localField"), impl + ": localField should be translated using the search type");
            assertEquals("owner_name", lookup.get("foreignField"), impl + ": foreignField should be translated using the lookup type");
        }
    }
}