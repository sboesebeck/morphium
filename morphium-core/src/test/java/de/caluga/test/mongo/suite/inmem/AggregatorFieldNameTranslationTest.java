package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.AggregatorImpl;
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
    public void groupEnumRefTranslatesFieldReference() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.group("$lastName")
               .sum("s1", AggEntity.Fields.itemCount)
               .avg("s2", AggEntity.Fields.itemCount)
               .min("s3", AggEntity.Fields.itemCount)
               .max("s4", AggEntity.Fields.itemCount)
               .first("s5", AggEntity.Fields.itemCount)
               .last("s6", AggEntity.Fields.itemCount)
               .push("s7", AggEntity.Fields.itemCount)
               .addToSet("s8", AggEntity.Fields.itemCount)
               .stdDevPop("s9", AggEntity.Fields.itemCount)
               .stdDevSamp("s10", AggEntity.Fields.itemCount)
               .end();

            Map<String, Object> group = groupParams(agg);
            assertEquals("$item_count", opRef(group, "s1", "$sum"), impl + ": sum ref should be translated");
            assertEquals("$item_count", opRef(group, "s2", "$avg"), impl + ": avg ref should be translated");
            assertEquals("$item_count", opRef(group, "s3", "$min"), impl + ": min ref should be translated");
            assertEquals("$item_count", opRef(group, "s4", "$max"), impl + ": max ref should be translated");
            assertEquals("$item_count", opRef(group, "s5", "$first"), impl + ": first ref should be translated");
            assertEquals("$item_count", opRef(group, "s6", "$last"), impl + ": last ref should be translated");
            assertEquals("$item_count", opRef(group, "s7", "$push"), impl + ": push ref should be translated");
            assertEquals("$item_count", opRef(group, "s8", "$addToSet"), impl + ": addToSet ref should be translated");
            assertEquals("$item_count", opRef(group, "s9", "$stdDevPop"), impl + ": stdDevPop ref should be translated");
            assertEquals("$item_count", opRef(group, "s10", "$stdDevSamp"), impl + ": stdDevSamp ref should be translated");
        }
    }

    @Test
    public void groupEnumOutputNameTranslates() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.group("$lastName")
               .sum(AggEntity.Fields.itemCount, AggEntity.Fields.itemCount)
               .push(AggEntity.Fields.firstName, "$some_raw_ref")
               .end();

            Map<String, Object> group = groupParams(agg);
            assertEquals("$item_count", opRef(group, "item_count", "$sum"),
                         impl + ": enum output name and ref should both be translated");
            assertFalse(group.containsKey("itemCount"), impl + ": untranslated itemCount should not appear");
            assertEquals("$some_raw_ref", opRef(group, "first_name", "$push"),
                         impl + ": enum output name translated, Object value passed verbatim");
        }
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

    @Test
    public void groupStringRefsStayVerbatimByDefault() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.group(Map.of("ln", "$lastName")).sum("total", "$itemCount").end();

            Map<String, Object> group = groupParams(agg);
            assertEquals("$itemCount", opRef(group, "total", "$sum"),
                         impl + ": legacy default must not translate group refs");
            @SuppressWarnings("unchecked")
            Map<String, Object> id = (Map<String, Object>) group.get("_id");
            assertEquals("$lastName", id.get("ln"), impl + ": legacy default must not translate group id refs");
        }
    }

    @Test
    public void translateFlagOnTranslatesGroupStringRefs() {
        morphium.getConfig().objectMappingSettings().setTranslateAggregationFieldNames(true);
        try {
            for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
                String impl = agg.getClass().getSimpleName();
                agg.group(Map.of("ln", "$lastName")).sum("total", "$itemCount").end();

                Map<String, Object> group = groupParams(agg);
                assertEquals("$item_count", opRef(group, "total", "$sum"),
                             impl + ": flag on must translate group refs");
                @SuppressWarnings("unchecked")
                Map<String, Object> id = (Map<String, Object>) group.get("_id");
                assertEquals("$last_name", id.get("ln"), impl + ": flag on must translate group id refs");
            }
        } finally {
            morphium.getConfig().objectMappingSettings().setTranslateAggregationFieldNames(false);
        }
    }

    @Test
    public void perAggregatorOverrideBeatsConfig() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.setTranslateAggregationFieldNames(true);
            agg.group("x").sum("total", "$itemCount").end();
            assertEquals("$item_count", opRef(groupParams(agg), "total", "$sum"),
                         impl + ": aggregator override true must translate although config is off");
        }

        morphium.getConfig().objectMappingSettings().setTranslateAggregationFieldNames(true);
        try {
            for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
                String impl = agg.getClass().getSimpleName();
                agg.setTranslateAggregationFieldNames(false);
                agg.group("x").sum("total", "$itemCount").end();
                assertEquals("$itemCount", opRef(groupParams(agg), "total", "$sum"),
                             impl + ": aggregator override false must win over config on");
            }
        } finally {
            morphium.getConfig().objectMappingSettings().setTranslateAggregationFieldNames(false);
        }
    }

    private ch.qos.logback.classic.spi.ILoggingEvent runAndCaptureWarn(Aggregator<AggEntity, Map> agg, Runnable pipelineBuilder) {
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(agg.getClass());
        ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender = new ch.qos.logback.core.read.ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            pipelineBuilder.run();
            return appender.list.stream()
                   .filter(ev -> ev.getLevel() == ch.qos.logback.classic.Level.WARN)
                   .findFirst().orElse(null);
        } finally {
            logger.detachAppender(appender);
        }
    }

    @Test
    public void warnsWhenTranslatedProjectKeyIsReferencedUntranslated() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            ch.qos.logback.classic.spi.ILoggingEvent warn = runAndCaptureWarn(agg, () -> {
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
            ch.qos.logback.classic.spi.ILoggingEvent warn = runAndCaptureWarn(agg, () -> {
                agg.project(Map.of("itemCount", "$item_count"));
                agg.group("$lastName").sum("total", "$item_count").end();
            });
            assertNull(warn, impl + ": referencing the translated name must not WARN");
        }
    }

    @Test
    public void noWarnWhenTranslateFlagResolvesTheReference() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.setTranslateAggregationFieldNames(true);
            ch.qos.logback.classic.spi.ILoggingEvent warn = runAndCaptureWarn(agg, () -> {
                agg.project(Map.of("itemCount", "$item_count"));
                agg.group("$lastName").sum("total", "$itemCount").end();
            });
            assertNull(warn, impl + ": with the flag on the reference is translated and consistent — no WARN");
        }
    }

    /** end-to-end repro of issue #208: with the flag on, the project/group pipeline from
     * the issue returns real sums instead of silent zeros */
    @Test
    public void issue208PipelineReturnsCorrectSumsWithFlagOn() {
        for (int i = 1; i <= 3; i++) {
            AggEntity e = new AggEntity();
            e.lastName = "smith";
            e.itemCount = i;
            morphium.store(e);
        }

        Aggregator<AggEntity, Map> agg = morphium.createAggregator(AggEntity.class, Map.class);
        agg.setTranslateAggregationFieldNames(true);
        agg.project(Map.of("lastName", "$last_name", "itemCount", "$item_count"));
        agg.group("$lastName").sum("total", "$itemCount").end();

        List<Map<String, Object>> result = agg.aggregateMap();
        assertEquals(1, result.size(), "one group expected");
        assertEquals(6, ((Number) result.get(0).get("total")).intValue(),
                     "sum must be 3+2+1=6, not the silent 0 from issue #208");
    }

    /** InMemAggregator wraps raw addFields values in Expr — normalize for comparison */
    private Object unwrapExpr(Object value) {
        return value instanceof Expr ? ((Expr) value).toQueryObject() : value;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> stagePart(Aggregator<AggEntity, Map> agg, String stageName) {
        Map<String, Object> stage = firstStage(agg);
        assertTrue(stage.containsKey(stageName), "stage should be " + stageName);
        return (Map<String, Object>) stage.get(stageName);
    }

    @Test
    public void graphLookupEnumTranslatesConnectFields() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.graphLookup(LookupEntity.class, Expr.field("owner_name"),
                            LookupEntity.Fields.ownerName, LookupEntity.Fields.ownerName,
                            "joined", null, null, null);

            Map<String, Object> gl = stagePart(agg, "$graphLookup");
            assertEquals("owner_name", gl.get("connectFromField"),
                         impl + ": enum connectFromField should be translated using the from type");
            assertEquals("owner_name", gl.get("connectToField"),
                         impl + ": enum connectToField should be translated using the from type");
        }
    }

    @Test
    public void graphLookupClassStringConnectFieldsTranslateWithFlagOn() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.setTranslateAggregationFieldNames(true);
            agg.graphLookup(LookupEntity.class, Expr.field("owner_name"),
                            "ownerName", "ownerName", "joined", null, null, null);

            Map<String, Object> gl = stagePart(agg, "$graphLookup");
            assertEquals("owner_name", gl.get("connectFromField"),
                         impl + ": flag on must translate String connectFromField against the from type");
            assertEquals("owner_name", gl.get("connectToField"),
                         impl + ": flag on must translate String connectToField against the from type");
        }
    }

    @Test
    public void addFieldsAndSortStayVerbatimByDefault() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.addFields(Map.of("itemCount", "$firstName"));

            Map<String, Object> af = stagePart(agg, "$addFields");
            assertTrue(af.containsKey("itemCount"), impl + ": legacy default must not translate addFields keys");
            assertEquals("$firstName", unwrapExpr(af.get("itemCount")),
                         impl + ": legacy default must not translate addFields refs");
        }

        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.sort(Map.of("itemCount", 1));
            assertTrue(stagePart(agg, "$sort").containsKey("itemCount"),
                       impl + ": legacy default must not translate sort(Map) keys");
        }
    }

    @Test
    public void addFieldsKeysAndRefsTranslateWithFlagOn() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.setTranslateAggregationFieldNames(true);
            agg.addFields(Map.of("itemCount", "$firstName"));

            Map<String, Object> af = stagePart(agg, "$addFields");
            assertTrue(af.containsKey("item_count"), impl + ": flag on must translate addFields keys");
            assertFalse(af.containsKey("itemCount"), impl + ": untranslated key must not appear with flag on");
            assertEquals("$first_name", unwrapExpr(af.get("item_count")),
                         impl + ": flag on must translate $-refs in addFields values");
        }
    }

    @Test
    public void setMapKeysTranslateWithFlagOn() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.setTranslateAggregationFieldNames(true);
            agg.set(Map.of("itemCount", Expr.field("first_name")));

            Map<String, Object> set = stagePart(agg, "$set");
            assertTrue(set.containsKey("item_count"), impl + ": flag on must translate set(Map) keys");
            assertFalse(set.containsKey("itemCount"), impl + ": untranslated key must not appear with flag on");
        }
    }

    @Test
    public void sortMapKeysTranslateWithFlagOn() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.setTranslateAggregationFieldNames(true);
            agg.sort(Map.of("itemCount", 1));

            Map<String, Object> sort = stagePart(agg, "$sort");
            assertTrue(sort.containsKey("item_count"), impl + ": flag on must translate sort(Map) keys");
            assertFalse(sort.containsKey("itemCount"), impl + ": untranslated key must not appear with flag on");
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