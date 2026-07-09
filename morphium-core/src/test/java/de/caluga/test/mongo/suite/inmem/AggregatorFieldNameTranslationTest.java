package de.caluga.test.mongo.suite.inmem;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.AggregatorImpl;
import de.caluga.morphium.aggregation.Expr;
import org.slf4j.LoggerFactory;
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
    public void refAndNameTranslateEnumFieldReferences() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            assertEquals("item_count", agg.name(AggEntity.Fields.itemCount),
                         impl + ": name() should translate to the Mongo field name");
            assertEquals("$item_count", agg.ref(AggEntity.Fields.itemCount),
                         impl + ": ref() should translate and $-prefix");

            agg.group("$lastName")
               .sum(agg.name(AggEntity.Fields.itemCount), agg.ref(AggEntity.Fields.itemCount))
               .push("raw", "$some_raw_ref")
               .end();

            Map<String, Object> group = groupParams(agg);
            assertEquals("$item_count", opRef(group, "item_count", "$sum"),
                         impl + ": name()/ref() should produce translated output name and reference");
            assertFalse(group.containsKey("itemCount"), impl + ": untranslated itemCount should not appear");
            assertEquals("$some_raw_ref", opRef(group, "raw", "$push"),
                         impl + ": plain string values stay verbatim");
        }
    }

    /** regression for the overload trap: an enum passed as VALUE must stay a value —
     * there are deliberately no (String, Enum) overloads on Group */
    @Test
    public void enumValuesAreNotCapturedAsFieldReferences() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.group("$lastName").push("status", AggEntity.Fields.firstName).end();
            assertEquals(AggEntity.Fields.firstName, opRef(groupParams(agg), "status", "$push"),
                         impl + ": an enum value must be passed through, not turned into a $-ref");
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
    public void literalValuesStayUntouchedWithFlagOn() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.setTranslateAggregationFieldNames(true);
            agg.addFields(Map.of("x", Map.of("$literal", "$firstName")));

            Map<String, Object> af = stagePart(agg, "$addFields");
            @SuppressWarnings("unchecked")
            Map<String, Object> literal = (Map<String, Object>) unwrapExpr(af.get("x"));
            assertEquals("$firstName", literal.get("$literal"),
                         impl + ": $literal content is data, not a field reference - must never be translated");
        }
    }

    @Test
    public void setMapValueRefsTranslateWithFlagOn() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.setTranslateAggregationFieldNames(true);
            agg.set(Map.of("copy", Expr.field("firstName")));

            Map<String, Object> set = stagePart(agg, "$set");
            assertEquals("$first_name", unwrapExpr(set.get("copy")),
                         impl + ": flag on must translate refs in set(Map) values");
        }
    }

    @Test
    public void graphLookupStartWithTranslatesWithFlagOn() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.setTranslateAggregationFieldNames(true);
            agg.graphLookup(LookupEntity.class, Expr.field("itemCount"),
                            "owner_name", "owner_name", "joined", null, null, null);

            Map<String, Object> gl = stagePart(agg, "$graphLookup");
            assertEquals("$item_count", unwrapExpr(gl.get("startWith")),
                         impl + ": startWith is evaluated on input docs and must translate against the search type");
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
    public void noWarnWhenTranslateFlagResolvesTheReference() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.setTranslateAggregationFieldNames(true);
            ILoggingEvent warn = runAndCaptureWarn(agg, () -> {
                agg.project(Map.of("itemCount", "$item_count"));
                agg.group("$lastName").sum("total", "$itemCount").end();
            });
            assertNull(warn, impl + ": with the flag on the reference is translated and consistent — no WARN");
        }
    }

    @Test
    public void dotPathRefsTranslateFirstSegmentWithFlagOn() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.setTranslateAggregationFieldNames(true);
            agg.group("$lastName").push("all", "$itemCount.sub").end();
            assertEquals("$item_count.sub", opRef(groupParams(agg), "all", "$push"),
                         impl + ": dot-path refs must translate their first segment");
        }
    }

    @Test
    public void dollarDollarVariablesStayUntouchedWithFlagOn() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.setTranslateAggregationFieldNames(true);
            agg.group("$lastName").push("all", "$$ROOT").end();
            assertEquals("$$ROOT", opRef(groupParams(agg), "all", "$push"),
                         impl + ": $$-variables must never be translated");
        }
    }

    @Test
    public void addFieldsExprValueTranslatesWithFlagOn() {
        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.setTranslateAggregationFieldNames(true);
            agg.addFields(Map.of("computed", Expr.field("firstName")));

            Map<String, Object> af = stagePart(agg, "$addFields");
            assertEquals("$first_name", unwrapExpr(af.get("computed")),
                         impl + ": flag on must translate refs inside Expr values too");
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

        for (Aggregator<AggEntity, Map> agg : bothImplementations()) {
            String impl = agg.getClass().getSimpleName();
            agg.setTranslateAggregationFieldNames(true);
            agg.project(Map.of("lastName", "$last_name", "itemCount", "$item_count"));
            agg.group("$lastName").sum("total", "$itemCount").end();

            List<Map<String, Object>> result = agg.aggregateMap();
            assertEquals(1, result.size(), impl + ": one group expected");
            assertEquals(6, ((Number) result.get(0).get("total")).intValue(),
                         impl + ": sum must be 3+2+1=6, not the silent 0 from issue #208");
        }
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