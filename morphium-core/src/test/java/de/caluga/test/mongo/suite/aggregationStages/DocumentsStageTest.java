package de.caluga.test.mongo.suite.aggregationStages;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * #254: the $documents stage - a literal document source. The stage replaces whatever came from
 * the source collection, so pipelines can run without any backing collection at all.
 */
@Tag("inmemory")
public class DocumentsStageTest extends MultiDriverTestBase {

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Aggregator<Object, Map> agg(Morphium morphium) {
        InMemoryDriver drv = (InMemoryDriver) morphium.getDriver();
        Aggregator<Object, Map> agg = (Aggregator<Object, Map>) drv.createAggregator(morphium, Object.class, Map.class);
        // $documents ignores the collection input entirely - the name only anchors the pipeline
        agg.setCollectionName("documents_stage_source");
        return agg;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void documentsActsAsLiteralSource(Morphium morphium) throws Exception {
        try (morphium) {
            Aggregator<Object, Map> agg = agg(morphium);
            agg.addOperator(UtilsMap.of("$documents", List.of(
                Doc.of("a", 1, "tag", "x"),
                Doc.of("a", 2, "tag", "y"),
                Doc.of("a", 3, "tag", "x")
            )));

            List<Map<String, Object>> res = agg.aggregateMap();
            assertEquals(3, res.size());
            assertEquals(1, res.get(0).get("a"));
            assertEquals(3, res.get(2).get("a"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void documentsFeedsFollowingStages(Morphium morphium) throws Exception {
        try (morphium) {
            Aggregator<Object, Map> agg = agg(morphium);
            agg.addOperator(UtilsMap.of("$documents", List.of(
                Doc.of("a", 1), Doc.of("a", 2), Doc.of("a", 3)
            )));
            agg.addOperator(UtilsMap.of("$match", Doc.of("a", Doc.of("$gte", 2))));

            List<Map<String, Object>> res = agg.aggregateMap();
            assertEquals(2, res.size());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void documentsIgnoresCollectionContent(Morphium morphium) throws Exception {
        try (morphium) {
            InMemoryDriver drv = (InMemoryDriver) morphium.getDriver();
            String db = morphium.getConfig().connectionSettings().getDatabase();
            drv.store(db, "documents_stage_source", new java.util.ArrayList<>(List.of(
                Doc.of("_id", new de.caluga.morphium.driver.MorphiumId(), "fromColl", true))), null);

            Aggregator<Object, Map> agg = agg(morphium);
            agg.addOperator(UtilsMap.of("$documents", List.of(Doc.of("a", 42))));

            List<Map<String, Object>> res = agg.aggregateMap();
            assertEquals(1, res.size());
            assertEquals(42, res.get(0).get("a"));
            assertNull(res.get(0).get("fromColl"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void documentsNonArrayThrows(Morphium morphium) throws Exception {
        try (morphium) {
            Aggregator<Object, Map> agg = agg(morphium);
            agg.addOperator(UtilsMap.of("$documents", "not an array"));

            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5858203, ex.getMongoCode());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void documentsNonDocumentElementThrows(Morphium morphium) throws Exception {
        try (morphium) {
            Aggregator<Object, Map> agg = agg(morphium);
            agg.addOperator(UtilsMap.of("$documents", List.of(Doc.of("a", 1), "junk")));

            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, agg::aggregateMap);
            assertEquals(5858203, ex.getMongoCode());
        }
    }
}
