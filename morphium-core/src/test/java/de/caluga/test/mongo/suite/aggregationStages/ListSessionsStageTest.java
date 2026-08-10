package de.caluga.test.mongo.suite.aggregationStages;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * #254: the $listSessions pipeline stage. The in-memory driver has no server-side session
 * tracking, so an empty result set is the honest answer - the pipeline must succeed, not throw.
 */
@Tag("inmemory")
public class ListSessionsStageTest extends MultiDriverTestBase {

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Aggregator<Object, Map> agg(Morphium m, Map<String, Object> spec) {
        InMemoryDriver drv = (InMemoryDriver) m.getDriver();
        Aggregator<Object, Map> agg = (Aggregator<Object, Map>) drv.createAggregator(m, Object.class, Map.class);
        agg.setCollectionName("system.sessions");
        agg.addOperator(UtilsMap.of("$listSessions", spec));
        return agg;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void emptySpec_returnsEmptyResult(Morphium morphium) throws Exception {
        try (morphium) {
            List<Map<String, Object>> res = agg(morphium, new java.util.HashMap<>()).aggregateMap();
            assertNotNull(res);
            assertTrue(res.isEmpty(), "no session tracking in the in-memory driver -> empty result");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void allUsersAndUserFilter_returnEmptyResult(Morphium morphium) throws Exception {
        try (morphium) {
            assertTrue(agg(morphium, Doc.of("allUsers", true)).aggregateMap().isEmpty());
            assertTrue(agg(morphium, Doc.of("users", List.of(Doc.of("user", "test", "db", "test")))).aggregateMap().isEmpty());
        }
    }
}
