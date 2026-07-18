package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.CountMongoCommand;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.commands.ExplainCommand.ExplainVerbosity;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Driver-level integration test for {@code explain} (Phase C, Task 6): built directly on B1's
 * {@link IndexPlanner} plan objects, {@code explain} must report {@code COLLSCAN} for an
 * unindexed query and {@code IXSCAN} (with the correct {@code indexName}/{@code keyPattern}) for
 * an indexed one, and {@code executionStats} (when requested) must be plausible - fed by actually
 * running the wrapped query, not guessed. Lives in the driver's own package, alongside
 * {@link InMemoryDriverIndexPlanningTest}, to follow the same direct-driver-command setup.
 */
@Tag("inmemory")
public class ExplainCommandTest {
    private final String db = "explaindb";
    private final String coll = "explaincoll";

    private InMemoryDriver freshDriverWithIndexedCollection(int docCount) throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of("counter", 1)))
                .execute();

        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            docs.add(Doc.of("counter", i, "other", i % 3));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();
        return drv;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> winningPlan(Map<String, Object> explain) {
        Map<String, Object> queryPlanner = (Map<String, Object>) explain.get("queryPlanner");
        assertNotNull(queryPlanner, "explain result must carry queryPlanner");
        Map<String, Object> winningPlan = (Map<String, Object>) queryPlanner.get("winningPlan");
        assertNotNull(winningPlan, "queryPlanner must carry winningPlan");
        return winningPlan;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> executionStats(Map<String, Object> explain) {
        Map<String, Object> stats = (Map<String, Object>) explain.get("executionStats");
        assertNotNull(stats, "explain result must carry executionStats when requested");
        return stats;
    }

    @Test
    void explainOnIndexedEqualityFindReturnsIxscanWithIndexName() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(500);

        Map<String, Object> explain = new FindCommand(drv).setDb(db).setColl(coll)
                .setFilter(Doc.of("counter", 42))
                .explain(ExplainVerbosity.queryPlanner);

        Map<String, Object> winningPlan = winningPlan(explain);
        assertEquals("IXSCAN", winningPlan.get("stage"));
        assertNotNull(winningPlan.get("indexName"), "an IXSCAN plan must carry the winning index's name");
        assertTrue(((Map<?, ?>) winningPlan.get("keyPattern")).containsKey("counter"),
                "keyPattern must describe the indexed field");
    }

    @Test
    void explainOnUnindexedFindReturnsCollscan() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(50);

        Map<String, Object> explain = new FindCommand(drv).setDb(db).setColl(coll)
                .setFilter(Doc.of("other", 1))
                .explain(ExplainVerbosity.queryPlanner);

        Map<String, Object> winningPlan = winningPlan(explain);
        assertEquals("COLLSCAN", winningPlan.get("stage"));
    }

    @Test
    void explainExecutionStatsForCollscanIsPlausible() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(50);

        Map<String, Object> explain = new FindCommand(drv).setDb(db).setColl(coll)
                .setFilter(Doc.of("other", 1))
                .explain(ExplainVerbosity.executionStats);

        assertEquals("COLLSCAN", winningPlan(explain).get("stage"));
        Map<String, Object> stats = executionStats(explain);
        assertEquals(50L, ((Number) stats.get("totalDocsExamined")).longValue(),
                "a COLLSCAN must examine every document in the collection");
        // one third of [0..49] % 3 == 1
        long expectedMatches = 0;
        for (int i = 0; i < 50; i++) {
            if (i % 3 == 1) expectedMatches++;
        }
        assertEquals(expectedMatches, ((Number) stats.get("nReturned")).longValue());
    }

    @Test
    void explainExecutionStatsForIxscanIsPlausible() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(500);

        Map<String, Object> explain = new FindCommand(drv).setDb(db).setColl(coll)
                .setFilter(Doc.of("counter", 42))
                .explain(ExplainVerbosity.executionStats);

        assertEquals("IXSCAN", winningPlan(explain).get("stage"));
        Map<String, Object> stats = executionStats(explain);
        assertEquals(1L, ((Number) stats.get("nReturned")).longValue());
        assertTrue(((Number) stats.get("totalDocsExamined")).longValue() <= 1,
                "an equality IXSCAN must not examine more docs than the single matching key");
    }

    @Test
    void explainOnIndexedEqualityCountReturnsIxscan() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(500);

        Map<String, Object> explain = new CountMongoCommand(drv).setDb(db).setColl(coll)
                .setQuery(Doc.of("counter", 42))
                .explain(ExplainVerbosity.executionStats);

        Map<String, Object> winningPlan = winningPlan(explain);
        assertEquals("IXSCAN", winningPlan.get("stage"));
        assertNotNull(winningPlan.get("indexName"));
        assertEquals(1L, ((Number) executionStats(explain).get("nReturned")).longValue());
    }

    @Test
    void explainOnUnindexedCountReturnsCollscan() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(50);

        Map<String, Object> explain = new CountMongoCommand(drv).setDb(db).setColl(coll)
                .setQuery(Doc.of("other", 1))
                .explain(ExplainVerbosity.queryPlanner);

        assertEquals("COLLSCAN", winningPlan(explain).get("stage"));
    }

    /**
     * InUnion decision (per the task brief's NEEDS_CONTEXT prompt): a whole-index {@code $in}
     * union is reported as a single {@code IXSCAN} on the union'd index (not a multi-plan shape) -
     * consistent with how real MongoDB explains an {@code $in} query against a single index.
     */
    @Test
    void explainOnInUnionQueryReturnsSingleIxscanOnTheUnionedIndex() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(500);

        Map<String, Object> explain = new FindCommand(drv).setDb(db).setColl(coll)
                .setFilter(Doc.of("counter", Doc.of("$in", List.of(1, 2, 3))))
                .explain(ExplainVerbosity.executionStats);

        Map<String, Object> winningPlan = winningPlan(explain);
        assertEquals("IXSCAN", winningPlan.get("stage"));
        assertNotNull(winningPlan.get("indexName"));
        assertEquals(3L, ((Number) executionStats(explain).get("nReturned")).longValue());
    }
}
