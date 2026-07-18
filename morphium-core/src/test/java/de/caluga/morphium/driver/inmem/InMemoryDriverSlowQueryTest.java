package de.caluga.morphium.driver.inmem;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.AggregateMongoCommand;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Slow-query WARN + counters (Phase C, Task 6, "Slow-Query-Metriken"): a {@code find}/{@code
 * count} that exceeds {@link InMemoryDriver#getSlowQueryThresholdMillis()} must log a single WARN
 * carrying the namespace, the query's sanitized SHAPE (field names/operators only - never the
 * literal values), the winning plan's stage, and docsExamined, plus bump {@link InMemoryDriver}'s
 * {@code slowQueries} (+ the matching per-stage) counter, both surfaced via {@link
 * InMemoryDriver#getStats()}.
 *
 * <p>Uses {@link InMemoryDriver#setSlowQueryThresholdMillis} to make every query "slow" (0ms
 * threshold) rather than trying to make an in-memory operation genuinely take &gt;100ms - same
 * determinism goal as the WARN-capture pattern used elsewhere (see
 * {@code AggregatorFieldNameTranslationTest.runAndCaptureWarns} in this module).
 */
@Tag("inmemory")
public class InMemoryDriverSlowQueryTest {
    private final String db = "slowquerydb";
    private final String coll = "slowquerycoll";

    private InMemoryDriver freshDriverWithIndexedCollection(int docCount) throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of("counter", 1)))
                .execute();

        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            docs.add(Doc.of("counter", i, "secretField", "topSecretValue-" + i));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(docs).execute();
        return drv;
    }

    private List<ILoggingEvent> runAndCaptureWarns(Runnable body) {
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(InMemoryDriver.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            body.run();
            // Scope to slow-query WARNs specifically - InMemoryDriver's logger also carries
            // unrelated WARNs (e.g. a compression-not-supported notice emitted while setting up
            // the aggregation pipeline's own Morphium instance) that would otherwise contaminate
            // the count.
            return appender.list.stream()
                    .filter(ev -> ev.getLevel() == Level.WARN)
                    .filter(ev -> ev.getFormattedMessage().startsWith("Slow query on"))
                    .collect(Collectors.toList());
        } finally {
            logger.detachAppender(appender);
        }
    }

    @Test
    void slowUnindexedFindLogsSanitizedWarnAndIncrementsCounters() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(20);
        drv.setSlowQueryThresholdMillis(0); // every find qualifies as "slow"

        long slowBefore = drv.slowQueries;
        long collScanBefore = drv.slowQueriesCollScan;

        List<ILoggingEvent> warns = runAndCaptureWarns(() -> {
            try {
                drv.find(db, coll, Doc.of("secretField", "topSecretValue-7"), null, null, 0, 0);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(1, warns.size(), "exactly one slow-query WARN expected");
        String message = warns.get(0).getFormattedMessage();

        // Structure (namespace, key/operator shape, stage, docsExamined) must be present...
        assertTrue(message.contains(db + "." + coll), "WARN must carry the namespace");
        assertTrue(message.contains("secretField"), "WARN must carry the filter's field name");
        assertTrue(message.contains("COLLSCAN"), "unindexed field must be reported as COLLSCAN");
        assertTrue(message.contains("docsExamined"), "WARN must carry docsExamined");

        // ...but never the literal value.
        assertFalse(message.contains("topSecretValue-7"), "WARN must NOT leak the filter's value");

        assertEquals(slowBefore + 1, drv.slowQueries);
        assertEquals(collScanBefore + 1, drv.slowQueriesCollScan);

        Map<String, Object> stats = drv.getStats();
        assertEquals(drv.slowQueries, ((Number) stats.get("slowQueries")).longValue());
        assertEquals(drv.slowQueriesCollScan, ((Number) stats.get("slowQueriesCollScan")).longValue());
    }

    @Test
    void slowIndexedFindReportsIxscanStage() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(20);
        drv.setSlowQueryThresholdMillis(0);

        long slowBefore = drv.slowQueries;
        long ixscanBefore = drv.slowQueriesIxscan;

        List<ILoggingEvent> warns = runAndCaptureWarns(() -> {
            try {
                drv.find(db, coll, Doc.of("counter", 5), null, null, 0, 0);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(1, warns.size());
        assertTrue(warns.get(0).getFormattedMessage().contains("IXSCAN"));
        assertEquals(slowBefore + 1, drv.slowQueries);
        assertEquals(ixscanBefore + 1, drv.slowQueriesIxscan);
    }

    @Test
    void fastQueryBelowThresholdDoesNotLogOrIncrementCounters() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(20);
        drv.setSlowQueryThresholdMillis(60_000); // effectively unreachable in a unit test

        long slowBefore = drv.slowQueries;

        List<ILoggingEvent> warns = runAndCaptureWarns(() -> {
            try {
                drv.find(db, coll, Doc.of("counter", 5), null, null, 0, 0);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(0, warns.size());
        assertEquals(slowBefore, drv.slowQueries);
    }

    @Test
    void slowCountAlsoIncrementsCounters() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(20);
        drv.setSlowQueryThresholdMillis(0);

        long slowBefore = drv.slowQueries;

        List<ILoggingEvent> warns = runAndCaptureWarns(() -> {
            try {
                drv.count(db, coll, Doc.of("counter", 5), null, null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(1, warns.size());
        assertEquals(slowBefore + 1, drv.slowQueries);
    }

    @Test
    void slowAggregateWithLeadingMatchAlsoLogsAndIncrementsCounters() throws Exception {
        InMemoryDriver drv = freshDriverWithIndexedCollection(20);
        drv.setSlowQueryThresholdMillis(0);

        long slowBefore = drv.slowQueries;

        List<ILoggingEvent> warns = runAndCaptureWarns(() -> {
            try {
                new AggregateMongoCommand(drv).setDb(db).setColl(coll)
                        .setPipeline(List.of(Doc.of("$match", Doc.of("counter", 5))))
                        .execute();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // InMemAggregator may itself issue an internal find() to materialize the collection before
        // running the pipeline - that legitimately logs its own (COLLSCAN) slow-query WARN in
        // addition to this method's own aggregate-level one, so assert "at least one, and one of
        // them is the IXSCAN for the $match stage" rather than an exact count.
        assertFalse(warns.isEmpty(), "an aggregate exceeding the threshold must also log a WARN");
        assertTrue(warns.stream().anyMatch(w -> w.getFormattedMessage().contains("IXSCAN")),
                "a leading $match on an indexed field must be reported as IXSCAN");
        assertEquals(slowBefore + warns.size(), drv.slowQueries);
    }

    @Test
    void defaultThresholdIs100Millis() {
        InMemoryDriver drv = new InMemoryDriver();
        assertEquals(100L, drv.getSlowQueryThresholdMillis(),
                "default slow-query threshold must be 100ms unless overridden via the system property");
    }
}
