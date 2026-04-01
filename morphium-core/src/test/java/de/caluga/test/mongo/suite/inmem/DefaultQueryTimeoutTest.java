package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.aggregation.AggregatorImpl;
import de.caluga.morphium.driver.commands.AggregateMongoCommand;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for {@code ConnectionSettings.defaultQueryTimeoutMS} — verifies that the
 * new setting is used as fallback for {@code Query.getMaxTimeMS()} and applied to
 * aggregation commands, while per-query overrides still take precedence.
 */
@Tag("inmemory")
public class DefaultQueryTimeoutTest extends MorphiumInMemTestBase {

    @Test
    public void queryFallsBackToDefaultQueryTimeoutMS() {
        // Default is 0 (no limit)
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        assertEquals(0, q.getMaxTimeMS(), "Default should be 0 (no limit)");
    }

    @Test
    public void queryUsesConfiguredDefaultQueryTimeoutMS() {
        morphium.getConfig().connectionSettings().setDefaultQueryTimeoutMS(45000);
        try {
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            assertEquals(45000, q.getMaxTimeMS(),
                    "Query should fall back to defaultQueryTimeoutMS from config");
        } finally {
            morphium.getConfig().connectionSettings().setDefaultQueryTimeoutMS(0);
        }
    }

    @Test
    public void perQueryMaxTimeMSOverridesDefault() {
        morphium.getConfig().connectionSettings().setDefaultQueryTimeoutMS(45000);
        try {
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q.setMaxTimeMS(5000);
            assertEquals(5000, q.getMaxTimeMS(),
                    "Per-query setMaxTimeMS should override the config default");
        } finally {
            morphium.getConfig().connectionSettings().setDefaultQueryTimeoutMS(0);
        }
    }

    @Test
    public void queryDoesNotUseMaxWaitTimeAsFallback() {
        // maxWaitTime is 1550 (set in MorphiumInMemTestBase.setup)
        // With the fix, Query should NOT fall back to maxWaitTime
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        assertEquals(0, q.getMaxTimeMS(),
                "Query should NOT fall back to maxWaitTime (1550) — should use defaultQueryTimeoutMS (0)");
    }

    @Test
    public void connectionSettingsDefaultIsZero() {
        // Verify that a fresh ConnectionSettings has defaultQueryTimeoutMS = 0
        MorphiumConfig cfg = new MorphiumConfig();
        assertEquals(0, cfg.connectionSettings().getDefaultQueryTimeoutMS(),
                "Default value of defaultQueryTimeoutMS should be 0");
    }

    @Test
    public void connectionSettingsSetterAndGetter() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings().setDefaultQueryTimeoutMS(60000);
        assertEquals(60000, cfg.connectionSettings().getDefaultQueryTimeoutMS());
        cfg.connectionSettings().setDefaultQueryTimeoutMS(0);
        assertEquals(0, cfg.connectionSettings().getDefaultQueryTimeoutMS());
    }

    @Test
    public void aggregatorImplAppliesDefaultQueryTimeoutMS() {
        morphium.getConfig().connectionSettings().setDefaultQueryTimeoutMS(30000);
        try {
            createUncachedObjects(5);
            // Use AggregatorImpl directly (not InMemAggregator) to test the timeout logic.
            // InMemAggregator overrides getAggregateCmd() and does not apply timeouts
            // (which is correct — InMem has no server-side maxTimeMS).
            AggregatorImpl<UncachedObject, UncachedObject> agg = new AggregatorImpl<>(morphium, UncachedObject.class, UncachedObject.class);
            agg.match(morphium.createQueryFor(UncachedObject.class));
            AggregateMongoCommand cmd = agg.getAggregateCmd();
            try {
                assertEquals(Integer.valueOf(30000), cmd.getMaxWaitTime(),
                        "Aggregation command should have defaultQueryTimeoutMS applied");
            } finally {
                cmd.releaseConnection();
            }
        } finally {
            morphium.getConfig().connectionSettings().setDefaultQueryTimeoutMS(0);
        }
    }

    @Test
    public void aggregatorImplOmitsTimeoutWhenDefaultIsZero() {
        morphium.getConfig().connectionSettings().setDefaultQueryTimeoutMS(0);
        createUncachedObjects(5);
        AggregatorImpl<UncachedObject, UncachedObject> agg = new AggregatorImpl<>(morphium, UncachedObject.class, UncachedObject.class);
        agg.match(morphium.createQueryFor(UncachedObject.class));
        AggregateMongoCommand cmd = agg.getAggregateCmd();
        try {
            assertNull(cmd.getMaxWaitTime(),
                    "Aggregation command should NOT set maxWaitTime when defaultQueryTimeoutMS is 0");
        } finally {
            cmd.releaseConnection();
        }
    }
}
