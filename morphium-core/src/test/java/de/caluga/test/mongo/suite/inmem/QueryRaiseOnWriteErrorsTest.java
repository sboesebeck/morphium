package de.caluga.test.mongo.suite.inmem;

import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MOR-059: the per-query override of {@code WriterSettings.raiseOnWriteErrors}, resolved like {@code translateUpdateFieldNames}. */
public class QueryRaiseOnWriteErrorsTest extends MorphiumInMemTestBase {

    @Test
    public void theQueryFollowsTheConfigurationUnlessItIsOverridden() {
        var q = morphium.createQueryFor(UncachedObject.class);
        assertNull(q.getRaiseOnWriteErrors(), "no override by default");
        assertTrue(q.isRaiseOnWriteErrors(), "the configuration default is on");

        q.setRaiseOnWriteErrors(false);
        assertFalse(q.isRaiseOnWriteErrors());
        q.setRaiseOnWriteErrors(true);
        assertTrue(q.isRaiseOnWriteErrors());
        q.setRaiseOnWriteErrors(null);
        assertTrue(q.isRaiseOnWriteErrors());

        morphium.getConfig().writerSettings().disableRaiseOnWriteErrors();

        try {
            assertFalse(morphium.createQueryFor(UncachedObject.class).isRaiseOnWriteErrors(), "follows the configuration");
            assertTrue(morphium.createQueryFor(UncachedObject.class).setRaiseOnWriteErrors(true).isRaiseOnWriteErrors(), "the override wins");
        } finally {
            morphium.getConfig().writerSettings().enableRaiseOnWriteErrors();
        }
    }

    @Test
    public void theOverrideIsKeptByClone() {
        var q = morphium.createQueryFor(UncachedObject.class).setRaiseOnWriteErrors(false);
        assertFalse(q.clone().isRaiseOnWriteErrors());
    }
}
