package de.caluga.test.mongo.suite.inmem;

import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MOR-059: the per-query override of raising on a rejected write. */
public class QueryRaiseOnWriteErrorsTest extends MorphiumInMemTestBase {

    @Test
    public void aQueryRaisesUnlessItIsToldNotTo() {
        var q = morphium.createQueryFor(UncachedObject.class);
        assertNull(q.getRaiseOnWriteErrors(), "no override by default");
        assertTrue(q.isRaiseOnWriteErrors(), "raising is the default");

        q.setRaiseOnWriteErrors(false);
        assertFalse(q.isRaiseOnWriteErrors());
        q.setRaiseOnWriteErrors(true);
        assertTrue(q.isRaiseOnWriteErrors());
        q.setRaiseOnWriteErrors(null);
        assertTrue(q.isRaiseOnWriteErrors());
    }

    @Test
    public void theOverrideIsKeptByClone() {
        var q = morphium.createQueryFor(UncachedObject.class).setRaiseOnWriteErrors(false);
        assertFalse(q.clone().isRaiseOnWriteErrors());
    }
}
