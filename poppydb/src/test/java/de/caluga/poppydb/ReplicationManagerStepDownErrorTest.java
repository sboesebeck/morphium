package de.caluga.poppydb;

import de.caluga.morphium.driver.MorphiumDriverException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * #364: a sync source that answers 13435 (NotPrimaryNoSecondaryOk) or 10107 (NotWritablePrimary)
 * has told us it is not the primary any more. The replication loop must recognise that shape
 * so it can back off instead of reconnecting to the same host at full speed until the
 * leadership change re-targets it - CPU the election needs at exactly that moment.
 */
public class ReplicationManagerStepDownErrorTest {

    private static MorphiumDriverException withCode(int code) {
        // Deliberately no telling words in the message: the code alone must be enough.
        MorphiumDriverException e = new MorphiumDriverException("Error: " + code + " - rejected");
        e.setMongoCode(code);
        return e;
    }

    @Test
    void stepDownCodesAreRecognised() {
        assertTrue(ReplicationManager.isSyncSourceSteppedDown(withCode(13435)), "NotPrimaryNoSecondaryOk");
        assertTrue(ReplicationManager.isSyncSourceSteppedDown(withCode(10107)), "NotWritablePrimary");
        assertTrue(ReplicationManager.isSyncSourceSteppedDown(withCode(189)), "PrimarySteppedDown");
        assertTrue(ReplicationManager.isSyncSourceSteppedDown(withCode(13436)),
                "NotPrimaryOrSecondary: a demoted source re-syncing after the leader change");
    }

    @Test
    void stepDownBackoffDoublesFromOneSecondToTenSecondsCap() {
        long b = ReplicationManager.nextStepDownBackoff(0);
        assertEquals(1000, b, "first retry after a second");
        b = ReplicationManager.nextStepDownBackoff(b);
        assertEquals(2000, b);
        b = ReplicationManager.nextStepDownBackoff(b);
        assertEquals(4000, b);
        b = ReplicationManager.nextStepDownBackoff(b);
        assertEquals(8000, b);
        b = ReplicationManager.nextStepDownBackoff(b);
        assertEquals(10_000, b, "capped at ten seconds");
        assertEquals(10_000, ReplicationManager.nextStepDownBackoff(b), "stays at the cap");
    }

    @Test
    void messageOnlyShapeIsRecognised() {
        assertTrue(ReplicationManager.isSyncSourceSteppedDown(
                new RuntimeException("Error: 13435 - not primary and read preference is primary")),
                "the code can arrive wrapped in a plain exception message");
        assertTrue(ReplicationManager.isSyncSourceSteppedDown(
                new RuntimeException("outer", withCode(13435))), "or as a cause");
    }

    @Test
    void aNumberThatMerelyLooksLikeTheCodeIsNotAStepDown() {
        // ports are random in tests, counts and ids carry any digits - only the driver's
        // "Error: CODE - ..." shape means the code
        assertFalse(ReplicationManager.isSyncSourceSteppedDown(
                new RuntimeException("Connection refused: localhost:13435")), "a port");
        assertFalse(ReplicationManager.isSyncSourceSteppedDown(
                new RuntimeException("13436 documents copied, then: broken pipe")), "a count");
        assertFalse(ReplicationManager.isSyncSourceSteppedDown(
                new RuntimeException("Connecting to primary at localhost:10107 failed")), "a port again");
        assertTrue(ReplicationManager.isSyncSourceSteppedDown(
                new RuntimeException("Error: 13436 - node is recovering")), "the driver's shape");
    }

    @Test
    void otherErrorsAreNot() {
        assertFalse(ReplicationManager.isSyncSourceSteppedDown(new RuntimeException("Connection refused")));
        assertFalse(ReplicationManager.isSyncSourceSteppedDown(withCode(146)));
        assertFalse(ReplicationManager.isSyncSourceSteppedDown(null));
    }
}
