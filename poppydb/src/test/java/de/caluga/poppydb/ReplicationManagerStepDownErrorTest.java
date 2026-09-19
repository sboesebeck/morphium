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
    void otherErrorsAreNot() {
        assertFalse(ReplicationManager.isSyncSourceSteppedDown(new RuntimeException("Connection refused")));
        assertFalse(ReplicationManager.isSyncSourceSteppedDown(withCode(146)));
        assertFalse(ReplicationManager.isSyncSourceSteppedDown(null));
    }
}
