package de.caluga.poppydb.netty;

import de.caluga.morphium.driver.MorphiumDriverException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * #373: the generic command catch-all built its {@code ok:0} failure response from the message
 * alone and dropped any MongoDB error code the failure carried, so a client saw {@code ok:0}
 * with a null mongoCode and could not act on it (retry, classify). The fast paths keep the code;
 * this pins the extraction the catch-all now uses to keep it too.
 */
public class GenericErrorCodePreservationTest {

    private static MorphiumDriverException coded(String msg, Object code) {
        MorphiumDriverException e = new MorphiumDriverException(msg);
        e.setMongoCode(code);
        return e;
    }

    @Test
    public void codeIsTakenFromADirectlyThrownDriverException() {
        assertEquals(146, MongoCommandHandler.deepestMongoCode(coded("refused", 146)));
    }

    @Test
    public void codeIsFoundThroughWrappingCauses() {
        // The driver's coded exception is what the generic dispatch usually wraps in a
        // RuntimeException / InvocationTargetException before it reaches the catch-all.
        Throwable wrapped = new RuntimeException("outer",
                new IllegalStateException("mid", coded("heap watermark", 146)));
        assertEquals(146, MongoCommandHandler.deepestMongoCode(wrapped));
    }

    @Test
    public void theDeepestCodeWins() {
        // Two coded exceptions in the chain: the deepest is the origin, matching how the message
        // is taken (getDeepestCauseMessage), so code and message describe the same failure.
        MorphiumDriverException deep = coded("origin", 146);
        MorphiumDriverException outer = coded("wrapper", 10107);
        outer.initCause(deep);
        assertEquals(146, MongoCommandHandler.deepestMongoCode(outer));
    }

    @Test
    public void aChainWithoutACodeReturnsNull() {
        Throwable noCode = new RuntimeException("boom", new IllegalStateException("inner"));
        assertNull(MongoCommandHandler.deepestMongoCode(noCode));
        assertNull(MongoCommandHandler.deepestMongoCode(coded("no code set", null)));
    }
}
