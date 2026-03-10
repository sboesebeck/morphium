package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.FunctionNotSupportedException;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumDriverNetworkException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests verifying that {@link MorphiumDriverException} is an unchecked
 * ({@code RuntimeException}) and that the exception hierarchy behaves correctly.
 * <p>
 * Rationale: MorphiumDriverException was changed from {@code extends Exception}
 * to {@code extends RuntimeException} so that callers of Morphium's persistence
 * API can catch database errors directly without the pervasive
 * {@code catch (MorphiumDriverException e) { throw new RuntimeException(e); }}
 * wrapping pattern. This aligns with the MongoDB Java driver convention
 * ({@code MongoException extends RuntimeException}) and modern Java persistence
 * frameworks (JPA, jOOQ, Spring Data).
 */
@Tag("inmemory")
class MorphiumDriverExceptionTest {

    // ------------------------------------------------------------------ //
    //  1. Type hierarchy assertions
    // ------------------------------------------------------------------ //

    @Test
    void morphiumDriverException_isRuntimeException() {
        var ex = new MorphiumDriverException("test error");
        assertInstanceOf(RuntimeException.class, ex,
            "MorphiumDriverException must extend RuntimeException");
    }

    @Test
    void morphiumDriverNetworkException_isRuntimeException() {
        var ex = new MorphiumDriverNetworkException("network error");
        assertInstanceOf(RuntimeException.class, ex,
            "MorphiumDriverNetworkException must extend RuntimeException");
        assertInstanceOf(MorphiumDriverException.class, ex,
            "MorphiumDriverNetworkException must extend MorphiumDriverException");
    }

    @Test
    void functionNotSupportedException_isRuntimeException() {
        var ex = new FunctionNotSupportedException("not supported");
        assertInstanceOf(RuntimeException.class, ex,
            "FunctionNotSupportedException must extend RuntimeException");
        assertInstanceOf(MorphiumDriverException.class, ex,
            "FunctionNotSupportedException must extend MorphiumDriverException");
    }

    @Test
    void morphiumDriverException_isNotCheckedException() {
        // Exception (checked) is a supertype of RuntimeException, so this
        // verifies the hierarchy is NOT checked.
        var ex = new MorphiumDriverException("test");
        // If MorphiumDriverException were checked (extends Exception but not
        // RuntimeException), this next assertion would still pass because
        // RuntimeException extends Exception. So we check the class hierarchy
        // explicitly.
        assertTrue(RuntimeException.class.isAssignableFrom(MorphiumDriverException.class),
            "MorphiumDriverException.class must be assignable to RuntimeException.class");
    }

    // ------------------------------------------------------------------ //
    //  2. Constructor / metadata preservation
    // ------------------------------------------------------------------ //

    @Test
    void messageOnly_constructor() {
        var ex = new MorphiumDriverException("something broke");
        assertEquals("something broke", ex.getMessage());
        assertNull(ex.getCause());
    }

    @Test
    void messageAndCause_constructor() {
        var cause = new IllegalStateException("root cause");
        var ex = new MorphiumDriverException("wrapper", cause);
        assertEquals("wrapper", ex.getMessage());
        assertSame(cause, ex.getCause());
    }

    @Test
    void fullContext_constructor() {
        var cause = new RuntimeException("io error");
        var query = Map.<String, Object>of("_id", "abc");
        var ex = new MorphiumDriverException("failed", cause, "myCollection", "myDb", query);

        assertEquals("failed", ex.getMessage());
        assertSame(cause, ex.getCause());
        assertEquals("myCollection", ex.getCollection());
        assertEquals("myDb", ex.getDb());
        assertEquals(query, ex.getQuery());
    }

    @Test
    void mongoCodeAndReason_preserved() {
        var ex = new MorphiumDriverException("dup key");
        ex.setMongoCode(11000);
        ex.setMongoReason("E11000 duplicate key error");

        assertEquals(11000, ex.getMongoCode());
        assertEquals("E11000 duplicate key error", ex.getMongoReason());
    }

    // ------------------------------------------------------------------ //
    //  3. Catch behaviour — the key use-case
    // ------------------------------------------------------------------ //

    @Test
    void canBeCaughtAsMorphiumDriverException() {
        assertThrows(MorphiumDriverException.class, () -> {
            throw new MorphiumDriverException("db error");
        });
    }

    @Test
    void canBeCaughtAsRuntimeException() {
        // This is the critical test: callers that catch RuntimeException
        // will now also catch MorphiumDriverException.
        assertThrows(RuntimeException.class, () -> {
            throw new MorphiumDriverException("db error");
        });
    }

    @Test
    void networkException_caughtByDriverExceptionCatch() {
        // Subclass should be caught by parent catch clause
        assertThrows(MorphiumDriverException.class, () -> {
            throw new MorphiumDriverNetworkException("timeout");
        });
    }

    @Test
    void propagatesThroughMethodWithoutThrowsDeclaration() {
        // Before the change, this pattern required a try-catch-wrap.
        // Now MorphiumDriverException propagates naturally through methods
        // that don't declare "throws MorphiumDriverException".
        assertThrows(MorphiumDriverException.class, () -> {
            methodWithoutThrowsClause();
        });
    }

    @Test
    void noDoubleWrapping_whenRethrown() {
        // Verify that catching and rethrowing does NOT create nested
        // RuntimeException(MorphiumDriverException) — the original exception
        // type is preserved.
        try {
            throw new MorphiumDriverException("original");
        } catch (MorphiumDriverException e) {
            // Simply rethrow — no wrapping needed anymore
            var rethrown = assertThrows(MorphiumDriverException.class, () -> { throw e; });
            assertEquals("original", rethrown.getMessage());
            assertNull(rethrown.getCause(), "No unnecessary cause wrapping");
        }
    }

    // ------------------------------------------------------------------ //
    //  Helpers
    // ------------------------------------------------------------------ //

    /**
     * Simulates a method that does NOT declare {@code throws MorphiumDriverException}
     * — which was the root cause of the pervasive wrapping pattern.
     */
    private void methodWithoutThrowsClause() {
        throw new MorphiumDriverException("propagates without declaration");
    }
}
