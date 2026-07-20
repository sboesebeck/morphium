package de.caluga.test.mongo.suite.base;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * waitForConditionToBecomeTrue is the timeout helper behind most messaging tests. On timeout it
 * threw {@code new AssertionError(failCB)} - passing the FailCallback LAMBDA as the detail message -
 * so every failure read "AssertionError: TestUtils$$Lambda/0x...@46c3a14d" and the caller's actual
 * message ("Answers not received", ...) was lost. The callback was never invoked either, so it did
 * not even reach the log. That made every flaky-messaging investigation start from zero.
 */
@Tag("core")
public class TestUtilsWaitDiagnosticsTest {

    @Test
    public void timeoutReportsTheCallersFailMessage() {
        AssertionError err = assertThrows(AssertionError.class,
            () -> TestUtils.waitForConditionToBecomeTrue(50, "Answers not received", () -> false));

        assertNotNull(err.getMessage(), "the failure must carry a message at all");
        assertTrue(err.getMessage().contains("Answers not received"),
            "the caller's message must survive to the assertion, got: " + err.getMessage());
        assertFalse(err.getMessage().contains("$$Lambda"),
            "the failure must not stringify the callback lambda: " + err.getMessage());
    }

    @Test
    public void timeoutInvokesTheFailCallback() {
        AtomicBoolean called = new AtomicBoolean(false);

        assertThrows(AssertionError.class,
            () -> TestUtils.waitForConditionToBecomeTrue(50, (dur, e) -> called.set(true), () -> false, null));

        assertTrue(called.get(), "the FailCallback must actually be invoked on timeout");
    }

    @Test
    public void satisfiedConditionStillReturnsNormally() {
        assertDoesNotThrow(() -> TestUtils.waitForConditionToBecomeTrue(1000, "must not fail", () -> true));
    }
}
