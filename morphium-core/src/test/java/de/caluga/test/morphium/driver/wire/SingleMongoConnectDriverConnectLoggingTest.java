package de.caluga.test.morphium.driver.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;

/**
 * A node that's unreachable (e.g. a PoppyDB/Mongo replica-set member that's down) makes
 * {@code SingleMongoConnectDriver#connect()}'s retry loop log a full ERROR-level stack trace on
 * EVERY retry attempt - {@link de.caluga.poppydb.election.ElectionNetworkClient} calls this
 * repeatedly (once per vote request / heartbeat) for as long as a peer stays down, producing
 * thousands of stack traces in the log for what is, from the caller's perspective, an entirely
 * expected and already-handled transient condition (ElectionNetworkClient's own callers already
 * log failures at debug/trace, treating them as retryable - see sendVoteRequest/sendAppendEntries).
 * A single unreachable host must not flood the log at ERROR with a stack trace per attempt; only
 * the final, retries-exhausted failure is worth a stack trace, and even that at WARN rather than
 * ERROR (the caller decides whether it's actually an error - the driver itself will keep trying
 * on the next call).
 */
public class SingleMongoConnectDriverConnectLoggingTest {

    /** A closed local port: refuses immediately, like an unreachable host. */
    private int deadPort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private List<ILoggingEvent> runAndCaptureLogs(Runnable body) {
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(SingleMongoConnectDriver.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            body.run();
            return appender.list;
        } finally {
            logger.detachAppender(appender);
        }
    }

    @Test
    void retryingAgainstADeadHostNeverLogsAtErrorLevel() throws Exception {
        int port = deadPort();
        SingleMongoConnectDriver drv = new SingleMongoConnectDriver();
        drv.setHostSeed(java.util.List.of("localhost:" + port));
        drv.setConnectionTimeout(300);
        drv.setRetriesOnNetworkError(2);
        drv.setSleepBetweenErrorRetries(10);

        List<ILoggingEvent> events = runAndCaptureLogs(() -> {
            try {
                drv.connect();
            } catch (Exception expected) {
                // retries exhausted - expected, this test is about what got logged along the way
            }
        });

        long errorCount = events.stream().filter(ev -> ev.getLevel() == Level.ERROR).count();
        assertEquals(0, errorCount, "no retry attempt against an unreachable host may log at ERROR: "
                + events.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.joining(" | ")));
    }

    @Test
    void retryingAgainstADeadHostLogsAtMostOneWarnWhenRetriesAreExhausted() throws Exception {
        int port = deadPort();
        SingleMongoConnectDriver drv = new SingleMongoConnectDriver();
        drv.setHostSeed(java.util.List.of("localhost:" + port));
        drv.setConnectionTimeout(300);
        drv.setRetriesOnNetworkError(2);
        drv.setSleepBetweenErrorRetries(10);

        List<ILoggingEvent> events = runAndCaptureLogs(() -> {
            try {
                drv.connect();
            } catch (Exception expected) {
            }
        });

        long warnCount = events.stream().filter(ev -> ev.getLevel() == Level.WARN).count();
        assertEquals(1, warnCount, "exactly one summary WARN once retries are exhausted, not one per attempt: "
                + events.stream().map(ev -> ev.getLevel() + ":" + ev.getFormattedMessage()).collect(Collectors.joining(" | ")));
    }

    @Test
    void connectStillThrowsAfterRetriesAreExhausted() throws Exception {
        int port = deadPort();
        SingleMongoConnectDriver drv = new SingleMongoConnectDriver();
        drv.setHostSeed(java.util.List.of("localhost:" + port));
        drv.setConnectionTimeout(300);
        drv.setRetriesOnNetworkError(1);
        drv.setSleepBetweenErrorRetries(10);

        assertTrue(org.junit.jupiter.api.Assertions.assertThrows(MorphiumDriverException.class, drv::connect)
                .getMessage().contains("max retries exceeded"));
    }
}
