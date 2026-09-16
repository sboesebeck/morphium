package de.caluga.poppydb;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #356: the bounded log tail behind {@code getLog: "global"}. Capacity is a hard cap, the oldest
 * line leaves first, and totalLinesWritten keeps counting past the window.
 */
public class LogRingBufferTest {

    private LoggingEvent event(String message) {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = context.getLogger("de.caluga.poppydb.test");
        return new LoggingEvent("fqcn", logger, Level.INFO, message, null, null);
    }

    @Test
    public void keepsTheNewestLinesAndCountsAll() {
        LogRingBuffer buffer = new LogRingBuffer(3);
        buffer.start();

        for (int i = 1; i <= 5; i++) {
            buffer.doAppend(event("line " + i));
        }

        List<String> lines = buffer.lines();
        assertThat(lines).hasSize(3);
        assertThat(lines.get(0)).endsWith("line 3");
        assertThat(lines.get(2)).endsWith("line 5");
        assertThat(buffer.totalLinesWritten())
                .as("mongod's totalLinesWritten counts past the window, so a reader can tell "
                    + "'3 lines' from '3 of 5'")
                .isEqualTo(5);
    }

    @Test
    public void formatsLevelLoggerAndMessage() {
        LogRingBuffer buffer = new LogRingBuffer(8);
        buffer.start();
        buffer.doAppend(event("hello operator"));

        String line = buffer.lines().get(0);
        assertThat(line).contains("INFO").contains("de.caluga.poppydb.test").endsWith("hello operator");
        assertThat(line).as("starts with an ISO timestamp").matches("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z .*");
    }

    @Test
    public void installIsIdempotentAndSeesRootLoggerOutput() {
        LogRingBuffer first = LogRingBuffer.install();
        LogRingBuffer second = LogRingBuffer.install();

        assertThat(second).as("one JVM, one root logger, one buffer").isSameAs(first);

        long before = first.totalLinesWritten();
        LoggerFactory.getLogger(LogRingBufferTest.class).warn("ring buffer probe {}", before);

        assertThat(first.totalLinesWritten()).isGreaterThan(before);
        assertThat(first.lines()).anySatisfy(l -> assertThat(l).contains("ring buffer probe " + before));
    }

    @Test
    public void rejectsNonPositiveCapacity() {
        assertThatThrownBy(() -> new LogRingBuffer(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void verbosityMapsToAndFromTheRootLevel() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        Level original = root.getLevel();

        try {
            assertThat(LogRingBuffer.setVerbosity(0)).isTrue();
            assertThat(root.getLevel()).isEqualTo(Level.INFO);
            assertThat(LogRingBuffer.currentVerbosity()).isEqualTo(0);

            LogRingBuffer.setVerbosity(1);
            assertThat(root.getLevel()).isEqualTo(Level.DEBUG);
            assertThat(LogRingBuffer.currentVerbosity()).isEqualTo(1);

            LogRingBuffer.setVerbosity(5);
            assertThat(root.getLevel()).isEqualTo(Level.TRACE);
            assertThat(LogRingBuffer.currentVerbosity()).isEqualTo(3);

            assertThatThrownBy(() -> LogRingBuffer.setVerbosity(6)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> LogRingBuffer.setVerbosity(-1)).isInstanceOf(IllegalArgumentException.class);
        } finally {
            root.setLevel(original);
        }
    }
}
