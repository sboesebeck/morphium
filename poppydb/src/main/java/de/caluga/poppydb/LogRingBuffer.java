package de.caluga.poppydb;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.AppenderBase;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded in-memory tail of the server log, answering {@code getLog: "global"} over the wire
 * (#356). {@code db.adminCommand({getLog: "global"})} is the first thing an operator types when
 * the database is reachable but the host is not - this is what makes it answer something.
 *
 * <p>A Logback appender on the root logger, so it sees exactly what the configured log level
 * lets through - raising the level at runtime ({@code setParameter: {logLevel: N}}) makes it
 * more talkative, same as the console. Capacity is a fixed number of lines (mongod keeps 1024
 * for its {@code global} RamLog); the oldest line falls off when the buffer is full.
 * {@link #totalLinesWritten()} keeps counting past that, mirroring mongod's field of the same
 * name, so a reader can tell "1024 lines" from "1024 of 3 million".
 *
 * <p>Rotation and retention stay where they are (Logback or logrotate); this is a diagnostic
 * window, not a log store.
 *
 * <p>Process-wide: one JVM has one root logger, so there is one shared buffer, installed once by
 * {@link #install()} and shared by every PoppyDB instance in the JVM. Embedding applications
 * that do not bind SLF4J to Logback get a buffer that stays empty - the command then answers an
 * empty log instead of failing startup.
 */
public final class LogRingBuffer extends AppenderBase<ILoggingEvent> {

    /** Lines retained for {@code getLog: "global"} - the same as mongod's RamLog. */
    public static final int DEFAULT_CAPACITY = 1024;

    private static final DateTimeFormatter TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX").withZone(ZoneOffset.UTC);

    private static final Object INSTALL_LOCK = new Object();
    private static volatile LogRingBuffer installed;

    private final int capacity;
    private final ArrayDeque<String> lines;
    private final AtomicLong totalLinesWritten = new AtomicLong();

    public LogRingBuffer() {
        this(DEFAULT_CAPACITY);
    }

    public LogRingBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }

        this.capacity = capacity;
        this.lines = new ArrayDeque<>(capacity);
        setName("poppydb-getLog");
    }

    /**
     * The JVM-wide buffer attached to the Logback root logger, attaching it on first call.
     * Idempotent: every PoppyDB instance in the JVM (tests start dozens) shares the one appender
     * instead of stacking a new one per server. Returns a detached, always-empty buffer when
     * SLF4J is not bound to Logback.
     */
    public static LogRingBuffer install() {
        LogRingBuffer current = installed;

        if (current != null) {
            return current;
        }

        synchronized (INSTALL_LOCK) {
            if (installed == null) {
                LogRingBuffer buffer = new LogRingBuffer();

                if (LoggerFactory.getILoggerFactory() instanceof LoggerContext context) {
                    buffer.setContext(context);
                    buffer.start();
                    context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).addAppender(buffer);
                }

                installed = buffer;
            }

            return installed;
        }
    }

    @Override
    protected void append(ILoggingEvent event) {
        String line = format(event);

        synchronized (lines) {
            if (lines.size() == capacity) {
                lines.pollFirst();
            }

            lines.addLast(line);
            // inside the monitor so a concurrent getLog never sees lines() and
            // totalLinesWritten() from two different moments
            totalLinesWritten.incrementAndGet();
        }
    }

    /** One log line in the shape of the CLI jar's console pattern, timestamp in UTC. */
    static String format(ILoggingEvent event) {
        StringBuilder sb = new StringBuilder(160);
        sb.append(TIMESTAMP.format(Instant.ofEpochMilli(event.getTimeStamp())))
          .append(' ').append(String.format("%-5s", event.getLevel()))
          .append(" [").append(event.getThreadName()).append("] ")
          .append(event.getLoggerName())
          .append(" - ").append(event.getFormattedMessage());
        IThrowableProxy t = event.getThrowableProxy();

        if (t != null) {
            sb.append(" - ").append(t.getClassName());

            if (t.getMessage() != null) {
                sb.append(": ").append(t.getMessage());
            }
        }

        return sb.toString();
    }

    /** Snapshot of the retained lines, oldest first. */
    public List<String> lines() {
        synchronized (lines) {
            return new ArrayList<>(lines);
        }
    }

    /** Lines ever appended, including those that have since fallen out of the window. */
    public long totalLinesWritten() {
        synchronized (lines) {
            return totalLinesWritten.get();
        }
    }

    public int capacity() {
        return capacity;
    }

    // ---- runtime log level: the natural companion of the buffer ---------------------------
    //
    // mongod's logLevel parameter is a verbosity 0..5; Logback thinks in levels. The mapping
    // below is the one the CLI's --log-level already implies: 0 is the INFO default, anything
    // above it is DEBUG, 3 and up TRACE. WARN/ERROR are not reachable this way - a verbosity
    // cannot be negative in mongod either. The mapping is lossy by nature: 2 sets the same level
    // as 1, 4 and 5 the same as 3, and currentVerbosity() reads back the quantized value (0, 1
    // or 3), not what was set.

    /** The root logger's level as a mongod-style verbosity, or -1 when SLF4J is not Logback. */
    public static int currentVerbosity() {
        if (!(LoggerFactory.getILoggerFactory() instanceof LoggerContext context)) {
            return -1;
        }

        Level level = context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).getEffectiveLevel();

        if (level.isGreaterOrEqual(Level.INFO)) {
            return 0;
        }

        return level == Level.DEBUG ? 1 : 3;
    }

    /**
     * Sets the root logger's level from a mongod-style verbosity (0..5). Returns false when
     * SLF4J is not bound to Logback, in which case nothing changes.
     */
    public static boolean setVerbosity(int verbosity) {
        if (verbosity < 0 || verbosity > 5) {
            throw new IllegalArgumentException("logLevel must be between 0 and 5: " + verbosity);
        }

        if (!(LoggerFactory.getILoggerFactory() instanceof LoggerContext context)) {
            return false;
        }

        Level level = verbosity == 0 ? Level.INFO : verbosity < 3 ? Level.DEBUG : Level.TRACE;
        context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(level);
        return true;
    }
}
