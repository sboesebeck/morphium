package de.caluga.morphium.driver.inmem;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.commands.WatchCommand;

/**
 * #380: {@code failHistoryLost} logged every call at ERROR, unthrottled. On a PoppyDB node that
 * had just emptied its store for a full sync, every resume attempt of the connected clients landed
 * behind the drop, and the node wrote 186,465 identical ERROR lines (66 MB) in five seconds -
 * burying everything else that happened in that window.
 *
 * <p>The same burst one layer up, in PoppyDB's {@code WatchCursorManager}, came out as 18 and 13
 * lines: that path throttles to the powers of two of the burst (1, 2, 4, 8, ...), which keeps the
 * beginning, the growth and the final order of magnitude, and resets after a quiet period. The
 * driver path now does the same. And, as for #331 and #361, the condition is an expected,
 * client-handled one - the stream is ended with a protocol-defined answer the client reacts to by
 * restarting - so the reported lines go out at INFO, not ERROR.
 *
 * <p>The wire contract is unchanged: every ended stream still carries the terminal error with the
 * ChangeStreamHistoryLost marker, whether its line was reported or not.
 */
public class HistoryLostLogThrottleTest {

    private static final String DB = "throttle_db";
    private static final String COLL = "data";

    /** The burst measured in the field was 186k; enough occurrences to cross several powers of two. */
    private static final int BURST = 143;

    private InMemoryDriver drv;
    private Logger driverLog;
    private ListAppender<ILoggingEvent> appender;
    private Level savedLevel;

    @BeforeEach
    public void setUp() {
        drv = new InMemoryDriver();
        drv.connect();
        drv.setServerMode(true);

        driverLog = (Logger) LoggerFactory.getLogger(InMemoryDriver.class);
        appender = new ListAppender<>();
        appender.start();
        savedLevel = driverLog.getLevel();
        driverLog.setLevel(Level.DEBUG);
        driverLog.addAppender(appender);
    }

    @AfterEach
    public void tearDown() {
        driverLog.detachAppender(appender);
        driverLog.setLevel(savedLevel);

        if (drv != null) {
            drv.close();
        }
    }

    private List<ILoggingEvent> historyLostAt(Level level) {
        return appender.list.stream()
                .filter(e -> e.getLevel() == level)
                .filter(e -> e.getFormattedMessage().contains("resume window lost"))
                .collect(Collectors.toList());
    }

    private static Map<String, Object> token(long sequence) {
        return Doc.of("_data", String.format(Locale.ROOT, "%016x", sequence));
    }

    private static final DriverTailableIterationCallback NOOP = new DriverTailableIterationCallback() {
        @Override
        public void incomingData(Map<String, Object> data, long dur) {
        }

        @Override
        public boolean isContinued() {
            return true;
        }
    };

    /**
     * Registers a stream that resumes from before a drop of its collection - the exact shape of
     * the field burst ("a namespace covered by this stream was dropped after resume token") - and
     * waits until the driver has ended it.
     */
    private WatchCommand resumeBehindTheDrop(long resumeFrom) throws Exception {
        CountDownLatch ended = new CountDownLatch(1);
        WatchCommand wcmd = new WatchCommand(drv).setDb(DB).setColl(COLL).setMaxTimeMS(100)
                .setResumeAfter(token(resumeFrom)).setCb(NOOP)
                .setOnTerminalError(reason -> ended.countDown());
        drv.readSingleAnswer(drv.runCommand(wcmd));
        assertThat(ended.await(5, TimeUnit.SECONDS)).as("the resume must be ended as history lost").isTrue();
        return wcmd;
    }

    @Test
    public void aHistoryLostBurstIsThrottledAndNotLoggedAsAnError() throws Exception {
        drv.insert(DB, COLL, List.of(Doc.of("_id", 1)), null, true);
        long beforeTheDrop = drv.getChangeStreamSequence();
        // The wipe of a full sync: the drop boundary lands behind every token the clients hold.
        drv.drop(DB, COLL, null);

        List<WatchCommand> ended = new ArrayList<>();

        for (int i = 0; i < BURST; i++) {
            ended.add(resumeBehindTheDrop(beforeTheDrop));
        }

        assertThat(ended).allSatisfy(w -> assertThat(w.getTerminalError())
                .as("the wire contract is unchanged: every ended stream carries the marker")
                .contains("ChangeStreamHistoryLost"));

        assertThat(historyLostAt(Level.ERROR))
                .as("an expected, client-handled condition must not be logged at ERROR")
                .isEmpty();
        // 1, 2, 4, 8, 16, 32, 64, 128 - the burst collapses to its powers of two
        assertThat(historyLostAt(Level.INFO))
                .as("a " + BURST + "-line burst must collapse to a handful of lines")
                .hasSize(8);
        assertThat(historyLostAt(Level.INFO).get(7).getFormattedMessage())
                .as("a reported line says where in the burst it is, so the order of magnitude is readable")
                .contains("occurrence 128");
        assertThat(historyLostAt(Level.DEBUG))
                .as("the rest is still there at DEBUG")
                .hasSize(BURST - 8);
    }
}
