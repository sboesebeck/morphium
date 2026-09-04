package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.poppydb.messaging.MessagingOptimizer;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * ChangeStreamHistoryLost is a protocol-defined answer, not a server failure: the client gets
 * code 286 and re-establishes its stream, exactly as against MongoDB. Logging it at ERROR turns
 * routine operation into alarm noise - a rolling restart of a three-node cluster produced ~143
 * ERROR lines in one minute across ~25 bus participants, one per change stream, because resume
 * tokens do not survive a change of primary (#361).
 *
 * <p>Same reasoning as #331 for the IOException family: expected, client-handled conditions
 * belong below ERROR; everything else about a getMore stays ERROR.
 *
 * <p>Only the quietened case is covered here. The counterpart - a genuine getMore failure still
 * logging at ERROR - has no cheap trigger: a getMore on an unknown cursor answers ok:1 with an
 * empty batch rather than failing, and the only other way into the error branch is a terminal
 * error, which is always a ChangeStreamHistoryLostException. The else branch keeps the original
 * log call unchanged.
 */
public class HistoryLostLogLevelTest {

    private static final String DB = "loglevel_db";
    private static final String COLL = "data";

    private InMemoryDriver drv;
    private WatchCursorManager cursorManager;
    private EmbeddedChannel ch;
    private final AtomicInteger msgId = new AtomicInteger(1);

    private Logger handlerLog;
    private ListAppender<ILoggingEvent> appender;
    private Level savedLevel;

    @BeforeEach
    public void setUp() {
        drv = new InMemoryDriver();
        drv.connect();
        drv.setServerMode(true);
        cursorManager = new WatchCursorManager();
        MessagingOptimizer optimizer = new MessagingOptimizer(drv);
        optimizer.setWatchCursorManager(cursorManager);
        ch = new EmbeddedChannel(new MongoCommandHandler(drv, cursorManager, new FindCursorRegistry(),
                optimizer, msgId, "0.0.0.0", 27017, "my-rs", List.of("localhost:27017"), true,
                "localhost:27017", 0, () -> null));

        handlerLog = (Logger) LoggerFactory.getLogger(MongoCommandHandler.class);
        appender = new ListAppender<>();
        appender.start();
        savedLevel = handlerLog.getLevel();
        handlerLog.setLevel(Level.DEBUG);
        handlerLog.addAppender(appender);
    }

    @AfterEach
    public void tearDown() {
        handlerLog.detachAppender(appender);
        handlerLog.setLevel(savedLevel);

        if (ch != null) {
            ch.finishAndReleaseAll();
        }

        if (drv != null) {
            drv.close();
        }
    }

    private Map<String, Object> sendCommand(Map<String, Object> cmd) {
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(cmd);
        ch.writeInbound(msg);
        ch.runPendingTasks();
        OpMsg reply = ch.readOutbound();
        assertThat(reply).as("the handler must answer: " + cmd.keySet()).isNotNull();
        return reply.getFirstDoc();
    }

    private List<ILoggingEvent> at(Level level) {
        return appender.list.stream().filter(e -> e.getLevel() == level).collect(Collectors.toList());
    }

    private String rendered(List<ILoggingEvent> events) {
        return events.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.joining(" | "));
    }

    @Test
    public void historyLostIsNotLoggedAsAnError() throws Exception {
        drv.insert(DB, COLL, List.of(Doc.of("_id", 1)), null, true);

        WatchCommand wcmd = new WatchCommand(drv).setDb(DB).setColl(COLL).setMaxTimeMS(5000);
        long cursorId = cursorManager.createWatchCursor(drv, wcmd);

        // Parked: nothing buffered, so the handler waits instead of answering right away.
        OpMsg getMore = new OpMsg();
        getMore.setMessageId(msgId.incrementAndGet());
        getMore.setFirstDoc(Doc.of("getMore", cursorId, "collection", COLL, "maxTimeMS", 5000, "$db", DB));
        appender.list.clear();
        ch.writeInbound(getMore);
        ch.runPendingTasks();
        assertThat((OpMsg) ch.readOutbound()).as("precondition: the getMore must be parked").isNull();

        // What a client hits after a change of primary: its token belongs to the old node's
        // sequence space, so this node can never serve it (#361).
        wcmd.setTerminalError("resume window lost for change stream on " + DB + "." + COLL
                + ": resume token 900 is beyond this driver's newest token 100"
                + " (foreign or reset sequence space)");

        OpMsg answer = null;
        long deadline = System.currentTimeMillis() + 5000;

        while (answer == null && System.currentTimeMillis() < deadline) {
            ch.runPendingTasks();
            answer = ch.readOutbound();

            if (answer == null) {
                Thread.sleep(20);
            }
        }

        assertThat(answer).as("the parked getMore must be answered once the stream is unservable").isNotNull();
        Map<String, Object> reply = answer.getFirstDoc();

        // Unchanged on the wire: the client still gets the code it needs to resync.
        assertThat(reply).as("full reply was: " + reply).containsKey("code");
        assertThat(((Number) reply.get("code")).intValue())
                .as("the client must still receive ChangeStreamHistoryLost: " + reply).isEqualTo(286);

        assertThat(at(Level.ERROR))
                .as("an expected, client-handled 286 must not be logged at ERROR: " + rendered(at(Level.ERROR)))
                .isEmpty();
        assertThat(rendered(at(Level.INFO)))
                .as("but it must stay visible - a change of primary should be readable in the log")
                .contains("foreign or reset sequence space");
    }


    /**
     * A failover hits every change stream at once - on a bus with a few dozen participants that
     * is a three-digit burst. One line per stream is unreadable even at INFO, so the burst is
     * throttled: occurrence 1, 2, 4, 8, ... are logged with their running count, the rest goes
     * to DEBUG. The counter resets after a quiet period, so the next failover starts over.
     */
    @Test
    public void aBurstIsThrottledToAFewLines() {
        int reported = 0;

        // The burst measured on acceptance: one per change stream across ~25 bus participants.
        for (int i = 0; i < 143; i++) {
            if (cursorManager.shouldReport(cursorManager.recordHistoryLost())) {
                reported++;
            }
        }

        // 1, 2, 4, 8, 16, 32, 64, 128
        assertThat(reported)
                .as("a 143-line burst must collapse to a handful of lines")
                .isEqualTo(8);
    }
}
