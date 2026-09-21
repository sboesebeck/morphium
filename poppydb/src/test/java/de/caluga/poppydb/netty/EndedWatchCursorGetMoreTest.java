package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.poppydb.messaging.MessagingOptimizer;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * #389: a getMore on a change stream cursor the server has just ended must be answered with
 * ChangeStreamHistoryLost (286) - in BOTH branches of the race. The parked branch always was;
 * but once the ended cursor had been removed from the registry, a later getMore fell through to
 * the generic path and was answered as an exhausted cursor ({@code ok:1, cursor.id: 0}), i.e.
 * "your stream ended normally". The client cannot tell that from a legitimate end of stream,
 * resumes in place with the same dead token, and gets the same answer again - the loop behind
 * #383 (~13,000 registrations/s on the incident node).
 *
 * <p>Same seam as {@link HistoryLostLogLevelTest}: {@link MongoCommandHandler} on an
 * {@link EmbeddedChannel}, the stream ended the way the driver ends one.
 */
public class EndedWatchCursorGetMoreTest {

    private static final String DB = "ended_cursor_db";
    private static final String COLL = "data";

    private InMemoryDriver drv;
    private WatchCursorManager cursorManager;
    private EmbeddedChannel ch;
    private final AtomicInteger msgId = new AtomicInteger(1);
    private final AtomicInteger docId = new AtomicInteger();

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
    }

    @AfterEach
    public void tearDown() {
        if (ch != null) {
            ch.finishAndReleaseAll();
        }

        if (drv != null) {
            drv.close();
        }
    }

    private Map<String, Object> getMore(long cursorId) throws Exception {
        OpMsg getMore = new OpMsg();
        getMore.setMessageId(msgId.incrementAndGet());
        getMore.setFirstDoc(Doc.of("getMore", cursorId, "collection", COLL, "maxTimeMS", 5000, "$db", DB));
        ch.writeInbound(getMore);
        OpMsg answer = null;
        long deadline = System.currentTimeMillis() + 5000;

        while (answer == null && System.currentTimeMillis() < deadline) {
            ch.runPendingTasks();
            answer = ch.readOutbound();

            if (answer == null) {
                Thread.sleep(20);
            }
        }

        assertThat(answer).as("getMore on cursor " + cursorId + " must be answered").isNotNull();
        return answer.getFirstDoc();
    }

    /** Ends the stream like the driver does after a foreign resume token (#361) or a wipe (#380). */
    private long endedStream() throws Exception {
        drv.insert(DB, COLL, List.of(Doc.of("_id", docId.incrementAndGet())), null, true);
        WatchCommand wcmd = new WatchCommand(drv).setDb(DB).setColl(COLL).setMaxTimeMS(5000);
        long cursorId = cursorManager.createWatchCursor(drv, wcmd);

        // the parked branch of the race: the getMore is waiting when the stream ends
        OpMsg parked = new OpMsg();
        parked.setMessageId(msgId.incrementAndGet());
        parked.setFirstDoc(Doc.of("getMore", cursorId, "collection", COLL, "maxTimeMS", 5000, "$db", DB));
        ch.writeInbound(parked);
        ch.runPendingTasks();
        assertThat((OpMsg) ch.readOutbound()).as("precondition: the getMore must be parked").isNull();

        wcmd.setTerminalError("ChangeStreamHistoryLost: resume window lost for change stream on " + DB + "." + COLL
                + ": resume token 900 is beyond this driver's newest token 100 (foreign or reset sequence space)");

        OpMsg answer = null;
        long deadline = System.currentTimeMillis() + 5000;

        while (answer == null && System.currentTimeMillis() < deadline) {
            ch.runPendingTasks();
            answer = ch.readOutbound();

            if (answer == null) {
                Thread.sleep(20);
            }
        }

        assertThat(answer).as("precondition: the parked getMore is answered when the stream ends").isNotNull();
        assertThat(((Number) answer.getFirstDoc().get("code")).intValue())
                .as("precondition: the parked branch answers 286").isEqualTo(286);
        assertThat(cursorManager.hasCursor(cursorId)).as("precondition: the ended cursor is gone").isFalse();
        return cursorId;
    }

    @Test
    public void aLateGetMoreOnAnEndedStreamIsHistoryLostNotAnExhaustedCursor() throws Exception {
        long cursorId = endedStream();

        // the other branch of the race: the client's getMore arrives after the removal
        Map<String, Object> late = getMore(cursorId);

        assertThat(late).as("full reply was: " + late).containsKey("code");
        assertThat(((Number) late.get("code")).intValue())
                .as("a getMore on an ended stream must say history lost, not 'exhausted': " + late)
                .isEqualTo(286);
        assertThat(String.valueOf(late.get("errmsg")))
                .as("the errmsg carries the marker ChangeStreamMonitor keys its recovery on")
                .contains("ChangeStreamHistoryLost");
    }

    @Test
    public void theEndedStreamIsForgottenAfterItsGrace() throws Exception {
        cursorManager.setEndedWatchCursorGraceMs(50);
        long cursorId = endedStream();
        Thread.sleep(150);

        Map<String, Object> late = getMore(cursorId);

        assertThat(late).as("after the grace the id is unknown again, like any other dead cursor: " + late)
                .doesNotContainKey("code");
    }

    @Test
    public void theMemoryOfEndedStreamsIsBounded() throws Exception {
        cursorManager.setEndedWatchCursorCapacity(2);
        long first = endedStream();
        long second = endedStream();
        long third = endedStream();

        assertThat(((Number) getMore(third).get("code")).intValue()).isEqualTo(286);
        assertThat(((Number) getMore(second).get("code")).intValue()).isEqualTo(286);
        assertThat(getMore(first)).as("the oldest entry is evicted once the capacity is reached")
                .doesNotContainKey("code");
    }
}
