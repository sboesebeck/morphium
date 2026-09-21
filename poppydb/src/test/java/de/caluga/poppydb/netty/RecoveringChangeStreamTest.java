package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
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
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.poppydb.messaging.MessagingOptimizer;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * #380: a node that empties its store for a full sync is RECOVERING and must not serve client
 * change streams - neither new registrations nor the streams that were registered while it was
 * still primary.
 *
 * <p>Observed during a rolling restart: the stepped-down primary decided on a full sync, dropped
 * every local database and reported itself INCOMPLETE - while 282 client connections stayed open
 * with their change streams registered. The wipe runs under {@code suppressChangeStreamEvents()},
 * so those streams never saw the drop: they sat parked through the wipe and would have continued
 * on the re-synced data with an undetectable gap, which is the one thing
 * {@code InMemoryDriver.failHistoryLost} exists to prevent. MongoDB ends every open cursor on the
 * transition to RECOVERING; PoppyDB now does the same, with the answer clients already handle:
 * ChangeStreamHistoryLost (286) - {@code ChangeStreamMonitor} discards its token and starts fresh,
 * {@code ReplicationManager} falls back to a full sync.
 *
 * <p>New registrations during the sync are refused by the RECOVERING gate in {@code preDispatch}
 * (13436, #356/#371); the first test pins that the gate covers change streams and that the refusal
 * is transient. The last test covers the instrumentation for the open question in #380 - what
 * drives ~40k registration attempts per second - which is answered by counting them.
 */
public class RecoveringChangeStreamTest {

    private static final String DB = "recovering_db";
    private static final String COLL = "data";

    private InMemoryDriver drv;
    private WatchCursorManager cursorManager;
    private EmbeddedChannel ch;
    private final AtomicInteger msgId = new AtomicInteger(1);
    private final AtomicBoolean syncing = new AtomicBoolean(false);

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
                "localhost:27017", 0, () -> null, null, syncing::get));

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

    /** The registration a client sends: an aggregate whose pipeline starts with $changeStream. */
    private static Map<String, Object> changeStreamRegistration(Map<String, Object> resumeAfter) {
        Doc changeStream = Doc.of();

        if (resumeAfter != null) {
            changeStream.put("resumeAfter", resumeAfter);
        }

        return Doc.of("aggregate", COLL, "pipeline", List.of(Doc.of("$changeStream", changeStream)),
                "cursor", Doc.of(), "$db", DB, "$readPreference", Doc.of("mode", "secondaryPreferred"));
    }

    private static Map<String, Object> token(long sequence) {
        return Doc.of("_data", String.format(Locale.ROOT, "%016x", sequence));
    }

    private static int codeOf(Map<String, Object> reply) {
        return reply.get("code") instanceof Number n ? n.intValue() : -1;
    }

    @SuppressWarnings("unchecked")
    private static long cursorIdOf(Map<String, Object> reply) {
        Map<String, Object> cursor = (Map<String, Object>) reply.get("cursor");
        assertThat(cursor).as("a served registration carries a cursor: " + reply).isNotNull();
        return ((Number) cursor.get("id")).longValue();
    }

    private List<ILoggingEvent> at(Level level) {
        return appender.list.stream().filter(e -> e.getLevel() == level).collect(Collectors.toList());
    }

    private String rendered(List<ILoggingEvent> events) {
        return events.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.joining(" | "));
    }

    /** Parks a getMore on the cursor: nothing is buffered, so the handler waits instead of answering. */
    private void parkGetMore(long cursorId) {
        OpMsg getMore = new OpMsg();
        getMore.setMessageId(msgId.incrementAndGet());
        getMore.setFirstDoc(Doc.of("getMore", cursorId, "collection", COLL, "maxTimeMS", 5000, "$db", DB));
        ch.writeInbound(getMore);
        ch.runPendingTasks();
        assertThat((OpMsg) ch.readOutbound()).as("precondition: the getMore must be parked").isNull();
    }

    private OpMsg awaitAnswer() throws InterruptedException {
        OpMsg answer = null;
        long deadline = System.currentTimeMillis() + 5000;

        while (answer == null && System.currentTimeMillis() < deadline) {
            ch.runPendingTasks();
            answer = ch.readOutbound();

            if (answer == null) {
                Thread.sleep(20);
            }
        }

        return answer;
    }

    /**
     * The RECOVERING gate (#356/#371) has to cover change stream registrations, and its answer has
     * to be one the client treats as transient: 13436 NotPrimaryOrSecondary is what a real
     * RECOVERING member answers, and the same registration is served once the sync is done.
     */
    @Test
    public void registrationIsRefusedWhileTheNodeIsSyncing() throws Exception {
        drv.insert(DB, COLL, List.of(Doc.of("_id", 1)), null, true);
        long resumeFrom = drv.getChangeStreamSequence();

        syncing.set(true);
        Map<String, Object> refused = sendCommand(changeStreamRegistration(token(resumeFrom)));

        assertThat(refused.get("ok")).as("full reply was: " + refused).isEqualTo(0.0);
        assertThat(codeOf(refused))
                .as("a syncing node answers like a RECOVERING member: " + refused)
                .isEqualTo(13436);
        assertThat(cursorManager.getStats().get("watchCursors"))
                .as("nothing may be registered on a node that cannot serve it")
                .isEqualTo(0);
        assertThat(at(Level.ERROR))
                .as("an expected, client-handled refusal is not an error: " + rendered(at(Level.ERROR)))
                .isEmpty();

        // Transient: the moment the sync is complete, the very same registration is served.
        syncing.set(false);
        Map<String, Object> served = sendCommand(changeStreamRegistration(token(resumeFrom)));

        assertThat(served.get("ok")).as("full reply was: " + served).isEqualTo(1.0);
        assertThat(cursorIdOf(served)).isNotZero();
    }

    /**
     * The streams registered BEFORE the wipe are the ones the field incident was about: the wipe
     * is invisible to them (suppressed events), so unless the node ends them they survive into the
     * re-synced data with a silent gap. Ending them answers a parked getMore at once with the code
     * the client keys its restart on - not after the full maxTimeMS with an empty batch that looks
     * like an idle stream.
     */
    @Test
    public void parkedStreamsAreEndedWhenTheStoreIsEmptiedForSync() throws Exception {
        drv.insert(DB, COLL, List.of(Doc.of("_id", 1)), null, true);
        long cursorId = cursorIdOf(sendCommand(changeStreamRegistration(null)));
        parkGetMore(cursorId);
        appender.list.clear();

        // What clearLocalDatabases() triggers on the node: the store is about to be emptied.
        cursorManager.failAllUnservable("ChangeStreamHistoryLost: this node is re-syncing its data - "
                + "restart the stream");

        OpMsg answer = awaitAnswer();
        assertThat(answer).as("the parked getMore must be answered the moment the node turns RECOVERING").isNotNull();
        Map<String, Object> reply = answer.getFirstDoc();

        assertThat(codeOf(reply))
                .as("the client must receive ChangeStreamHistoryLost so it discards its token: " + reply)
                .isEqualTo(286);
        assertThat(String.valueOf(reply.get("errmsg")))
                .as("the errmsg carries the marker ChangeStreamMonitor and ReplicationManager key on")
                .contains("ChangeStreamHistoryLost");
        assertThat(cursorManager.hasCursor(cursorId))
                .as("an ended stream leaves no cursor behind")
                .isFalse();
        assertThat(at(Level.ERROR))
                .as("ending streams for a sync is expected operation, not an error: " + rendered(at(Level.ERROR)))
                .isEmpty();
    }

    /**
     * Instrumentation for the open question in #380 (~40k attempts per second, origin unknown):
     * every change stream registration that reaches this node is counted, before the RECOVERING
     * gate so refused attempts count too, and the numbers are readable via serverStatus.
     */
    @Test
    public void changeStreamRegistrationsAreCounted() {
        drv.insert(DB, COLL, List.of(Doc.of("_id", 1)), null, true);
        long resumeFrom = drv.getChangeStreamSequence();

        sendCommand(changeStreamRegistration(null));
        sendCommand(changeStreamRegistration(token(resumeFrom)));
        syncing.set(true);
        assertThat(codeOf(sendCommand(changeStreamRegistration(token(resumeFrom))))).isEqualTo(13436);
        syncing.set(false);

        assertThat(cursorManager.getChangeStreamRegistrations())
                .as("every registration that arrived, served or refused")
                .isEqualTo(3);
        assertThat(cursorManager.getChangeStreamResumeRegistrations())
                .as("the ones that carried a resume token")
                .isEqualTo(2);

        Map<String, Object> status = sendCommand(Doc.of("serverStatus", 1, "$db", "admin"));
        @SuppressWarnings("unchecked")
        Map<String, Object> changeStreams = (Map<String, Object>) status.get("changeStreams");

        assertThat(changeStreams).as("serverStatus reply: " + status).isNotNull();
        assertThat(((Number) changeStreams.get("registrations")).longValue()).isEqualTo(3);
        assertThat(((Number) changeStreams.get("resumeRegistrations")).longValue()).isEqualTo(2);
        assertThat(((Number) changeStreams.get("open")).longValue())
                .as("the two served registrations are open")
                .isEqualTo(2);
    }
}
