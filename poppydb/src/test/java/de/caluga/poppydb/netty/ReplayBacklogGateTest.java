package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.poppydb.messaging.MessagingOptimizer;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * A secondary's replication resume (the aggregate whose change-stream {@code resumeAfter} carries
 * {@code poppyResumeSequence}) that is admitted for replay but whose replay backlog exceeds the
 * watch cursor's byte budget is doomed: the replay fills the cursor queue past the budget, the
 * cursor is killed mid-replay ("slow/absent consumer"), and the consumer retries the same token -
 * a livelock that costs a minutes-long sand-hour per attempt while a full re-sync is a seconds-long
 * snapshot. The {@code poppyResumeSequence} gate in {@code MongoCommandHandler#processChangeStream}
 * therefore answers ChangeStreamHistoryLost (286) up front when the backlog exceeds the budget, so
 * the secondary falls back to a full re-sync instead of admitting the oversized replay.
 */
public class ReplayBacklogGateTest {

    private static final String DB = "replay_gate_db";
    private static final String COLL = "data";

    private InMemoryDriver drv;
    private WatchCursorManager cursorManager;
    private EmbeddedChannel ch;
    private final AtomicInteger msgId = new AtomicInteger(1);
    private final AtomicBoolean syncing = new AtomicBoolean(false);

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
    }

    @AfterEach
    public void tearDown() {
        ch.finishAndReleaseAll();
        cursorManager.shutdown();
        drv.close();
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

    private static Map<String, Object> changeStreamRegistration(Map<String, Object> resumeAfter) {
        Doc changeStream = Doc.of();

        if (resumeAfter != null) {
            changeStream.put("resumeAfter", resumeAfter);
        }

        return Doc.of("aggregate", COLL, "pipeline", List.of(Doc.of("$changeStream", changeStream)),
                "cursor", Doc.of(), "$db", DB, "$readPreference", Doc.of("mode", "secondaryPreferred"));
    }

    private static Map<String, Object> token(long sequence) {
        // The replication resume marker: PoppyDB secondaries tag their resumeAfter with
        // poppyResumeSequence (their lastAppliedSequence) in addition to the _data token - the
        // gate in processChangeStream only fires on this marker. Plain client change-stream resumes
        // keep their previous behaviour.
        return Doc.of("_data", String.format(Locale.ROOT, "%016x", sequence),
                "poppyResumeSequence", sequence);
    }

    private static int codeOf(Map<String, Object> reply) {
        return reply.get("code") instanceof Number n ? n.intValue() : -1;
    }

    /** A resumeAfter as SingleMongoConnection re-sends it on an in-place restart: the _data token only. */
    private static Map<String, Object> dataOnlyToken(long sequence) {
        return Doc.of("_data", String.format(Locale.ROOT, "%016x", sequence));
    }

    /** The registration a replication watch sends: marked with poppyReplicationWatch and its member. */
    private static Map<String, Object> markedRegistration(Map<String, Object> resumeAfter) {
        Map<String, Object> cmd = changeStreamRegistration(resumeAfter);
        cmd.put("poppyReplicationWatch", true);
        cmd.put("poppyMember", "secondary-under-test:17018");
        return cmd;
    }

    /**
     * The in-place restart bypass: after a cursor kill, SingleMongoConnection re-sends the
     * aggregate with the last event's _data token and no poppyResumeSequence. For a watch that
     * carries the replication marker the gate must read the sequence from _data and answer 286
     * for an oversized backlog - otherwise every restart is admitted, killed, restarted (the
     * testrunner kill storm).
     */
    @Test
    public void markedWatchResumingByDataTokenOnlyIsGatedLikeAMarkedSequence() throws Exception {
        long resumeFrom = drv.getChangeStreamSequence();
        for (int i = 0; i < 20; i++) {
            drv.store(DB, COLL, List.of(Doc.of("_id", i, "payload", "x".repeat(1024))), null);
        }
        cursorManager.setCursorQueueByteBudget(2048);

        Map<String, Object> reply = sendCommand(markedRegistration(dataOnlyToken(resumeFrom)));

        assertThat(reply.get("ok")).as("full reply was: " + reply).isEqualTo(0.0);
        assertThat(codeOf(reply))
                .as("a marked watch resuming by _data over an oversized backlog must get 286: " + reply)
                .isEqualTo(286);
        assertThat(reply.get("codeName")).isEqualTo("ChangeStreamHistoryLost");
    }

    /**
     * The production bus livelock of 21.10.2026: a client change stream (no marker) resuming by
     * _data over a backlog larger than the cursor's byte budget. Admitting it meant: replay fills
     * the queue past the budget, the cursor is killed within a second, the driver re-sends the
     * aggregate with the same token - once per second for twenty minutes, the consumer never saw
     * another event. The gate now answers 286 to clients too, and the errmsg carries the marker
     * the Morphium client classifies by (ChangeStreamMonitor discards its token and starts fresh).
     */
    @Test
    public void unmarkedClientResumeOverAnOversizedByteBacklogIsAnsweredHistoryLost() throws Exception {
        long resumeFrom = drv.getChangeStreamSequence();
        for (int i = 0; i < 20; i++) {
            drv.store(DB, COLL, List.of(Doc.of("_id", i, "payload", "x".repeat(1024))), null);
        }
        cursorManager.setCursorQueueByteBudget(2048);

        Map<String, Object> reply = sendCommand(changeStreamRegistration(dataOnlyToken(resumeFrom)));

        assertThat(reply.get("ok")).as("full reply was: " + reply).isEqualTo(0.0);
        assertThat(codeOf(reply)).as("a doomed client replay must get 286: " + reply).isEqualTo(286);
        assertThat(reply.get("codeName")).isEqualTo("ChangeStreamHistoryLost");
        assertThat(String.valueOf(reply.get("errmsg")))
                .as("the client classifies by message text, so the errmsg has to carry the marker")
                .startsWith("ChangeStreamHistoryLost");
    }

    /**
     * The count cap is the second bound: with small events the byte budget is nowhere near, but a
     * replay of more events than the queue holds is killed just the same. On the production bus
     * 10,000 small bus events were ~37 MB against a 256 MB budget - the byte-only gate let every
     * one of those resumes through.
     */
    @Test
    public void clientResumeOverMoreEventsThanTheQueueHoldsIsAnsweredHistoryLost() throws Exception {
        long resumeFrom = drv.getChangeStreamSequence();
        for (int i = 0; i < 20; i++) {
            drv.store(DB, COLL, List.of(Doc.of("_id", i, "v", i)), null);
        }
        cursorManager.setCursorQueueByteBudget(0);          // byte bound off
        cursorManager.setMaxCursorQueueSizeForTest(5);      // 20 events do not fit

        Map<String, Object> reply = sendCommand(changeStreamRegistration(dataOnlyToken(resumeFrom)));

        assertThat(reply.get("ok")).as("full reply was: " + reply).isEqualTo(0.0);
        assertThat(codeOf(reply)).as("more replay events than queue capacity must 286: " + reply).isEqualTo(286);
        assertThat(String.valueOf(reply.get("errmsg"))).contains("cursor-queue-events");
    }

    /** A backlog that fits both bounds is served exactly as before - the gate is size-based, not an always-reject. */
    @Test
    public void clientResumeOverAFittingBacklogIsServed() throws Exception {
        long resumeFrom = drv.getChangeStreamSequence();
        for (int i = 0; i < 3; i++) {
            drv.store(DB, COLL, List.of(Doc.of("_id", i, "v", i)), null);
        }
        cursorManager.setCursorQueueByteBudget(64 * 1024);
        cursorManager.setMaxCursorQueueSizeForTest(5);

        Map<String, Object> reply = sendCommand(changeStreamRegistration(dataOnlyToken(resumeFrom)));

        assertThat(reply.get("ok")).as("full reply was: " + reply).isEqualTo(1.0);
        assertThat(reply).containsKey("cursor");
    }

    /**
     * A client stream is judged by ITS namespace only: a consumer of a quiet collection resuming
     * while another collection in the same database was busy has next to nothing to replay and
     * must be served - sending it away would silently drop the few events it could have replayed.
     * The replication watch stays namespace-blind, it receives everything.
     */
    @Test
    public void clientResumeOnAQuietCollectionIsNotGatedByABusyNeighbour() throws Exception {
        long resumeFrom = drv.getChangeStreamSequence();
        drv.store(DB, COLL, List.of(Doc.of("_id", "mine", "v", 1)), null);
        for (int i = 0; i < 20; i++) {
            drv.store(DB, "busy_neighbour", List.of(Doc.of("_id", i, "payload", "x".repeat(1024))), null);
        }
        cursorManager.setCursorQueueByteBudget(2048);
        cursorManager.setMaxCursorQueueSizeForTest(5);

        Map<String, Object> reply = sendCommand(changeStreamRegistration(dataOnlyToken(resumeFrom)));

        assertThat(reply.get("ok")).as("the quiet collection's backlog is one event: " + reply).isEqualTo(1.0);
        assertThat(reply).containsKey("cursor");

        Map<String, Object> replication = sendCommand(markedRegistration(dataOnlyToken(resumeFrom)));
        assertThat(codeOf(replication))
                .as("the replication watch receives every namespace and is gated on the total: " + replication)
                .isEqualTo(286);
    }

    /**
     * A token that is not a PoppyDB sequence (a foreign primary's, a malformed one) stays ungated
     * here: the existing asynchronous path (#320/#383 - failHistoryLost at replay, 286 on the first
     * getMore, driver backoff) handles it, and this gate must not change that choreography.
     */
    @Test
    public void clientResumeWithAForeignTokenStaysOnTheAsynchronousPath() throws Exception {
        for (int i = 0; i < 20; i++) {
            drv.store(DB, COLL, List.of(Doc.of("_id", i, "payload", "x".repeat(1024))), null);
        }
        cursorManager.setCursorQueueByteBudget(2048);

        Map<String, Object> reply = sendCommand(changeStreamRegistration(Doc.of("_data", "826F1A2B3C0000000129")));

        assertThat(reply.get("ok")).as("a foreign token is not this gate's business: " + reply).isEqualTo(1.0);
        assertThat(reply).containsKey("cursor");
    }

    /** A marked watch that resumes at the newest sequence has no backlog and is served as before. */
    @Test
    public void markedWatchResumingByDataTokenAtNewestIsServed() throws Exception {
        for (int i = 0; i < 3; i++) {
            drv.store(DB, COLL, List.of(Doc.of("_id", i, "v", i)), null);
        }
        cursorManager.setCursorQueueByteBudget(2048);
        long newest = drv.getChangeStreamSequence();

        Map<String, Object> reply = sendCommand(markedRegistration(dataOnlyToken(newest)));

        assertThat(reply.get("ok")).as("full reply was: " + reply).isEqualTo(1.0);
        assertThat(reply).containsKey("cursor");
    }

    /**
     * The gate turns a replay admission into 286 when the backlog to replay exceeds the cursor
     * budget, instead of admitting the resume and letting the replay blow the budget mid-way and
     * loop. ~20 KB of backlog against a 2 KB budget must answer ChangeStreamHistoryLost, not serve
     * a cursor.
     */
    @Test
    public void oversizedBacklogIsAnsweredHistoryLostInsteadOfAdmitted() throws Exception {
        long resumeFrom = drv.getChangeStreamSequence();
        // ~20 KB of buffered events behind the resume point.
        for (int i = 0; i < 20; i++) {
            drv.store(DB, COLL, List.of(Doc.of("_id", i, "payload", "x".repeat(1024))), null);
        }
        cursorManager.setCursorQueueByteBudget(2048);

        Map<String, Object> reply = sendCommand(changeStreamRegistration(token(resumeFrom)));

        assertThat(reply.get("ok")).as("full reply was: " + reply).isEqualTo(0.0);
        assertThat(codeOf(reply))
                .as("an oversized replay backlog must answer ChangeStreamHistoryLost: " + reply)
                .isEqualTo(286);
        assertThat(reply.get("codeName")).isEqualTo("ChangeStreamHistoryLost");
    }

    /**
     * The gate is size-based, not an always-reject: resuming exactly at the newest sequence has an
     * empty backlog, so the identical registration path serves a live cursor. This pins that the
     * new check did not turn every replication resume into a re-sync.
     */
    @Test
    public void resumeAtNewestWithNoBacklogIsStillServed() throws Exception {
        for (int i = 0; i < 3; i++) {
            drv.store(DB, COLL, List.of(Doc.of("_id", i, "payload", "x".repeat(1024))), null);
        }
        long newest = drv.getChangeStreamSequence();
        cursorManager.setCursorQueueByteBudget(2048);

        Map<String, Object> reply = sendCommand(changeStreamRegistration(token(newest)));

        assertThat(reply.get("ok")).as("full reply was: " + reply).isEqualTo(1.0);
        Map<String, Object> cursor = (Map<String, Object>) reply.get("cursor");
        assertThat(cursor).as("a resume at the newest sequence is served a cursor").isNotNull();
    }

    /**
     * Budget disabled (0) means no upper bound on the backlog: the gate must not fire and the
     * resume is admitted exactly as before this option existed.
     */
    @Test
    public void resumeIsServedWhenTheByteBudgetIsDisabled() throws Exception {
        long resumeFrom = drv.getChangeStreamSequence();
        for (int i = 0; i < 20; i++) {
            drv.store(DB, COLL, List.of(Doc.of("_id", i, "payload", "x".repeat(1024))), null);
        }
        cursorManager.setCursorQueueByteBudget(0);

        Map<String, Object> reply = sendCommand(changeStreamRegistration(token(resumeFrom)));

        assertThat(reply.get("ok")).as("full reply was: " + reply).isEqualTo(1.0);
    }

    /**
     * When the global cursor budget is the binding constraint (per-cursor generous, fleet-wide
     * tiny), the gate must still answer 286: admitting a replay that fits this cursor but pushes
     * the fleet over the global cap would be killed mid-replay by the global check - the same
     * livelock, one layer up. This pins that the gate and the global budget are not incoherent.
     */
    @Test
    public void oversizedBacklogIsAnsweredHistoryLostWhenTheGlobalBudgetIsBinding() throws Exception {
        long resumeFrom = drv.getChangeStreamSequence();
        for (int i = 0; i < 20; i++) {
            drv.store(DB, COLL, List.of(Doc.of("_id", i, "payload", "x".repeat(1024))), null);
        }
        cursorManager.setCursorQueueByteBudget(64 * 1024 * 1024); // per-cursor: generous
        cursorManager.setGlobalCursorByteBudget(2048);            // fleet-wide: binding

        Map<String, Object> reply = sendCommand(changeStreamRegistration(token(resumeFrom)));

        assertThat(reply.get("ok")).as("full reply was: " + reply).isEqualTo(0.0);
        assertThat(codeOf(reply))
            .as("a backlog over the global headroom must 286, not be admitted then killed: " + reply)
            .isEqualTo(286);
    }

    /**
     * A global budget that is on but has ample headroom must not turn an otherwise admissible
     * resume into a re-sync - the effective bound is the tighter of the two, not "global on = reject".
     */
    @Test
    public void resumeIsServedWhenTheGlobalBudgetHasHeadroom() throws Exception {
        long resumeFrom = drv.getChangeStreamSequence();
        for (int i = 0; i < 3; i++) {
            drv.store(DB, COLL, List.of(Doc.of("_id", i, "payload", "x".repeat(1024))), null);
        }
        cursorManager.setCursorQueueByteBudget(0);          // no per-cursor bound
        cursorManager.setGlobalCursorByteBudget(64 * 1024 * 1024); // global on, plenty of room

        Map<String, Object> reply = sendCommand(changeStreamRegistration(token(resumeFrom)));

        assertThat(reply.get("ok")).as("full reply was: " + reply).isEqualTo(1.0);
    }
}