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