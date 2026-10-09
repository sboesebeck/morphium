package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.poppydb.ReplicationCoordinator;
import de.caluga.poppydb.messaging.MessagingOptimizer;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * Replication flow control, write parking (spec 3.4/3.5): with the gate engaged, a gated
 * write (insert/update/delete/findAndModify/bulkWrite) is held back instead of executed -
 * the socket stops reading (autoRead false), so the client driver's send blocks in TCP
 * backpressure. Commands behind a parked write wait in wire order; when the gate opens or a
 * write's max-wait elapses, the FIFO drains through the normal path on the channel's event
 * loop; a role change answers every parked write with NotWritablePrimary (10107) without
 * executing it; a dropped connection discards its parked queue.
 */
public class FlowControlParkingTest {

    private static final String DB = "fcpark";
    private static final String COLL = "data";
    private static final String MEMBER = "member-a:27017";

    private static final long BUDGET = 6 * 1024; // 3 events of ~1KB: past 50%, below the kill

    private InMemoryDriver drv;
    private WatchCursorManager cursorManager;
    private EmbeddedChannel ch;
    private final AtomicInteger msgId = new AtomicInteger(1);
    private final AtomicBoolean syncing = new AtomicBoolean(false);
    private final AtomicLong gateClock = new AtomicLong(0);
    private long replicationCursor = -1;

    @BeforeEach
    public void setUp() {
        drv = new InMemoryDriver();
        drv.connect();
        drv.setServerMode(true);
        cursorManager = new WatchCursorManager();
        cursorManager.setCursorQueueByteBudget(BUDGET);
        MessagingOptimizer optimizer = new MessagingOptimizer(drv);
        optimizer.setWatchCursorManager(cursorManager);
        // A current primary WITH a replication coordinator: the same guard postWrite uses.
        ch = new EmbeddedChannel(new MongoCommandHandler(drv, cursorManager, new FindCursorRegistry(),
                optimizer, msgId, "0.0.0.0", 27017, "my-rs", List.of("localhost:27017"), true,
                "localhost:27017", 0, () -> new ReplicationCoordinator(3), null, syncing::get));
    }

    @AfterEach
    public void tearDown() {
        ch.finishAndReleaseAll();
        cursorManager.shutdown();
        drv.close();
    }

    // -- helpers --

    /** Sends one command and returns its answer, or null when the command was parked. */
    private Map<String, Object> sendCommand(Map<String, Object> cmd) {
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(cmd);
        ch.writeInbound(msg);
        ch.runPendingTasks();
        OpMsg reply = ch.readOutbound();
        return reply == null ? null : reply.getFirstDoc();
    }

    private Map<String, Object> insertDoc(int id) {
        return Doc.of("insert", COLL, "documents", List.of(Doc.of("_id", id, "v", "x")),
                "ordered", true, "$db", DB);
    }

    private Map<String, Object> findDoc() {
        return Doc.of("find", COLL, "filter", Doc.of(), "$db", DB);
    }

    /**
     * Engages the gate the way the wiring step feeds it: a replication cursor that has queued
     * more than high-water bytes of undelivered events. Returns the cursor id (used by
     * {@link #releaseGate(long)}).
     */
    private long engageGate() throws Exception {
        WatchCommand wcmd = new WatchCommand(drv).setDb(DB).setColl(COLL).setMaxTimeMS(30000);
        replicationCursor = cursorManager.createWatchCursor(drv, wcmd, true, MEMBER);
        for (int i = 0; i < 3; i++) {
            drv.store(DB, COLL, List.of(Doc.of("_id", "fill-" + i, "payload", "x".repeat(1024))), null);
        }
        awaitCondition(() -> cursorManager.bufferedEventCount(replicationCursor) >= 3);
        awaitCondition(cursorManager.replicationFlowControl()::isEngaged);
        return replicationCursor;
    }

    /** Drains the replication cursor below low water: the gate opens and releases parked writes. */
    private void releaseGate(long cursorId) throws Exception {
        while (cursorManager.bufferedEventCount(cursorId) > 0) {
            cursorManager.getMore(cursorId, 100).get(5, TimeUnit.SECONDS);
        }
        // The release listener ran on THIS thread and scheduled the drains on the channel loop.
        ch.runPendingTasks();
    }

    private void awaitCondition(java.util.function.BooleanSupplier cond) throws Exception {
        for (int i = 0; i < 250 && !cond.getAsBoolean(); i++) {
            Thread.sleep(20);
        }
    }

    /** Pumps the event loop until an outbound reply appears (max ~1 s). */
    private Map<String, Object> awaitReply() throws Exception {
        for (int i = 0; i < 100; i++) {
            OpMsg reply = ch.readOutbound();
            if (reply != null) {
                return reply.getFirstDoc();
            }
            ch.runPendingTasks();
            Thread.sleep(10);
        }
        return null;
    }

    /** How many documents with this _id exist - the fill events of the gate use other ids. */
    private long countWithId(int id) {
        return drv.count(DB, COLL, Doc.of("_id", id), null, null);
    }

    // -- tests --

    /**
     * The gate engaged: a gated write is parked, no reply is sent, and the channel stops
     * reading - the client driver blocks in send, which is the actual backpressure.
     */
    @Test
    public void engagedGateParksAnInsertAndDisablesAutoRead() throws Exception {
        engageGate();

        Map<String, Object> reply = sendCommand(insertDoc(1));

        assertThat(reply).as("a gated write must be parked, not answered").isNull();
        assertThat(ch.config().isAutoRead())
                .as("parking must turn off auto-read so the socket buffers the client")
                .isFalse();
        assertThat(countWithId(1)).as("a parked write must not have executed").isZero();
    }

    /**
     * Wire ordering: a read arriving behind a parked write waits with it (it must observe the
     * write). On release both execute in order through the normal path and auto-read returns.
     */
    @Test
    public void commandsBehindAParkedWriteWaitAndRunInOrderOnRelease() throws Exception {
        long cursorId = engageGate();

        assertThat(sendCommand(insertDoc(1))).isNull();
        assertThat(sendCommand(findDoc())).as("a find behind a parked write must wait too").isNull();

        releaseGate(cursorId);

        Map<String, Object> insertReply = awaitReply();
        assertThat(insertReply).as("the parked insert must be released first").isNotNull();
        assertThat(insertReply.get("ok")).isEqualTo(1.0);
        Map<String, Object> findReply = awaitReply();
        assertThat(findReply).as("the find behind it must run after it").isNotNull();
        assertThat(findReply.get("ok")).isEqualTo(1.0);
        assertThat(ch.config().isAutoRead())
                .as("an empty FIFO must turn auto-read back on").isTrue();
        assertThat(countWithId(1)).as("the released insert must have executed").isEqualTo(1);
    }

    /**
     * The fast path: with the gate open a write executes exactly as before this feature, and
     * auto-read stays on.
     */
    @Test
    public void aWriteWithAnOpenGateExecutesImmediately() {
        Map<String, Object> reply = sendCommand(insertDoc(1));

        assertThat(reply).as("an ungated write must be answered immediately").isNotNull();
        assertThat(reply.get("ok")).isEqualTo(1.0);
        assertThat(ch.config().isAutoRead()).isTrue();
    }

    /**
     * max-wait: with an injected gate clock, the 100 ms expiry tick releases exactly the
     * write whose first-park time is past maxWait - it executes even though the gate is still
     * engaged - while the younger write stays parked.
     */
    @Test
    public void maxWaitReleasesExactlyTheExpiredWrite() throws Exception {
        cursorManager.setReplicationFlowControlForTest(gateClock::get);
        engageGate();

        assertThat(sendCommand(insertDoc(1))).isNull(); // parked at gate-clock 0
        gateClock.set(1000);
        assertThat(sendCommand(insertDoc(2))).isNull(); // parked at gate-clock 1000

        // 10s + 1 past the first park: only the first write's max-wait (default 10s) elapsed.
        gateClock.set(10_001);

        Map<String, Object> expiredReply = awaitReply();
        assertThat(expiredReply).as("the expired write must be released by the max-wait tick").isNotNull();
        assertThat(expiredReply.get("ok")).isEqualTo(1.0);

        // give a spurious second release a moment to show up
        for (int i = 0; i < 20; i++) {
            OpMsg unexpected = ch.readOutbound();
            assertThat(unexpected).as("the next write must stay parked").isNull();
            ch.runPendingTasks();
            Thread.sleep(10);
        }
        assertThat(cursorManager.replicationFlowControl().isEngaged())
                .as("the gate must still be engaged").isTrue();
        assertThat(ch.config().isAutoRead()).isFalse();
        assertThat(countWithId(1)).as("the expired write must have executed").isEqualTo(1);
        assertThat(countWithId(2)).as("the younger write must stay parked").isZero();
    }

    /**
     * Two commands on one connection: the first expiring must not stop the max-wait machinery
     * for the second. The parked counter counts EVERY queued command (not just the first one of
     * a connection), so it stays positive after the first released and the ticker keeps running;
     * the second write is then released at ITS OWN entry time plus max-wait, not at a restarted
     * one, and the counter returns to zero once everything drained.
     */
    @Test
    public void aSecondParkedWriteStillExpiresAfterTheFirstWasReleasedByTimeout() throws Exception {
        cursorManager.setReplicationFlowControlForTest(gateClock::get);
        engageGate();

        assertThat(sendCommand(insertDoc(1))).isNull(); // parked at gate-clock 0
        gateClock.set(1000);
        assertThat(sendCommand(insertDoc(2))).isNull(); // parked at gate-clock 1000

        assertThat(cursorManager.flowControlParkedWriteCount())
                .as("every queued command must be counted exactly once")
                .isEqualTo(2);

        // The first write reaches its max-wait (0 + 10s); the second (1000 + 10s) must not.
        gateClock.set(10_001);
        Map<String, Object> first = awaitReply();
        assertThat(first).as("the first write must expire").isNotNull();
        assertThat(countWithId(1)).isEqualTo(1);

        assertThat(cursorManager.flowControlParkedWriteCount())
                .as("the second write is still queued and must keep the ticker alive")
                .isEqualTo(1);
        assertThat(cursorManager.isFlowControlExpiryScheduledForTest())
                .as("the ticker must stay scheduled while a write is still parked")
                .isTrue();

        // Several ticks with the clock still at 10_001: the second write is not due yet.
        for (int i = 0; i < 30; i++) {
            OpMsg early = ch.readOutbound();
            assertThat(early).as("the second write's own max-wait has not elapsed yet").isNull();
            ch.runPendingTasks();
            Thread.sleep(10);
        }

        // 1000 + 10s: its own schedule, not a restarted one (which would be 10_001 + 10s).
        gateClock.set(11_001);
        Map<String, Object> second = awaitReply();
        assertThat(second).as("the second write must expire on its own schedule").isNotNull();
        assertThat(countWithId(2)).isEqualTo(1);

        assertThat(cursorManager.flowControlParkedWriteCount())
                .as("the counter must return to zero once everything drained")
                .isZero();
    }

    /**
     * A dropped connection discards its parked queue without executing anything - the client
     * never got a reply, exactly as if the abort happened before sending.
     */
    @Test
    public void channelInactiveDropsParkedWritesWithoutExecuting() throws Exception {
        engageGate();
        assertThat(sendCommand(insertDoc(1))).isNull();
        assertThat(sendCommand(insertDoc(2))).isNull();

        ch.finishAndReleaseAll(); // fires channelInactive

        assertThat(countWithId(1)).as("dropped parked writes must never execute").isZero();
        assertThat(countWithId(2)).as("dropped parked writes must never execute").isZero();
        OpMsg dropped = ch.readOutbound();
        assertThat(dropped).as("dropped parked writes must never be answered").isNull();
    }

    /**
     * Role change: failParkedWritesNotPrimary answers every parked write with
     * NotWritablePrimary (10107) on its event loop, executes nothing, and restores auto-read.
     */
    @Test
    public void failParkedWritesNotPrimaryAnswers10107AndExecutesNothing() throws Exception {
        engageGate();
        assertThat(sendCommand(insertDoc(1))).isNull();
        assertThat(sendCommand(insertDoc(2))).isNull();

        cursorManager.failParkedWritesNotPrimary();
        ch.runPendingTasks();

        Map<String, Object> first = awaitReply();
        assertThat(first).isNotNull();
        assertThat(first.get("code")).as("full reply was: " + first).isEqualTo(10107);
        assertThat(first.get("codeName")).isEqualTo("NotWritablePrimary");
        Map<String, Object> second = awaitReply();
        assertThat(second).isNotNull();
        assertThat(second.get("code")).isEqualTo(10107);
        assertThat(countWithId(1)).as("a NotWritablePrimary answer must not execute the write").isZero();
        assertThat(countWithId(2)).as("a NotWritablePrimary answer must not execute the write").isZero();
        assertThat(ch.config().isAutoRead()).isTrue();
    }

    /**
     * Replication-internal commands and plain reads are NEVER parked, whatever the gate does:
     * a secondary's progress report and a client read run immediately while the gate is
     * engaged (a gated write parks under the same condition).
     */
    @Test
    public void replicationInternalCommandsAndReadsAreNeverParked() throws Exception {
        engageGate();

        Map<String, Object> progress = sendCommand(Doc.of("replSetProgress", 1,
                "secondaryAddress", "secondary-1:27017", "sequenceNumber", 42, "$db", "admin"));
        assertThat(progress).as("a secondary's progress report must never park").isNotNull();
        assertThat(progress.get("ok")).isEqualTo(1.0);

        Map<String, Object> find = sendCommand(findDoc());
        assertThat(find).as("a plain read must never park").isNotNull();
        assertThat(find.get("ok")).isEqualTo(1.0);

        // same condition parks a gated write - proves the gate was actually engaged
        assertThat(sendCommand(insertDoc(1))).isNull();
    }
}