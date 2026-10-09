package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.poppydb.messaging.MessagingOptimizer;
import de.caluga.poppydb.netty.WatchCursorManager.WatchCursorState;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * The replication-watch marker (spec: replication flow control, step 1): a PoppyDB secondary tags
 * the aggregate that establishes its replication watch with two top-level fields,
 * {@code poppyReplicationWatch: true} and {@code poppyMember: "<host:port>"}. The primary's
 * {@code MongoCommandHandler#processChangeStream} reads both at registration and stores them on
 * the {@code WatchCursorState}, and {@code WatchCursorManager} keeps the replication cursors in a
 * dedicated map so a later flow-control gate can brake writers without scanning every cursor.
 *
 * <p>Until this step a replication cursor was only recognizable by the {@code poppyResumeSequence}
 * marker in its resumeAfter token - and that token exists only from the first resume, not when a
 * fresh secondary registers. The new top-level marker is present on every registration.
 */
public class ReplicationWatchMarkerTest {

    private static final String DB = "replmarker";
    private static final String COLL = "data";
    private static final String MEMBER = "serv-msg2:27017";

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

    private static Map<String, Object> changeStreamRegistration(Map<String, Object> resumeAfter,
                                                                boolean replicationMarker) {
        Doc changeStream = Doc.of();

        if (resumeAfter != null) {
            changeStream.put("resumeAfter", resumeAfter);
        }

        Doc cmd = Doc.of("aggregate", COLL, "pipeline", List.of(Doc.of("$changeStream", changeStream)),
                "cursor", Doc.of(), "$db", DB, "$readPreference", Doc.of("mode", "secondaryPreferred"));

        if (replicationMarker) {
            // The replication-watch marker: top-level fields on the aggregate, as the
            // ReplicationManager sends them (plain client change streams never carry them).
            cmd.put("poppyReplicationWatch", true);
            cmd.put("poppyMember", MEMBER);
        }

        return cmd;
    }

    private static long servedCursorId(Map<String, Object> reply) {
        assertThat(reply.get("ok")).as("full reply was: " + reply).isEqualTo(1.0);
        Map<String, Object> cursor = (Map<String, Object>) reply.get("cursor");
        assertThat(cursor).as("the registration must be served a cursor: " + reply).isNotNull();
        return ((Number) cursor.get("id")).longValue();
    }

    /**
     * An aggregate carrying the marker must register as a replication cursor and carry the
     * member address the secondary reported - the signal the flow-control gate keys on.
     */
    @Test
    public void aggregateWithMarkerRegistersAsReplicationWithMemberAddress() {
        Map<String, Object> reply = sendCommand(changeStreamRegistration(null, true));
        long cursorId = servedCursorId(reply);

        assertThat(cursorManager.isReplicationCursor(cursorId))
                .as("a cursor registered with the marker must be tracked as a replication cursor")
                .isTrue();

        WatchCursorState state = cursorManager.replicationCursorStates().get(cursorId);
        assertThat(state).as("the replication snapshot must contain the cursor").isNotNull();
        assertThat(state.replication).isTrue();
        assertThat(state.memberAddress).isEqualTo(MEMBER);
    }

    /**
     * A plain client change stream (messaging, application watches) must NOT be tracked as
     * replication - only the marker counts, never the mere fact of being a change stream.
     */
    @Test
    public void aggregateWithoutMarkerDoesNotRegisterAsReplication() {
        Map<String, Object> reply = sendCommand(changeStreamRegistration(null, false));
        long cursorId = servedCursorId(reply);

        assertThat(cursorManager.isReplicationCursor(cursorId))
                .as("a plain change stream must not be tracked as a replication cursor").isFalse();
        assertThat(cursorManager.replicationCursorStates())
                .as("the replication snapshot must stay empty for client streams").isEmpty();
        assertThat(cursorManager.maxReplicationQueuedBytes())
                .as("no replication cursors means no backlog to report").isEmpty();
    }

    /**
     * A replication RESUME also carries the marker on its aggregate - but the poppyResumeSequence
     * token in resumeAfter alone must not count as replication: before this step that token was
     * the only replication tell, and it exists only from the first resume on. A client change
     * stream that happens to use poppyResumeSequence stays a client stream.
     */
    @Test
    public void resumeAfterWithPoppyResumeSequenceButNoMarkerIsNotReplication() throws Exception {
        long sequence = drv.getChangeStreamSequence() + 1;
        drv.store(DB, COLL, List.of(Doc.of("_id", 1, "v", "x")), null);
        Map<String, Object> reply = sendCommand(changeStreamRegistration(
                Doc.of("_data", String.format(java.util.Locale.ROOT, "%016x", sequence),
                        "poppyResumeSequence", sequence), false));
        long cursorId = servedCursorId(reply);

        assertThat(cursorManager.isReplicationCursor(cursorId))
                .as("the poppyResumeSequence token alone must not mark a cursor as replication")
                .isFalse();
        assertThat(cursorManager.replicationCursorStates()).isEmpty();
    }

    /**
     * A killed replication cursor (client disconnect, budget kill, killCursors) must leave the
     * replication map - otherwise a dead secondary would hold the flow-control gate engaged
     * forever, exactly the state the spec's remove-on-drop rule exists to prevent.
     */
    @Test
    public void removingTheCursorDropsItFromTheReplicationMap() {
        long cursorId = servedCursorId(sendCommand(changeStreamRegistration(null, true)));
        assertThat(cursorManager.isReplicationCursor(cursorId)).isTrue();

        sendCommand(Doc.of("killCursors", COLL, "cursors", List.of(cursorId), "$db", DB));

        assertThat(cursorManager.isReplicationCursor(cursorId))
                .as("a killed cursor must leave the replication map").isFalse();
        assertThat(cursorManager.replicationCursorStates()).isEmpty();
    }

    /**
     * The max-over-replication-cursors helper must return the largest queued backlog and the
     * member holding it, and must NOT see client cursors - a single client change stream with a
     * larger backlog than every secondary must not masquerade as "the slowest member".
     */
    @Test
    public void maxReplicationQueuedBytesCoversOnlyReplicationCursors() throws Exception {
        cursorManager.setCursorQueueByteBudget(64 * 1024 * 1024);

        WatchCommand replA = new WatchCommand(drv).setDb(DB).setColl("a")
                .setPoppyReplicationWatch(true).setPoppyMember("member-a");
        long replAId = cursorManager.createWatchCursor(drv, replA, true, "member-a");

        WatchCommand replB = new WatchCommand(drv).setDb(DB).setColl("b")
                .setPoppyReplicationWatch(true).setPoppyMember("member-b");
        long replBId = cursorManager.createWatchCursor(drv, replB, true, "member-b");

        WatchCommand client = new WatchCommand(drv).setDb(DB).setColl("c");
        long clientId = cursorManager.createWatchCursor(drv, client);

        for (int i = 0; i < 3; i++) {
            drv.store(DB, "a", List.of(Doc.of("_id", i, "payload", "x".repeat(3072))), null);
        }
        for (int i = 0; i < 3; i++) {
            drv.store(DB, "b", List.of(Doc.of("_id", i, "payload", "x".repeat(512))), null);
        }
        for (int i = 0; i < 5; i++) {
            // The client cursor accumulates MORE than either secondary: if the helper saw it, the
            // max would be its backlog, not the secondary's.
            drv.store(DB, "c", List.of(Doc.of("_id", i, "payload", "x".repeat(4096))), null);
        }

        awaitCondition(() -> cursorsBuffered(replAId, 3) && cursorsBuffered(replBId, 3) && cursorsBuffered(clientId, 5));

        long replABytes = cursorManager.queuedByteCount(replAId);
        long replBBytes = cursorManager.queuedByteCount(replBId);
        long clientBytes = cursorManager.queuedByteCount(clientId);

        assertThat(clientBytes).as("precondition: the client backlog must exceed both secondaries")
                .isGreaterThan(replABytes).isGreaterThan(replBBytes);
        assertThat(replABytes).as("precondition: member-a must be the slower secondary")
                .isGreaterThan(replBBytes);

        var backlog = cursorManager.maxReplicationQueuedBytes();
        assertThat(backlog).isPresent();
        assertThat(backlog.get().queuedBytes()).isEqualTo(replABytes);
        assertThat(backlog.get().memberAddress()).isEqualTo("member-a");
    }

    private boolean cursorsBuffered(long cursorId, int events) {
        return cursorManager.bufferedEventCount(cursorId) >= events;
    }

    private void awaitCondition(java.util.function.BooleanSupplier cond) throws Exception {
        for (int i = 0; i < 250 && !cond.getAsBoolean(); i++) {
            Thread.sleep(20);
        }
    }
}