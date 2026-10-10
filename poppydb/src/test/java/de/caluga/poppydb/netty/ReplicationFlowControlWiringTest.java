package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

/**
 * Replication flow control, wiring step (spec 3.3): the hysteresis state machine lives in
 * {@link ReplicationFlowControl}; this test pins that {@link WatchCursorManager} FEEDS it the
 * right signal - queued bytes of the REPLICATION cursors only - at the right moments (offer,
 * drain, remove). The gate then engages past high water, releases parked writes under low
 * water and on removal, and never engages for client cursors or a disabled byte budget.
 *
 * <p>All events are ~1KB, the budget is 6 KB: 3 buffered events land between 50% and 100%
 * (engaging past high water without tripping the kill), a single getMore drains them to 0
 * (far below the 25% low water mark).
 */
public class ReplicationFlowControlWiringTest {

    private static final String DB = "flowwire";
    private static final String COLL = "data";
    private static final String MEMBER = "member-a:27017";

    /** 3 events of ~1KB fill this past 50% but stay below 100% (no kill). */
    private static final long BUDGET = 6 * 1024;

    private InMemoryDriver drv;
    private WatchCursorManager cursors;

    @BeforeEach
    public void setUp() {
        drv = new InMemoryDriver();
        drv.connect();
        drv.setServerMode(true);
        cursors = new WatchCursorManager();
        cursors.setCursorQueueByteBudget(BUDGET);
    }

    @AfterEach
    public void tearDown() {
        if (cursors != null) {
            cursors.shutdown();
        }

        if (drv != null) {
            drv.close();
        }
    }

    private long replicationCursor(String member) {
        WatchCommand wcmd = new WatchCommand(drv).setDb(DB).setColl(COLL)
                .setPoppyReplicationWatch(true).setPoppyMember(member);
        return cursors.createWatchCursor(drv, wcmd, true, member);
    }

    private long clientCursor() {
        WatchCommand wcmd = new WatchCommand(drv).setDb(DB).setColl(COLL);
        return cursors.createWatchCursor(drv, wcmd);
    }

    /** ~1KB of payload per event so a handful of events crosses the 50% mark of a 6 KB budget. */
    private void insertLarge(int id) {
        drv.store(DB, COLL, List.of(Doc.of("_id", id, "payload", "x".repeat(1024))), null);
    }

    private void awaitCondition(java.util.function.BooleanSupplier cond) throws Exception {
        for (int i = 0; i < 250 && !cond.getAsBoolean(); i++) {
            Thread.sleep(20);
        }
    }

    private void fillPastHighWater(long cursorId) throws Exception {
        for (int i = 0; i < 3; i++) {
            insertLarge(i);
        }
        awaitCondition(() -> cursors.bufferedEventCount(cursorId) >= 3);
        awaitCondition(cursors.replicationFlowControl()::isEngaged);
    }

    /**
     * A replication cursor whose queued bytes pass 50 percent of the budget must engage the
     * gate, and the snapshot must name the member holding the backlog - the signal the
     * handler reads to answer "is a write allowed to run right now".
     */
    @Test
    public void aReplicationCursorPastHighWaterEngagesTheGate() throws Exception {
        long cursorId = replicationCursor(MEMBER);
        fillPastHighWater(cursorId);

        ReplicationFlowControl gate = cursors.replicationFlowControl();
        assertThat(gate.isEngaged())
                .as("a replication cursor past high water must close the gate").isTrue();
        Map<String, Object> snap = gate.statusSnapshot();
        assertThat((String) snap.get("slowestMember"))
                .as("the snapshot must name the member whose backlog closed the gate, keyed per cursor")
                .startsWith(MEMBER + "/");
        assertThat((Long) snap.get("slowestFillPercent")).isGreaterThanOrEqualTo(50L);
    }

    /**
     * A client change stream (messaging, application watches) with the identical fill must NOT
     * engage the gate: a hung client may never slow the database down. Only the replication
     * marker counts.
     */
    @Test
    public void aClientCursorWithTheSameFillDoesNotEngageTheGate() throws Exception {
        long cursorId = clientCursor();

        for (int i = 0; i < 3; i++) {
            insertLarge(i);
        }
        awaitCondition(() -> cursors.bufferedEventCount(cursorId) >= 3);

        assertThat(cursors.replicationFlowControl().isEngaged())
                .as("a client cursor must never close the flow-control gate").isFalse();
        assertThat(cursors.replicationFlowControl().statusSnapshot().get("slowestMember"))
                .isNull();
    }

    /**
     * The full cycle: engage, park two writes, drain the replication cursor below low water -
     * the gate opens on the spot and the release listener receives exactly the parked tokens,
     * in FIFO order, synchronously with the drain (no timers involved).
     */
    @Test
    public void drainingBelowLowWaterReleasesParkedWritesInFifoOrder() throws Exception {
        long cursorId = replicationCursor(MEMBER);
        fillPastHighWater(cursorId);
        ReplicationFlowControl gate = cursors.replicationFlowControl();

        List<Object> released = new ArrayList<>();
        gate.setReleaseListener(released::addAll);
        assertThat(gate.parkIfEngaged("w1"))
                .as("precondition: the engaged gate must park the first write").isTrue();
        assertThat(gate.parkIfEngaged("w2"))
                .as("precondition: the engaged gate must park the second write").isTrue();

        // One getMore drains all three buffered events: the queued bytes drop from ~55% to 0,
        // far below the 25% low water mark, and the gate must open immediately.
        assertThat(cursors.getMore(cursorId, 100).get(5, TimeUnit.SECONDS)).hasSize(3);

        assertThat(gate.isEngaged())
                .as("a drain below low water must open the gate").isFalse();
        assertThat(released)
                .as("the release listener must receive the parked writes in FIFO order")
                .containsExactly("w1", "w2");
    }

    /**
     * Removing the engaged replication cursor (kill, disconnect, retire) re-evaluates the gate:
     * a dead secondary must not keep the gate closed - the removal alone is the release trigger.
     */
    @Test
    public void removingTheEngagedReplicationCursorReleases() throws Exception {
        long cursorId = replicationCursor(MEMBER);
        fillPastHighWater(cursorId);
        ReplicationFlowControl gate = cursors.replicationFlowControl();

        List<Object> released = new ArrayList<>();
        gate.setReleaseListener(released::addAll);
        assertThat(gate.parkIfEngaged("w1")).isTrue();

        cursors.killCursor(cursorId);

        assertThat(gate.isEngaged())
                .as("a dead secondary must not hold the gate closed").isFalse();
        assertThat(released).containsExactly("w1");
    }

    /**
     * cursor-queue-budget 0 disables the byte bound and with it the flow-control reference
     * size: no budget means no percentage, and the gate must stay open however full the cursor
     * looks. Same rule as the kill path (no budget, no kill).
     */
    @Test
    public void aDisabledByteBudgetNeverEngagesTheGate() throws Exception {
        cursors.setCursorQueueByteBudget(0);
        long cursorId = replicationCursor(MEMBER);

        for (int i = 0; i < 5; i++) {
            insertLarge(i);
        }
        awaitCondition(() -> cursors.bufferedEventCount(cursorId) >= 5);

        ReplicationFlowControl gate = cursors.replicationFlowControl();
        assertThat(gate.isEngaged()).isFalse();
        assertThat(gate.statusSnapshot().get("slowestFillPercent")).isEqualTo(0L);
    }

    /**
     * A secondary can register before it knows its own address (poppyMember absent, null on
     * the state): the gate rejects null member keys, so the manager must key the cursor by
     * cursor-<id> instead - the snapshot then names that fallback, not null.
     */
    @Test
    public void aReplicationCursorWithoutMemberAddressIsKeyedByCursorId() throws Exception {
        long cursorId = replicationCursor(null);
        fillPastHighWater(cursorId);

        assertThat(cursors.replicationFlowControl().statusSnapshot().get("slowestMember"))
                .as("a replication cursor without a member address must be keyed by cursor-<id>")
                .isEqualTo("cursor-" + cursorId);
    }
}