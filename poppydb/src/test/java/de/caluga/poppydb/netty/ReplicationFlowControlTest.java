package de.caluga.poppydb.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for the section 3.3 hysteresis gate of the replication flow-control design
 * (2026-10-09). The gate is a standalone state machine with no Netty: its inputs are the
 * buffered bytes of replication cursors (per member) and its outputs are opaque write tokens
 * that park while the gate is closed and come back in FIFO order when it opens, when a token's
 * max-wait elapses, or when the node loses the primary role (reset).
 *
 * <p>All tests run against the default settings (high 50, low 25 percent of the budget,
 * max-wait 10 s), a 1000-byte budget (percentages map directly to hundreds of bytes), and an
 * injected {@link AtomicLong clock} so every timestamp - park time, expire evaluation, engage
 * stamp, release accounting - is deterministic instead of wall-clock dependent.
 */
public class ReplicationFlowControlTest {

    private static final String MEMBER_A = "replica-a:27017";
    private static final String MEMBER_B = "replica-b:27017";

    private final ReplicationFlowControlSettings settings = ReplicationFlowControlSettings.defaults();
    private final AtomicLong budget = new AtomicLong(1000L);
    private final AtomicLong clock = new AtomicLong(0L);
    private final List<List<Object>> releasedBatches = new ArrayList<>();
    private final ReplicationFlowControl gate = newGate();

    private ReplicationFlowControl newGate() {
        ReplicationFlowControl g = new ReplicationFlowControl(settings, budget::get, clock::get);
        g.setReleaseListener(releasedBatches::add);
        return g;
    }

    /**
     * The gate closes when the slowest replication cursor reaches high water: 49 percent
     * stays free, 50 percent (the high water mark) engages. The engage stamp comes from the
     * injected clock.
     */
    @Test
    public void gateEngagesWhenTheSlowestReplicationCursorReachesHighWater() {
        clock.set(1_000_000);
        gate.onReplicationCursorBytes(MEMBER_A, 400); // 40% - below high water
        assertThat(gate.isEngaged()).isFalse();

        gate.onReplicationCursorBytes(MEMBER_A, 500); // 50% - exactly high water
        assertThat(gate.isEngaged()).isTrue();
        Map<String, Object> snap = gate.statusSnapshot();
        assertThat(snap.get("engagements")).isEqualTo(1L);
        assertThat(snap.get("slowestMember")).isEqualTo(MEMBER_A);
        assertThat(snap.get("slowestFillPercent")).isEqualTo(50L);
        assertThat(snap.get("engagedSinceMs")).isEqualTo(1_000_000L);
    }

    /**
     * Below high water a free gate stays free even inside the hysteresis band - draining is
     * the only thing that must release, and there is nothing to release yet.
     */
    @Test
    public void theGateStaysFreeBetweenLowAndHighWater() {
        gate.onReplicationCursorBytes(MEMBER_A, 300); // 30% - between the water marks
        assertThat(gate.isEngaged()).isFalse();

        gate.onReplicationCursorBytes(MEMBER_A, 490); // 49% - just below high water
        assertThat(gate.isEngaged()).isFalse();
    }

    /**
     * Hysteresis: the gate engages at >= high water and only releases strictly below low
     * water. After an engage at 50%, 49% still holds; exactly 25% still holds (release is
     * "below" low water); 24% opens the gate.
     */
    @Test
    public void theGateHoldsThroughTheHysteresisBandAfterEngaging() {
        gate.onReplicationCursorBytes(MEMBER_A, 500); // engage at high water
        assertThat(gate.isEngaged()).isTrue();

        gate.onReplicationCursorBytes(MEMBER_A, 490); // 49% - hysteresis band
        assertThat(gate.isEngaged()).as("49% after an engage must hold the gate closed").isTrue();

        gate.onReplicationCursorBytes(MEMBER_A, 250); // exactly low water
        assertThat(gate.isEngaged()).as("the gate opens only strictly below low water").isTrue();

        gate.onReplicationCursorBytes(MEMBER_A, 249); // below low water
        assertThat(gate.isEngaged()).isFalse();
        assertThat(gate.statusSnapshot().get("engagedSinceMs")).isEqualTo(0L);
    }

    /**
     * The gate follows the slowest member, not the one that happened to change: one secondary
     * at the limit is enough to brake, and once it drains below low water the gate opens even
     * though the other member is still inside the hysteresis band.
     */
    @Test
    public void theSlowestMemberDrivesTheGate() {
        gate.onReplicationCursorBytes(MEMBER_A, 100); // 10%
        gate.onReplicationCursorBytes(MEMBER_B, 600); // 60% - B is the slowest
        assertThat(gate.isEngaged()).isTrue();
        assertThat(gate.statusSnapshot().get("slowestMember")).isEqualTo(MEMBER_B);

        gate.onReplicationCursorBytes(MEMBER_B, 200); // 20% - below low water
        assertThat(gate.isEngaged()).isFalse();
        assertThat(gate.statusSnapshot().get("slowestMember")).isEqualTo(MEMBER_B);
    }

    /**
     * Removing a replication cursor (kill, disconnect, retire) re-evaluates immediately: the
     * slowest member disappears, and the gate opens on the spot - a dead secondary never holds
     * the gate longer than until its kill.
     */
    @Test
    public void removingTheSlowestMemberReEvaluatesImmediately() {
        gate.onReplicationCursorBytes(MEMBER_A, 100);
        gate.onReplicationCursorBytes(MEMBER_B, 700); // engage via B
        assertThat(gate.isEngaged()).isTrue();

        gate.onReplicationCursorRemoved(MEMBER_B); // slowest gone; A alone at 10%
        assertThat(gate.isEngaged()).isFalse();
        assertThat(gate.statusSnapshot().get("slowestMember")).isEqualTo(MEMBER_A);
    }

    /**
     * Removing a member that is NOT the slowest must not open the gate while the slowest
     * member still fills the queue.
     */
    @Test
    public void removingANonSlowestMemberKeepsTheGateClosed() {
        gate.onReplicationCursorBytes(MEMBER_A, 700); // slowest
        gate.onReplicationCursorBytes(MEMBER_B, 100);
        assertThat(gate.isEngaged()).isTrue();

        gate.onReplicationCursorRemoved(MEMBER_B);
        assertThat(gate.isEngaged()).as("the slowest member still fills the queue").isTrue();
        assertThat(gate.statusSnapshot().get("slowestMember")).isEqualTo(MEMBER_A);
    }

    /**
     * Removing the LAST replication cursor while writes are parked releases them all through
     * the listener in FIFO order - the dead-secondary path never strands a write.
     */
    @Test
    public void removingTheLastReplicationCursorReleasesTheParkedWrites() {
        gate.onReplicationCursorBytes(MEMBER_A, 600); // engage
        assertThat(gate.parkIfEngaged("w1")).isTrue();
        assertThat(gate.parkIfEngaged("w2")).isTrue();

        gate.onReplicationCursorRemoved(MEMBER_A);

        assertThat(gate.isEngaged()).isFalse();
        assertThat(releasedBatches).hasSize(1);
        assertThat(releasedBatches.get(0)).containsExactly("w1", "w2");
    }

    /**
     * When the gate opens, every parked write comes back to the release listener in FIFO
     * order - order on the wire is part of the protocol and must not be rearranged.
     */
    @Test
    public void parkedWritesAreReleasedInFifoOrderWhenTheGateOpens() {
        gate.onReplicationCursorBytes(MEMBER_A, 600); // engage
        Object w1 = new Object();
        Object w2 = new Object();
        Object w3 = new Object();
        clock.set(100);
        assertThat(gate.parkIfEngaged(w1)).isTrue();
        assertThat(gate.parkIfEngaged(w2)).isTrue();
        assertThat(gate.parkIfEngaged(w3)).isTrue();
        clock.set(10_000);

        gate.onReplicationCursorBytes(MEMBER_A, 100); // release

        assertThat(gate.isEngaged()).isFalse();
        assertThat(releasedBatches).hasSize(1);
        assertThat(releasedBatches.get(0)).containsExactly(w1, w2, w3);
        Map<String, Object> snap = gate.statusSnapshot();
        assertThat(snap.get("parkedWrites")).isEqualTo(0L);
        assertThat(snap.get("totalParked")).isEqualTo(3L);
    }

    /**
     * max-wait: expireDue returns exactly the tokens whose park time exceeds maxWaitMs (10 s
     * per the defaults) and counts them as releasedByTimeout; the listener is NOT invoked - the
     * caller releases exactly the returned tokens. Both park time and expiry evaluation read
     * the injected clock, so the wait accounting is exact.
     */
    @Test
    public void expireDueReleasesExactlyTheOverdueWritesAndCountsThem() {
        gate.onReplicationCursorBytes(MEMBER_A, 600); // engage so parkIfEngaged accepts
        clock.set(100);
        Object w1 = new Object();
        assertThat(gate.parkIfEngaged(w1)).isTrue();
        clock.set(5_100);
        Object w2 = new Object();
        assertThat(gate.parkIfEngaged(w2)).isTrue();

        clock.set(11_100); // cutoff 1100 - only w1 is overdue
        List<Object> due = gate.expireDue();
        assertThat(due).containsExactly(w1);
        assertThat(releasedBatches).as("expired tokens are released by the caller, not the listener")
            .isEmpty();
        Map<String, Object> snap = gate.statusSnapshot();
        assertThat(snap.get("releasedByTimeout")).isEqualTo(1L);
        assertThat(snap.get("parkedWrites")).isEqualTo(1L);
        assertThat(snap.get("totalWaitMs")).isEqualTo(11_000L);
        assertThat(snap.get("maxWaitMs")).isEqualTo(11_000L);

        clock.set(16_100); // cutoff 6100 - w2 is overdue now
        due = gate.expireDue();
        assertThat(due).containsExactly(w2);
        assertThat(gate.statusSnapshot().get("releasedByTimeout")).isEqualTo(2L);
        assertThat(gate.statusSnapshot().get("parkedWrites")).isEqualTo(0L);
        assertThat(gate.statusSnapshot().get("totalWaitMs")).isEqualTo(22_000L);
        assertThat(gate.statusSnapshot().get("maxWaitMs")).isEqualTo(11_000L);
    }

    /**
     * A closed connection: unpark drops the single token without a release event, the other
     * parked writes still come back when the gate opens. The dropped write was never executed
     * and the client got no reply - same as an abort before sending.
     */
    @Test
    public void unparkRemovesATokenWithoutReleasingIt() {
        gate.onReplicationCursorBytes(MEMBER_A, 600); // engage
        Object w1 = new Object();
        Object w2 = new Object();
        assertThat(gate.parkIfEngaged(w1)).isTrue();
        assertThat(gate.parkIfEngaged(w2)).isTrue();

        gate.unpark(w2); // connection closed

        gate.onReplicationCursorBytes(MEMBER_A, 100); // gate opens
        assertThat(releasedBatches).hasSize(1);
        assertThat(releasedBatches.get(0)).containsExactly(w1);
        Map<String, Object> snap = gate.statusSnapshot();
        assertThat(snap.get("parkedWrites")).isEqualTo(0L);
        assertThat(snap.get("totalParked")).as("cumulative parks count the un-parked write too")
            .isEqualTo(2L);
    }

    /**
     * Role change: reset hands every parked token to the caller in FIFO order (it answers them
     * with NotWritablePrimary), opens the gate, forgets the members, and the gate can engage
     * again on a fresh primary cycle.
     */
    @Test
    public void resetReturnsAllParkedWritesAndOpensTheGate() {
        gate.onReplicationCursorBytes(MEMBER_A, 600); // engage
        Object w1 = new Object();
        Object w2 = new Object();
        assertThat(gate.parkIfEngaged(w1)).isTrue();
        assertThat(gate.parkIfEngaged(w2)).isTrue();

        List<Object> tokens = gate.reset();

        assertThat(tokens).containsExactly(w1, w2);
        assertThat(releasedBatches).as("reset hands the tokens to the caller, not the listener")
            .isEmpty();
        Map<String, Object> snap = gate.statusSnapshot();
        assertThat(gate.isEngaged()).isFalse();
        assertThat(snap.get("parkedWrites")).isEqualTo(0L);
        assertThat(snap.get("slowestMember")).isNull();

        gate.onReplicationCursorBytes(MEMBER_A, 600); // a fresh primary cycle re-engages
        assertThat(gate.isEngaged()).isTrue();
        assertThat(gate.statusSnapshot().get("engagements")).isEqualTo(2L);
    }

    /**
     * A budget of 0 means no reference size (cursor-queue-budget disabled): the fill cannot be
     * expressed as a percentage, the gate never engages, and parking is refused.
     */
    @Test
    public void aZeroBudgetDisablesEngagement() {
        budget.set(0);
        gate.onReplicationCursorBytes(MEMBER_A, 1_000_000);
        assertThat(gate.isEngaged()).isFalse();
        assertThat(gate.statusSnapshot().get("slowestFillPercent")).isEqualTo(0L);
        assertThat(gate.parkIfEngaged("w")).as("no reference size means nothing to park against")
            .isFalse();
    }

    /**
     * replication-flow-control=false keeps the gate permanently open - the regression switch
     * that restores today's behaviour (kill instead of brake) - and parking stays refused.
     */
    @Test
    public void disabledSettingsNeverEngage() {
        ReplicationFlowControl off = new ReplicationFlowControl(
            new ReplicationFlowControlSettings(false, 50, 25, 10_000), budget::get, clock::get);
        off.onReplicationCursorBytes(MEMBER_A, 900); // 90% of the budget
        assertThat(off.isEngaged()).isFalse();
        assertThat(off.parkIfEngaged("w")).isFalse();
        assertThat(off.statusSnapshot().get("enabled")).isEqualTo(false);
    }

    /**
     * The park decision is atomic: while the gate is open - free, disabled or no reference
     * size - parkIfEngaged refuses without parking, so a write that raced a release executes
     * immediately instead of sitting in the queue until its max-wait.
     */
    @Test
    public void parkIfEngagedReturnsFalseWithoutParkingWhenTheGateIsOpen() {
        gate.onReplicationCursorBytes(MEMBER_A, 300); // 30% - free, between the water marks
        assertThat(gate.parkIfEngaged("w")).isFalse();
        Map<String, Object> snap = gate.statusSnapshot();
        assertThat(snap.get("parkedWrites")).isEqualTo(0L);
        assertThat(snap.get("totalParked")).isEqualTo(0L);

        // the refused write left no trace: engaging and releasing again must not release it
        gate.onReplicationCursorBytes(MEMBER_A, 600); // engage
        assertThat(gate.isEngaged()).isTrue();
        gate.onReplicationCursorBytes(MEMBER_A, 100); // release, nothing parked
        assertThat(releasedBatches).isEmpty();
    }

    /**
     * While the gate is engaged, parkIfEngaged parks the token and reports success; the write
     * is released with the others when the gate opens.
     */
    @Test
    public void parkIfEngagedParksAndReturnsTrueWhileTheGateIsEngaged() {
        gate.onReplicationCursorBytes(MEMBER_A, 600); // engage
        assertThat(gate.parkIfEngaged("w")).isTrue();

        Map<String, Object> snap = gate.statusSnapshot();
        assertThat(snap.get("parkedWrites")).isEqualTo(1L);
        assertThat(snap.get("totalParked")).isEqualTo(1L);

        clock.set(50_000);
        gate.onReplicationCursorBytes(MEMBER_A, 100); // release
        assertThat(releasedBatches).hasSize(1);
        assertThat(releasedBatches.get(0)).containsExactly("w");
    }

    /**
     * Every timestamp comes from the injected clock, on the gate-open path AND the max-wait
     * path, so the wait accounting is exact: park at +500, release at +1000 = 500 ms waited;
     * park at +200000100, expire at +2000010100 = 10 s waited.
     */
    @Test
    public void waitAccountingUsesTheInjectedClockExactly() {
        clock.set(1_000_000);
        gate.onReplicationCursorBytes(MEMBER_A, 600); // engage, stamp = 1000000
        assertThat(gate.statusSnapshot().get("engagedSinceMs")).isEqualTo(1_000_000L);

        clock.set(1_000_500);
        assertThat(gate.parkIfEngaged("w1")).isTrue(); // parked at 1000500

        clock.set(1_001_000);
        gate.onReplicationCursorBytes(MEMBER_A, 100); // release at 1001000
        Map<String, Object> snap = gate.statusSnapshot();
        assertThat(snap.get("totalWaitMs")).isEqualTo(500L);
        assertThat(snap.get("maxWaitMs")).isEqualTo(500L);
        assertThat(releasedBatches.get(0)).containsExactly("w1");

        // second cycle, released by max-wait instead: same clock, same exactness
        clock.set(2_000_000);
        gate.onReplicationCursorBytes(MEMBER_A, 600); // engage again
        clock.set(2_000_100);
        assertThat(gate.parkIfEngaged("w2")).isTrue(); // parked at 2000100

        clock.set(2_010_100); // exactly maxWaitMs later
        assertThat(gate.expireDue()).containsExactly("w2");
        snap = gate.statusSnapshot();
        assertThat(snap.get("totalWaitMs")).isEqualTo(500L + 10_000L);
        assertThat(snap.get("maxWaitMs")).isEqualTo(10_000L);
        assertThat(snap.get("releasedByTimeout")).isEqualTo(1L);
    }

    /**
     * The one-token-per-key overload: re-arming the SAME token while it is already parked must
     * neither duplicate it nor restart its wait - a key's token stays anchored to its oldest
     * waiting entry, so a second write behind the first cannot inherit a fresh max-wait window.
     * Once the token was released (expired or gate-opened), re-arming with the next entry's time
     * is a fresh token that expires at that new anchor.
     */
    @Test
    public void reArmingTheSameTokenKeepsItsOriginalAnchor() {
        gate.onReplicationCursorBytes(MEMBER_A, 600); // engage
        clock.set(0);
        assertThat(gate.parkIfEngaged(MEMBER_A, 0L)).isTrue();

        clock.set(9_000);
        assertThat(gate.parkIfEngaged(MEMBER_A, 9_000L)).isTrue(); // re-arm: no duplicate
        assertThat(gate.statusSnapshot().get("parkedWrites")).isEqualTo(1L);

        clock.set(10_000); // 0 + maxWaitMs - the ORIGINAL anchor, not the re-arm at 9000
        assertThat(gate.expireDue()).containsExactly(MEMBER_A);

        clock.set(12_000);
        assertThat(gate.parkIfEngaged(MEMBER_A, 12_000L)).isTrue(); // fresh token after the eject
        clock.set(21_999);
        assertThat(gate.expireDue()).as("the fresh anchor is 12_000 + 10_000").isEmpty();
        clock.set(22_000);
        assertThat(gate.expireDue()).containsExactly(MEMBER_A);
    }

    /**
     * engagements counts every free-to-engaged transition, not every fill update while engaged.
     */
    @Test
    public void engagementsCountsEveryEngageTransition() {
        gate.onReplicationCursorBytes(MEMBER_A, 600); // engage
        gate.onReplicationCursorBytes(MEMBER_A, 700); // still engaged
        gate.onReplicationCursorBytes(MEMBER_A, 100); // release
        gate.onReplicationCursorBytes(MEMBER_A, 600); // engage again

        Map<String, Object> snap = gate.statusSnapshot();
        assertThat(snap.get("engagements")).isEqualTo(2L);
        assertThat(snap.get("engaged")).isEqualTo(true);
    }

    /**
     * The release listener contract: it may reenter the gate without deadlock - the listener
     * always runs outside the gate's monitor, so the reentrant calls (a fresh park decision, a
     * snapshot read) work. The reentrant park is refused because the gate has just opened.
     */
    @Test
    public void theReleaseListenerMayReenterTheGate() {
        ReplicationFlowControl g = new ReplicationFlowControl(settings, budget::get, clock::get);
        List<Object> released = new ArrayList<>();
        AtomicBoolean listenerWorked = new AtomicBoolean(false);
        g.setReleaseListener(tokens -> {
            released.addAll(tokens);
            // reenter: the gate has just opened, so a fresh park must not be accepted
            listenerWorked.set(!g.parkIfEngaged("follow-up") && g.statusSnapshot().containsKey("maxWaitMs"));
        });

        g.onReplicationCursorBytes(MEMBER_A, 600); // engage
        assertThat(g.parkIfEngaged("w1")).isTrue();
        assertThat(g.parkIfEngaged("w2")).isTrue();
        g.onReplicationCursorBytes(MEMBER_A, 100); // release

        assertThat(released).containsExactly("w1", "w2");
        assertThat(listenerWorked).isTrue();
        assertThat(g.statusSnapshot().get("parkedWrites")).isEqualTo(0L);
    }

    /**
     * The snapshot exposes exactly the fields of the serverStatus poppyFlowControl section
     * (spec 3.7), in the documented key order, with live values.
     */
    @Test
    public void theSnapshotReportsTheSection37Fields() {
        gate.onReplicationCursorBytes(MEMBER_A, 600); // engage
        assertThat(gate.parkIfEngaged("w")).isTrue();

        Map<String, Object> snap = gate.statusSnapshot();
        assertThat(snap.keySet()).containsExactly(
            "enabled", "engaged", "engagedSinceMs", "slowestMember", "slowestFillPercent",
            "parkedWrites", "totalParked", "totalWaitMs", "maxWaitMs", "releasedByTimeout",
            "engagements");
        assertThat(snap.get("enabled")).isEqualTo(true);
        assertThat(snap.get("engaged")).isEqualTo(true);
        assertThat(snap.get("slowestMember")).isEqualTo(MEMBER_A);
        assertThat(snap.get("slowestFillPercent")).isEqualTo(60L);
        assertThat(snap.get("parkedWrites")).isEqualTo(1L);
        assertThat(snap.get("totalParked")).isEqualTo(1L);
    }

    /**
     * The constructor rejects water marks that would make the hysteresis meaningless (low >=
     * high, percentages outside 1..100), a non-positive max-wait and a null clock - the same
     * rules the ConfigInspector validates on the configuration side.
     */
    @Test
    public void invalidSettingsAreRejected() {
        assertThatThrownBy(() -> new ReplicationFlowControl(
            new ReplicationFlowControlSettings(true, 25, 50, 10_000), budget::get, clock::get))
            .isInstanceOf(IllegalArgumentException.class); // low >= high
        assertThatThrownBy(() -> new ReplicationFlowControl(
            new ReplicationFlowControlSettings(true, 0, 25, 10_000), budget::get, clock::get))
            .isInstanceOf(IllegalArgumentException.class); // percentage below 1
        assertThatThrownBy(() -> new ReplicationFlowControl(
            new ReplicationFlowControlSettings(true, 101, 25, 10_000), budget::get, clock::get))
            .isInstanceOf(IllegalArgumentException.class); // percentage above 100
        assertThatThrownBy(() -> new ReplicationFlowControl(
            new ReplicationFlowControlSettings(true, 50, 25, 0), budget::get, clock::get))
            .isInstanceOf(IllegalArgumentException.class); // max-wait not positive
        assertThatThrownBy(() -> new ReplicationFlowControl(settings, budget::get, null))
            .isInstanceOf(IllegalArgumentException.class); // no clock
    }

    /**
     * The two-argument convenience constructor defaults the clock to the wall clock, so the
     * wiring step can use it without an explicit clock and the gate still stamps real time.
     */
    @Test
    public void theTwoArgConstructorDefaultsTheClockToWallTime() {
        ReplicationFlowControl wall = new ReplicationFlowControl(settings, budget::get);
        long before = System.currentTimeMillis();
        wall.onReplicationCursorBytes(MEMBER_A, 600); // engage stamps engagedSince from the wall clock
        assertThat((Long) wall.statusSnapshot().get("engagedSinceMs"))
            .isBetween(before, System.currentTimeMillis());
    }

    /**
     * A negative queuedBytes delta (an accounting overshoot on the caller side) must not
     * produce a negative fill or a spurious release evaluation.
     */
    @Test
    public void aNegativeQueuedBytesDeltaCannotEngageTheGate() {
        gate.onReplicationCursorBytes(MEMBER_A, -5);
        assertThat(gate.isEngaged()).isFalse();
        assertThat(gate.statusSnapshot().get("slowestFillPercent")).isEqualTo(0L);
    }
}