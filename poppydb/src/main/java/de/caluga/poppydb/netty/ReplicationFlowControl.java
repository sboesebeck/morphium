package de.caluga.poppydb.netty;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Section 3.3 of the replication flow-control design (2026-10-09): the hysteresis gate a
 * PoppyDB primary uses to brake writers before a lagging replication cursor reaches its byte
 * budget. The signal is the fill of the slowest replication cursor - the same size that today
 * triggers the kill. When that fill reaches high water the gate engages and writers park;
 * when it drops below low water the gate opens and every parked write is released in FIFO
 * order. Between the two water marks the gate keeps its current state, so a fill of 49
 * percent after an engage at 50 percent still holds.
 *
 * <p>The gate is deliberately dumb about WHAT the parked things are: the caller hands over
 * opaque tokens via {@link #parkIfEngaged(Object)} and gets them back through the release
 * listener (gate open), from {@link #expireDue()} (a token's max-wait elapsed - the caller
 * releases exactly the returned tokens), or from {@link #reset()} (role change - the caller
 * answers them with NotWritablePrimary). The caller owns the per-connection FIFOs and the
 * event-loop dispatch; this class only decides WHO may run and WHEN.
 *
 * <p>Thread safety: one private monitor guards all mutable state, and listeners are never
 * invoked while it is held - the release fires after the synchronized block exits. That keeps
 * a listener (or the thread that polled expireDue) free to reenter this gate without
 * deadlocking. {@link #isEngaged()} reads a volatile flag so the writer fast path does not
 * pay for the monitor.
 *
 * <p>Water-mark semantics: engage when the slowest fill reaches {@code highWaterPercent} or
 * above, release only when it drops strictly below {@code lowWaterPercent}. The effective
 * budget is read from the supplier on every evaluation; a budget of 0 or less means no
 * reference size and the gate never engages (cursor-queue-budget disabled), and
 * {@code enabled=false} keeps the gate permanently open. {@link #parkIfEngaged(Object)} is
 * atomic under the monitor: it parks only while the gate is engaged and returns false
 * otherwise, so a write whose check-and-park raced a release executes immediately instead of
 * stalling in the queue until its max-wait. Parked tokens are compared with
 * {@link Object#equals(Object)} for {@link #unpark(Object)}.
 *
 * <p>Time: every timestamp - park time, expire evaluation, engage stamp, release accounting -
 * comes from an injected {@link java.util.function.LongSupplier clock}, defaulting to
 * {@link System#currentTimeMillis()} in the two-argument constructor. Tests inject a
 * controllable clock so the wait accounting is exact and wall-clock independent.
 *
 * <p>Cumulative statistics (totalParked, totalWaitMs, maxWaitMs, releasedByTimeout,
 * engagements) survive {@link #reset()}; only the engagement state, the park queue and the
 * per-member fills are cleared, so a later re-election starts from a clean slate. Wait time
 * is accounted for writes that were actually released (gate open or max-wait); tokens dropped
 * by {@link #unpark(Object)} or answered with NotWritablePrimary after {@link #reset()} never
 * executed and do not count.
 */
public class ReplicationFlowControl {

    /** Receives the tokens of parked writes that may now run, in FIFO order. */
    public interface ReleaseListener {
        void writesReleased(List<Object> tokens);
    }

    private final ReplicationFlowControlSettings settings;
    private final LongSupplier budgetSupplier;
    private final LongSupplier clock;
    private final Object monitor = new Object();
    private final Map<String, Long> memberFillBytes = new HashMap<>();
    private final ArrayDeque<ParkedWrite> parked = new ArrayDeque<>();

    private volatile boolean engaged;
    private volatile ReleaseListener releaseListener;

    private long engagedSinceMs;
    private String slowestMember;
    private long slowestFillPercent;

    // cumulative statistics, guarded by the monitor; surfaced in serverStatus (spec 3.7)
    private long totalParked;
    private long totalWaitMs;
    private long observedMaxWaitMs;
    private long releasedByTimeout;
    private long engagements;

    private record ParkedWrite(Object token, long parkedAt) {}

    /**
     * Convenience constructor: the clock defaults to the wall clock, good for production
     * wiring; tests inject a controllable clock via the three-argument constructor.
     */
    public ReplicationFlowControl(ReplicationFlowControlSettings settings, LongSupplier budgetSupplier) {
        this(settings, budgetSupplier, System::currentTimeMillis);
    }

    public ReplicationFlowControl(ReplicationFlowControlSettings settings, LongSupplier budgetSupplier, LongSupplier clock) {
        if (settings == null || budgetSupplier == null || clock == null) {
            throw new IllegalArgumentException("settings, budgetSupplier and clock must not be null");
        }
        int high = settings.highWaterPercent();
        int low = settings.lowWaterPercent();
        if (high < 1 || high > 100 || low < 1 || low > 100) {
            throw new IllegalArgumentException("water marks must be percentages in 1..100");
        }
        if (low >= high) {
            throw new IllegalArgumentException("lowWaterPercent must be below highWaterPercent");
        }
        if (settings.maxWaitMs() <= 0) {
            throw new IllegalArgumentException("maxWaitMs must be positive");
        }
        this.settings = settings;
        this.budgetSupplier = budgetSupplier;
        this.clock = clock;
    }

    /**
     * Registers the listener that receives released write tokens. Called outside the monitor,
     * so the listener may reenter this gate; a listener may be replaced at any time.
     */
    public void setReleaseListener(ReleaseListener listener) {
        this.releaseListener = listener;
    }

    /** Writer fast path: true while the gate is closed and writers must park before executing. */
    public boolean isEngaged() {
        return engaged;
    }

    /**
     * A replication cursor's buffered bytes changed - offered (bytes grew) or drained (bytes
     * shrank). Stores the member fill and re-walks the hysteresis against the current budget.
     * May synchronously open the gate and hand parked writes to the release listener.
     */
    public void onReplicationCursorBytes(String memberAddress, long queuedBytes) {
        if (memberAddress == null) {
            throw new IllegalArgumentException("memberAddress must not be null");
        }
        List<Object> toRelease;
        synchronized (monitor) {
            // the fill is a byte counter on the caller side; an overshoot delta must not go negative
            memberFillBytes.put(memberAddress, Math.max(0, queuedBytes));
            toRelease = evaluateLocked();
        }
        fireRelease(toRelease);
    }

    /**
     * A replication cursor disappeared - kill, disconnect, retire. The member leaves the max
     * immediately, so a dead secondary never holds the gate longer than until its kill.
     */
    public void onReplicationCursorRemoved(String memberAddress) {
        List<Object> toRelease;
        synchronized (monitor) {
            memberFillBytes.remove(memberAddress);
            toRelease = evaluateLocked();
        }
        fireRelease(toRelease);
    }

    /**
     * Atomic check-and-park: parks the write only while the gate is engaged and reports
     * whether it parked. The caller uses the return value to decide between parking and
     * executing immediately - false means the gate is open at this instant (free, disabled or
     * no reference size), so running the write right away cannot race a release that already
     * drained the park queue. The token is opaque to the gate; it comes back in FIFO order
     * through the release listener, or from {@link #expireDue()} / {@link #reset()}. The wait
     * starts at the injected clock's current reading.
     */
    public boolean parkIfEngaged(Object token) {
        if (token == null) {
            throw new IllegalArgumentException("token must not be null");
        }
        synchronized (monitor) {
            // engaged is only ever true while enabled and the budget is a reference size
            // (evaluateLocked enforces both), so this single check covers all three cases
            if (!engaged) {
                return false;
            }
            parked.addLast(new ParkedWrite(token, clock.getAsLong()));
            totalParked++;
            return true;
        }
    }

    /**
     * Check-and-park with an explicit entry time, for a caller that keeps ONE token per key (a
     * connection whose FIFO holds several parked writes re-arms the SAME token once its oldest
     * entry ran). An equal token that is already parked is left untouched - never duplicated, and
     * its wait is NEVER restarted - so the key's expiry stays anchored to its oldest waiting
     * entry; only a token that was released or expired is re-added, with the given (next entry's)
     * time. Returns false exactly when the gate is not engaged, like the one-argument form.
     */
    public boolean parkIfEngaged(Object token, long parkedAtMs) {
        if (token == null) {
            throw new IllegalArgumentException("token must not be null");
        }
        synchronized (monitor) {
            if (!engaged) {
                return false;
            }
            for (ParkedWrite w : parked) {
                if (w.token().equals(token)) {
                    return true; // already armed - keep its anchor, add nothing
                }
            }
            parked.addLast(new ParkedWrite(token, parkedAtMs));
            totalParked++;
            return true;
        }
    }

    /**
     * Returns the tokens whose park time exceeds maxWaitMs (evaluated against the injected
     * clock), in FIFO order, and counts them as releasedByTimeout. The release listener is NOT
     * invoked here - the caller releases exactly the returned tokens through the same path it
     * would use for listener-delivered ones. A token released this way has waited its
     * max-wait: the event lands in the queue and the existing kill path is the upper bound
     * again.
     */
    public List<Object> expireDue() {
        List<Object> expired = new ArrayList<>();
        synchronized (monitor) {
            long nowMs = clock.getAsLong();
            long cutoff = nowMs - settings.maxWaitMs();
            for (Iterator<ParkedWrite> it = parked.iterator(); it.hasNext(); ) {
                ParkedWrite w = it.next();
                if (w.parkedAt() <= cutoff) {
                    it.remove();
                    expired.add(w.token());
                    releasedByTimeout++;
                    accountWaitLocked(nowMs, w.parkedAt());
                }
            }
        }
        return expired;
    }

    /**
     * Removes one parked write because its connection closed. The token is dropped without a
     * release event - nothing of it was executed and the client got no reply, the same as an
     * abort before sending. A null token is a benign no-op.
     */
    public void unpark(Object token) {
        if (token == null) {
            return;
        }
        synchronized (monitor) {
            parked.removeIf(w -> w.token().equals(token));
        }
    }

    /**
     * Role change (primary lost while writes are parked): opens the gate, forgets every
     * replication member, and hands all parked tokens back to the caller in FIFO order so it
     * can answer them with NotWritablePrimary. The release listener is NOT invoked. Cumulative
     * statistics survive; a later re-election starts from a clean slate.
     */
    public List<Object> reset() {
        List<Object> tokens = new ArrayList<>();
        synchronized (monitor) {
            engaged = false;
            engagedSinceMs = 0;
            memberFillBytes.clear();
            slowestMember = null;
            slowestFillPercent = 0;
            while (!parked.isEmpty()) {
                tokens.add(parked.poll().token());
            }
        }
        return tokens;
    }

    /** The serverStatus poppyFlowControl section (spec 3.7), one consistent snapshot. */
    public Map<String, Object> statusSnapshot() {
        synchronized (monitor) {
            Map<String, Object> snap = new LinkedHashMap<>();
            snap.put("enabled", settings.enabled());
            snap.put("engaged", engaged);
            snap.put("engagedSinceMs", engagedSinceMs);
            snap.put("slowestMember", slowestMember);
            snap.put("slowestFillPercent", slowestFillPercent);
            snap.put("parkedWrites", (long) parked.size());
            snap.put("totalParked", totalParked);
            snap.put("totalWaitMs", totalWaitMs);
            snap.put("maxWaitMs", observedMaxWaitMs);
            snap.put("releasedByTimeout", releasedByTimeout);
            snap.put("engagements", engagements);
            return snap;
        }
    }

    /**
     * Recomputed the slowest member, walks the hysteresis, and returns the tokens to release
     * when the gate opens (or null). Must run with the monitor held; the caller fires the
     * release listener AFTER the synchronized block exits.
     */
    private List<Object> evaluateLocked() {
        long budget = budgetSupplier.getAsLong();
        long maxFill = 0;
        String slowest = null;
        for (Map.Entry<String, Long> e : memberFillBytes.entrySet()) {
            if (e.getValue() > maxFill) {
                maxFill = e.getValue();
                slowest = e.getKey();
            }
        }
        slowestMember = slowest;
        // like the cursor budgets, a zero budget means "not configured": no reference size
        slowestFillPercent = budget > 0 ? maxFill * 100 / budget : 0;
        if (engaged) {
            if (slowestFillPercent < settings.lowWaterPercent()) {
                long now = clock.getAsLong();
                engaged = false;
                engagedSinceMs = 0;
                List<Object> tokens = new ArrayList<>(parked.size());
                while (!parked.isEmpty()) {
                    ParkedWrite w = parked.poll();
                    tokens.add(w.token());
                    accountWaitLocked(now, w.parkedAt());
                }
                return tokens.isEmpty() ? null : tokens;
            }
        } else if (settings.enabled() && budget > 0
                && slowestFillPercent >= settings.highWaterPercent()) {
            engaged = true;
            engagedSinceMs = clock.getAsLong();
            engagements++;
        }
        return null;
    }

    private void accountWaitLocked(long releasedAt, long parkedAt) {
        long waitMs = Math.max(0, releasedAt - parkedAt);
        totalWaitMs += waitMs;
        observedMaxWaitMs = Math.max(observedMaxWaitMs, waitMs);
    }

    private void fireRelease(List<Object> tokens) {
        ReleaseListener l = releaseListener;
        if (l != null && tokens != null && !tokens.isEmpty()) {
            l.writesReleased(tokens);
        }
    }
}