package de.caluga.poppydb;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cumulative write-concern wait counters of one server, exposed as
 * {@code serverStatus.writeConcernWaits} and recorded into by {@link ReplicationCoordinator}.
 *
 * <p>Owned by {@link PoppyDB}, not by a coordinator: a leadership change replaces the
 * coordinator, and the numbers an operator reads after an incident must not restart with it.
 */
public class WriteConcernWaitStats {

    private final AtomicLong acknowledged = new AtomicLong();
    private final AtomicLong timedOut = new AtomicLong();
    private final AtomicLong noSecondaries = new AtomicLong();
    private final AtomicLong steppedDown = new AtomicLong();

    void record(ReplicationCoordinator.WaitOutcome outcome) {
        switch (outcome) {
            case ACKNOWLEDGED:
                acknowledged.incrementAndGet();
                break;
            case TIMEOUT:
                timedOut.incrementAndGet();
                break;
            case NO_SECONDARIES:
                noSecondaries.incrementAndGet();
                break;
            case STEPPED_DOWN:
                steppedDown.incrementAndGet();
                break;
            default:
                break;
        }
    }

    /**
     * The {@code serverStatus.writeConcernWaits} document.
     *
     * @param waiting      waits registered right now
     * @param oldestWaitMs age of the oldest registered wait, 0 when none is registered
     */
    public Map<String, Object> snapshot(int waiting, long oldestWaitMs) {
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("waiting", waiting);
        snapshot.put("oldestWaitMs", oldestWaitMs);
        snapshot.put("acknowledged", acknowledged.get());
        snapshot.put("timedOut", timedOut.get());
        snapshot.put("noSecondaries", noSecondaries.get());
        snapshot.put("steppedDown", steppedDown.get());
        return snapshot;
    }

    /** The section on a node that has no coordinator (a secondary): nothing is waiting there. */
    public static Map<String, Object> emptySnapshot() {
        return new WriteConcernWaitStats().snapshot(0, 0);
    }
}
