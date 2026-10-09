package de.caluga.morphium.driver;

import java.util.Map;

/**
 * Created by stephan on 29.07.16.
 */
public interface DriverTailableIterationCallback {
    /**
     * @param data - incoming data
     * @param dur  - duration since start
     */
    void incomingData(Map<String, Object> data, long dur);

    boolean isContinued();

    /**
     * Called by the watch loop when the server answers a getMore with an EMPTY batch
     * (no events). This is the liveness heartbeat of a healthy-but-idle stream: the
     * server answers within maxTimeMS even without events, so a consumer that only
     * refreshes its staleness timer in {@link #incomingData} would wrongly declare a
     * quiet stream stale after 30s and force a needless reconnect.
     * <p>
     * Default no-op so existing implementors (and the PoppyDB server-side watch) are
     * unaffected; the ReplicationManager overrides it to refresh its staleness tracker.
     * <p>
     * IMPORTANT: this is deliberately NOT called while the reader is blocked inside
     * {@link #incomingData} (byte-budget backpressure) - a blocked reader never reaches
     * the empty-batch handling, so its staleness timer keeps running and the watch is
     * still correctly declared stale/dead.
     */
    default void onIdleBatch() {
    }
}
