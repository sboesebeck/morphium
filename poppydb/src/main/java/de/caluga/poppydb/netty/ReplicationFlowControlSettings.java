package de.caluga.poppydb.netty;

/**
 * Settings of the replication flow-control gate on a primary (see {@link ReplicationFlowControl}):
 * whether writers are braked at all, at which fill level of the slowest secondary's
 * replication-watch queue (percent of {@code cursor-queue-budget}) braking starts and stops, and
 * how long a single write may be held back before it runs regardless.
 * <p>
 * Validated on construction so a bad configuration fails at startup (ConfigInspector, CLI) with
 * the offending rule in the message, never as a silently inert gate.
 *
 * @param enabled          the configured switch; off means the gate never engages
 * @param highWaterPercent fill level at which the gate engages (1..100, above low)
 * @param lowWaterPercent  fill level below which the gate releases (1..100, below high)
 * @param maxWaitMs        longest a single write is parked, in milliseconds (> 0)
 */
public record ReplicationFlowControlSettings(boolean enabled, int highWaterPercent, int lowWaterPercent, long maxWaitMs) {

    public ReplicationFlowControlSettings {
        if (highWaterPercent < 1 || highWaterPercent > 100) {
            throw new IllegalArgumentException("high-water must be a percentage in 1..100, got: " + highWaterPercent);
        }
        if (lowWaterPercent < 1 || lowWaterPercent > 100) {
            throw new IllegalArgumentException("low-water must be a percentage in 1..100, got: " + lowWaterPercent);
        }
        if (lowWaterPercent >= highWaterPercent) {
            throw new IllegalArgumentException("low-water (" + lowWaterPercent + ") must be below high-water ("
                + highWaterPercent + ")");
        }
        if (maxWaitMs <= 0) {
            throw new IllegalArgumentException("max-wait must be positive, got: " + maxWaitMs + " ms");
        }
    }

    /** The defaults decided for the feature: on, brake at 50 percent, release below 25, max-wait 10 s. */
    public static ReplicationFlowControlSettings defaults() {
        return new ReplicationFlowControlSettings(true, 50, 25, 10_000);
    }
}
