package de.caluga.test.morphium.testutil.proxy;

/** Client-visible fault to inject on a {@link WireProxy}. See the design spec's "The freeze
 * mode" section for exactly what each value does to already-open vs. brand-new connections. */
public enum FaultMode {
    /** Forward normally in both directions. */
    passthrough,
    /** Accept new connections and leave existing ones open, but never read/write/close either
     * side - simulates a frozen process (kill -STOP): the client's read() must time out, never
     * see EOF or a reset. */
    freeze,
    /** Sever existing connections with a hard RST; refuse new connection attempts outright -
     * simulates a truly-gone process (kill -9). */
    reset,
    /** Sever existing connections with a clean FIN; refuse new connection attempts outright,
     * the same as reset - simulates "this specific route to the node is gone" even though the
     * real node (e.g. after a clean stepdown) may still be alive elsewhere. See the design
     * spec's "New connection attempts during a fault" for why close and reset agree on refusing
     * new connections despite differing on how they sever existing ones. */
    close
}
