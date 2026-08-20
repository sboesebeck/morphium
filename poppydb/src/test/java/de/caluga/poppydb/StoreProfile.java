package de.caluga.poppydb;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Tag;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.messaging.Msg;

/**
 * Shared profiling harness for #327: decomposes the {@code store(Msg)} vs raw-insert gap with
 * EXACT instruments only — wall time and {@link ThreadMXBean#getCurrentThreadCpuTime} on the
 * calling thread — so the claim "mapping costs ~50us" can be checked without sampling error.
 *
 * <p>Two modes:
 * <ul>
 *   <li>{@code doc} (default): raw phase inserts a tiny 3-field {@code Doc}; store phase
 *       maps+stores a full {@code Msg}. This is the LOOSE comparison: document size differs.</li>
 *   <li>{@code equalized}: the raw phase inserts the PRE-MAPPED Msg document (same bytes the
 *       store phase produces) into the same collection, so both sides send the identical
 *       document. This is the STRICT comparison: only the entity pipeline differs.</li>
 * </ul>
 *
 * <p>Phases alternate in blocks (default 3s x 6 blocks, i.e. three A/B pairs) and the median
 * block is reported per side, matching the interleaved-control protocol. Run it with the JFR
 * kata under {@code src/test/resources/store-profile.jfc} to also capture jdk.ThreadCPULoad and
 * allocation events:
 *
 * <pre>
 *   # loose comparison, JFR for ThreadCPULoad / ObjectAllocationSample
 *   JAVA_TOOL_OPTIONS="-XX:StartFlightRecording=filename=/tmp/sp.jfr,settings=src/test/resources/store-profile.jfc,disk=true,dumponexit=true" \
 *     mvn -o -pl poppydb test-compile exec:java \
 *     -Dexec.mainClass=de.caluga.poppydb.StoreProfile -Dexec.classpathScope=test
 *
 *   # strict comparison, 5s blocks
 *   ... StoreProfile equalized 5
 * </pre>
 * <p>The layer decomposition lives in {@link WireRoundtripCostBenchmark} (raw driver vs PooledDriver
 * vs store vs messaging); this harness owns the per-op wall/CPU split of the {@code store} vs raw
 * gap and the frame attribution. When one says something about the gap, the other must stay
 * consistent with it.
 */
@Tag("manual")
public class StoreProfile {

    static final ThreadMXBean TMB = ManagementFactory.getThreadMXBean();

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "doc";
        int blockSeconds = args.length > 1 ? Integer.parseInt(args[1]) : 3;
        int pairs = 3;

        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        PoppyDB server = new PoppyDB(port, "127.0.0.1", 100, 10);
        server.start();
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            try (Socket probe = new Socket()) {
                probe.connect(new InetSocketAddress("127.0.0.1", port), 250);
                break;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) throw e;
                Thread.sleep(50);
            }
        }

        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings().setDatabase("storeprof");
        cfg.clusterSettings().getHostSeed().clear();
        cfg.clusterSettings().addHostToSeed("127.0.0.1:" + port);
        cfg.driverSettings().setDriverName("PooledDriver");
        cfg.connectionSettings().setMaxConnections(20).setMinConnections(2);

        try (Morphium m = new Morphium(cfg)) {
            Msg msg = new Msg("storeprof", "bench", "x", 300_000);
            msg.setSender("bench-sender");
            msg.setSenderHost("localhost");
            Map<String, Object> preMapped = "equalized".equals(mode) ? m.getMapper().serialize(msg) : null;

            for (int i = 0; i < 500; i++) {
                if ("equalized".equals(mode)) rawInsert(m, i, preMapped); else rawInsertSmall(m, i);
                m.store(msg);
            }

            List<double[]> rawBlocks = new ArrayList<>();
            List<double[]> storeBlocks = new ArrayList<>();
            long blockNs = blockSeconds * 1_000_000_000L;
            int[] id = {500};   // warmup consumed 0..499 in the same collection
            for (int p = 0; p < pairs; p++) {
                Thread.currentThread().setName("probe:raw-" + p);
                rawBlocks.add(runBlock("raw", "equalized".equals(mode)
                        ? () -> rawInsertEqualized(m, id[0]++, preMapped)
                        : () -> rawInsertSmall(m, id[0]++), blockNs));
                Thread.currentThread().setName("probe:store-" + p);
                // Reusing one Msg instance would give it an _id after the first store, routing
                // every later store to StoreMongoCommand (update $set + upsert, see
                // MorphiumWriterImpl.store) instead of InsertMongoCommand — i.e. measuring a
                // find+modify against an existing doc, not an insert. Clearing the id keeps the
                // store phase a genuine insert like the raw phase.
                storeBlocks.add(runBlock("store", () -> {
                    m.store(msg);
                    msg.setMsgId(null);
                }, blockNs));
            }

            report("raw", median(rawBlocks));
            report("store", median(storeBlocks));
        }
        server.shutdown();
        System.exit(0);
    }

    private interface Op {
        void run() throws Exception;
    }

    /** Runs one block; returns {wallUsPerOp, cpuUsPerOp, ops}. */
    private static double[] runBlock(String label, Op op, long blockNs) throws Exception {
        System.out.printf("BLKSTART %s ms=%d%n", label, System.currentTimeMillis());
        long w0 = System.nanoTime();
        // CPU on the calling thread is measured with ThreadMXBean, NOT JFR: on macOS/JDK 21,
        // jdk.ThreadCPULoad reports ~0.00s for a thread the kernel says burns tens of µs/op
        // (only ~2 events/thread per 10s interval in the shipped store-profile.jfc), and
        // jdk.ExecutionSample under-delivered ~8x in the same run (113 samples where ~935 were
        // due). JFR told us the gap was a "hidden wait"; this instrument showed it was compute.
        long c0 = TMB.getCurrentThreadCpuTime();
        long ops = 0;
        while (System.nanoTime() - w0 < blockNs) {
            op.run();
            ops++;
        }
        long wEl = System.nanoTime() - w0;
        long cEl = TMB.getCurrentThreadCpuTime() - c0;
        double wall = wEl / 1000.0 / ops;
        double cpu = cEl / 1000.0 / ops;
        System.out.printf("BLOCK %-5s wall=%9.1f us/op  cpu=%9.1f us/op  ops=%d%n", label, wall, cpu, ops);
        return new double[]{wall, cpu, ops};
    }

    private static double median(List<double[]> blocks) {
        List<Double> wall = new ArrayList<>();
        for (double[] b : blocks) wall.add(b[0]);
        wall.sort(null);
        return wall.get(wall.size() / 2);
    }

    private static void report(String what, double wall) {
        System.out.printf("PHASERESULT %-6s median-wall=%9.1f us/op%n", what, wall);
    }

    private static void rawInsertSmall(Morphium m, int i) throws Exception {
        rawInsert(m, i, Doc.of("_id", i, "v", "x"));
    }

    /** Raw insert of the same document shape the store phase uses, but a unique _id. */
    private static void rawInsertEqualized(Morphium m, int i, Map<String, Object> preMapped) throws Exception {
        Map<String, Object> d = new java.util.HashMap<>(preMapped);
        d.put("_id", i);
        rawInsert(m, i, d);
    }

    private static void rawInsert(Morphium m, int i, Map<String, Object> doc) throws Exception {
        InsertMongoCommand cmd = new InsertMongoCommand(m.getDriver().getPrimaryConnection(null))
                .setDb("storeprof").setColl("raw")
                .setDocuments(List.of(doc));
        try {
            cmd.execute();
        } finally {
            cmd.releaseConnection();
        }
    }
}