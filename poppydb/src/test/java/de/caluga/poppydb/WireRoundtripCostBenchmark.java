package de.caluga.poppydb;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

/**
 * Where does the per-message cost of a synchronous send actually go?
 *
 * <p>The one-way messaging benchmark plateaus at roughly 320µs per operation single-threaded,
 * which is a lot for an in-memory insert over loopback (a bare loopback round trip is 30-50µs).
 * #276 assumed the answer was per-command fixed cost on the server and tried to amortise it by
 * fusing commands; that turned out to be unmeasurable, because the mechanism could never engage
 * (see the issue). This benchmark decomposes the cost instead of guessing at it:
 *
 * <ul>
 *   <li><b>insert, in-process</b> - InMemoryDriver.insert with no wire at all: the floor.</li>
 *   <li><b>ping, over the wire</b> - a command that does no data work: wire encode/decode,
 *       socket syscalls, command dispatch, reply.</li>
 *   <li><b>insert, over the wire</b> - the real thing. Minus "ping" this is the insert's share,
 *       minus "in-process insert" the transport's.</li>
 * </ul>
 *
 * <p>It then A/Bs the three things the client wire path does per message that look avoidable:
 * no {@code TCP_NODELAY} on the client socket (the server sets it, the client never does), a
 * {@code setSoTimeout} syscall before every single read, and an unbuffered socket stream that
 * costs one read syscall for the 16-byte header and another for the body.
 *
 * <p>Manual: {@code mvn -o -pl poppydb test -Dtest=WireRoundtripCostBenchmark -Dtest.excludeTags=}
 * Results are printed as greppable {@code WIRECOST } lines.
 */
@Tag("manual")
public class WireRoundtripCostBenchmark {

    private static final Logger log = LoggerFactory.getLogger(WireRoundtripCostBenchmark.class);
    private static final int WARMUP = 2000;
    private static final int OPS = 20000;
    private static final AtomicInteger MSG_ID = new AtomicInteger(1);

    private PoppyDB server;

    @AfterEach
    public void tearDown() {
        if (server != null) {
            server.shutdown();
            server = null;
        }
    }

    /** One configuration of the client side of the wire. */
    private record WireConfig(String label, boolean tcpNoDelay, boolean bufferedIn,
                              boolean setSoTimeoutPerRead) {}

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private PoppyDB startServer() throws Exception {
        int port = freePort();
        PoppyDB srv = new PoppyDB(port, "127.0.0.1", 100, 10);
        srv.start();
        long deadline = System.currentTimeMillis() + 10_000;

        while (true) {
            try (Socket probe = new Socket()) {
                probe.connect(new InetSocketAddress("127.0.0.1", port), 250);
                break;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }

                Thread.sleep(50);
            }
        }

        return srv;
    }

    /** Median of repeated measurements - the mean is too easy to skew with one GC pause. */
    private static double median(List<Double> values) {
        List<Double> sorted = new ArrayList<>(values);
        sorted.sort(null);
        int n = sorted.size();
        return n % 2 == 1 ? sorted.get(n / 2) : (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2;
    }

    private static void report(String what, double microsPerOp) {
        log.info(String.format(Locale.ROOT, "WIRECOST %-38s %8.1f µs/op   (%.0f ops/s)",
                what, microsPerOp, 1_000_000.0 / microsPerOp));
    }

    /** Round trips one command per iteration and returns µs/op. */
    private double measureWire(int port, WireConfig cfg, boolean insert, int ops) throws Exception {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", port), 2000);
        s.setTcpNoDelay(cfg.tcpNoDelay());
        s.setSoTimeout(15000);

        try (OutputStream out = s.getOutputStream()) {
            InputStream in = cfg.bufferedIn()
                ? new BufferedInputStream(s.getInputStream(), 16 * 1024)
                : s.getInputStream();

            for (int i = 0; i < WARMUP; i++) {
                roundtrip(s, out, in, cfg, insert, i);
            }

            long start = System.nanoTime();

            for (int i = 0; i < ops; i++) {
                roundtrip(s, out, in, cfg, insert, i);
            }

            long elapsed = System.nanoTime() - start;
            return elapsed / 1000.0 / ops;
        } finally {
            s.close();
        }
    }

    private void roundtrip(Socket s, OutputStream out, InputStream in, WireConfig cfg,
                           boolean insert, int i) throws Exception {
        OpMsg msg = new OpMsg();
        msg.setMessageId(MSG_ID.incrementAndGet());
        msg.setFlags(0);
        msg.setFirstDoc(insert
            ? Doc.of("insert", "wirebench",
                     "documents", List.of(Doc.of("_id", i, "v", "payload")),
                     "$db", "wirecost")
            : Doc.of("ping", 1, "$db", "admin"));
        out.write(msg.bytes());
        out.flush();

        // what SingleMongoConnection.readNextMessage does before every read
        if (cfg.setSoTimeoutPerRead()) {
            s.setSoTimeout(15000);
        }

        WireProtocolMessage reply = WireProtocolMessage.parseFromStream(in);

        if (reply == null) {
            throw new IllegalStateException("no reply");
        }
    }

    @Test
    public void whereDoesThePerMessageCostGo() throws Exception {
        // ---- floor: the insert itself, no wire ----
        InMemoryDriver direct = new InMemoryDriver();
        direct.connect();
        double inProcess;

        try {
            for (int i = 0; i < WARMUP; i++) {
                direct.store("wirecost", "inproc", List.of(Doc.of("_id", i, "v", "payload")), null);
            }

            long start = System.nanoTime();

            for (int i = 0; i < OPS; i++) {
                direct.store("wirecost", "inproc", List.of(Doc.of("_id", i, "v", "payload")), null);
            }

            inProcess = (System.nanoTime() - start) / 1000.0 / OPS;
        } finally {
            direct.close();
        }

        report("insert, in-process (no wire)", inProcess);

        // ---- over the wire, as the driver does it today ----
        server = startServer();
        int port = server.getPort();
        WireConfig asIs = new WireConfig("as the driver does it", false, false, true);
        double pingAsIs = measureWire(port, asIs, false, OPS);
        double insertAsIs = measureWire(port, asIs, true, OPS);
        report("ping, over the wire", pingAsIs);
        report("insert, over the wire", insertAsIs);
        report("  -> transport share of an insert", pingAsIs);
        report("  -> insert share over the wire", insertAsIs - pingAsIs);
        report("  -> insert cost in-process", inProcess);

        // ---- A/B the three avoidable per-message costs on the client ----
        List<WireConfig> configs = List.of(
            asIs,
            new WireConfig("+ TCP_NODELAY", true, false, true),
            new WireConfig("+ buffered input", false, true, true),
            new WireConfig("+ no setSoTimeout per read", false, false, false),
            new WireConfig("all three", true, true, false));

        for (WireConfig cfg : configs) {
            List<Double> runs = new ArrayList<>();

            for (int r = 0; r < 3; r++) {
                runs.add(measureWire(port, cfg, true, OPS));
            }

            report("insert, " + cfg.label(), median(runs));
        }

        // ---- and now the layers the application actually goes through ----
        // A raw round trip is ~50µs, but the messaging benchmark plateaus around 320µs per
        // sendMessage single-threaded. This locates the difference: how much is the driver
        // (pooling, object mapping, write concern) and how much is the messaging layer on top.
        measureClientStack(port);
    }

    /**
     * Follow-up to the JFR profile on #327: it found the store/raw-insert gap is dominated by a
     * per-op wait (a condition node allocated on nearly every store), not by mapping compute, and
     * suspected the writer queue or the pool checkout.
     *
     * <p>WARNING on the wall-clock numbers this method prints: they are useful only as a rough
     * shape. On a busy workstation they swing by more than the ~25µs effect under investigation
     * (observed across runs: raw insert 65-129µs, and once a Msg-sized document measured FASTER
     * than a tiny one, which is impossible and simply means the noise won). The COUNTERS are the
     * point here - they are exact regardless of scheduling.
     *
     * <p>The writer queue is ruled out by reading the code: {@code submitAndBlockIfNecessary}
     * runs the operation INLINE when no callback is given, so a synchronous store never hands
     * off to an executor. That leaves the pool - and the pool can be asked directly instead of
     * inferred from allocation samples: {@code queue.poll(100ms)} only allocates a condition node
     * when the queue is EMPTY, so if the borrow really waits, it must show up as borrows without
     * an idle connection.
     */
    @Test
    public void howTheConnectionPoolBehavesDuringStore() throws Exception {
        server = startServer();
        int ops = 3000;

        try (Morphium m = new Morphium(cfg("wirecost_pool", port(server)))) {
            Thread.sleep(500);   // let the pool settle at minConnections

            int id = 0;

            for (int i = 0; i < 300; i++) {
                rawInsert(m, id++);
            }

            Map<MorphiumDriver.DriverStatsKey, Double> before = m.getDriver().getDriverStats();
            long start = System.nanoTime();

            for (int i = 0; i < ops; i++) {
                rawInsert(m, id++);
            }

            double rawUs = (System.nanoTime() - start) / 1000.0 / ops;
            Map<MorphiumDriver.DriverStatsKey, Double> afterRaw = m.getDriver().getDriverStats();

            for (int i = 0; i < 300; i++) {
                m.store(benchMsg());
            }

            start = System.nanoTime();

            for (int i = 0; i < ops; i++) {
                m.store(benchMsg());
            }

            double storeUs = (System.nanoTime() - start) / 1000.0 / ops;
            Map<MorphiumDriver.DriverStatsKey, Double> afterStore = m.getDriver().getDriverStats();

            // ---- the control, done properly: interleaved, repeated, same collection ----
            // Sequential phases into different collections cannot resolve a ~25µs effect - they
            // differ in collection size and in JIT/GC state, and one run each gives no spread to
            // judge by. So: alternate the two variants in blocks, several rounds, both writing
            // into the SAME collection, and compare medians.
            interleavedDocumentControl(m, ops);

            // Same wire traffic, same borrows - so is the rest simply the DOCUMENT? A Msg maps to
            // far more fields than {_id, v}, and a bigger document costs more on the server too
            // (insert, index maintenance, change-stream event materialisation). Insert exactly
            // the document store(Msg) would send, with the mapping done OUTSIDE the timed loop.
            List<Map<String, Object>> premapped = new ArrayList<>(ops + 300);

            for (int i = 0; i < ops + 300; i++) {
                Map<String, Object> mapped = m.getMapper().serialize(benchMsg());
                mapped.put("_id", "pre-" + i);
                premapped.add(mapped);
            }

            for (int i = 0; i < 300; i++) {
                rawInsertDoc(m, premapped.get(i));
            }

            start = System.nanoTime();

            for (int i = 0; i < ops; i++) {
                rawInsertDoc(m, premapped.get(300 + i));
            }

            double premappedUs = (System.nanoTime() - start) / 1000.0 / ops;

            // THREADS_WAITING_FOR_CONNECTION is an instantaneous gauge, not a cumulative
            // counter - read after the loop it is always 0 and proves nothing. Sample it from a
            // second thread WHILE the loop runs: if the store path really waits ~22µs of every
            // ~145µs op, roughly one sample in seven should catch a waiter.
            for (int i = 0; i < 300; i++) {
                m.store(benchMsg());
            }

            java.util.concurrent.atomic.AtomicBoolean sampling = new java.util.concurrent.atomic.AtomicBoolean(true);
            java.util.concurrent.atomic.AtomicInteger samples = new java.util.concurrent.atomic.AtomicInteger();
            java.util.concurrent.atomic.AtomicInteger sawWaiter = new java.util.concurrent.atomic.AtomicInteger();
            Thread sampler = new Thread(() -> {
                while (sampling.get()) {
                    double waiting = m.getDriver().getDriverStats()
                        .getOrDefault(MorphiumDriver.DriverStatsKey.THREADS_WAITING_FOR_CONNECTION, 0.0);
                    samples.incrementAndGet();

                    if (waiting > 0) {
                        sawWaiter.incrementAndGet();
                    }

                    java.util.concurrent.locks.LockSupport.parkNanos(200_000);   // ~0.2ms
                }
            }, "pool-gauge-sampler");
            sampler.setDaemon(true);
            sampler.start();

            for (int i = 0; i < ops; i++) {
                m.store(benchMsg());
            }

            sampling.set(false);
            sampler.join(2000);
            log.info("WIRECOST-POOL waiters during store: {} of {} samples saw a thread waiting for a connection",
                     sawWaiter.get(), samples.get());

            report("raw insert, tiny doc", rawUs);
            report("raw insert, Msg-sized doc (no mapping)", premappedUs);
            report("morphium.store(Msg)", storeUs);
            reportStats("raw insert   ", before, afterRaw, ops);
            reportStats("store(Msg)   ", afterRaw, afterStore, ops);
        }
    }

    /**
     * Is the store/raw-insert gap the mapping, or simply the bigger document? Inserts the exact
     * document {@code store(Msg)} would send - mapped OUTSIDE the timed loop - against
     * {@code store(Msg)} itself, into the same collection, alternating in blocks so collection
     * growth and JIT state affect both equally.
     */
    private void interleavedDocumentControl(Morphium m, int ops) throws Exception {
        String coll = m.getMapper().getCollectionName(Msg.class);
        int rounds = 6;
        int block = 1000;
        List<Double> premappedRuns = new ArrayList<>();
        List<Double> storeRuns = new ArrayList<>();

        // pre-map everything up front: the mapping cost must not land in the timed loop
        List<Map<String, Object>> docs = new ArrayList<>(rounds * block);

        for (int i = 0; i < rounds * block; i++) {
            Map<String, Object> mapped = m.getMapper().serialize(benchMsg());
            mapped.put("_id", "ctl-" + i);
            docs.add(mapped);
        }

        // warm both paths before the first measured block
        for (int i = 0; i < 300; i++) {
            m.store(benchMsg());
        }

        int docIdx = 0;
        // GC is the one cost that is neither CPU-on-main nor a park: a stop-the-world pause stops
        // main without sampling it as busy and without a park event, so it lands exactly in the
        // "wall - CPU - park" bucket the profile could not account for. The store path allocates
        // ~4x as much, so this is worth counting rather than assuming.
        long premappedGcMs = 0;
        long premappedGcCount = 0;
        long storeGcMs = 0;
        long storeGcCount = 0;

        // Exact per-thread CPU time, not 20ms execution sampling: the work runs inline on this
        // thread, so this settles "is the gap compute or waiting" without inference.
        //
        // Use ThreadMXBean, NOT JFR, for this: jdk.ThreadCPULoad was found to report ~0.00s of
        // CPU for a thread that the kernel says burns tens of µs per op (macOS, JDK 21), and
        // jdk.ExecutionSample under-delivered by ~8x in the same run (113 samples where ~935
        // were due). Any "hidden wait" computed as wall - CPU - park on top of those numbers is
        // an artifact of the subtraction, not a stall.
        int totalOps = rounds * block;
        java.lang.management.ThreadMXBean threads = java.lang.management.ManagementFactory.getThreadMXBean();
        long premappedCpuNanos = 0;
        long storeCpuNanos = 0;

        for (int round = 0; round < rounds; round++) {
            long[] gcBefore = gcTotals();
            long cpuBefore = threads.getCurrentThreadCpuTime();
            long start = System.nanoTime();

            for (int i = 0; i < block; i++) {
                rawInsertDoc(m, coll, docs.get(docIdx++));
            }

            premappedRuns.add((System.nanoTime() - start) / 1000.0 / block);
            premappedCpuNanos += threads.getCurrentThreadCpuTime() - cpuBefore;
            long[] gcMid = gcTotals();
            premappedGcCount += gcMid[0] - gcBefore[0];
            premappedGcMs += gcMid[1] - gcBefore[1];
            cpuBefore = threads.getCurrentThreadCpuTime();
            start = System.nanoTime();

            for (int i = 0; i < block; i++) {
                m.store(benchMsg());
            }

            storeRuns.add((System.nanoTime() - start) / 1000.0 / block);
            storeCpuNanos += threads.getCurrentThreadCpuTime() - cpuBefore;
            long[] gcAfter = gcTotals();
            storeGcCount += gcAfter[0] - gcMid[0];
            storeGcMs += gcAfter[1] - gcMid[1];
        }

        log.info(String.format(Locale.ROOT,
                "WIRECOST-CONTROL CPU on this thread  premapped: %.1f µs/op   store(Msg): %.1f µs/op"
                + "   -> CPU difference %.1f µs/op",
                premappedCpuNanos / 1000.0 / totalOps, storeCpuNanos / 1000.0 / totalOps,
                (storeCpuNanos - premappedCpuNanos) / 1000.0 / totalOps));
        log.info(String.format(Locale.ROOT,
                "WIRECOST-CONTROL GC  premapped: %d collections, %d ms (%.1f µs/op)   "
                + "store(Msg): %d collections, %d ms (%.1f µs/op)",
                premappedGcCount, premappedGcMs, premappedGcMs * 1000.0 / totalOps,
                storeGcCount, storeGcMs, storeGcMs * 1000.0 / totalOps));

        double premapped = median(premappedRuns);
        double store = median(storeRuns);
        report("CONTROL raw insert of the mapped Msg doc", premapped);
        report("CONTROL morphium.store(Msg)", store);
        report("CONTROL  -> difference (the mapping's own cost)", store - premapped);
        log.info("WIRECOST-CONTROL premapped runs={} store runs={}", premappedRuns, storeRuns);
    }

    /** {collection count, total collection time in ms} across all collectors. */
    private static long[] gcTotals() {
        long count = 0;
        long millis = 0;

        for (java.lang.management.GarbageCollectorMXBean b
                : java.lang.management.ManagementFactory.getGarbageCollectorMXBeans()) {
            if (b.getCollectionCount() > 0) {
                count += b.getCollectionCount();
                millis += b.getCollectionTime();
            }
        }

        return new long[] {count, millis};
    }

    private void rawInsertDoc(Morphium m, String coll, Map<String, Object> doc) throws Exception {
        InsertMongoCommand cmd = new InsertMongoCommand(m.getDriver().getPrimaryConnection(null))
            .setDb("wirecost_pool").setColl(coll)
            .setDocuments(List.of(doc));

        try {
            cmd.execute();
        } finally {
            cmd.releaseConnection();
        }
    }

    private void rawInsertDoc(Morphium m, Map<String, Object> doc) throws Exception {
        InsertMongoCommand cmd = new InsertMongoCommand(m.getDriver().getPrimaryConnection(null))
            .setDb("wirecost_pool").setColl("premapped")
            .setDocuments(List.of(doc));

        try {
            cmd.execute();
        } finally {
            cmd.releaseConnection();
        }
    }

    private static int port(PoppyDB srv) {
        return srv.getPort();
    }

    private void reportStats(String label, Map<MorphiumDriver.DriverStatsKey, Double> before,
                             Map<MorphiumDriver.DriverStatsKey, Double> after, int ops) {
        StringBuilder sb = new StringBuilder("WIRECOST-POOL ").append(label);

        for (MorphiumDriver.DriverStatsKey k : List.of(
                MorphiumDriver.DriverStatsKey.CONNECTIONS_BORROWED,
                MorphiumDriver.DriverStatsKey.CONNECTIONS_RELEASED,
                MorphiumDriver.DriverStatsKey.CONNECTIONS_OPENED,
                MorphiumDriver.DriverStatsKey.CONNECTIONS_CLOSED,
                MorphiumDriver.DriverStatsKey.THREADS_WAITING_FOR_CONNECTION,
                MorphiumDriver.DriverStatsKey.MSG_SENT)) {
            double delta = after.getOrDefault(k, 0.0) - before.getOrDefault(k, 0.0);
            sb.append(String.format(Locale.ROOT, "  %s=%.0f (%.2f/op)", k, delta, delta / ops));
        }

        log.info(sb.toString());
    }

    private MorphiumConfig cfg(String db, int port) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings().setDatabase(db);
        cfg.clusterSettings().getHostSeed().clear();
        cfg.clusterSettings().addHostToSeed("127.0.0.1:" + port);
        cfg.driverSettings().setDriverName("PooledDriver");
        cfg.connectionSettings().setMaxConnections(20).setMinConnections(2);
        return cfg;
    }

    private void rawInsert(Morphium m, int i) throws Exception {
        InsertMongoCommand cmd = new InsertMongoCommand(m.getDriver().getPrimaryConnection(null))
            .setDb("wirecost_stack").setColl("raw")
            .setDocuments(List.of(Doc.of("_id", i, "v", "x")));

        try {
            cmd.execute();
        } finally {
            cmd.releaseConnection();   // borrowed from the pool - 20 leaks and it is empty
        }
    }

    private Msg benchMsg() {
        Msg msg = new Msg("wirecost", "bench", "x", 300_000);
        msg.setSender("bench-sender");
        msg.setSenderHost("localhost");
        return msg;
    }

    private void measureClientStack(int port) throws Exception {
        int ops = 3000;   // this path is ~10x slower, so fewer iterations

        try (Morphium m = new Morphium(cfg("wirecost_stack", port))) {
            // (a) driver-level insert of a plain Doc: pool checkout + wire + server insert
            int id = 0;

            for (int i = 0; i < 500; i++) {
                rawInsert(m, id++);
            }

            long start = System.nanoTime();

            for (int i = 0; i < ops; i++) {
                rawInsert(m, id++);
            }

            report("insert via PooledDriver (no mapping)", (System.nanoTime() - start) / 1000.0 / ops);

            // (b) morphium.store of a Msg entity: adds the entity pipeline and object mapping,
            // but none of the messaging layer. Msg refuses to be stored without a sender
            // (@PreStore), which messaging would normally fill in.
            for (int i = 0; i < 500; i++) {
                m.store(benchMsg());
            }

            start = System.nanoTime();

            for (int i = 0; i < ops; i++) {
                m.store(benchMsg());
            }

            report("morphium.store(Msg) (mapping)", (System.nanoTime() - start) / 1000.0 / ops);

            // (c) the real thing: messaging on top
            MorphiumMessaging sender = m.createMessaging();
            sender.start();

            try {
                for (int i = 0; i < 500; i++) {
                    sender.sendMessage(new Msg("wirecost", "bench", "x", 300_000));
                }

                start = System.nanoTime();

                for (int i = 0; i < ops; i++) {
                    sender.sendMessage(new Msg("wirecost", "bench", "x", 300_000));
                }

                report("messaging.sendMessage (no receiver)", (System.nanoTime() - start) / 1000.0 / ops);

                // (d) the same send, but with somebody watching the collection. A plain change
                // stream (no messaging) isolates the server's fan-out work on the write path
                // from everything a real receiver does on top of it.
                ChangeStreamMonitor watcher = new ChangeStreamMonitor(m, sender.getCollectionName(), true);
                watcher.addListener(evt -> true);
                watcher.start();

                try {
                    Thread.sleep(1500);   // let the watch register before the clock starts

                    for (int i = 0; i < 300; i++) {
                        sender.sendMessage(new Msg("wirecost", "bench", "x", 300_000));
                    }

                    start = System.nanoTime();

                    for (int i = 0; i < ops; i++) {
                        sender.sendMessage(new Msg("wirecost", "bench", "x", 300_000));
                    }

                    report("messaging.sendMessage (1 change stream)",
                           (System.nanoTime() - start) / 1000.0 / ops);
                } finally {
                    watcher.terminate();
                }
            } finally {
                sender.terminate();
            }
        }

        // (e) the real benchmark shape: a second Morphium with a messaging receiver that
        // actually consumes the topic.
        try (Morphium sendM = new Morphium(cfg("wirecost_stack", port));
             Morphium recvM = new Morphium(cfg("wirecost_stack", port))) {
            MorphiumMessaging receiver = recvM.createMessaging();
            java.util.concurrent.atomic.AtomicInteger received = new java.util.concurrent.atomic.AtomicInteger();
            receiver.addListenerForTopic("wirecost_recv", (mq, msg) -> {
                received.incrementAndGet();
                return null;
            });
            receiver.start();

            MorphiumMessaging sender2 = sendM.createMessaging();
            sender2.start();

            try {
                Thread.sleep(3000);   // both sides registered, as the one-way benchmark does

                for (int i = 0; i < 300; i++) {
                    sender2.sendMessage(new Msg("wirecost_recv", "bench", "x", 300_000));
                }

                long start2 = System.nanoTime();

                for (int i = 0; i < ops; i++) {
                    sender2.sendMessage(new Msg("wirecost_recv", "bench", "x", 300_000));
                }

                report("messaging.sendMessage (live receiver)",
                       (System.nanoTime() - start2) / 1000.0 / ops);
                log.info("WIRECOST   receiver consumed {} of {}", received.get(), ops + 300);
            } finally {
                sender2.terminate();
                receiver.terminate();
            }
        }
    }
}
