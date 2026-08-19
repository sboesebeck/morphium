package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;

/**
 * BATCH send throughput: does routing {@code Msg} inserts through Morphium's
 * {@code @WriteBuffer} (which flushes as real bulk-insert requests, see
 * {@code BufferedMorphiumWriterImpl#flushQueueToMongo}) actually raise the pure send-side
 * throughput of {@code sendMessage()}, and at what cost to receipt latency?
 *
 * <p>This is the counterpart to {@link MessagingOneWayThroughputBenchmark}'s unbatched
 * numbers, and to the README's Kafka-vs-Morphium batching discussion ("no system reaches
 * [100K+ msg/s] without batching") — same idea, applied to Morphium+PoppyDB/MongoDB
 * themselves via a custom {@code Msg} subclass instead of a different broker.
 *
 * <p>Seven runs sequentially per backend, each on its own topic against the same listener:
 * plain {@code Msg} (unbatched baseline, one insert per {@code sendMessage()}); {@link
 * BatchMsg10}/{@link BatchMsg100} ({@code @WriteBuffer(size=10/100)}, poll-and-WAIT batching)
 * each at two {@code writeBufferTimeGranularity} settings — Morphium's out-of-the-box default
 * (100ms — the housekeeping thread's poll interval, and therefore a hard throughput ceiling
 * of roughly {@code size / 0.1s} once producers outrun it) and a tightened value (5ms), to
 * separate "does @WriteBuffer help at all" from "does it help once the one global knob that
 * gates it is tuned"; and {@code bulk10}/{@code bulk100} — genuine client-driven batching via
 * {@code morphium.insert(List<Msg>, ...)} with no callback (synchronous, one bulk-insert wire
 * command per chunk, no housekeeping poll or WAIT-blocking involved at all — the
 * Kafka-producer-style shape). For each: {@code sendRate} is measured from the first to the
 * last returning send call (the "pure send throughput" batching targets); {@code
 * endToEndRate} / last-message latency is measured to the receiver instead, to put a number
 * on the necessary tradeoff — a buffered/batched message isn't visible to a change-stream
 * listener until its batch is actually written.
 *
 * <p>Tagged {@code manual}: a benchmark, not a regression test. Run explicitly:
 *
 * <pre>
 *   # PoppyDB (in-process server):
 *   mvn -pl morphium-core,poppydb -am surefire:test \
 *     -Dtest=MessagingBatchSendThroughputBenchmark#batchSendThroughputPoppyDB -Dtest.excludeTags=
 *
 *   # MongoDB (external, e.g. local single-node RS on localhost:27017):
 *   mvn -pl morphium-core,poppydb -am surefire:test \
 *     -Dtest=MessagingBatchSendThroughputBenchmark#batchSendThroughputMongoDB -Dtest.excludeTags= \
 *     -Dmorphium.uri=mongodb://localhost:27017/morphium_tests
 * </pre>
 *
 * Results are printed as greppable lines: {@code BATCH-RESULT backend=... variant=...}.
 */
@Tag("manual")
public class MessagingBatchSendThroughputBenchmark {

    private static final int MESSAGES = 5000;
    private static final int SENDER_THREADS = 4;
    private static final long RECEIVE_DEADLINE_MS = 300_000;
    // @WriteBuffer's timeout is dual-purpose: (a) periodic flush-by-age fallback in the
    // housekeeping thread, so the last, possibly-incomplete batch doesn't linger forever,
    // and (b) the max time a blocked producer (STRATEGY.WAIT, buffer at capacity) waits for
    // the housekeeping thread to drain it before giving up with a RuntimeException. At 4
    // sender threads hammering a 10/100-sized buffer, the buffer saturates in well under a
    // millisecond and stays saturated until the next housekeeping tick - this needs
    // headroom, not a short interval. MongoDB (network + majority-ack per bulk flush) is
    // measurably slower per flush than PoppyDB in-process - 5s was enough there but too
    // tight against a local RS (observed: "maxWaitTime/timeout exceeded" on batch100); 20s
    // is generous headroom for both, since @WriteBuffer's timeout is one shared,
    // compile-time-fixed value per entity class across both backends.
    private static final int FLUSH_TIMEOUT_MS = 20_000;
    // The housekeeping thread only drains a buffer on its poll tick, so its interval acts as
    // a hard throughput ceiling of roughly (size / granularitySeconds) msg/s - see the
    // "batching backfires at default granularity" finding in docs/v5-vs-v6-performance.md.
    // Batched variants run once at Morphium's default (100ms, a naive @WriteBuffer-only
    // setup) and once with the housekeeping poll tightened, to separate "does @WriteBuffer
    // help at all" from "does it help once the one global knob that gates it is tuned".
    private static final int DEFAULT_GRANULARITY_MS = 100;
    private static final int TUNED_GRANULARITY_MS = 5;

    /** Unbatched baseline: one insert per sendMessage(), same as plain Msg. */
    @Entity(polymorph = true)
    public static class UnbatchedMsg extends Msg {
        public UnbatchedMsg() { super(); }
        public UnbatchedMsg(String topic, String msg, String value, long ttl) { super(topic, msg, value, ttl); }
    }

    @Entity(polymorph = true)
    @WriteBuffer(size = 10, timeout = FLUSH_TIMEOUT_MS, strategy = WriteBuffer.STRATEGY.WAIT)
    public static class BatchMsg10 extends Msg {
        public BatchMsg10() { super(); }
        public BatchMsg10(String topic, String msg, String value, long ttl) { super(topic, msg, value, ttl); }
    }

    @Entity(polymorph = true)
    @WriteBuffer(size = 100, timeout = FLUSH_TIMEOUT_MS, strategy = WriteBuffer.STRATEGY.WAIT)
    public static class BatchMsg100 extends Msg {
        public BatchMsg100() { super(); }
        public BatchMsg100(String topic, String msg, String value, long ttl) { super(topic, msg, value, ttl); }
    }

    private enum Variant {
        UNBATCHED("unbatched", 0, false) {
            @Override Msg create(String topic) { return new UnbatchedMsg(topic, "bench", "x", 300_000); }
        },
        BATCH_10("batch10", 10, false) {
            @Override Msg create(String topic) { return new BatchMsg10(topic, "bench", "x", 300_000); }
        },
        BATCH_100("batch100", 100, false) {
            @Override Msg create(String topic) { return new BatchMsg100(topic, "bench", "x", 300_000); }
        },
        // Genuine client-driven batching: no @WriteBuffer, no housekeeping poll, no WAIT
        // blocking - just morphium.insert(List<Msg>, ...) with a null callback, which
        // MorphiumWriterImpl runs SYNCHRONOUSLY in the calling thread (submitAndBlockIfNecessary)
        // as ONE bulk-insert wire command per chunk. This is the Kafka-producer-style shape:
        // the client decides the batch, one round-trip carries it, no server-side polling
        // in between. Plain Msg (no @WriteBuffer) so a stray sendMessage() elsewhere would
        // still behave normally.
        BULK_10("bulk10", 10, true) {
            @Override Msg create(String topic) { return new UnbatchedMsg(topic, "bench", "x", 300_000); }
        },
        BULK_100("bulk100", 100, true) {
            @Override Msg create(String topic) { return new UnbatchedMsg(topic, "bench", "x", 300_000); }
        };

        final String label;
        final int batchSize;
        final boolean bulk;

        Variant(String label, int batchSize, boolean bulk) {
            this.label = label;
            this.batchSize = batchSize;
            this.bulk = bulk;
        }

        abstract Msg create(String topic);
    }

    private PoppyDB server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.shutdown();
            server = null;
        }
    }

    @Test
    public void batchSendThroughputPoppyDB() throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        server = new PoppyDB(port, "localhost", 100, 10);
        server.start();
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("localhost", port), 250);
                break;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) throw e;
                Thread.sleep(50);
            }
        }

        runAllVariants("poppydb", "localhost:" + port);
    }

    @Test
    public void batchSendThroughputMongoDB() throws Exception {
        String uri = System.getProperty("morphium.uri", System.getenv("MONGODB_URI"));
        assumeTrue(uri != null && !uri.isBlank(),
            "no external MongoDB configured - pass -Dmorphium.uri=mongodb://host1,host2/db");

        String hostsPart = uri.replaceFirst("^mongodb://", "");
        if (hostsPart.contains("/")) {
            hostsPart = hostsPart.substring(0, hostsPart.indexOf('/'));
        }
        runAllVariants("mongodb", hostsPart.split(","));
    }

    private void runAllVariants(String backendLabel, String... hostSeed) throws Exception {
        String db = "batch_bench_" + System.currentTimeMillis();

        // Unbatched baseline and genuine client-driven bulk insert: neither involves
        // @WriteBuffer, granularity is irrelevant to both - run once under the default config.
        try (Morphium receiverMorphium = new Morphium(cfg(db, DEFAULT_GRANULARITY_MS, hostSeed));
             Morphium senderMorphium = new Morphium(cfg(db, DEFAULT_GRANULARITY_MS, hostSeed))) {
            runVariant(backendLabel, Variant.UNBATCHED, DEFAULT_GRANULARITY_MS, receiverMorphium, senderMorphium);
            runVariant(backendLabel, Variant.BULK_10, DEFAULT_GRANULARITY_MS, receiverMorphium, senderMorphium);
            runVariant(backendLabel, Variant.BULK_100, DEFAULT_GRANULARITY_MS, receiverMorphium, senderMorphium);
        }

        // Batched variants: once at Morphium's out-of-the-box default (naive "just add
        // @WriteBuffer" setup), once with the housekeeping poll tightened - separates
        // "does @WriteBuffer help at all" from "does it help once tuned".
        for (int granularityMs : new int[] {DEFAULT_GRANULARITY_MS, TUNED_GRANULARITY_MS}) {
            try (Morphium receiverMorphium = new Morphium(cfg(db, granularityMs, hostSeed));
                 Morphium senderMorphium = new Morphium(cfg(db, granularityMs, hostSeed))) {
                runVariant(backendLabel, Variant.BATCH_10, granularityMs, receiverMorphium, senderMorphium);
                runVariant(backendLabel, Variant.BATCH_100, granularityMs, receiverMorphium, senderMorphium);
            }
        }
    }

    private void runVariant(String backendLabel, Variant variant, int granularityMs,
        Morphium receiverMorphium, Morphium senderMorphium) throws Exception {
        String topic = "batchbench-" + variant.label + "-g" + granularityMs;
        MorphiumMessaging receiver = receiverMorphium.createMessaging();
        AtomicInteger received = new AtomicInteger();
        AtomicLong lastReceiptNanos = new AtomicLong();
        receiver.addListenerForTopic(topic, (mq, msg) -> {
            received.incrementAndGet();
            lastReceiptNanos.set(System.nanoTime());
            return null; // one-way: never answer
        });
        receiver.start();

        MorphiumMessaging sender = senderMorphium.createMessaging();
        sender.start();

        // let both messaging instances register their change streams before the clock starts
        Thread.sleep(3000);

        long start = System.nanoTime();

        String senderId = sender.getSenderId();
        String collectionName = sender.getCollectionName();
        List<Thread> senders = new ArrayList<>();
        java.util.concurrent.atomic.AtomicReference<Throwable> senderFailure = new java.util.concurrent.atomic.AtomicReference<>();
        int perThread = MESSAGES / SENDER_THREADS;
        for (int t = 0; t < SENDER_THREADS; t++) {
            Thread worker = new Thread(() -> {
                try {
                    if (variant.bulk) {
                        int remaining = perThread;
                        while (remaining > 0) {
                            int chunkSize = Math.min(variant.batchSize, remaining);
                            List<Msg> chunk = new ArrayList<>(chunkSize);
                            for (int c = 0; c < chunkSize; c++) {
                                Msg m = variant.create(topic);
                                m.setSender(senderId);
                                m.setSenderHost("bulk-bench");
                                chunk.add(m);
                            }
                            // null callback -> submitAndBlockIfNecessary runs this
                            // synchronously in THIS thread as one bulk-insert wire command
                            senderMorphium.insert(chunk, collectionName, null);
                            remaining -= chunkSize;
                        }
                    } else {
                        for (int i = 0; i < perThread; i++) {
                            sender.sendMessage(variant.create(topic));
                        }
                    }
                } catch (Throwable t2) {
                    senderFailure.compareAndSet(null, t2);
                }
            }, "batch-sender-" + variant.label + "-g" + granularityMs + "-" + t);
            senders.add(worker);
            worker.start();
        }
        for (Thread worker : senders) {
            worker.join();
        }
        long sendDoneNanos = System.nanoTime() - start;

        if (senderFailure.get() != null) {
            throw new RuntimeException("sender thread failed for variant " + variant.label
                + " granularity=" + granularityMs + "ms - backend " + backendLabel, senderFailure.get());
        }

        int expected = perThread * SENDER_THREADS;
        long receiveDeadline = System.currentTimeMillis() + RECEIVE_DEADLINE_MS;
        while (received.get() < expected && System.currentTimeMillis() < receiveDeadline) {
            Thread.sleep(50);
        }
        long totalNanos = lastReceiptNanos.get() - start;

        assertEquals(expected, received.get(),
            "every sent message must arrive (batch variant " + variant.label + " granularity="
            + granularityMs + "ms) - backend " + backendLabel);

        double sendRate = expected / (sendDoneNanos / 1e9);
        double endToEndRate = expected / (totalNanos / 1e9);
        double lastMsgLatencyMs = (lastReceiptNanos.get() - (start + sendDoneNanos)) / 1e6;
        System.out.printf(
            "BATCH-RESULT backend=%s variant=%s batchSize=%d granularityMs=%d messages=%d "
            + "senderThreads=%d sendSeconds=%.2f sendRate=%.0f msg/s endToEndSeconds=%.2f "
            + "endToEndRate=%.0f msg/s lastMsgLatencyAfterSendMs=%.1f%n",
            backendLabel, variant.label, variant.batchSize, granularityMs, expected, SENDER_THREADS,
            sendDoneNanos / 1e9, sendRate, totalNanos / 1e9, endToEndRate, lastMsgLatencyMs);

        receiver.terminate();
        sender.terminate();
    }

    private MorphiumConfig cfg(String db, int granularityMs, String... hostSeed) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings().setDatabase(db);
        cfg.clusterSettings().getHostSeed().clear();
        for (String h : hostSeed) {
            cfg.clusterSettings().addHostToSeed(h);
        }
        cfg.driverSettings().setDriverName("PooledDriver");
        cfg.connectionSettings().setMaxConnections(20).setMinConnections(2);
        cfg.writerSettings().setWriteBufferTimeGranularity(granularityMs);
        return cfg;
    }
}
