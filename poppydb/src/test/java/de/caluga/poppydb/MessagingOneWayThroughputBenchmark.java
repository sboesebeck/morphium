package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;

/**
 * ONE-WAY messaging throughput: N messages from a sender to a single listening receiver,
 * measured from first send to last RECEIPT — no replies, no request/response round-trip.
 * This is the counterpart to the round-trip (ping-pong) numbers in
 * docs/v5-vs-v6-performance.md ("Messaging Performance by Backend": 89 msg/s MongoDB RS,
 * 223 msg/s PoppyDB) and exists to give the README comparison table a SOURCED one-way figure
 * per backend instead of the historic, no-longer-reproducible "~8K msg/s" claim.
 *
 * <p>Tagged {@code manual}: this is a benchmark, not a regression test — its assertions only
 * pin that every message arrived, never a rate (rates depend entirely on the host). Run it
 * explicitly, ideally on the same infrastructure as the other benchmark numbers:
 *
 * <pre>
 *   # PoppyDB (in-process server):
 *   mvn -pl morphium-core,poppydb -am surefire:test \
 *     -Dtest=MessagingOneWayThroughputBenchmark#oneWayThroughputPoppyDB -Dtest.excludeTags=
 *
 *   # MongoDB (external, e.g. the 3-node homelab RS):
 *   mvn -pl morphium-core,poppydb -am surefire:test \
 *     -Dtest=MessagingOneWayThroughputBenchmark#oneWayThroughputMongoDB -Dtest.excludeTags= \
 *     -Dmorphium.uri=mongodb://mongo1:27017,mongo2:27017/morphium_tests
 * </pre>
 *
 * Results are printed as a single greppable line: {@code ONEWAY-RESULT backend=... rate=...}.
 */
@Tag("manual")
public class MessagingOneWayThroughputBenchmark {

    private static final String TOPIC = "onewaybench";
    private static final int MESSAGES = 5000;
    private static final int SENDER_THREADS = 4;
    private static final long RECEIVE_DEADLINE_MS = 300_000;

    private PoppyDB server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.shutdown();
            server = null;
        }
    }

    @Test
    public void oneWayThroughputPoppyDB() throws Exception {
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

        runOneWay("poppydb", "localhost:" + port);
    }

    @Test
    public void oneWayThroughputMongoDB() throws Exception {
        String uri = System.getProperty("morphium.uri", System.getenv("MONGODB_URI"));
        assumeTrue(uri != null && !uri.isBlank(),
            "no external MongoDB configured - pass -Dmorphium.uri=mongodb://host1,host2/db");

        // minimal parse: mongodb://host1:port,host2:port/db (no credentials - benchmark infra)
        String hostsPart = uri.replaceFirst("^mongodb://", "");
        if (hostsPart.contains("/")) {
            hostsPart = hostsPart.substring(0, hostsPart.indexOf('/'));
        }
        runOneWay("mongodb", hostsPart.split(","));
    }

    private void runOneWay(String backendLabel, String... hostSeed) throws Exception {
        String db = "oneway_bench_" + System.currentTimeMillis();

        try (Morphium receiverMorphium = new Morphium(cfg(db, hostSeed));
             Morphium senderMorphium = new Morphium(cfg(db, hostSeed))) {

            MorphiumMessaging receiver = receiverMorphium.createMessaging();
            AtomicInteger received = new AtomicInteger();
            receiver.addListenerForTopic(TOPIC, (mq, msg) -> {
                received.incrementAndGet();
                return null; // one-way: never answer
            });
            receiver.start();

            MorphiumMessaging sender = senderMorphium.createMessaging();
            sender.start();

            // let both messaging instances register their change streams before the clock starts
            Thread.sleep(3000);

            long start = System.nanoTime();

            List<Thread> senders = new ArrayList<>();
            int perThread = MESSAGES / SENDER_THREADS;
            for (int t = 0; t < SENDER_THREADS; t++) {
                Thread worker = new Thread(() -> {
                    for (int i = 0; i < perThread; i++) {
                        // 5min TTL so no message can expire mid-run on a slow backend
                        sender.sendMessage(new Msg(TOPIC, "bench", "x", 300_000));
                    }
                }, "oneway-sender-" + t);
                senders.add(worker);
                worker.start();
            }
            for (Thread worker : senders) {
                worker.join();
            }
            long sendDoneNanos = System.nanoTime() - start;

            int expected = perThread * SENDER_THREADS;
            long receiveDeadline = System.currentTimeMillis() + RECEIVE_DEADLINE_MS;
            while (received.get() < expected && System.currentTimeMillis() < receiveDeadline) {
                Thread.sleep(50);
            }
            long totalNanos = System.nanoTime() - start;

            assertEquals(expected, received.get(),
                "every sent message must arrive (one-way) - backend " + backendLabel);

            double sendRate = expected / (sendDoneNanos / 1e9);
            double endToEndRate = expected / (totalNanos / 1e9);
            System.out.printf(
                "ONEWAY-RESULT backend=%s messages=%d senderThreads=%d sendSeconds=%.2f sendRate=%.0f msg/s "
                + "endToEndSeconds=%.2f endToEndRate=%.0f msg/s%n",
                backendLabel, expected, SENDER_THREADS, sendDoneNanos / 1e9, sendRate,
                totalNanos / 1e9, endToEndRate);

            receiver.terminate();
            sender.terminate();
        }
    }

    private MorphiumConfig cfg(String db, String... hostSeed) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings().setDatabase(db);
        cfg.clusterSettings().getHostSeed().clear();
        for (String h : hostSeed) {
            cfg.clusterSettings().addHostToSeed(h);
        }
        cfg.driverSettings().setDriverName("PooledDriver");
        cfg.connectionSettings().setMaxConnections(20).setMinConnections(2);
        return cfg;
    }
}
