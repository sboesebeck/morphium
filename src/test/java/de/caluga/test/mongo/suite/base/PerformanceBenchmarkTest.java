package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.messaging.SingleCollectionMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.support.TestConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Performance benchmarks for Morphium v6
 * Tests key improvements: PooledDriver, InMemory queries, Messaging
 * 
 * Run with: mvn test -Dtest=PerformanceBenchmarkTest
 * Configure via: -Dmorphium.hostSeed=host1:port1,host2:port2
 *                -Dmorphium.user=... -Dmorphium.pass=... -Dmorphium.authDb=admin
 */
@Tag("benchmark")
public class PerformanceBenchmarkTest {

    private static Morphium morphium;
    private static final int WARMUP_ITERATIONS = 100;

    @BeforeAll
    static void setup() throws Exception {
        MorphiumConfig cfg = TestConfig.load();
        cfg.connectionSettings()
           .setMaxConnections(100)
           .setMinConnections(10);
        
        morphium = new Morphium(cfg);
        morphium.dropCollection(BenchEntity.class);
        Thread.sleep(200);
        
        // Pre-populate with test data
        System.out.println("\n========================================");
        System.out.println("MORPHIUM v6 PERFORMANCE BENCHMARK");
        System.out.println("========================================");
        System.out.println("Setting up test data...");
        
        List<BenchEntity> entities = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            BenchEntity e = new BenchEntity();
            e.counter = i;
            e.name = "Entity_" + i;
            e.category = "cat_" + (i % 100);
            entities.add(e);
        }
        morphium.storeList(entities);
        System.out.println("Test data ready: 10000 entities\n");
    }

    @AfterAll
    static void teardown() {
        if (morphium != null) {
            try {
                morphium.dropCollection(BenchEntity.class);
                morphium.dropCollection(BenchWriteEntity.class);
            } catch (Exception ignored) {}
            morphium.close();
        }
    }

    @Test
    void benchmarkConnectionPooling() throws Exception {
        System.out.println("=== CONNECTION POOL BENCHMARK ===");
        System.out.println("Tests per-host locking (v6) vs global lock (v5)");
        
        int threads = 20;
        int opsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        
        // Warmup
        System.out.println("Warmup...");
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            morphium.createQueryFor(BenchEntity.class).countAll();
        }
        
        // Benchmark: Concurrent reads
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicLong totalOps = new AtomicLong(0);
        
        long start = System.nanoTime();
        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < opsPerThread; i++) {
                        morphium.createQueryFor(BenchEntity.class)
                                .f("counter").eq(i % 1000)
                                .get();
                        totalOps.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await(120, TimeUnit.SECONDS);
        long elapsed = System.nanoTime() - start;
        
        double opsPerSec = totalOps.get() / (elapsed / 1_000_000_000.0);
        System.out.printf("Concurrent reads (%d threads, %d ops each):%n", threads, opsPerThread);
        System.out.printf("  Total ops:   %,d%n", totalOps.get());
        System.out.printf("  Time:        %.2f ms%n", elapsed / 1_000_000.0);
        System.out.printf("  Throughput:  %,.0f ops/sec%n", opsPerSec);
        System.out.printf("  Avg latency: %.2f ms%n", elapsed / 1_000_000.0 / totalOps.get());
        System.out.println();
        
        executor.shutdown();
    }

    @Test
    void benchmarkInMemoryInQuery() {
        System.out.println("=== $in QUERY BENCHMARK ===");
        System.out.println("Tests O(n+m) hash-based matching (v6) vs O(n*m) linear scan (v5)");
        
        // Create large $in list
        List<Integer> inValues = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            inValues.add(i * 2); // Even numbers 0-998
        }
        
        // Warmup
        for (int i = 0; i < 10; i++) {
            morphium.createQueryFor(BenchEntity.class)
                    .f("counter").in(inValues)
                    .asList();
        }
        
        // Benchmark
        long start = System.nanoTime();
        int iterations = 50;
        int totalResults = 0;
        for (int i = 0; i < iterations; i++) {
            List<BenchEntity> results = morphium.createQueryFor(BenchEntity.class)
                    .f("counter").in(inValues)
                    .asList();
            totalResults += results.size();
        }
        long elapsed = System.nanoTime() - start;
        
        System.out.printf("$in query (500 values against 10000 docs):%n");
        System.out.printf("  Iterations:    %d%n", iterations);
        System.out.printf("  Avg results:   %d%n", totalResults / iterations);
        System.out.printf("  Total time:    %.2f ms%n", elapsed / 1_000_000.0);
        System.out.printf("  Avg per query: %.2f ms%n", elapsed / 1_000_000.0 / iterations);
        System.out.println();
    }

    @Test
    void benchmarkBulkWrites() throws Exception {
        System.out.println("=== BULK WRITE BENCHMARK ===");
        System.out.println("Tests write throughput with lazy copy optimization (v6)");
        
        morphium.dropCollection(BenchWriteEntity.class);
        Thread.sleep(200);
        
        int batchSize = 1000;
        int batches = 10;
        
        long start = System.nanoTime();
        for (int b = 0; b < batches; b++) {
            List<BenchWriteEntity> batch = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                BenchWriteEntity e = new BenchWriteEntity();
                e.value = b * batchSize + i;
                e.data = "Data_" + e.value;
                batch.add(e);
            }
            morphium.storeList(batch);
        }
        long elapsed = System.nanoTime() - start;
        
        int totalDocs = batchSize * batches;
        double docsPerSec = totalDocs / (elapsed / 1_000_000_000.0);
        
        System.out.printf("Bulk inserts (%,d docs in %d batches):%n", totalDocs, batches);
        System.out.printf("  Time:       %.2f ms%n", elapsed / 1_000_000.0);
        System.out.printf("  Throughput: %,.0f docs/sec%n", docsPerSec);
        System.out.println();
        
        morphium.dropCollection(BenchWriteEntity.class);
    }

    @Test
    void benchmarkMessaging() throws Exception {
        System.out.println("=== MESSAGING BENCHMARK ===");
        System.out.println("Tests SingleCollectionMessaging throughput");
        
        SingleCollectionMessaging sender = new SingleCollectionMessaging(morphium, 100, false);
        SingleCollectionMessaging receiver = new SingleCollectionMessaging(morphium, 100, false);
        
        sender.start();
        receiver.start();
        
        int messageCount = 500;
        AtomicLong received = new AtomicLong(0);
        CountDownLatch allReceived = new CountDownLatch(messageCount);
        
        receiver.addListenerForTopic("benchmark", (messaging, msg) -> {
            received.incrementAndGet();
            allReceived.countDown();
            return null;
        });
        
        Thread.sleep(500); // Let listeners register
        
        // Send messages
        long start = System.nanoTime();
        for (int i = 0; i < messageCount; i++) {
            Msg m = new Msg("benchmark", "test", "value_" + i);
            m.setExclusive(false);
            sender.sendMessage(m);
        }
        
        // Wait for all to be received (max 30s)
        boolean complete = allReceived.await(30, TimeUnit.SECONDS);
        long elapsed = System.nanoTime() - start;
        
        double msgsPerSec = received.get() / (elapsed / 1_000_000_000.0);
        
        System.out.printf("Messaging (%d messages):%n", messageCount);
        System.out.printf("  Received:    %d%s%n", received.get(), complete ? "" : " (TIMEOUT!)");
        System.out.printf("  Time:        %.2f ms%n", elapsed / 1_000_000.0);
        System.out.printf("  Throughput:  %,.0f msgs/sec%n", msgsPerSec);
        System.out.printf("  Avg latency: %.2f ms%n", elapsed / 1_000_000.0 / Math.max(1, received.get()));
        System.out.println();
        
        sender.terminate();
        receiver.terminate();
    }

    @Entity
    public static class BenchEntity {
        @Id
        public MorphiumId id;
        public int counter;
        public String name;
        public String category;
    }

    @Entity
    public static class BenchWriteEntity {
        @Id
        public MorphiumId id;
        public int value;
        public String data;
    }
}
