package de.caluga.test.morphium.driver.pool;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.PooledDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress test to reproduce connection pool deadlock/convoy issues.
 * 
 * The deadlock occurred when:
 * 1. Many threads borrow connections simultaneously
 * 2. Threads finish and try to release connections
 * 3. LinkedBlockingQueue.add() blocks on putLock under high contention
 * 4. This creates a lock convoy - threads pile up waiting
 * 
 * Fix: Use offer() with timeout instead of add()
 */
public class ConnectionPoolStressTest {
    
    private static final Logger log = LoggerFactory.getLogger(ConnectionPoolStressTest.class);
    
    private Morphium morphium;
    
    @BeforeEach
    void setUp() throws Exception {
        MorphiumConfig config = new MorphiumConfig();
        config.setDatabase("morphium_stress_test");
        config.addHostToSeed("localhost:27017");
        
        // Small pool to maximize contention
        config.setMaxConnections(5);
        config.setMinConnections(2);
        
        // Short timeouts to detect blocking quickly
        config.setMaxWaitTime(5000);
        config.setConnectionTimeout(2000);
        
        config.setDriverName(PooledDriver.driverName);
        
        morphium = new Morphium(config);
        
        // Wait for connection pool to initialize
        Thread.sleep(500);
    }
    
    @AfterEach
    void tearDown() {
        if (morphium != null) {
            morphium.close();
        }
    }
    
    /**
     * Stress test: Many concurrent threads doing rapid borrow/release cycles.
     * This should expose lock convoy issues in releaseConnection().
     */
    @Test
    void testHighContentionBorrowRelease() throws Exception {
        int numThreads = 50;
        int operationsPerThread = 100;
        int timeoutSeconds = 30;
        
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicLong maxLatencyMs = new AtomicLong(0);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    
                    for (int op = 0; op < operationsPerThread; op++) {
                        long start = System.currentTimeMillis();
                        try {
                            // Simple operation that borrows and releases a connection
                            morphium.getDriver().getDBStats("morphium_stress_test");
                            
                            long latency = System.currentTimeMillis() - start;
                            maxLatencyMs.updateAndGet(current -> Math.max(current, latency));
                            
                            // Warn if operation took too long (potential convoy)
                            if (latency > 1000) {
                                log.warn("Thread {} op {} took {}ms - possible lock convoy!", 
                                        threadId, op, latency);
                            }
                            
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            errors.add(e);
                            log.error("Thread {} op {} failed: {}", threadId, op, e.getMessage());
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }
        
        // Start all threads simultaneously
        log.info("Starting {} threads with {} operations each...", numThreads, operationsPerThread);
        long testStart = System.currentTimeMillis();
        startLatch.countDown();
        
        // Wait for completion with timeout
        boolean completed = doneLatch.await(timeoutSeconds, TimeUnit.SECONDS);
        long testDuration = System.currentTimeMillis() - testStart;
        
        executor.shutdownNow();
        
        // Report results
        log.info("Test completed in {}ms", testDuration);
        log.info("Success: {}, Errors: {}, Max latency: {}ms", 
                successCount.get(), errorCount.get(), maxLatencyMs.get());
        
        if (!errors.isEmpty()) {
            log.error("First error: ", errors.peek());
        }
        
        // Assertions
        assertTrue(completed, "Test timed out after " + timeoutSeconds + "s - likely deadlock!");
        assertTrue(errorCount.get() < numThreads, "Too many errors: " + errorCount.get());
        assertTrue(maxLatencyMs.get() < 10000, "Max latency too high: " + maxLatencyMs.get() + "ms - possible convoy");
    }
    
    /**
     * Test rapid connect/disconnect cycles under load.
     * This stresses the pool resize logic.
     */
    @Test
    void testRapidOperationsWithVaryingLoad() throws Exception {
        int rounds = 5;
        int threadsPerRound = 20;
        int opsPerThread = 50;
        
        AtomicInteger totalSuccess = new AtomicInteger(0);
        AtomicInteger totalErrors = new AtomicInteger(0);
        
        for (int round = 0; round < rounds; round++) {
            log.info("Round {} of {}", round + 1, rounds);
            
            ExecutorService executor = Executors.newFixedThreadPool(threadsPerRound);
            List<Future<Integer>> futures = new ArrayList<>();
            
            for (int t = 0; t < threadsPerRound; t++) {
                futures.add(executor.submit(() -> {
                    int success = 0;
                    for (int op = 0; op < opsPerThread; op++) {
                        try {
                            // Mix of read and write operations
                            if (op % 3 == 0) {
                                morphium.getDriver().getDBStats("morphium_stress_test");
                            } else {
                                morphium.getDriver().listDatabases();
                            }
                            success++;
                        } catch (Exception e) {
                            totalErrors.incrementAndGet();
                        }
                    }
                    return success;
                }));
            }
            
            for (Future<Integer> f : futures) {
                try {
                    totalSuccess.addAndGet(f.get(30, TimeUnit.SECONDS));
                } catch (TimeoutException e) {
                    fail("Operation timed out in round " + round + " - possible deadlock");
                }
            }
            
            executor.shutdown();
            
            // Brief pause between rounds to let pool stabilize
            Thread.sleep(100);
        }
        
        log.info("Total success: {}, errors: {}", totalSuccess.get(), totalErrors.get());
        assertTrue(totalErrors.get() < totalSuccess.get() / 10, 
                "Error rate too high: " + totalErrors.get() + "/" + totalSuccess.get());
    }
    
    /**
     * Simulate the exact scenario from production:
     * Many concurrent operations that rapidly borrow and release connections.
     * Uses a mix of read operations to stress the pool.
     */
    @Test
    void testMessagingStyleOperations() throws Exception {
        int numThreads = 30;
        int operationsPerThread = 100;
        int timeoutSeconds = 60;
        
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicLong maxLatencyMs = new AtomicLong(0);
        
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    
                    for (int op = 0; op < operationsPerThread; op++) {
                        long start = System.currentTimeMillis();
                        try {
                            // Mix of operations that borrow/release connections
                            switch (op % 3) {
                                case 0 -> morphium.getDriver().getDBStats("morphium_stress_test");
                                case 1 -> morphium.getDriver().listDatabases();
                                case 2 -> morphium.getDriver().listCollections("morphium_stress_test", null);
                            }
                            
                            long latency = System.currentTimeMillis() - start;
                            maxLatencyMs.updateAndGet(current -> Math.max(current, latency));
                            
                            if (latency > 2000) {
                                log.warn("Thread {} op {} took {}ms!", threadId, op, latency);
                            }
                            
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            if (errorCount.get() <= 5) {
                                log.error("Thread {} op {} failed: {}", threadId, op, e.getMessage());
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }
        
        log.info("Starting messaging-style stress test: {} threads x {} ops", numThreads, operationsPerThread);
        long testStart = System.currentTimeMillis();
        startLatch.countDown();
        
        boolean completed = doneLatch.await(timeoutSeconds, TimeUnit.SECONDS);
        long testDuration = System.currentTimeMillis() - testStart;
        
        executor.shutdownNow();
        
        log.info("Test completed in {}ms", testDuration);
        log.info("Success: {}, Errors: {}, Max latency: {}ms", 
                successCount.get(), errorCount.get(), maxLatencyMs.get());
        
        int expectedTotal = numThreads * operationsPerThread;
        double successRate = (double) successCount.get() / expectedTotal;
        
        assertTrue(completed, "Test timed out - likely deadlock!");
        assertTrue(successRate > 0.9, "Success rate too low: " + (successRate * 100) + "%");
        assertTrue(maxLatencyMs.get() < 15000, "Max latency too high: " + maxLatencyMs.get() + "ms");
    }
}
