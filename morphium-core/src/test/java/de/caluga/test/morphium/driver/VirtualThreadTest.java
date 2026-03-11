package de.caluga.test.morphium.driver;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify that virtual threads are working in JDK 21
 */
@Tag("driver")
public class VirtualThreadTest {

    @Test
    public void testVirtualThreadCreation() throws InterruptedException {
        var executor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
        var counter = new AtomicInteger(0);
        var latch = new CountDownLatch(1000);

        for (int i = 0; i < 1000; i++) {
            executor.submit(() -> {
                counter.incrementAndGet();
                latch.countDown();
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals(1000, counter.get());
        executor.close();
    }

    @Test
    public void testIsVirtualThread() {
        var virtualThread = Thread.ofVirtual().start(() -> {
            assertTrue(Thread.currentThread().isVirtual());
        });

        try {
            virtualThread.join();
        } catch (InterruptedException e) {
            fail("Thread interrupted");
        }
    }

    @Test
    public void testVirtualThreadNaming() throws InterruptedException {
        var factory = Thread.ofVirtual().name("test-", 0).factory();
        var executor = Executors.newThreadPerTaskExecutor(factory);
        var latch = new CountDownLatch(1);
        var threadName = new String[1];

        executor.submit(() -> {
            threadName[0] = Thread.currentThread().getName();
            latch.countDown();
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(threadName[0].startsWith("test-"));
        executor.close();
    }
}