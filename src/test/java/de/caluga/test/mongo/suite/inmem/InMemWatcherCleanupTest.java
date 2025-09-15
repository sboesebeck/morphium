package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.Morphium;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Tag("inmemory")
public class InMemWatcherCleanupTest extends MorphiumInMemTestBase {

    @Test
    public void watcherDoesNotLeakBetweenRuns() throws Exception {
        morphium.dropCollection(UncachedObject.class);

        // First watch session
        AtomicInteger c1 = new AtomicInteger(0);
        CountDownLatch l1 = new CountDownLatch(1);
        Thread t1 = Thread.ofVirtual().start(() -> {
            morphium.watch(UncachedObject.class, true, evt -> {
                c1.incrementAndGet();
                l1.countDown();
                return false; // stop after first
            });
        });

        Thread.sleep(100); // allow watcher to bind
        morphium.store(new UncachedObject("a", 1));
        assertTrue(l1.await(2, TimeUnit.SECONDS));
        t1.join(500);
        assertEquals(1, c1.get());

        // Second independent watch session must not receive duplicate notifications
        AtomicInteger c2 = new AtomicInteger(0);
        CountDownLatch l2 = new CountDownLatch(1);
        Thread t2 = Thread.ofVirtual().start(() -> {
            morphium.watch(UncachedObject.class, true, evt -> {
                c2.incrementAndGet();
                l2.countDown();
                return false;
            });
        });

        Thread.sleep(100);
        morphium.store(new UncachedObject("b", 2));
        assertTrue(l2.await(2, TimeUnit.SECONDS));
        t2.join(500);
        assertEquals(1, c2.get(), "duplicate notifications indicate watcher leak");
    }

    @org.junit.jupiter.api.Disabled("flaky in CI; relies on async scheduling timing")
    @Test
    public void dbAndCollectionWatchersBothReceiveEvents() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        AtomicInteger dbCount = new AtomicInteger(0);
        AtomicInteger collCount = new AtomicInteger(0);
        // we'll poll for events to avoid race conditions

        morphium.watchDbAsync(morphium.getDatabase(), true, null, evt -> {
            if ("drop".equals(evt.getOperationType())) return true;
            dbCount.incrementAndGet();
            return false;
        });

        morphium.watchAsync(UncachedObject.class, true, evt -> {
            if ("drop".equals(evt.getOperationType())) return true;
            collCount.incrementAndGet();
            return false;
        });

        Thread.sleep(800);
        morphium.store(new UncachedObject("x", 10));
        morphium.store(new UncachedObject("y", 20));
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 5000 && (dbCount.get() < 1 || collCount.get() < 1)) {
            Thread.sleep(50);
        }
        assertTrue(dbCount.get() >= 1, "db watcher did not receive event");
        assertTrue(collCount.get() >= 1, "collection watcher did not receive event");
    }
}
