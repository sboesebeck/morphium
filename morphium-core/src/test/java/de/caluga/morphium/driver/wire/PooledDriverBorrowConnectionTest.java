package de.caluga.morphium.driver.wire;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.MorphiumDriverException;

/**
 * Found via a real run on mongo1/mongo2.fritz.box: after a failover, a reader thread got stuck
 * forever inside borrowConnection() with no exception ever thrown and no "read failed" log line
 * ever printed - readOk simply stopped incrementing for the rest of the test. Root cause:
 * borrowConnection()'s do-while loop recomputes its deadline (System.currentTimeMillis() +
 * serverSelectionTimeout) on every iteration, including every time it discards a stale
 * (disconnected) connection pulled from the pool. If the pool keeps handing back stale
 * connections faster than the timeout window, the deadline keeps getting pushed out and the
 * "could not get connection in time" throw is never reached - an effectively unbounded wait,
 * even though the method's whole contract is "give up after serverSelectionTimeout".
 */
public class PooledDriverBorrowConnectionTest {

    @Test
    public void borrowConnectionThrowsWithinTimeoutEvenWhenThePoolKeepsHandingBackStaleConnections()
            throws Exception {
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed("node1:27017");
        drv.setServerSelectionTimeout(500);

        Host host = new Host("node1", 27017);
        drv.hosts.put("node1:27017", host);

        // Keep re-supplying stale (con==null, so borrowConnection's "is this connection still
        // alive" check discards it) entries faster than the 100ms poll interval, for well beyond
        // the configured timeout - simulates a pool that keeps handing back dead connections
        // (e.g. left over from a host that just changed roles during a failover) instead of
        // simply staying empty.
        AtomicBoolean keepFeeding = new AtomicBoolean(true);
        Thread feeder = new Thread(() -> {
            while (keepFeeding.get()) {
                host.getConnectionPool().offer(new PooledDriver.ConnectionContainer(null));
                try {
                    Thread.sleep(20);
                } catch (InterruptedException ignored) {
                    return;
                }
            }
        }, "stale-connection-feeder");
        feeder.start();
        try {
            long start = System.currentTimeMillis();
            assertThrows(MorphiumDriverException.class, () -> drv.borrowConnection("node1:27017"));
            long elapsed = System.currentTimeMillis() - start;
            assertTrue(elapsed < 1500,
                    "borrowConnection must give up within roughly its configured "
                            + "serverSelectionTimeout (500ms) even when the pool keeps handing back "
                            + "stale connections, instead of having its deadline reset on every one "
                            + "- took " + elapsed + "ms");
        } finally {
            keepFeeding.set(false);
            feeder.join();
        }
    }
}
