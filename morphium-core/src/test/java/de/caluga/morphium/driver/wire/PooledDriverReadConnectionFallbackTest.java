package de.caluga.morphium.driver.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.ReadPreference;

/**
 * Found via a real failover run against mongo1/mongo2.fritz.box (DriverFailoverProxyTest,
 * writeReadRecoverAfterCleanStepdown in the full 5-scenario suite): after the old primary's
 * proxy was permanently faulted and the other data node took over, WRITES recovered fine
 * (getPrimaryConnection borrows from the adopted new primary) but READS never recovered at
 * all - readOk stayed frozen for the whole 25s window while writeOk climbed from 4 to 87 on
 * the very same driver instance. Root cause was getReadConnection()'s fall-through chain:
 * <ul>
 * <li>PRIMARY_PREFERRED skipped the healthy primary whenever its idle pool was momentarily
 *     empty (typical right after failover: connections still being created / all borrowed by
 *     concurrent writers) and dropped straight into the secondary-only loop;</li>
 * <li>the SECONDARY loop excludes primaryNode from its round-robin, so with the only other
 *     data node dead it retried the dead host for retriesOnNetworkError wraps at a full
 *     serverSelectionTimeout each - over 30s inside ONE read call with the test's settings
 *     (observed live: a single countAll() from 23:32:05 to 23:32:31), never once touching
 *     the healthy primary, although SECONDARY_PREFERRED (and the NEAREST/PRIMARY_PREFERRED
 *     fall-throughs that land there) semantically mean "secondary if available, OTHERWISE
 *     primary".</li>
 * </ul>
 */
public class PooledDriverReadConnectionFallbackTest {

    /** A "connected" connection without a real socket - just enough for borrowConnection()'s
     * liveness checks (con != null, sourcePort != 0, isConnected()). */
    private static final class FakeLiveConnection extends SingleMongoConnection {
        private final int sourcePort;

        FakeLiveConnection(int sourcePort) {
            this.sourcePort = sourcePort;
        }

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public int getSourcePort() {
            return sourcePort;
        }

        @Override
        public void close() {
            // no real socket
        }
    }

    private static final String SECONDARY = "secondary:27017";
    private static final String PRIMARY = "primary:27017";

    /** RS-mode driver whose secondary is unreachable (empty pool, no heartbeat running to ever
     * refill it, so borrowConnection times out after serverSelectionTimeout) and whose primary
     * host exists; the primary's pool content is up to the individual test. primaryNode is
     * adopted through the production code path (handleHelloResult), same as
     * PooledDriverPrimaryDiscoveryTest. */
    private PooledDriver driverWithDeadSecondary() {
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed(SECONDARY, PRIMARY);
        drv.setReplicaSet(true);
        drv.setServerSelectionTimeout(300);
        drv.setRetriesOnNetworkError(10);
        drv.setSleepBetweenErrorRetries(100);

        drv.hosts.put(SECONDARY, new Host("secondary", 27017));
        drv.hosts.put(PRIMARY, new Host("primary", 27017));

        HelloResult hello = new HelloResult();
        hello.setWritablePrimary(true);
        hello.setMe(PRIMARY);
        hello.setHosts(List.of(SECONDARY, PRIMARY));
        drv.handleHelloResult(hello, PRIMARY);
        assertEquals(PRIMARY, drv.getPrimaryNode(), "harness check: primary must be adopted");
        return drv;
    }

    @Test
    public void secondaryPreferredFallsBackToPrimaryWhenNoSecondaryIsReachable() throws Exception {
        PooledDriver drv = driverWithDeadSecondary();
        FakeLiveConnection primaryCon = new FakeLiveConnection(4711);
        drv.hosts.get(PRIMARY).getConnectionPool().offer(new PooledDriver.ConnectionContainer(primaryCon));

        long start = System.currentTimeMillis();
        MongoConnection con = drv.getReadConnection(ReadPreference.secondaryPreferred());
        long elapsed = System.currentTimeMillis() - start;

        assertSame(primaryCon, con,
                "secondaryPreferred with no reachable secondary must fall back to the primary's "
                        + "connection instead of failing");
        // One failed wrap over the dead secondary (serverSelectionTimeout=300ms) plus the
        // primary borrow - NOT retriesOnNetworkError(10) wraps at 300ms+100ms sleep each
        // (4s+) followed by an exception.
        assertTrue(elapsed < 2000,
                "fallback to primary must happen after the FIRST failed round-robin wrap, not "
                        + "after exhausting all retries on the dead secondary - took " + elapsed + "ms");
    }

    @Test
    public void primaryPreferredBorrowsFromPrimaryEvenWhileItsIdlePoolIsMomentarilyEmpty() throws Exception {
        PooledDriver drv = driverWithDeadSecondary();
        // Primary healthy but its idle pool is empty RIGHT NOW (the post-failover situation:
        // connections still being created / all borrowed); the "heartbeat" refills it shortly
        // after. The old pool-emptiness precondition skipped the primary entirely in this exact
        // situation and sent the read into the secondary-only loop, which can never succeed here.
        FakeLiveConnection primaryCon = new FakeLiveConnection(4712);
        Thread refill = new Thread(() -> {
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                return;
            }
            drv.hosts.get(PRIMARY).getConnectionPool().offer(new PooledDriver.ConnectionContainer(primaryCon));
        }, "pool-refill");
        refill.start();
        try {
            long start = System.currentTimeMillis();
            MongoConnection con = drv.getReadConnection(ReadPreference.primaryPreferred());
            long elapsed = System.currentTimeMillis() - start;

            assertSame(primaryCon, con,
                    "primaryPreferred must borrow from the healthy primary (waiting deadline-bounded "
                            + "for its pool to refill), not skip it because the pool was empty at the "
                            + "moment of the check");
            assertTrue(elapsed < 2000,
                    "read must recover as soon as the primary's pool is refilled - took " + elapsed + "ms");
        } finally {
            refill.join();
        }
    }

    @Test
    public void strictSecondaryNeverFallsBackToPrimary() {
        PooledDriver drv = driverWithDeadSecondary();
        drv.setRetriesOnNetworkError(1);
        drv.hosts.get(PRIMARY).getConnectionPool()
           .offer(new PooledDriver.ConnectionContainer(new FakeLiveConnection(4713)));

        // A strict SECONDARY preference is a hard constraint - with no reachable secondary it
        // must throw, never silently serve the read from the primary.
        assertThrows(MorphiumDriverException.class, () -> drv.getReadConnection(ReadPreference.secondary()));
    }
}
