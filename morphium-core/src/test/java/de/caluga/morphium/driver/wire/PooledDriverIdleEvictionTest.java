package de.caluga.morphium.driver.wire;

import de.caluga.morphium.driver.wire.PooledDriver.ConnectionContainer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The heartbeat sweep's eviction decision. Two properties have to hold at the same time:
 *
 * <ul>
 * <li>a pool that grew during a burst must shrink back down once the surplus connections
 * sit idle for maxConnectionIdleTime (lastUsed tracks application use only - the heartbeat
 * hello traffic running over pooled connections every second must not keep them "warm"),</li>
 * <li>the base stock of minConnectionsPerHost must NOT be idle-evicted. Before this rule,
 * every pooled connection of an idle client was closed after maxConnectionIdleTime (30s
 * default) and immediately re-created by the refill loop - a full TCP(+TLS+auth) handshake
 * every 30s per pooled connection, forever. Measured in production on a 3-node RS with ~22
 * long-lived clients: 1.5-4.3 new connections/s per node, ~2 million reconnects in 61h,
 * with only 150-220 connections open at any time.</li>
 * </ul>
 *
 * <p>Lives in the driver's package to reach the package-private decision method. The decision
 * is tested against explicit timestamps - no waiting, no timing dependence.
 */
@Tag("driver")
public class PooledDriverIdleEvictionTest {

    private static final long NOW = 1_000_000_000L;

    private PooledDriver driver() {
        PooledDriver drv = new PooledDriver();
        drv.setMinConnectionsPerHost(2);
        drv.setMaxConnectionsPerHost(10);
        drv.setMaxConnectionIdleTime(30_000);
        drv.setMaxConnectionLifetime(600_000);
        return drv;
    }

    private ConnectionContainer connection(long idleForMs, long ageMs) {
        ConnectionContainer c = new ConnectionContainer(new SingleMongoConnection());
        c.setLastUsed(NOW - idleForMs);
        c.setCreated(NOW - ageMs);
        return c;
    }

    @Test
    public void recentlyUsedConnectionIsKept() {
        PooledDriver drv = driver();
        ConnectionContainer c = connection(1_000, 60_000);

        assertThat(drv.shouldEvictPooledConnection(c, NOW, 10))
                .as("a connection the application used moments ago must never be evicted")
                .isFalse();
    }

    @Test
    public void idleSurplusAboveMinimumIsEvicted() {
        PooledDriver drv = driver();
        ConnectionContainer c = connection(31_000, 60_000);

        assertThat(drv.shouldEvictPooledConnection(c, NOW, 5))
                .as("after a burst the pool must shrink: idle connections above "
                        + "minConnectionsPerHost have to go")
                .isTrue();
    }

    @Test
    public void evictionMayShrinkExactlyToTheMinimum() {
        PooledDriver drv = driver();
        ConnectionContainer c = connection(31_000, 60_000);

        // 2 connections remain if we evict - exactly minConnectionsPerHost. Allowed.
        assertThat(drv.shouldEvictPooledConnection(c, NOW, 2)).isTrue();
    }

    @Test
    public void idleBaseStockIsNotEvicted() {
        PooledDriver drv = driver();
        ConnectionContainer c = connection(31_000, 60_000);

        // Evicting would drop the host below minConnectionsPerHost - the refill loop would
        // re-create the connection immediately. That close-and-reopen cycle was the
        // production churn (up to 4.3 new connections/s per RS node with zero load).
        assertThat(drv.shouldEvictPooledConnection(c, NOW, 1))
                .as("an idle connection must not be evicted when the host would fall below "
                        + "minConnectionsPerHost - it would be re-created immediately (churn)")
                .isFalse();
    }

    @Test
    public void lifetimeCapAppliesEvenBelowTheMinimum() {
        PooledDriver drv = driver();
        // recently used, but past maxConnectionLifetime
        ConnectionContainer c = connection(1_000, 601_000);

        assertThat(drv.shouldEvictPooledConnection(c, NOW, 0))
                .as("maxConnectionLifetime is the hard recycling bound and beats the "
                        + "minConnectionsPerHost floor - the refill loop replaces it")
                .isTrue();
    }
}
