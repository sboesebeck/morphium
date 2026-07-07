package de.caluga.morphium.changestream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.MorphiumDriverException;

/**
 * Error handling of the changestream watch loop. Transient connection-pool
 * errors during a primary failover must lead to a retry - never to a permanent
 * shutdown of the monitor (messaging depends on it and has no other restart path).
 */
public class ChangeStreamMonitorErrorHandlingTest {

    private Morphium morphium;
    private ChangeStreamMonitor monitor;

    @BeforeEach
    public void setup() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.driverSettings().setDriverName("InMemDriver");
        cfg.connectionSettings().setDatabase("csm_error_test");
        cfg.connectionSettings().setSleepBetweenNetworkErrorRetries(1);
        cfg.clusterSettings().setHostSeed(new ArrayList<>());
        morphium = new Morphium(cfg);
        monitor = new ChangeStreamMonitor(morphium, "some_collection", false);
    }

    @AfterEach
    public void teardown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    @Test
    public void retriesOnNoSuchHost() {
        // thrown by PooledDriver.borrowConnection when the (dead) primary was just
        // evicted from the hosts map - it is re-added by the next heartbeat, so
        // this is transient during failover
        boolean cont = monitor.handleWatchError(
            new MorphiumDriverException("No such host: serv-msg1:27017"));
        assertTrue(cont, "monitor must retry after transient host eviction (failover)");
    }

    @Test
    public void retriesOnConnectionClosed() {
        boolean cont = monitor.handleWatchError(
            new MorphiumDriverException("Connection was closed"));
        assertTrue(cont);
    }

    @Test
    public void retriesOnGenericNetworkError() {
        boolean cont = monitor.handleWatchError(
            new MorphiumDriverException("Could not get connection to serv-msg1:27017 in time 30000ms"));
        assertTrue(cont);
    }

    @Test
    public void terminatesWhenMonitorStopped() throws Exception {
        // ensure the monitor thread is not started; we only flip the flag
        monitor.terminate();
        boolean cont = monitor.handleWatchError(
            new MorphiumDriverException("No such host: serv-msg1:27017"));
        assertFalse(cont, "stopped monitor must not continue");
    }
}
