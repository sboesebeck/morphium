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

    // --- stream-state handling -------------------------------------------------------------
    // "cursor is null" means the watch read a full reply that was NOT a watch reply - the
    // connection delivered someone else's (stale) answer. Releasing such a connection back
    // to the pool poisons the next borrower (seen 2026-07-20 as "Illegal opcode" during an
    // unrelated write). The monitor must close it so the pool discards it.

    private java.util.concurrent.atomic.AtomicBoolean injectTrackingConnection() throws Exception {
        var closed = new java.util.concurrent.atomic.AtomicBoolean(false);
        var con = new de.caluga.morphium.driver.wire.SingleMongoConnection() {
            @Override
            public void close() {
                closed.set(true);
            }
            @Override
            public boolean isConnected() {
                return !closed.get();
            }
        };
        var f = ChangeStreamMonitor.class.getDeclaredField("activeConnection");
        f.setAccessible(true);
        f.set(monitor, con);
        return closed;
    }

    @Test
    public void closesConnectionWhenReplyWasNotAWatchReply() throws Exception {
        var closed = injectTrackingConnection();

        boolean cont = monitor.handleWatchError(
            new MorphiumDriverException("Could not watch - cursor is null"));

        assertTrue(cont, "must still retry");
        assertTrue(closed.get(), "connection with unknown stream state must be closed, not pooled");
    }

    @Test
    public void closesConnectionOnNullReply() throws Exception {
        var closed = injectTrackingConnection();

        boolean cont = monitor.handleWatchError(
            new MorphiumDriverException("Could not watch - reply is null"));

        assertTrue(cont, "must still retry");
        assertTrue(closed.get(), "connection with unknown stream state must be closed, not pooled");
    }

    @Test
    public void closesConnectionOnUnclassifiedError() throws Exception {
        var closed = injectTrackingConnection();

        boolean cont = monitor.handleWatchError(
            new MorphiumDriverException("something completely unexpected"));

        assertTrue(cont, "must still retry");
        assertTrue(closed.get(), "unknown error - connection state unknown - close to be safe");
    }

    // --- watch-established notification ----------------------------------------------------
    // Messaging polls once per (re-)establishment to catch up on messages inserted while the
    // stream was down. The hook must fire on every establishment, not only the first.

    @Test
    public void notifiesListenerOnWatchEstablishment() throws Exception {
        var established = new java.util.concurrent.atomic.AtomicInteger();
        monitor.addWatchEstablishedListener(established::incrementAndGet);

        monitor.start(); // blocks until the watch is established (up to 2s)
        try {
            long until = System.currentTimeMillis() + 2000;
            while (established.get() == 0 && System.currentTimeMillis() < until) {
                Thread.sleep(20);
            }
            assertTrue(established.get() >= 1, "listener must fire when the watch is established");
        } finally {
            monitor.terminate();
        }
    }

    // --- stream liveness -------------------------------------------------------------------
    // The watch loop receives a server reply at least every maxTimeMS (empty batch on no
    // events) and stamps its time on the WatchCommand. isStreamLive() exposes that heartbeat:
    // messaging skips its fallback poll for topics whose stream is provably alive and in sync,
    // and polls immediately when a stream goes silent - instead of blindly polling on a timer.

    private void injectActiveWatch(long lastReplyAt) throws Exception {
        var cmd = new de.caluga.morphium.driver.commands.WatchCommand(null);
        cmd.setMaxTimeMS(250);
        cmd.setLastReplyAt(lastReplyAt);
        var f = ChangeStreamMonitor.class.getDeclaredField("activeWatch");
        f.setAccessible(true);
        f.set(monitor, cmd);
    }

    @Test
    public void streamIsNotLiveWithoutAnActiveWatch() {
        assertFalse(monitor.isStreamLive(), "no active watch - nothing can be live");
    }

    @Test
    public void streamIsNotLiveBeforeTheFirstReply() throws Exception {
        injectActiveWatch(0);
        assertFalse(monitor.isStreamLive(), "no reply yet - liveness unknown, treat as not live");
    }

    @Test
    public void streamIsLiveWithARecentReply() throws Exception {
        injectActiveWatch(System.currentTimeMillis());
        assertTrue(monitor.isStreamLive(), "reply just arrived - stream is provably alive");
    }

    @Test
    public void streamIsNotLiveWhenRepliesStopped() throws Exception {
        injectActiveWatch(System.currentTimeMillis() - 60_000);
        assertFalse(monitor.isStreamLive(),
            "no reply for a minute although the server must answer within maxTimeMS - suspicious");
    }

    // --- resume-token adoption -------------------------------------------------------------
    // watch() publishes its freshest token (incl. postBatchResumeToken from empty batches) on
    // the WatchCommand when it dies. The monitor builds a NEW WatchCommand for every retry, so
    // it must adopt that token - otherwise a consumer that never saw an event restarts at "now"
    // and loses everything written during the gap.

    private Object monitorToken() throws Exception {
        var f = ChangeStreamMonitor.class.getDeclaredField("lastResumeToken");
        f.setAccessible(true);
        return f.get(monitor);
    }

    @Test
    public void adoptsResumeTokenFromDeadWatchCommand() throws Exception {
        var cmd = new de.caluga.morphium.driver.commands.WatchCommand(null);
        cmd.setResumeAfter(java.util.Map.of("_data", "TOKEN-FROM-WATCH"));

        monitor.adoptResumeTokenFrom(cmd);

        assertTrue(java.util.Map.of("_data", "TOKEN-FROM-WATCH").equals(monitorToken()),
            "monitor must resume the next watch from the token the dying watch published");
    }

    @Test
    public void adoptionToleratesNullCommandAndNullToken() throws Exception {
        monitor.adoptResumeTokenFrom(null);
        assertTrue(monitorToken() == null, "null command must not touch the token");

        monitor.adoptResumeTokenFrom(new de.caluga.morphium.driver.commands.WatchCommand(null));
        assertTrue(monitorToken() == null, "command without token must not clear an existing one");
    }

    @Test
    public void keepsConnectionOnHistoryLost() throws Exception {
        var closed = injectTrackingConnection();

        boolean cont = monitor.handleWatchError(
            new MorphiumDriverException("PlanExecutor error during aggregation :: caused by :: ChangeStreamHistoryLost"));

        assertTrue(cont, "must retry with fresh stream");
        assertFalse(closed.get(),
            "history-lost is a proper server error reply - the stream is aligned, connection is fine");
    }
}
