package de.caluga.test.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.poppydb.PoppyDB;
import de.caluga.test.mongo.suite.base.TestUtils;
import de.caluga.test.mongo.suite.data.UncachedObject;

/**
 * Reproduces the 2026-08-21 ACC bus outage (#329): a PoppyDB restart resets the change-stream
 * sequence space, so a connected client's resume token becomes "foreign" and the server
 * (correctly) ends each resume attempt with 286 ChangeStreamHistoryLost. The client's
 * ChangeStreamMonitor discards the token - but before the fix, run()'s finally-block adoption
 * resurrected the discarded token from the dead WatchCommand, and the client hammered the
 * server with the same dead token forever (~3.3k errors/s per primary in the incident).
 *
 * Uses a raw ChangeStreamMonitor (not messaging): messaging's polling path and PoppyDB's
 * fastChangeStream registration deliver messages even over a broken stream and would mask the
 * loop. With a raw monitor both assertions discriminate: without the fix no post-restart event
 * is ever delivered (the stream never recovers) AND the history-lost discard count grows for
 * as long as one watches.
 */
@Tag("server")
public class ServerRestartResumeTest {

    private final Logger log = LoggerFactory.getLogger(ServerRestartResumeTest.class);

    private int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void startServer(PoppyDB srv, int port) throws Exception {
        // bind may race the previous instance's socket teardown - retry the start itself
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            try {
                srv.start();
                break;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(100);
            }
        }
        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("localhost", port), 250);
                return;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(50);
            }
        }
    }

    private Morphium connect(int port) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.clusterSettings().setHostSeed("localhost:" + port);
        cfg.connectionSettings().setDatabase("restart_resume_test");
        cfg.connectionSettings().setMaxConnections(10);
        return new Morphium(cfg);
    }

    @Test
    public void monitorRecoversAfterServerRestartWithoutResumeHammering() throws Exception {
        int port = freePort();
        PoppyDB srv = new PoppyDB(port, "localhost", 100, 1);
        startServer(srv, port);

        // count the monitor's history-lost discards ("... - discarding resume token and
        // restarting fresh"): once after the restart is healthy, unbounded growth is #329
        var csmLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ChangeStreamMonitor.class);
        var discards = new ListAppender<ILoggingEvent>();
        discards.start();
        csmLogger.addAppender(discards);

        Morphium watcherMorphium = connect(port);
        Morphium writerMorphium = connect(port);
        ChangeStreamMonitor monitor = null;
        try {
            AtomicInteger events = new AtomicInteger();
            monitor = new ChangeStreamMonitor(watcherMorphium, "uncached_object", false);
            monitor.addListener(evt -> {
                events.incrementAndGet();
                return true;
            });
            monitor.start();

            // Phase 1: pump events so the monitor's resume token sits far ahead of anything
            // the restarted server will reach during the test (the incident had client tokens
            // at ~2.8M against the fresh server's ~400) - a small token would be outgrown in
            // milliseconds and the foreign-sequence-space 286 would never fire
            for (int i = 0; i < 20; i++) {
                var bulk = new ArrayList<UncachedObject>();
                for (int j = 0; j < 100; j++) {
                    bulk.add(new UncachedObject("pump", i * 100 + j));
                }
                writerMorphium.storeList(bulk);
            }
            TestUtils.waitForConditionToBecomeTrue(20_000, "monitor never saw the pumped events",
                () -> events.get() >= 500);

            // Phase 2: the incident - the restart resets the sequence space, the monitor's
            // token is now from a foreign/reset space
            srv.shutdown();
            writerMorphium.close();
            srv = new PoppyDB(port, "localhost", 100, 1);
            startServer(srv, port);

            // Phase 3: recovery - post-restart writes must reach the monitor again. Without
            // the fix the monitor loops on the dead token and never delivers another event.
            writerMorphium = connect(port);
            int before = events.get();
            final Morphium writer = writerMorphium;
            AtomicInteger seq = new AtomicInteger();
            TestUtils.waitForConditionToBecomeTrue(45_000, "no event after restart - stream never recovered (#329)",
                () -> {
                    writer.store(new UncachedObject("after-restart", seq.incrementAndGet()));
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return events.get() > before;
                });

            // Phase 4: no hammering - the discard count must be tiny and must stop growing
            long settled = countDiscards(discards);
            Thread.sleep(5_000);
            long later = countDiscards(discards);
            log.info("history-lost discards: {} after recovery, {} five seconds later", settled, later);
            assertEquals(settled, later,
                "discard count kept growing after recovery - the monitor loops on a dead resume token (#329)");
            assertTrue(later >= 1, "the stale token must have been discarded via the history-lost path at least once");
            assertTrue(later <= 4,
                "expected at most one history-lost discard (plus reconnect slack), got " + later + " (#329 loop)");
        } finally {
            csmLogger.detachAppender(discards);
            if (monitor != null) {
                monitor.terminate();
            }
            watcherMorphium.close();
            writerMorphium.close();
            srv.shutdown();
        }
    }

    private long countDiscards(ListAppender<ILoggingEvent> appender) {
        synchronized (appender.list) {
            return appender.list.stream()
                   .filter(ev -> ev.getFormattedMessage().contains("discarding resume token"))
                   .count();
        }
    }
}
