package de.caluga.test.poppydb;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.poppydb.PoppyDB;

/**
 * #383, the real thing end to end: a client whose resume token is from a foreign sequence space
 * (the previous primary's, before a restart or failover) against a live PoppyDB. The server
 * counts what it counted during the incident - {@code serverStatus.changeStreams.registrations}
 * - and the client must not turn one dead token into a burst of registrations.
 *
 * <p>Why the burst exists at all: the server ends such a stream asynchronously, right after the
 * registration was answered ok. Whether the client's first getMore finds the ended stream still
 * parked (answer: 286, the monitor discards the token and sleeps) or already removed (answer: an
 * exhausted cursor, id 0) is a race - and the exhausted-cursor answer made the watch loop
 * re-register in place with the same dead token, immediately, without the monitor ever seeing
 * an error. Which way the race lands here depends on the machine; the bound must hold either
 * way. The deterministic reproduction of the exhausted-cursor shape is
 * {@code ChangeStreamMonitorHistoryLostBackoffTest} in morphium-core.
 */
@Tag("server")
public class ForeignResumeTokenRegistrationBurstTest {

    private static final long MEASURE_MS = 5_000;

    private PoppyDB srv;
    private Morphium morphium;
    private ChangeStreamMonitor monitor;

    @AfterEach
    public void tearDown() {
        if (monitor != null) {
            monitor.terminate();
        }
        if (morphium != null) {
            morphium.close();
        }
        if (srv != null) {
            srv.shutdown();
        }
    }

    @Test
    @Timeout(60)
    public void foreignResumeTokenDoesNotCauseARegistrationBurst() throws Exception {
        int port = freePort();
        srv = new PoppyDB(port, "localhost", 100, 1);
        startServer(srv, port);

        MorphiumConfig cfg = new MorphiumConfig();
        cfg.clusterSettings().setHostSeed("localhost:" + port);
        cfg.connectionSettings().setDatabase("foreign_token_test");
        cfg.connectionSettings().setMaxConnections(10);
        morphium = new Morphium(cfg);

        monitor = new ChangeStreamMonitor(morphium, "uncached_object", false, 200, null);
        // the token a client brings along from the previous primary: far beyond anything this
        // fresh server has ever issued (the incident had client tokens at ~2.8M against ~400)
        Field f = ChangeStreamMonitor.class.getDeclaredField("lastResumeToken");
        f.setAccessible(true);
        f.set(monitor, Doc.of("_data", String.format(Locale.ROOT, "%016x", 2_800_000L)));

        long before = registrations();
        monitor.startAsync();
        Thread.sleep(MEASURE_MS);
        monitor.terminate();
        long during = registrations() - before;

        System.out.println("#383 [real PoppyDB, foreign token] registrations in " + MEASURE_MS + " ms: " + during
                + " (serverStatus.changeStreams: " + changeStreams() + ")");
        assertThat(during)
                .as("one dead resume token must cost a handful of registrations, not a burst")
                .isBetween(1L, 10L);
    }

    private long registrations() throws Exception {
        return ((Number) changeStreams().get("registrations")).longValue();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> changeStreams() throws Exception {
        MongoConnection con = morphium.getDriver().getPrimaryConnection(null);
        try {
            int id = con.sendCommand(new GenericCommand(con).fromMap(Doc.of("serverStatus", 1, "$db", "admin")));
            return (Map<String, Object>) con.readSingleAnswer(id).get("changeStreams");
        } finally {
            morphium.getDriver().releaseConnection(con);
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void startServer(PoppyDB srv, int port) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        srv.start();
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
}
