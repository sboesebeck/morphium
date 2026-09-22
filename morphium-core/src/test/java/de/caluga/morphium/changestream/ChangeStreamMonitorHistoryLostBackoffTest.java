package de.caluga.morphium.changestream;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

/**
 * #383: after a ChangeStreamHistoryLost (286) answer a client must not re-register its change
 * stream in a tight loop. Measured on a PoppyDB node during a rolling restart: ~5,000
 * registrations from ~250 connections inside 383 ms - ~20 attempts per client, no pause.
 *
 * <p>The test speaks the wire protocol end to end - {@link ChangeStreamMonitor#run()} over a
 * real {@link PooledDriver} against a stub server that answers EVERY change-stream registration
 * (or every getMore, as PoppyDB does when the resume token is from a foreign sequence space)
 * with 286 - and counts what the server counts: registrations per unit of time. It is the
 * client-side twin of the {@code serverStatus.changeStreams.registrations} counter of #380.
 */
@Tag("core")
public class ChangeStreamMonitorHistoryLostBackoffTest {

    private static final long MEASURE_MS = 5_000;

    private Morphium morphium;
    private PooledDriver driver;
    private HistoryLostServer server;

    @AfterEach
    public void tearDown() {
        if (driver != null) {
            driver.close();
        }
        if (morphium != null) {
            morphium.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    @Timeout(60)
    public void registrationRejectedWithHistoryLostIsRetriedWithBackoff() throws Exception {
        server = new HistoryLostServer(HistoryLostServer.Mode.REJECT_REGISTRATION);
        Measurement m = measureRegistrations();

        assertThat(m.total)
                .as("the monitor keeps retrying - 286 is transient (a failover), never terminal")
                .isGreaterThanOrEqualTo(2);
        assertThat(m.inWindow)
                .as("registrations in %d ms against a server that answers every registration "
                        + "with 286 - must be backed off, not a tight loop", MEASURE_MS)
                .isLessThanOrEqualTo(10);
    }

    @Test
    @Timeout(60)
    public void firstGetMoreRejectedWithHistoryLostIsRetriedWithBackoff() throws Exception {
        // PoppyDB's shape: the aggregate registers fine (ok:1, cursor id), the first getMore
        // finds the stream ended unservable and answers 286.
        server = new HistoryLostServer(HistoryLostServer.Mode.REJECT_FIRST_GETMORE);
        Measurement m = measureRegistrations();

        assertThat(m.total).isGreaterThanOrEqualTo(2);
        assertThat(m.inWindow)
                .as("registrations in %d ms against a server whose first getMore answers 286 - "
                        + "must be backed off, not a tight loop", MEASURE_MS)
                .isLessThanOrEqualTo(10);
    }

    @Test
    @Timeout(60)
    public void cursorGoneAfterRegistrationIsRetriedWithBackoff() throws Exception {
        // The shape measured against a live PoppyDB (see ForeignResumeTokenRegistrationBurstTest
        // in poppydb): the server ends a stream with a foreign resume token asynchronously right
        // after answering the aggregate; when the cursor is already gone by the time the first
        // getMore arrives, the answer is an exhausted cursor (ok:1, id 0) - and the watch loop
        // re-registered in place with the same dead token, immediately, without the monitor's
        // error classifier ever running.
        server = new HistoryLostServer(HistoryLostServer.Mode.CURSOR_GONE_ON_GETMORE);
        Measurement m = measureRegistrations();

        assertThat(m.total).isGreaterThanOrEqualTo(2);
        assertThat(m.inWindow)
                .as("registrations in %d ms against a server whose getMore answers 'cursor exhausted' "
                        + "for every fresh stream - must be backed off, not a tight loop", MEASURE_MS)
                .isLessThanOrEqualTo(10);
    }

    /**
     * {@code inWindow}: registrations within {@link #MEASURE_MS} of the first one - the rate,
     * which must be a backoff and not a loop. {@code total}: registrations seen until either a
     * second one arrived or {@link #RETRY_DEADLINE_MS} passed - whether the monitor retries at
     * all. Two numbers rather than one, because the second registration is at the mercy of the
     * host: one second of backoff plus a fresh connection took more than four seconds on the
     * test runner under a load of 27 (2026-09-22), and a fixed 5 s window then read "gave up".
     */
    record Measurement(int inWindow, int total) {
    }

    private static final long RETRY_DEADLINE_MS = 30_000;

    private Measurement measureRegistrations() throws Exception {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.driverSettings().setDriverName("InMemDriver");
        cfg.connectionSettings().setDatabase("csm_backoff_test");
        cfg.clusterSettings().setHostSeed(new ArrayList<>());
        morphium = new Morphium(cfg);

        driver = new PooledDriver();
        driver.setHostSeed(List.of("localhost:" + server.getPort()));
        driver.setConnectionTimeout(1000);
        driver.setMaxWaitTime(2000);
        driver.setServerSelectionTimeout(2000);
        driver.setHeartbeatFrequency(1000);
        driver.connect();

        ChangeStreamMonitor monitor = new ChangeStreamMonitor(morphium, "some_collection", false, 200, null);
        Field f = ChangeStreamMonitor.class.getDeclaredField("dedicatedConnection");
        f.setAccessible(true);
        f.set(monitor, driver);

        monitor.startAsync();
        long start = System.currentTimeMillis();
        while (server.registrations.get() == 0 && System.currentTimeMillis() - start < RETRY_DEADLINE_MS) {
            Thread.sleep(20);
        }
        assertThat(server.registrations.get()).as("the monitor must register at all").isGreaterThan(0);
        long windowEnd = server.firstRegistrationAt.get() + MEASURE_MS;
        long wait = windowEnd - System.currentTimeMillis();
        if (wait > 0) {
            Thread.sleep(wait);
        }
        int inWindow = server.registrations.get();
        while (server.registrations.get() < 2 && System.currentTimeMillis() - start < RETRY_DEADLINE_MS) {
            Thread.sleep(50);
        }
        monitor.terminate();

        int total = server.registrations.get();
        System.out.println("#383 [" + server.mode + "] registrations in " + MEASURE_MS + " ms: " + inWindow
                + ", total until the retry was seen: " + total
                + " (with resumeAfter: " + server.resumeRegistrations.get() + ", getMores: " + server.getMores.get()
                + ", first->last registration " + (server.lastRegistrationAt.get() - server.firstRegistrationAt.get()) + " ms)");
        return new Measurement(inWindow, total);
    }

    /**
     * Speaks just enough OP_MSG for a PooledDriver: hello for the handshake/heartbeat,
     * 286 for change streams. Counts registrations the way the server side of #380 does.
     */
    static class HistoryLostServer implements AutoCloseable {
        enum Mode { REJECT_REGISTRATION, REJECT_FIRST_GETMORE, CURSOR_GONE_ON_GETMORE }

        final Mode mode;
        final AtomicInteger registrations = new AtomicInteger();
        final AtomicInteger resumeRegistrations = new AtomicInteger();
        final AtomicInteger getMores = new AtomicInteger();
        final AtomicLong firstRegistrationAt = new AtomicLong();
        final AtomicLong lastRegistrationAt = new AtomicLong();

        private final ServerSocket serverSocket;
        private final List<Socket> clients = new CopyOnWriteArrayList<>();
        private final AtomicInteger msgId = new AtomicInteger(1000);
        private final AtomicLong cursorIds = new AtomicLong(4711);
        private volatile boolean running = true;
        private final int port;

        HistoryLostServer(Mode mode) throws Exception {
            this.mode = mode;
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
            port = serverSocket.getLocalPort();
            Thread acceptor = new Thread(this::acceptLoop, "history-lost-server-acceptor");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        int getPort() {
            return port;
        }

        private void acceptLoop() {
            while (running) {
                try {
                    Socket s = serverSocket.accept();
                    clients.add(s);
                    Thread t = new Thread(() -> serve(s), "history-lost-server-conn");
                    t.setDaemon(true);
                    t.start();
                } catch (Exception e) {
                    return;
                }
            }
        }

        @SuppressWarnings("unchecked")
        private void serve(Socket s) {
            try {
                InputStream in = s.getInputStream();
                OutputStream out = s.getOutputStream();

                while (running && !s.isClosed()) {
                    WireProtocolMessage incoming = WireProtocolMessage.parseFromStream(in);

                    if (incoming == null) {
                        return;
                    }

                    OpMsg request;

                    if (incoming instanceof OpCompressed) {
                        OpMsg m = new OpMsg();
                        m.setMessageId(incoming.getMessageId());
                        m.parsePayload(((OpCompressed) incoming).getCompressedMessage(), 0);
                        request = m;
                    } else if (incoming instanceof OpMsg) {
                        request = (OpMsg) incoming;
                    } else {
                        continue;
                    }

                    Map<String, Object> doc = request.getFirstDoc();
                    Map<String, Object> answer;

                    if (doc.containsKey("aggregate")) {
                        long now = System.currentTimeMillis();
                        firstRegistrationAt.compareAndSet(0, now);
                        lastRegistrationAt.set(now);
                        registrations.incrementAndGet();
                        List<Map<String, Object>> pipeline = (List<Map<String, Object>>) doc.get("pipeline");
                        Map<String, Object> cs = pipeline == null || pipeline.isEmpty() ? null
                                : (Map<String, Object>) pipeline.get(0).get("$changeStream");

                        if (cs != null && cs.get("resumeAfter") != null) {
                            resumeRegistrations.incrementAndGet();
                        }

                        if (mode == Mode.REJECT_REGISTRATION) {
                            answer = historyLost();
                        } else {
                            answer = Doc.of("ok", 1.0, "cursor", Doc.of(
                                    "id", cursorIds.incrementAndGet(),
                                    "ns", doc.get("$db") + "." + doc.get("aggregate"),
                                    "firstBatch", List.of(),
                                    "postBatchResumeToken", Doc.of("_data", "TOKEN-" + registrations.get())));
                        }
                    } else if (doc.containsKey("getMore")) {
                        getMores.incrementAndGet();

                        if (mode == Mode.CURSOR_GONE_ON_GETMORE) {
                            // what PoppyDB's generic getMore path answers for a cursor it no longer has
                            answer = Doc.of("ok", 1.0, "cursor", Doc.of(
                                    "id", 0L, "ns", doc.get("$db") + "." + doc.get("collection"), "nextBatch", List.of()));
                        } else {
                            answer = historyLost();
                        }
                    } else if (doc.containsKey("killCursors")) {
                        answer = Doc.of("ok", 1.0, "cursorsKilled", doc.get("cursors"));
                    } else {
                        String hostPort = "localhost:" + port;
                        answer = Doc.of("ok", (Object) 1.0, "isWritablePrimary", true, "helloOk", true)
                                .add("hosts", List.of(hostPort))
                                .add("primary", hostPort)
                                .add("maxBsonObjectSize", 16 * 1024 * 1024)
                                .add("maxMessageSizeBytes", 48000000)
                                .add("maxWriteBatchSize", 100000)
                                .add("localTime", new Date());
                    }

                    OpMsg reply = new OpMsg();
                    reply.setMessageId(msgId.incrementAndGet());
                    reply.setResponseTo(request.getMessageId());
                    reply.setFirstDoc(answer);
                    out.write(reply.bytes());
                    out.flush();
                }
            } catch (Exception e) {
                // connection died or server shutting down
            }
        }

        /** Exactly what PoppyDB's MongoCommandHandler answers a parked getMore with. */
        private static Map<String, Object> historyLost() {
            return Doc.of("ok", 0.0, "code", 286, "codeName", "ChangeStreamHistoryLost",
                    "errmsg", "ChangeStreamHistoryLost: resume window lost for change stream on "
                            + "csm_backoff_test.some_collection: resume token is beyond this driver's newest token");
        }

        @Override
        public void close() {
            running = false;

            for (Socket s : clients) {
                try {
                    s.close();
                } catch (Exception ignored) {
                }
            }

            try {
                serverSocket.close();
            } catch (Exception ignored) {
            }
        }
    }
}
