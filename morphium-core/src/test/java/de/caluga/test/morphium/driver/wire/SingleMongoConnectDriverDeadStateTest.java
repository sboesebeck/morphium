package de.caluga.test.morphium.driver.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

/**
 * Regression tests for issue #310: a SingleMongoConnectDriver whose connection dropped could end
 * up permanently dead - no connection, self-repair heartbeat cancelled - while every subsequent
 * {@code getConnection()} handed out a ConnectionWrapper around {@code null}. That wrapper is
 * non-null (so callers' null checks pass) but every delegating call threw a plain
 * {@code RuntimeException} instead of a {@code MorphiumDriverException}, so retry/failover logic
 * keyed on the driver exception type never engaged.
 *
 * These tests do not need a running MongoDB: they use a minimal in-process fake that speaks just
 * enough wire protocol to answer every OP_MSG with a hello reply - which is all the driver's
 * connect handshake and heartbeat need.
 */
public class SingleMongoConnectDriverDeadStateTest {

    /** A closed local port: refuses immediately, like an unreachable host. */
    private int deadPort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private SingleMongoConnectDriver newDriver(int port) {
        SingleMongoConnectDriver drv = new SingleMongoConnectDriver();
        drv.setHostSeed(List.of("localhost:" + port));
        drv.setConnectionTimeout(500);
        drv.setRetriesOnNetworkError(1);
        drv.setSleepBetweenErrorRetries(10);
        drv.setMaxWaitTime(2000);
        drv.setHeartbeatFrequency(250);
        return drv;
    }

    // ------------------------------------------------------------------
    // 1. getConnection() must never hand out a wrapper around null.
    // ------------------------------------------------------------------

    @Test
    void getConnectionOnDriverWithoutConnectionAndDeadHostThrowsMorphiumDriverException() throws Exception {
        int port = deadPort();
        SingleMongoConnectDriver drv = newDriver(port);

        try {
            // connection == null here. The old code returned new ConnectionWrapper(null) -
            // a non-null object whose every use throws a plain RuntimeException. It must
            // either connect (impossible here - host is dead) or throw MorphiumDriverException.
            assertThrows(MorphiumDriverException.class, () -> {
                MongoConnection con = drv.getConnection();
                // if we even got a connection object handed out, it must be usable -
                // using it must not be the first point of failure
                con.getConnectedTo();
            });
        } finally {
            drv.close();
        }
    }

    @Test
    void closedDriverWithHostDownThrowsButRecoversWhenHostReturns() throws Exception {
        int port;

        try (FakeMongod server = new FakeMongod(0)) {
            port = server.getPort();
            SingleMongoConnectDriver drv = newDriver(port);

            try {
                drv.connect();
                assertTrue(drv.isConnected(), "driver should be connected to the fake mongod");
                // simulate what the broken heartbeat recovery did: close() nulls the connection
                // and cancels the heartbeat
                drv.close();
                server.stop();

                // host down + no connection: must surface as MorphiumDriverException,
                // NOT as a "successful" wrapper around null
                assertThrows(MorphiumDriverException.class, () -> {
                    MongoConnection con = drv.getConnection();
                    con.getConnectedTo();
                });

                // host comes back on the same port: the driver must be able to recover
                try (FakeMongod restarted = new FakeMongod(port)) {
                    MongoConnection con = drv.getConnection();
                    assertNotNull(con);
                    assertTrue(con.isConnected(), "connection handed out after recovery must be usable");
                    assertTrue(drv.isConnected(), "driver must have recovered");
                    drv.releaseConnection(con);
                }
            } finally {
                drv.close();
            }
        }
    }

    // ------------------------------------------------------------------
    // 2. A failed heartbeat reconnect must not kill the heartbeat:
    //    the driver has to keep healing itself.
    // ------------------------------------------------------------------

    @Test
    void heartbeatSurvivesFailedReconnectAndRecoversWhenHostReturns() throws Exception {
        int port;
        SingleMongoConnectDriver drv;

        try (FakeMongod server = new FakeMongod(0)) {
            port = server.getPort();
            drv = newDriver(port);
            drv.connect();
            assertTrue(drv.isConnected(), "driver should be connected to the fake mongod");
            // kill the server hard (RST) so the next heartbeat hello fails with a driver
            // exception and enters the recovery path: close(); sleep; connect() - where
            // connect() fails because the host is down
            server.stop();
            // give the heartbeat (250ms) time to notice, run its recovery and fail the
            // reconnect at least once - in the old code this cancelled the heartbeat for good
            Thread.sleep(4000);
        }

        try {
            // host returns on the same port: a driver with a live self-repair heartbeat
            // reconnects on one of the next ticks; the old (dead) driver never does
            try (FakeMongod restarted = new FakeMongod(port)) {
                long deadline = System.currentTimeMillis() + 15000;

                while (!drv.isConnected() && System.currentTimeMillis() < deadline) {
                    Thread.sleep(200);
                }

                assertTrue(drv.isConnected(),
                    "driver must recover on its own after the host comes back - "
                    + "the heartbeat must survive a failed reconnect (#310)");
                MongoConnection con = drv.getConnection();
                assertTrue(con.isConnected());
                drv.releaseConnection(con);
            }
        } finally {
            drv.close();
        }
    }

    // ------------------------------------------------------------------
    // 3. Failure type: connection problems must surface as
    //    MorphiumDriverException, not as plain RuntimeException.
    // ------------------------------------------------------------------

    @Test
    void usingAReleasedConnectionThrowsMorphiumDriverException() throws Exception {
        try (FakeMongod server = new FakeMongod(0)) {
            SingleMongoConnectDriver drv = newDriver(server.getPort());

            try {
                drv.connect();
                MongoConnection con = drv.getConnection();
                assertTrue(con.isConnected());
                drv.releaseConnection(con);
                // the wrapper's delegate is gone now - using it is an error, but it has to be
                // the driver's exception type so callers catching MorphiumDriverException work
                assertThrows(MorphiumDriverException.class, con::getConnectedTo);
            } finally {
                drv.close();
            }
        }
    }

    /**
     * A discarded driver must not keep a scheduler thread alive (#311). Every driver owns a
     * private {@code ScheduledThreadPoolExecutor} whose threads are named {@code SCCon_*}; when
     * close() left it running, each closed driver cost one idle daemon thread for the lifetime
     * of the process - and a node that redials a flapping peer discards drivers continuously.
     */
    @Test
    void closeShutsDownTheDriversSchedulerButTheDriverStaysReusable() throws Exception {
        try (FakeMongod server = new FakeMongod(0)) {
            // Relative to a baseline, not absolute: other drivers may legitimately be alive in
            // this JVM when the suite runs as a whole.
            long baseline = sconThreadsAlive();

            SingleMongoConnectDriver drv = new SingleMongoConnectDriver();
            drv.setHostSeed("127.0.0.1:" + server.getPort());
            drv.setHeartbeatFrequency(50);
            drv.connect();

            assertTrue(drv.isConnected(), "precondition: the driver is connected");
            assertTrue(sconThreadsAlive() > baseline, "precondition: the driver runs a scheduler thread");

            drv.close();

            long deadline = System.currentTimeMillis() + 5000;

            while (sconThreadsAlive() > baseline && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }

            assertEquals(baseline, sconThreadsAlive(),
                    "close() must shut the driver's scheduler down - a discarded driver may not "
                            + "keep an SCCon_* thread alive");

            // ... and reviving the driver must still work: startHeartbeat needs a live executor
            MongoConnection con = drv.getConnection();
            assertNotNull(con, "a closed driver must still be usable again");
            assertTrue(drv.isConnected(), "the revived driver must be connected");
            drv.close();
        }
    }

    /**
     * Counts live driver scheduler threads. They are daemon threads named {@code SCCon_<n>}
     * (see the thread factory in SingleMongoConnectDriver).
     */
    private static long sconThreadsAlive() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .filter(t -> t.getName().startsWith("SCCon_"))
                .count();
    }

    // ==================================================================
    // Minimal fake mongod: answers every OP_MSG with a hello reply.
    // ==================================================================

    private static class FakeMongod implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final List<Socket> clients = new CopyOnWriteArrayList<>();
        private final AtomicInteger msgId = new AtomicInteger(1000);
        private volatile boolean running = true;
        private final int port;

        FakeMongod(int port) throws Exception {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress("127.0.0.1", port));
            this.port = serverSocket.getLocalPort();
            Thread acceptor = new Thread(this::acceptLoop, "fake-mongod-acceptor-" + this.port);
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
                    Thread t = new Thread(() -> serve(s), "fake-mongod-conn-" + port);
                    t.setDaemon(true);
                    t.start();
                } catch (Exception e) {
                    return; // socket closed - shutting down
                }
            }
        }

        private void serve(Socket s) {
            try {
                InputStream in = s.getInputStream();
                OutputStream out = s.getOutputStream();

                while (running && !s.isClosed()) {
                    WireProtocolMessage incoming = WireProtocolMessage.parseFromStream(in);

                    if (incoming == null) {
                        return; // EOF - client disconnected
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

                    String hostPort = "localhost:" + port;
                    OpMsg reply = new OpMsg();
                    reply.setMessageId(msgId.incrementAndGet());
                    reply.setResponseTo(request.getMessageId());
                    reply.setFirstDoc(Doc.of(
                            "ok", (Object) 1.0,
                            "isWritablePrimary", true,
                            "helloOk", true)
                        .add("hosts", List.of(hostPort))
                        .add("primary", hostPort)
                        .add("maxBsonObjectSize", 16 * 1024 * 1024)
                        .add("maxMessageSizeBytes", 48000000)
                        .add("maxWriteBatchSize", 100000)
                        .add("localTime", new Date()));
                    out.write(reply.bytes());
                    out.flush();
                }
            } catch (Exception e) {
                // connection died or server shutting down - fine
            }
        }

        /** Hard shutdown: RST all client connections so reads/writes fail immediately. */
        void stop() {
            running = false;

            for (Socket s : clients) {
                try {
                    s.setSoLinger(true, 0); // close with RST, not FIN
                } catch (Exception ignored) {
                }

                try {
                    s.close();
                } catch (Exception ignored) {
                }
            }

            clients.clear();

            try {
                serverSocket.close();
            } catch (Exception ignored) {
            }
        }

        @Override
        public void close() {
            stop();
        }
    }
}
