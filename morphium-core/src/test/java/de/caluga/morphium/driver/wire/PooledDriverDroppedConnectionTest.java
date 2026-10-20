package de.caluga.morphium.driver.wire;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumDriverNetworkException;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The production shape of a "the server dropped our pooled connections" event, end to end through
 * {@link PooledDriver}: a connection sits idle in the pool, the server closes it (its own pool
 * expiry, a restart, an idle-timeout on a load balancer), and the pooled connection still looks
 * connected locally - so the next borrower's request goes out on a dead socket.
 *
 * <p>What must happen then: a retryable network error, never a {@code null} answer. Returning
 * null used to surface as an NPE in the command wrappers (here: {@code listDatabases()}'s
 * {@code res.get("databases")}) or, on plain reads, as a silently empty result set. The write path
 * additionally has to recover on its own - it re-resolves a connection and re-sends.
 */
@Tag("core")
public class PooledDriverDroppedConnectionTest {

    private CannedBackend backend;
    private PooledDriver drv;

    /** Minimal MongoDB-wire backend: answers hello/listDatabases/insert, and can drop every open
     * connection the way a server dropping its idle client connections does (clean FIN). */
    private static final class CannedBackend implements AutoCloseable {
        private final ServerSocket listener;
        private final List<Socket> open = Collections.synchronizedList(new ArrayList<>());
        private final Thread acceptor;
        private volatile boolean running = true;

        CannedBackend() throws IOException {
            listener = new ServerSocket(0);
            acceptor = new Thread(this::acceptLoop, "canned-backend");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        int port() {
            return listener.getLocalPort();
        }

        private void acceptLoop() {
            while (running) {
                try {
                    Socket s = listener.accept();
                    open.add(s);
                    Thread t = new Thread(() -> serve(s), "canned-backend-conn");
                    t.setDaemon(true);
                    t.start();
                } catch (IOException e) {
                    return;
                }
            }
        }

        private void serve(Socket s) {
            try {
                while (running && !s.isClosed()) {
                    WireProtocolMessage req = WireProtocolMessage.parseFromStream(s.getInputStream());

                    if (req == null) {
                        return; // peer gone
                    }

                    OpMsg msg = (OpMsg) req;
                    Map<String, Object> doc = msg.getFirstDoc();
                    OpMsg resp = new OpMsg();
                    resp.setMessageId(msg.getMessageId() + 1000);
                    resp.setResponseTo(msg.getMessageId());
                    resp.setFirstDoc(replyFor(doc));

                    synchronized (s) {
                        s.getOutputStream().write(resp.bytes());
                        s.getOutputStream().flush();
                    }
                }
            } catch (Exception ignored) {
            } finally {
                open.remove(s);
                try {
                    s.close();
                } catch (IOException ignored) {
                }
            }
        }

        private static Map<String, Object> replyFor(Map<String, Object> cmd) {
            if (cmd.containsKey("hello") || cmd.containsKey("isMaster")) {
                Doc hello = Doc.of("ok", 1.0,
                                    "isWritablePrimary", true,
                                    "helloOk", true,
                                    "maxWireVersion", 21,
                                    "maxBsonObjectSize", 16 * 1024 * 1024,
                                    "maxMessageSizeBytes", 48_000_000);
                hello.put("maxWriteBatchSize", 100_000);
                return hello;
            }

            if (cmd.containsKey("listDatabases")) {
                return Doc.of("ok", 1.0, "databases", List.of(Doc.of("name", "test")));
            }

            if (cmd.containsKey("insert")) {
                return Doc.of("ok", 1.0, "n", 1);
            }

            return Doc.of("ok", 1.0);
        }

        /** Drops every currently open connection with a clean FIN - no reply for anything in
         * flight, exactly like a pool expiry on the server side. New connections are still
         * accepted (the real host is alive, it just discarded the idle sockets). */
        void dropAllConnections() {
            for (Socket s : new ArrayList<>(open)) {
                try {
                    s.close();
                } catch (IOException ignored) {
                }
            }
            open.clear();
        }

        @Override
        public void close() {
            running = false;
            dropAllConnections();

            try {
                listener.close();
            } catch (IOException ignored) {
            }

            acceptor.interrupt();
        }
    }

    private void connectDriver() throws Exception {
        backend = new CannedBackend();

        drv = new PooledDriver();
        drv.setHostSeed("127.0.0.1:" + backend.port());
        drv.setMinConnectionsPerHost(1);
        drv.setMaxConnectionsPerHost(1);
        drv.setConnectionTimeout(1000);
        drv.setMaxWaitTime(1000);
        drv.setServerSelectionTimeout(5000);
        drv.setRetriesOnNetworkError(1);
        drv.setSleepBetweenErrorRetries(50);
        // the initial heartbeat cycle runs right away; keep every later one out of the test so
        // only the borrow path can replace a connection
        drv.setHeartbeatFrequency(3_600_000);
        drv.connect();

        // let the initial heartbeat cycle finish its host check so it cannot race the commands
        Thread.sleep(500);
    }

    @AfterEach
    public void tearDown() {
        if (drv != null) {
            try {
                drv.close();
            } catch (Exception ignored) {
            }
        }

        if (backend != null) {
            backend.close();
        }
    }

    @Test
    public void commandOnServerDroppedConnectionThrowsInsteadOfNpe() throws Exception {
        connectDriver();
        List<String> before = drv.listDatabases();
        assertThat(before).containsExactly("test");

        backend.dropAllConnections();

        Throwable t = catchThrowable(() -> drv.listDatabases());

        assertThat(t).as("a dropped pooled connection must surface as a retryable network error, not as null->NPE")
                .isInstanceOf(MorphiumDriverNetworkException.class);
        assertThat(t).isNotInstanceOf(NullPointerException.class);
    }

    @Test
    public void writeRecoversOnAFreshConnectionAfterTheServerDroppedThePool() throws Exception {
        connectDriver();
        assertThat(drv.listDatabases()).containsExactly("test");

        backend.dropAllConnections();

        InsertMongoCommand insert = new InsertMongoCommand(drv.getPrimaryConnection(null));
        insert.setDb("test").setColl("coll").setDocuments(List.of(Doc.of("_id", 1, "val", "x")));

        Map<String, Object> result = insert.execute();

        assertThat(result).as("the write must have been retried on a fresh connection").isNotNull();
        assertThat(result.get("ok")).isEqualTo(1.0);
        assertThat(result.get("n")).isEqualTo(1);
        insert.releaseConnection();
    }

    @Test
    public void thePoolRefillsAndReadsWorkAgain() throws Exception {
        connectDriver();
        assertThat(drv.listDatabases()).containsExactly("test");

        backend.dropAllConnections();

        // the dead connection is still in the pool and is borrowed (and discarded) by the first
        // command - that one fails with the retryable network error asserted above. The second
        // command then waits in borrowConnection for the connection waiter to create a fresh one.
        Throwable firstAttempt = catchThrowable(() -> drv.getCollStats("test", "coll"));
        assertThat(firstAttempt).isInstanceOf(MorphiumDriverNetworkException.class);

        Map<String, Object> stats = drv.getCollStats("test", "coll");

        assertThat(stats).as("a fresh connection must have been created to serve the command").isNotNull();
        assertThat(stats.get("ok")).isEqualTo(1.0);
    }

    @Test
    public void listDatabasesOnADisconnectedDriverThrowsInsteadOfReturningNull() throws Exception {
        // never connected - the state a caller sees during startup, mid-failover, or right after
        // the server dropped every pooled connection: listDatabases() must say so, not answer
        // null (which read exactly like "there are no databases")
        drv = new PooledDriver();
        drv.setHostSeed("127.0.0.1:1");

        Throwable t = catchThrowable(() -> drv.listDatabases());

        assertThat(t).isInstanceOf(MorphiumDriverException.class)
                .hasMessageContaining("Not connected");
    }
}
