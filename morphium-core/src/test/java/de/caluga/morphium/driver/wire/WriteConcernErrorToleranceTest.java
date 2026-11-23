package de.caluga.morphium.driver.wire;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

/**
 * #529: a write answer that carries a {@code writeConcernError} keeps {@code ok: 1} - the write
 * itself was executed, only the concern failed. Code 189 (PrimarySteppedDown) is PoppyDB's new
 * answer for a wait that lost its primary, and it must be treated exactly like code 64
 * (WriteConcernFailed): handed to the caller, and above all NOT re-sent. A re-send would apply a
 * non-idempotent update twice, which is precisely why the server answers a write concern error
 * instead of a top-level not-primary error.
 *
 * <p>Stub node over the real wire protocol (pattern of
 * {@code ReadCommandStepDownStubServerTest}): it counts the inserts that reach it, so "no
 * re-send" is measured, not assumed.
 */
@Tag("core")
public class WriteConcernErrorToleranceTest {

    private StubNode node;
    private PooledDriver driver;

    @AfterEach
    public void tearDown() {
        if (driver != null) {
            driver.close();
        }
        if (node != null) {
            node.close();
        }
    }

    private void start(int writeConcernErrorCode) throws Exception {
        node = new StubNode(writeConcernErrorCode);
        driver = new PooledDriver();
        driver.setHostSeed(List.of(node.address()));
        driver.setConnectionTimeout(1000);
        driver.setMaxWaitTime(2000);
        driver.setServerSelectionTimeout(3000);
        driver.setHeartbeatFrequency(500);
        driver.setRetriesOnNetworkError(3);
        driver.setSleepBetweenErrorRetries(50);
        driver.connect();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> insert(String id) throws Exception {
        MongoConnection con = driver.getPrimaryConnection(null);
        try {
            InsertMongoCommand cmd = new InsertMongoCommand(con)
                    .setDb("wctest")
                    .setColl("docs")
                    .setDocuments(List.of(Doc.of("_id", id, "v", 1)))
                    .setWriteConcern(Doc.of("w", 2, "wtimeout", 1000));
            return cmd.execute();
        } finally {
            driver.releaseConnection(con);
        }
    }

    @Test
    @Timeout(30)
    public void aSteppedDownWriteConcernIsHandedUpOnceAndNeverReSent() throws Exception {
        start(189);

        Map<String, Object> result = insert("stepped-down");

        assertThat(result).as("the insert itself succeeded - ok:1 and a writeConcernError").isNotNull();
        assertThat(result.get("writeConcernError")).as("code 189 must reach the caller").isNotNull();
        assertThat(((Map<String, Object>) result.get("writeConcernError")).get("code"))
                .isEqualTo(189);
        assertThat(node.inserts.get()).as("a write concern error must not be re-sent").isEqualTo(1);
    }

    @Test
    @Timeout(30)
    public void aWriteConcernTimeoutIsHandledTheSameWay() throws Exception {
        start(64);

        Map<String, Object> result = insert("timed-out");

        assertThat(result.get("writeConcernError")).as("code 64 must reach the caller").isNotNull();
        assertThat(((Map<String, Object>) result.get("writeConcernError")).get("code"))
                .isEqualTo(64);
        assertThat(node.inserts.get()).as("a write concern error must not be re-sent").isEqualTo(1);
    }

    /** Speaks just enough OP_MSG for a PooledDriver: hello for handshake/heartbeat, one insert. */
    static class StubNode implements AutoCloseable {
        final AtomicInteger inserts = new AtomicInteger();

        private final int writeConcernErrorCode;
        private final String address;
        private final ServerSocket serverSocket;
        private final List<Socket> clients = new CopyOnWriteArrayList<>();
        private final AtomicInteger msgId = new AtomicInteger(1000);
        private final AtomicBoolean running = new AtomicBoolean(true);

        StubNode(int writeConcernErrorCode) throws Exception {
            this.writeConcernErrorCode = writeConcernErrorCode;
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
            address = "127.0.0.1:" + serverSocket.getLocalPort();
            Thread acceptor = new Thread(this::acceptLoop, "wc-stub-acceptor-" + serverSocket.getLocalPort());
            acceptor.setDaemon(true);
            acceptor.start();
        }

        String address() {
            return address;
        }

        private void acceptLoop() {
            while (running.get()) {
                try {
                    Socket s = serverSocket.accept();
                    clients.add(s);
                    Thread t = new Thread(() -> serve(s), "wc-stub-conn-" + serverSocket.getLocalPort());
                    t.setDaemon(true);
                    t.start();
                } catch (Exception e) {
                    return;
                }
            }
        }

        private void serve(Socket s) {
            try {
                InputStream in = s.getInputStream();
                OutputStream out = s.getOutputStream();

                while (running.get() && !s.isClosed()) {
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

                    if (doc.containsKey("insert")) {
                        inserts.incrementAndGet();
                        // The write WAS executed - only its concern failed.
                        answer = Doc.of("ok", 1.0, "n", 1)
                                .add("writeConcernError", Doc.of(
                                        "code", writeConcernErrorCode,
                                        "codeName", writeConcernErrorCode == 189
                                                ? "PrimarySteppedDown" : "WriteConcernFailed",
                                        "errmsg", "write concern could not be satisfied"));
                    } else if (doc.containsKey("hello") || doc.containsKey("isMaster")
                            || doc.containsKey("ismaster")) {
                        answer = Doc.of("ok", (Object) 1.0, "helloOk", true)
                                .add("isWritablePrimary", true)
                                .add("secondary", false)
                                .add("setName", "rsStub")
                                .add("hosts", List.of(address))
                                .add("me", address)
                                .add("maxBsonObjectSize", 16 * 1024 * 1024)
                                .add("maxMessageSizeBytes", 48000000)
                                .add("maxWriteBatchSize", 100000)
                                .add("localTime", new Date());
                    } else {
                        answer = Doc.of("ok", 1.0);
                    }

                    OpMsg reply = new OpMsg();
                    reply.setMessageId(msgId.incrementAndGet());
                    reply.setResponseTo(request.getMessageId());
                    reply.setFirstDoc(answer);
                    out.write(reply.bytes());
                    out.flush();
                }
            } catch (Exception e) {
                // connection died or node shutting down
            }
        }

        @Override
        public void close() {
            running.set(false);

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
