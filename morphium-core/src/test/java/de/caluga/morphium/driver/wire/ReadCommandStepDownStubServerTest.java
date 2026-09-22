package de.caluga.morphium.driver.wire;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.commands.ListCollectionsCommand;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

/**
 * #393 end to end over the wire: a {@link PooledDriver} against two stub nodes. Node A is the
 * primary the driver discovers; its first primary read is answered with 13435
 * (NotPrimaryNoSecondaryOk) and from that moment its hello says "not primary, the primary is
 * B" - the shape of a stepped-down mongod / PoppyDB node. The read must come back with B's
 * answer, after exactly one retry, and well before the driver's heartbeat could have noticed
 * the change (the heartbeat is set to 5 s here, the read must finish in under 3 s): the
 * re-resolution has to be driven by the rejected read itself.
 *
 * <p>The negative twin: a read that asked for {@code secondaryPreferred} and is answered 13435
 * is the caller's configuration error and stays an exception, without any retry.
 */
@Tag("core")
public class ReadCommandStepDownStubServerTest {

    private StubNode nodeA;
    private StubNode nodeB;
    private PooledDriver driver;

    @AfterEach
    public void tearDown() {
        if (driver != null) {
            driver.close();
        }
        if (nodeA != null) {
            nodeA.close();
        }
        if (nodeB != null) {
            nodeB.close();
        }
    }

    private void startClusterAndConnect() throws Exception {
        startClusterAndConnect(5000);
    }

    private void startClusterAndConnect(int heartbeatMs) throws Exception {
        Topology topology = new Topology();
        nodeA = new StubNode(topology);
        nodeB = new StubNode(topology);
        topology.primary = nodeA.address();
        topology.members = List.of(nodeA.address(), nodeB.address());

        driver = new PooledDriver();
        driver.setHostSeed(List.of(nodeA.address(), nodeB.address()));
        driver.setConnectionTimeout(1000);
        driver.setMaxWaitTime(2000);
        driver.setServerSelectionTimeout(3000);
        driver.setHeartbeatFrequency(heartbeatMs);
        driver.setRetriesOnNetworkError(3);
        driver.setSleepBetweenErrorRetries(50);
        driver.connect();

        assertThat(driver.getPrimaryNode()).isEqualTo(nodeA.address());
    }

    @Test
    @Timeout(30)
    public void primaryReadRejectedWithNotPrimaryIsRetriedOnTheNewPrimary() throws Exception {
        startClusterAndConnect();
        // A steps down as it answers the read: 13435 now, and its hello names B from then on
        nodeA.rejectNextReadAndStepDownTo(nodeB);

        long start = System.currentTimeMillis();
        List<String> collections = driver.listCollections("testdb", null);
        long duration = System.currentTimeMillis() - start;

        assertThat(collections).containsExactly("uncached_object");
        assertThat(nodeA.reads.get()).as("the stepped-down node is asked once").isEqualTo(1);
        assertThat(nodeB.reads.get()).as("exactly one retry, on the new primary").isEqualTo(1);
        assertThat(driver.getPrimaryNode()).isEqualTo(nodeB.address());
        assertThat(duration)
            .as("the retry must not wait for the 5 s heartbeat - the rejected read re-resolves the primary")
            .isLessThan(3000);
    }

    @Test
    @Timeout(30)
    public void steppedDownNodeStillNamingItselfAsPrimaryIsNotBelieved() throws Exception {
        // the shape PoppyDB showed after #392: isWritablePrimary:false together with
        // primary:<itself>, while B already is the primary. The driver must not stay on A on the
        // strength of that hello - it clears its primary and the heartbeat finds B.
        startClusterAndConnect(1000);
        nodeA.rejectNextReadAndStepDownTo(nodeB);
        nodeA.keepAdvertisingItselfAsPrimary();

        List<String> collections = driver.listCollections("testdb", null);

        assertThat(collections).containsExactly("uncached_object");
        assertThat(nodeA.reads.get()).as("the stepped-down node is asked once").isEqualTo(1);
        assertThat(nodeB.reads.get()).as("the retry lands on the real primary").isEqualTo(1);
        assertThat(driver.getPrimaryNode()).isEqualTo(nodeB.address());
    }

    @Test
    @Timeout(30)
    public void secondaryPreferredReadRejectedWithNotPrimaryIsNotRetried() throws Exception {
        startClusterAndConnect();
        // B stays secondary and answers the read with 13435 - a server that does not honour the
        // read preference, or a caller that sent the wrong one: not a failover
        nodeB.rejectNextRead();

        MongoConnection con = driver.getReadConnection(ReadPreference.secondaryPreferred());
        assertThat(con.getConnectedTo()).isEqualTo(nodeB.address());
        ListCollectionsCommand cmd = new ListCollectionsCommand(con);
        cmd.setDb("testdb").setNameOnly(true);

        try {
            assertThatThrownBy(cmd::execute)
                .isInstanceOf(MorphiumDriverException.class)
                .hasMessageContaining("13435");
        } finally {
            if (cmd.getConnection() != null) {
                cmd.releaseConnection();
            }
        }

        assertThat(nodeB.reads.get()).isEqualTo(1);
        assertThat(nodeA.reads.get()).as("no retry on the primary for a secondaryPreferred read").isEqualTo(0);
    }

    /** what both nodes agree on: who is primary, who is in the set */
    static class Topology {
        volatile String primary;
        volatile List<String> members = List.of();
    }

    /**
     * Speaks just enough OP_MSG for a PooledDriver: hello for handshake and heartbeat,
     * listCollections as the read under test, ok:1 for everything else.
     */
    static class StubNode implements AutoCloseable {
        final AtomicInteger reads = new AtomicInteger();

        private final Topology topology;
        private final ServerSocket serverSocket;
        private final List<Socket> clients = new CopyOnWriteArrayList<>();
        private final AtomicInteger msgId = new AtomicInteger(1000);
        private final AtomicBoolean rejectNextRead = new AtomicBoolean();
        private volatile StubNode stepDownTo;
        private volatile boolean advertiseSelfAsPrimary;
        private volatile boolean running = true;

        StubNode(Topology topology) throws Exception {
            this.topology = topology;
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
            Thread acceptor = new Thread(this::acceptLoop, "stub-node-acceptor-" + serverSocket.getLocalPort());
            acceptor.setDaemon(true);
            acceptor.start();
        }

        String address() {
            return "127.0.0.1:" + serverSocket.getLocalPort();
        }

        /** the next read is answered 13435; the node's role does not change */
        void rejectNextRead() {
            rejectNextRead.set(true);
        }

        /** the next read is answered 13435, and from then on this node's hello names {@code successor} */
        void rejectNextReadAndStepDownTo(StubNode successor) {
            stepDownTo = successor;
            rejectNextRead.set(true);
        }

        /** this node's hello keeps naming this node as primary, whatever the topology says */
        void keepAdvertisingItselfAsPrimary() {
            advertiseSelfAsPrimary = true;
        }

        private boolean isPrimary() {
            return address().equals(topology.primary);
        }

        private void acceptLoop() {
            while (running) {
                try {
                    Socket s = serverSocket.accept();
                    clients.add(s);
                    Thread t = new Thread(() -> serve(s), "stub-node-conn-" + serverSocket.getLocalPort());
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

                    if (doc.containsKey("listCollections")) {
                        reads.incrementAndGet();

                        if (rejectNextRead.getAndSet(false)) {
                            StubNode successor = stepDownTo;
                            if (successor != null) {
                                topology.primary = successor.address();
                            }
                            answer = Doc.of("ok", 0.0, "code", 13435, "codeName", "NotPrimaryNoSecondaryOk",
                                    "errmsg", "not primary and secondaryOk=false");
                        } else {
                            answer = Doc.of("ok", 1.0, "cursor", Doc.of(
                                    "id", 0L,
                                    "ns", doc.get("$db") + ".$cmd.listCollections",
                                    "firstBatch", List.of(Doc.of("name", "uncached_object", "type", "collection"))));
                        }
                    } else if (doc.containsKey("hello") || doc.containsKey("isMaster") || doc.containsKey("ismaster")) {
                        boolean primary = isPrimary();
                        answer = Doc.of("ok", (Object) 1.0, "helloOk", true)
                                .add("isWritablePrimary", primary)
                                .add("secondary", !primary)
                                .add("setName", "rsStub")
                                .add("hosts", topology.members)
                                .add("me", address())
                                .add("maxBsonObjectSize", 16 * 1024 * 1024)
                                .add("maxMessageSizeBytes", 48000000)
                                .add("maxWriteBatchSize", 100000)
                                .add("localTime", new Date());
                        String advertised = advertiseSelfAsPrimary ? address() : topology.primary;
                        if (advertised != null) {
                            answer.put("primary", advertised);
                        }
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
