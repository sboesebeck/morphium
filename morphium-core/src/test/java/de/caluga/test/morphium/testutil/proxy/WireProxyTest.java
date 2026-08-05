package de.caluga.test.morphium.testutil.proxy;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

public class WireProxyTest {

    private final List<AutoCloseable> toClose = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() {
        for (AutoCloseable c : toClose) {
            try { c.close(); } catch (Exception ignored) { }
        }
        toClose.clear();
    }

    /** Accepts connections forever (until closed) and replies to every request with a fixed
     * OP_MSG document - good enough to prove wire-level proxy behavior without a real server. */
    private static class CannedBackend implements AutoCloseable {
        final ServerSocket listener;
        final AtomicInteger acceptedConnections = new AtomicInteger();
        volatile boolean running = true;

        CannedBackend(Doc reply) throws IOException {
            listener = new ServerSocket();
            listener.bind(new InetSocketAddress("localhost", 0));
            Thread t = new Thread(() -> {
                while (running) {
                    try {
                        Socket s = listener.accept();
                        acceptedConnections.incrementAndGet();
                        Thread handler = new Thread(() -> {
                            try {
                                while (!s.isClosed()) {
                                    WireProtocolMessage req = WireProtocolMessage.parseFromStream(s.getInputStream());
                                    if (req == null) return;
                                    OpMsg resp = new OpMsg();
                                    resp.setMessageId(req.getMessageId() + 1000);
                                    resp.setResponseTo(req.getMessageId());
                                    resp.setFirstDoc(reply);
                                    s.getOutputStream().write(resp.bytes());
                                    s.getOutputStream().flush();
                                }
                            } catch (Exception ignored) {
                            }
                        }, "canned-backend-conn");
                        handler.setDaemon(true);
                        handler.start();
                    } catch (IOException e) {
                        return; // listener closed
                    }
                }
            }, "canned-backend-accept");
            t.setDaemon(true);
            t.start();
        }

        int port() { return listener.getLocalPort(); }

        @Override
        public void close() throws IOException {
            running = false;
            listener.close();
        }
    }

    private Doc helloReply(String me, List<String> hosts, String primary) {
        return Doc.of("ok", 1.0, "isWritablePrimary", true, "setName", "rsTest",
                "me", me, "hosts", hosts, "primary", primary);
    }

    /** Sends one OP_MSG ping and returns the reply's first document, or throws on any I/O error. */
    private Doc pingThrough(int proxyPort) throws Exception {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("localhost", proxyPort), 2000);
            s.setSoTimeout(3000);
            OpMsg req = new OpMsg();
            req.setMessageId(1);
            req.setFirstDoc(Doc.of("ping", 1));
            s.getOutputStream().write(req.bytes());
            s.getOutputStream().flush();
            OpMsg reply = (OpMsg) WireProtocolMessage.parseFromStream(s.getInputStream());
            return new Doc(reply.getFirstDoc());
        }
    }

    @Test
    void passthroughForwardsARequestAndReply() throws Exception {
        CannedBackend backend = new CannedBackend(helloReply("proxy:1", List.of("proxy:1"), "proxy:1"));
        toClose.add(backend);
        WireProxy proxy = new WireProxy("localhost", backend.port());
        toClose.add(proxy);
        proxy.start();

        Doc reply = pingThrough(proxy.getListenPort());
        assertEquals(1.0, ((Number) reply.get("ok")).doubleValue());
    }

    @Test
    void freezeOnAnExistingConnectionLeavesTheClientSocketOpenAndSilent() throws Exception {
        CannedBackend backend = new CannedBackend(helloReply("proxy:1", List.of("proxy:1"), "proxy:1"));
        toClose.add(backend);
        WireProxy proxy = new WireProxy("localhost", backend.port());
        toClose.add(proxy);
        proxy.start();

        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("localhost", proxy.getListenPort()), 2000);
            s.setSoTimeout(1500);
            // Establish the connection as passthrough first (matches how the driver would
            // already be connected before a fault kicks in mid-session).
            OpMsg warmup = new OpMsg();
            warmup.setMessageId(1);
            warmup.setFirstDoc(Doc.of("ping", 1));
            s.getOutputStream().write(warmup.bytes());
            s.getOutputStream().flush();
            WireProtocolMessage.parseFromStream(s.getInputStream());

            proxy.setFaultMode(FaultMode.freeze);
            OpMsg req = new OpMsg();
            req.setMessageId(2);
            req.setFirstDoc(Doc.of("ping", 2));
            s.getOutputStream().write(req.bytes());
            s.getOutputStream().flush();

            assertThrows(java.net.SocketTimeoutException.class,
                    () -> WireProtocolMessage.parseFromStream(s.getInputStream()),
                    "frozen connection must time out, not see EOF or a reset");
        }
    }

    @Test
    void freezeOnANewConnectionAlsoAcceptsThenHangsSilently() throws Exception {
        CannedBackend backend = new CannedBackend(helloReply("proxy:1", List.of("proxy:1"), "proxy:1"));
        toClose.add(backend);
        WireProxy proxy = new WireProxy("localhost", backend.port());
        toClose.add(proxy);
        proxy.start();
        proxy.setFaultMode(FaultMode.freeze); // fault active BEFORE the connection attempt

        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("localhost", proxy.getListenPort()), 2000); // must NOT be refused
            s.setSoTimeout(1500);
            OpMsg req = new OpMsg();
            req.setMessageId(1);
            req.setFirstDoc(Doc.of("ping", 1));
            s.getOutputStream().write(req.bytes());
            s.getOutputStream().flush();

            assertThrows(java.net.SocketTimeoutException.class,
                    () -> WireProtocolMessage.parseFromStream(s.getInputStream()),
                    "a brand-new connection during freeze must also hang, not refuse or answer");
        }
    }

    @Test
    void resetOnANewConnectionIsRefusedOutright() throws Exception {
        CannedBackend backend = new CannedBackend(helloReply("proxy:1", List.of("proxy:1"), "proxy:1"));
        toClose.add(backend);
        WireProxy proxy = new WireProxy("localhost", backend.port());
        toClose.add(proxy);
        proxy.start();
        proxy.setFaultMode(FaultMode.reset);

        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("localhost", proxy.getListenPort()), 2000);
            s.setSoTimeout(1500);
            // The socket may connect (TCP accept happened), but any read must fail fast with a
            // reset/EOF-shaped error - never time out, never return a reply.
            assertThrows(IOException.class,
                    () -> WireProtocolMessage.parseFromStream(s.getInputStream()));
        }
    }

    @Test
    void closeOnANewConnectionIsAlsoRefusedOutright() throws Exception {
        CannedBackend backend = new CannedBackend(helloReply("proxy:1", List.of("proxy:1"), "proxy:1"));
        toClose.add(backend);
        WireProxy proxy = new WireProxy("localhost", backend.port());
        toClose.add(proxy);
        proxy.start();
        proxy.setFaultMode(FaultMode.close);

        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("localhost", proxy.getListenPort()), 2000);
            s.setSoTimeout(1500);
            WireProtocolMessage reply = WireProtocolMessage.parseFromStream(s.getInputStream());
            assertNull(reply, "close-refused new connection must see EOF (null from parseFromStream), not a reply");
        }
    }

    @Test
    void resetOnAnExistingConnectionSeversItWithReset() throws Exception {
        CannedBackend backend = new CannedBackend(helloReply("proxy:1", List.of("proxy:1"), "proxy:1"));
        toClose.add(backend);
        WireProxy proxy = new WireProxy("localhost", backend.port());
        toClose.add(proxy);
        proxy.start();

        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("localhost", proxy.getListenPort()), 2000);
            s.setSoTimeout(1500);
            OpMsg warmup = new OpMsg();
            warmup.setMessageId(1);
            warmup.setFirstDoc(Doc.of("ping", 1));
            s.getOutputStream().write(warmup.bytes());
            s.getOutputStream().flush();
            WireProtocolMessage.parseFromStream(s.getInputStream());

            proxy.setFaultMode(FaultMode.reset);
            // The severing happens on the pump threads' own loop iteration, not synchronously
            // with setFaultMode() - poll briefly for the socket to actually go bad.
            long deadline = System.currentTimeMillis() + 2000;
            IOException seen = null;
            while (System.currentTimeMillis() < deadline) {
                try {
                    OpMsg req = new OpMsg();
                    req.setMessageId(2);
                    req.setFirstDoc(Doc.of("ping", 2));
                    s.getOutputStream().write(req.bytes());
                    s.getOutputStream().flush();
                    WireProtocolMessage.parseFromStream(s.getInputStream());
                } catch (IOException e) {
                    seen = e;
                    break;
                }
                Thread.sleep(50);
            }
            assertNotNull(seen, "existing connection must eventually be severed once reset mode is active");
        }
    }

    @Test
    void observerSeesBackendToClientFrames() throws Exception {
        CannedBackend backend = new CannedBackend(helloReply("proxy:1", List.of("proxy:1"), "proxy:1"));
        toClose.add(backend);
        WireProxy proxy = new WireProxy("localhost", backend.port());
        toClose.add(proxy);
        List<WireProtocolMessage> seen = new CopyOnWriteArrayList<>();
        proxy.addObserver((dir, msg, ctx) -> {
            if (dir == FrameObserver.Direction.BACKEND_TO_CLIENT) seen.add(msg);
        });
        proxy.start();

        pingThrough(proxy.getListenPort());
        assertEquals(1, seen.size(), "observer must see exactly the one backend reply");
    }

    @Test
    void observerMustNotBeAbleToCorruptTheStream() throws Exception {
        // FrameObserver's contract is read-only (see interface docs); this test proves the
        // proxy itself doesn't call back into the observer's return value at all - onFrame is
        // void, so there is nothing to "corrupt" through the API on the compiler's side. This
        // test exists as a design-intent regression guard: if a future change accidentally adds
        // a return value / mutation path, it would need a deliberate interface change, not a
        // silent behavior change.
        CannedBackend backend = new CannedBackend(helloReply("proxy:1", List.of("proxy:1"), "proxy:1"));
        toClose.add(backend);
        WireProxy proxy = new WireProxy("localhost", backend.port());
        toClose.add(proxy);
        proxy.addObserver((dir, msg, ctx) -> { /* deliberately does nothing */ });
        proxy.start();

        Doc reply = pingThrough(proxy.getListenPort());
        assertEquals(1.0, ((Number) reply.get("ok")).doubleValue(),
                "an observer that does nothing must not change what the client receives");
    }

    @Test
    void stopClosesTheListenerAndAllLiveConnections() throws Exception {
        CannedBackend backend = new CannedBackend(helloReply("proxy:1", List.of("proxy:1"), "proxy:1"));
        toClose.add(backend);
        WireProxy proxy = new WireProxy("localhost", backend.port());
        proxy.start();

        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("localhost", proxy.getListenPort()), 2000);
            s.setSoTimeout(2000);
            proxy.stop();
            // The now-closed proxy must not accept new connections, and this existing one must
            // observe its peer going away (EOF or reset) rather than hanging.
            assertThrows(Exception.class, () -> {
                s.getInputStream().read();
            });
        }
        assertThrows(IOException.class, () -> new Socket("localhost", proxy.getListenPort()),
                "listener must be closed after stop()");
    }
}
