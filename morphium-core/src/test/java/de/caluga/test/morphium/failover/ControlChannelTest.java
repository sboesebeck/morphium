package de.caluga.test.morphium.failover;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

public class ControlChannelTest {

    private ServerSocket listener;
    private ControlChannel channel;

    @AfterEach
    void tearDown() throws Exception {
        if (channel != null) channel.close();
        if (listener != null) listener.close();
    }

    /** Accepts one connection, replies to replSetGetStatus with a two-member status doc, and to
     * anything else with ok:1. No SCRAM handshake - this is the no-auth path only. */
    private int startCannedRsBackend() throws IOException {
        listener = new ServerSocket();
        listener.bind(new InetSocketAddress("localhost", 0));
        Thread t = new Thread(() -> {
            try {
                Socket s = listener.accept();
                while (!s.isClosed()) {
                    WireProtocolMessage req = WireProtocolMessage.parseFromStream(s.getInputStream());
                    if (req == null) return;
                    Map<String, Object> reqDoc = ((OpMsg) req).getFirstDoc();
                    OpMsg resp = new OpMsg();
                    resp.setMessageId(((OpMsg) req).getMessageId() + 1000);
                    resp.setResponseTo(((OpMsg) req).getMessageId());
                    if (reqDoc.containsKey("replSetGetStatus")) {
                        resp.setFirstDoc(Doc.of("ok", 1.0, "members", List.of(
                                Doc.of("name", "localhost:19101", "stateStr", "PRIMARY"),
                                Doc.of("name", "localhost:19102", "stateStr", "SECONDARY"))));
                    } else {
                        resp.setFirstDoc(Doc.of("ok", 1.0));
                    }
                    s.getOutputStream().write(resp.bytes());
                    s.getOutputStream().flush();
                }
            } catch (Exception ignored) {
            }
        }, "canned-rs-backend");
        t.setDaemon(true);
        t.start();
        return listener.getLocalPort();
    }

    /** Replies ok:1 to the first request (the connect() hello handshake), then closes the
     * socket cleanly - without replying - to the next one. Models a real mongod closing the
     * connection instead of answering (e.g. some replSetStepDown paths) via a clean FIN at a
     * message boundary: {@code WireProtocolMessage#parseFromStream} returns null there rather
     * than throwing, so {@code SingleMongoConnection#sendAndWaitForReply} returns null too. */
    private int startCannedBackendThatClosesAfterHello() throws IOException {
        listener = new ServerSocket();
        listener.bind(new InetSocketAddress("localhost", 0));
        Thread t = new Thread(() -> {
            try {
                Socket s = listener.accept();
                WireProtocolMessage hello = WireProtocolMessage.parseFromStream(s.getInputStream());
                if (hello == null) return;
                OpMsg resp = new OpMsg();
                resp.setMessageId(((OpMsg) hello).getMessageId() + 1000);
                resp.setResponseTo(((OpMsg) hello).getMessageId());
                resp.setFirstDoc(Doc.of("ok", 1.0));
                s.getOutputStream().write(resp.bytes());
                s.getOutputStream().flush();
                WireProtocolMessage.parseFromStream(s.getInputStream());
                s.close();
            } catch (Exception ignored) {
            }
        }, "canned-closing-backend");
        t.setDaemon(true);
        t.start();
        return listener.getLocalPort();
    }

    /** Replies to every request (including the connect() hello handshake) with
     * {@code ok: 0, errmsg, code} - models a command mongod refuses outright, e.g.
     * {@code replSetStepDown} when no secondary is caught up. */
    private int startCannedBackendThatRefusesEveryCommand() throws IOException {
        listener = new ServerSocket();
        listener.bind(new InetSocketAddress("localhost", 0));
        Thread t = new Thread(() -> {
            try {
                Socket s = listener.accept();
                while (!s.isClosed()) {
                    WireProtocolMessage req = WireProtocolMessage.parseFromStream(s.getInputStream());
                    if (req == null) return;
                    OpMsg resp = new OpMsg();
                    resp.setMessageId(((OpMsg) req).getMessageId() + 1000);
                    resp.setResponseTo(((OpMsg) req).getMessageId());
                    resp.setFirstDoc(Doc.of("ok", 0.0, "errmsg", "No electable secondaries caught up", "code", 262));
                    s.getOutputStream().write(resp.bytes());
                    s.getOutputStream().flush();
                }
            } catch (Exception ignored) {
            }
        }, "canned-refusing-backend");
        t.setDaemon(true);
        t.start();
        return listener.getLocalPort();
    }

    @Test
    void commandThrowsWhenServerRepliesOkZero() throws Exception {
        int port = startCannedBackendThatRefusesEveryCommand();
        channel = new ControlChannel("localhost", port, null, null, null);
        de.caluga.morphium.driver.MorphiumDriverException ex = assertThrows(
                de.caluga.morphium.driver.MorphiumDriverException.class,
                () -> channel.command(Doc.of("replSetStepDown", 60, "$db", "admin")));
        assertTrue(ex.getMessage().contains("No electable secondaries caught up"),
                "exception message must include the server's errmsg: " + ex.getMessage());
    }

    @Test
    void commandTolerateCloseDoesNotSwallowAnOkZeroReply() throws Exception {
        int port = startCannedBackendThatRefusesEveryCommand();
        channel = new ControlChannel("localhost", port, null, null, null);
        assertThrows(de.caluga.morphium.driver.MorphiumDriverException.class,
                () -> channel.commandTolerateClose(Doc.of("replSetStepDown", 60, "$db", "admin")),
                "an ok:0 reply is a real reply, not a connection-closed-instead-of-replying case - "
                        + "commandTolerateClose must not swallow it");
    }

    @Test
    void commandRoundTripsWithoutAuth() throws Exception {
        int port = startCannedRsBackend();
        channel = new ControlChannel("localhost", port, null, null, null);
        Map<String, Object> reply = channel.command(Doc.of("ping", 1));
        assertEquals(1.0, ((Number) reply.get("ok")).doubleValue());
    }

    @Test
    void membersParsesTheReplSetGetStatusReply() throws Exception {
        int port = startCannedRsBackend();
        channel = new ControlChannel("localhost", port, null, null, null);
        List<Map<String, Object>> members = channel.members();
        assertEquals(2, members.size());
        assertEquals("localhost:19101", members.get(0).get("name"));
        assertEquals("PRIMARY", members.get(0).get("stateStr"));
    }

    @Test
    void commandTolerateCloseReturnsNullWhenPeerClosesInsteadOfReplying() throws Exception {
        int port = startCannedBackendThatClosesAfterHello();
        channel = new ControlChannel("localhost", port, null, null, null);
        Map<String, Object> reply = channel.commandTolerateClose(Doc.of("replSetStepDown", 60, "$db", "admin"));
        assertNull(reply);
    }

    @Test
    void pollReturnsTrueAsSoonAsConditionIsMet() throws Exception {
        int port = startCannedRsBackend();
        channel = new ControlChannel("localhost", port, null, null, null);
        long start = System.currentTimeMillis();
        boolean[] flips = {false};
        new Thread(() -> {
            try { Thread.sleep(200); } catch (InterruptedException ignored) { }
            flips[0] = true;
        }).start();
        assertTrue(channel.poll(2000, () -> flips[0]));
        assertTrue(System.currentTimeMillis() - start < 2000);
    }

    @Test
    void pollReturnsFalseOnTimeout() throws Exception {
        int port = startCannedRsBackend();
        channel = new ControlChannel("localhost", port, null, null, null);
        assertFalse(channel.poll(300, () -> false));
    }
}
