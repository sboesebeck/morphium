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
    void pollReturnsTrueAssoonAsConditionIsMet() throws Exception {
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
