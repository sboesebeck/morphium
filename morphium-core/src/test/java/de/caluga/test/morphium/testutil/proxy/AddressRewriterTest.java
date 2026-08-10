package de.caluga.test.morphium.testutil.proxy;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

public class AddressRewriterTest {

    private Map<String, String> map() {
        return Map.of(
                "backend1:27017", "localhost:19001",
                "backend2:27017", "localhost:19002",
                "backend3:27017", "localhost:19003");
    }

    @Test
    void rewritesMeHostsAndPrimaryOnAHelloShapedReply() {
        AddressRewriter rw = new AddressRewriter(map());
        OpMsg hello = new OpMsg();
        hello.setFirstDoc(Doc.of(
                "ok", 1.0, "isWritablePrimary", true, "setName", "rsTest",
                "me", "backend1:27017",
                "hosts", List.of("backend1:27017", "backend2:27017", "backend3:27017"),
                "primary", "backend1:27017"));

        WireProtocolMessage result = rw.rewrite(hello);

        Map<String, Object> doc = ((OpMsg) result).getFirstDoc();
        assertEquals("localhost:19001", doc.get("me"));
        assertEquals("localhost:19001", doc.get("primary"));
        @SuppressWarnings("unchecked")
        List<String> hosts = (List<String>) doc.get("hosts");
        assertEquals(List.of("localhost:19001", "localhost:19002", "localhost:19003"), hosts);
    }

    @Test
    void leavesNonTopologyRepliesUntouched() {
        AddressRewriter rw = new AddressRewriter(map());
        OpMsg ping = new OpMsg();
        ping.setFirstDoc(Doc.of("ok", 1.0, "n", 3)); // no setName/hosts - not a hello reply
        WireProtocolMessage result = rw.rewrite(ping);
        assertSame(ping, result, "a structurally-non-hello reply must pass through identically");
    }

    @Test
    void leavesReplSetGetStatusRepliesUntouched() {
        // Deliberately not rewritten - replSetGetStatus only ever travels the control channel,
        // directly to the backend, never through a proxy (see design spec's address-rewrite
        // section). This is a documentation-by-test guard against accidentally "fixing" it later
        // without a real consumer driving that decision.
        AddressRewriter rw = new AddressRewriter(map());
        OpMsg status = new OpMsg();
        status.setFirstDoc(Doc.of("ok", 1.0, "set", "rsTest",
                "members", List.of(Doc.of("name", "backend1:27017", "stateStr", "PRIMARY"))));
        WireProtocolMessage result = rw.rewrite(status);
        assertSame(status, result);
    }

    @Test
    void unmappedHostPassesThroughUnchanged() {
        // Defensive: an address the map doesn't know about (shouldn't happen given discovery
        // reads the map from the same replSetGetStatus call - see Task 4) is left as-is rather
        // than silently dropped or nulled, so a mapping bug fails loudly downstream (the
        // driver connects to a real backend address) instead of corrupting the document.
        AddressRewriter rw = new AddressRewriter(map());
        OpMsg hello = new OpMsg();
        hello.setFirstDoc(Doc.of("ok", 1.0, "setName", "rsTest",
                "me", "unknownHost:27017", "hosts", List.of("unknownHost:27017")));
        WireProtocolMessage result = rw.rewrite(hello);
        Map<String, Object> doc = ((OpMsg) result).getFirstDoc();
        assertEquals("unknownHost:27017", doc.get("me"));
    }
}
