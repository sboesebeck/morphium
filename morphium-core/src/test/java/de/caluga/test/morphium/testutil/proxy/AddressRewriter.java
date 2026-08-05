package de.caluga.test.morphium.testutil.proxy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

/**
 * Rewrites {@code hello}/{@code isMaster} replies (detected structurally by the presence of
 * {@code setName} together with {@code hosts}) so a driver connected through {@link WireProxy}
 * instances only ever learns proxy addresses, never real backend ones. See the design spec's
 * "Address rewrite (backend→client direction only)" - {@code replSetGetStatus} is deliberately
 * NOT rewritten (it never travels through a proxy in this design).
 */
public class AddressRewriter implements ResponseRewriter {
    private final Map<String, String> backendToProxy;

    /** @param backendToProxy exact server-reported "host:port" strings (see the design spec's
     *  "Map-key invariant") mapped to their proxy's "host:port". */
    public AddressRewriter(Map<String, String> backendToProxy) {
        this.backendToProxy = backendToProxy;
    }

    @Override
    public WireProtocolMessage rewrite(WireProtocolMessage reply) {
        if (!(reply instanceof OpMsg msg)) {
            return reply;
        }
        Map<String, Object> doc = msg.getFirstDoc();
        if (doc == null || !doc.containsKey("setName") || !doc.containsKey("hosts")) {
            return reply; // not a hello/isMaster-shaped reply - leave untouched
        }

        if (doc.get("me") instanceof String me) {
            doc.put("me", map(me));
        }
        if (doc.get("primary") instanceof String primary) {
            doc.put("primary", map(primary));
        }
        if (doc.get("hosts") instanceof List<?> hosts) {
            List<String> rewritten = new ArrayList<>(hosts.size());
            for (Object h : hosts) {
                rewritten.add(map(String.valueOf(h)));
            }
            doc.put("hosts", rewritten);
        }
        msg.setFirstDoc(doc);
        return msg;
    }

    private String map(String backendAddress) {
        return backendToProxy.getOrDefault(backendAddress, backendAddress);
    }
}
