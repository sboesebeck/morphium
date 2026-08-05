package de.caluga.morphium.driver.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Primary discovery from a secondary's hello response: when the primary itself is
 * unreachable (e.g. app restart while old primary is down), primaryNode can only be
 * learned from hello.getPrimary() as advertised by a secondary. The advertised name
 * must be matched against the hosts map case-insensitively and with default port
 * (replica set configs often use different casing than the client seed, e.g.
 * SERV-MSG1:27017 vs serv-msg1:27017).
 */
public class PooledDriverPrimaryDiscoveryTest {

    private HelloResult helloFromSecondary(String me, String advertisedPrimary, List<String> hosts) {
        HelloResult h = new HelloResult();
        h.setWritablePrimary(false);
        h.setSecondary(true);
        h.setMe(me);
        h.setPrimary(advertisedPrimary);
        h.setHosts(hosts);
        return h;
    }

    @Test
    public void adoptsAdvertisedPrimaryWithDifferentCasing() {
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed("serv-msg1:27017", "serv-msg2:27017", "serv-msg3:27017");

        // replica set advertises members in UPPERCASE, as seen with Windows-style host names
        HelloResult hello = helloFromSecondary(
                "SERV-MSG2:27017",
                "SERV-MSG1:27017",
                List.of("SERV-MSG1:27017", "SERV-MSG2:27017", "SERV-MSG3:27017"));

        drv.handleHelloResult(hello, "serv-msg2:27017");

        assertEquals("serv-msg1:27017", drv.getPrimaryNode(),
            "advertised primary must be adopted despite casing difference");
    }

    @Test
    public void adoptsAdvertisedPrimaryWithoutPort() {
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed("node1:27017", "node2:27017", "node3:27017");

        HelloResult hello = helloFromSecondary(
                "node2:27017",
                "node1", // no port - must default to 27017
                List.of("node1:27017", "node2:27017", "node3:27017"));

        drv.handleHelloResult(hello, "node2:27017");

        assertEquals("node1:27017", drv.getPrimaryNode());
    }

    @Test
    public void ignoresAdvertisedPrimaryNotPartOfReplicaset() {
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed("node1:27017", "node2:27017", "node3:27017");

        HelloResult hello = helloFromSecondary(
                "node2:27017",
                "unknownhost:27017",
                List.of("node1:27017", "node2:27017", "node3:27017"));

        drv.handleHelloResult(hello, "node2:27017");

        assertEquals(null, drv.getPrimaryNode(), "unknown advertised primary must not be adopted");
    }

    private HelloResult helloAsPrimary(String me, List<String> hosts) {
        HelloResult h = new HelloResult();
        h.setWritablePrimary(true);
        h.setMe(me);
        h.setHosts(hosts);
        return h;
    }

    /**
     * Found via a real run against mongo1/mongo2.fritz.box + an arbiter: a rapid double
     * failover (the ex-primary steps down, node B briefly wins, then a much-higher-priority
     * node C immediately wins a priority takeover from B) left the driver stuck retrying
     * connections to the FIRST ex-primary for 20+ seconds, even though B's own hello reply -
     * the one telling the driver "I'm not primary anymore" - already named the real new
     * primary (C) in its own {@code primary} field. handleHelloResult's "not primary anymore"
     * branch discarded that information and just nulled primaryNode, relying entirely on some
     * future, unrelated hello call happening to arrive from C to recover - which can take
     * arbitrarily long if C's own heartbeat cycle hasn't come around yet.
     */
    @Test
    public void adoptsTheAdvertisedPrimaryImmediatelyWhenTheBelievedPrimaryStepsDown() {
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed("node1:27017", "node2:27017", "node3:27017");

        // node2 is believed primary (e.g. from an earlier hello reply).
        drv.handleHelloResult(helloAsPrimary("node2:27017",
                List.of("node1:27017", "node2:27017", "node3:27017")), "node2:27017");
        assertEquals("node2:27017", drv.getPrimaryNode(), "harness check: node2 must be primary first");

        // node2 itself now reports it's no longer primary, but its own reply already names
        // node3 as the real new primary (a rapid double-failover: node2 briefly won, then a
        // higher-priority node3 immediately took over via priority takeover).
        HelloResult steppedDown = helloFromSecondary("node2:27017", "node3:27017",
                List.of("node1:27017", "node2:27017", "node3:27017"));
        drv.handleHelloResult(steppedDown, "node2:27017");

        assertEquals("node3:27017", drv.getPrimaryNode(),
                "must adopt the newly-advertised primary immediately from the SAME reply that "
                        + "revoked the old one, not null it out and wait for a future lucky hello");
    }
}
