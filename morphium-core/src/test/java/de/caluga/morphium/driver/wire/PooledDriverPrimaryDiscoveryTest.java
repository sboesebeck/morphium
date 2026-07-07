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
}
