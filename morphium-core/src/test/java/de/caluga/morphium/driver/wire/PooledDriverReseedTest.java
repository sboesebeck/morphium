package de.caluga.morphium.driver.wire;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #233: after a full replica-set outage every host gets
 * evicted from the PooledDriver's hosts map (onConnectionError after
 * MAX_FAILURES). Since the heartbeat only iterates that map and
 * handleHelloResult() — the only place re-adding hosts — only runs from
 * heartbeat threads, an empty map used to leave the driver dead forever, even
 * after the cluster returned. The heartbeat now re-seeds from the host seed.
 */
@Tag("core")
public class PooledDriverReseedTest {

    @SuppressWarnings("unchecked")
    private Map<String, Host> hostsOf(PooledDriver drv) throws Exception {
        Field f = PooledDriver.class.getDeclaredField("hosts");
        f.setAccessible(true);
        return (Map<String, Host>) f.get(drv);
    }

    @Test
    public void reseedsFromHostSeedWhenAllHostsWereEvicted() throws Exception {
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed("MongoA.example.com", "mongob.example.com:27018");
        assertThat(hostsOf(drv)).isEmpty();

        drv.reseedIfAllHostsEvicted();

        assertThat(hostsOf(drv).keySet())
        .containsExactlyInAnyOrder("mongoa.example.com:27017", "mongob.example.com:27018");
    }

    @Test
    public void reseededHostsStartWithoutFailures() throws Exception {
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed("mongoa.example.com");

        drv.reseedIfAllHostsEvicted();

        Host h = hostsOf(drv).get("mongoa.example.com:27017");
        assertThat(h).isNotNull();
        assertThat(h.getFailures()).isZero();
    }

    @Test
    public void doesNotTouchHostsWhileAnyHostIsStillKnown() throws Exception {
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed("mongoa.example.com", "mongob.example.com");
        Map<String, Host> hosts = hostsOf(drv);
        hosts.put("mongob.example.com:27017", new Host("mongob.example.com", 27017));

        drv.reseedIfAllHostsEvicted();

        assertThat(hosts.keySet()).containsExactly("mongob.example.com:27017");
    }

    @Test
    public void emptyHostSeedStaysEmpty() throws Exception {
        PooledDriver drv = new PooledDriver();

        drv.reseedIfAllHostsEvicted();

        assertThat(hostsOf(drv)).isEmpty();
    }
}
