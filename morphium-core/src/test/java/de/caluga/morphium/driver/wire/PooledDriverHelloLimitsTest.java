package de.caluga.morphium.driver.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Wire limits from the hello handshake: the server advertises maxMessageSizeBytes,
 * maxWriteBatchSize and maxBsonObjectSize precisely so clients bound what they send.
 * SingleMongoConnectDriver adopted them all along - the PooledDriver (the one everything
 * actually uses) silently kept DriverBase's field defaults, which on top of that did not
 * match MongoDB's real limits (16MB message instead of 48MB, a 12*1025*1024 typo for the
 * BSON limit, batch size 1000 instead of 100000).
 */
public class PooledDriverHelloLimitsTest {

    private HelloResult primaryHello() {
        HelloResult h = new HelloResult();
        h.setWritablePrimary(true);
        h.setMe("node1:27017");
        h.setHosts(List.of("node1:27017"));
        return h;
    }

    @Test
    public void adoptsWireLimitsFromHello() {
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed("node1:27017");

        HelloResult h = primaryHello();
        h.setMaxMessageSizeBytes(30 * 1024 * 1024);
        h.setMaxWriteBatchSize(50000);
        h.setMaxBsonObjectSize(8 * 1024 * 1024);

        drv.handleHelloResult(h, "node1:27017");

        assertEquals(30 * 1024 * 1024, drv.getMaxMessageSize(), "maxMessageSizeBytes from hello must be adopted");
        assertEquals(50000, drv.getMaxWriteBatchSize(), "maxWriteBatchSize from hello must be adopted");
        assertEquals(8 * 1024 * 1024, drv.getMaxBsonObjectSize(), "maxBsonObjectSize from hello must be adopted");
    }

    @Test
    public void helloWithoutLimitsKeepsTheDefaults() {
        PooledDriver drv = new PooledDriver();
        drv.setHostSeed("node1:27017");

        HelloResult h = primaryHello();
        h.setMaxMessageSizeBytes(null);
        h.setMaxWriteBatchSize(null);
        h.setMaxBsonObjectSize(null);

        drv.handleHelloResult(h, "node1:27017");

        assertEquals(48 * 1024 * 1024, drv.getMaxMessageSize());
        assertEquals(100000, drv.getMaxWriteBatchSize());
        assertEquals(16 * 1024 * 1024, drv.getMaxBsonObjectSize());
    }

    @Test
    public void defaultsMatchMongoDBsRealLimits() {
        PooledDriver drv = new PooledDriver();

        assertEquals(48 * 1024 * 1024, drv.getMaxMessageSize(), "MongoDB's maxMessageSizeBytes is 48MB");
        assertEquals(100000, drv.getMaxWriteBatchSize(), "MongoDB's maxWriteBatchSize is 100000");
        assertEquals(16 * 1024 * 1024, drv.getMaxBsonObjectSize(), "MongoDB's maxBsonObjectSize is 16MB");
    }
}
