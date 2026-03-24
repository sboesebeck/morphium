package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.HelloResult;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("core")
public class HelloResultTest {

    @Test
    public void testFromMsgStandardMongoDB() {
        // Standard MongoDB hello response — all types correct
        Doc msg = Doc.of("helloOk", true, "isWritablePrimary", true,
                "maxBsonObjectSize", 16777216, "maxMessageSizeBytes", 48000000)
            .add("localTime", new Date(1711200000000L))
            .add("minWireVersion", 0)
            .add("maxWireVersion", 21)
            .add("readOnly", false)
            .add("ok", 1.0)
            .add("saslSupportedMechs", List.of("SCRAM-SHA-256", "SCRAM-SHA-1"));

        HelloResult result = HelloResult.fromMsg(msg);

        assertNotNull(result);
        assertTrue(result.isOk());
        assertEquals(true, result.getHelloOk());
        assertEquals(true, result.getWritablePrimary());
        assertEquals(new Date(1711200000000L), result.getLocalTime());
        assertEquals(List.of("SCRAM-SHA-256", "SCRAM-SHA-1"), result.getSaslSupportedMechs());
        assertEquals(0, result.getMinWireVersion());
        assertEquals(21, result.getMaxWireVersion());
    }

    @Test
    public void testFromMsgCosmosDBLocalTimeAsLong() {
        // Cosmos DB returns localTime as Long (epoch millis) instead of Date
        long epochMillis = 1711200000000L;
        Doc msg = Doc.of("helloOk", true, "localTime", epochMillis, "ok", 1.0);

        HelloResult result = HelloResult.fromMsg(msg);

        assertNotNull(result);
        assertNotNull(result.getLocalTime(), "localTime should be converted from Long to Date");
        assertEquals(new Date(epochMillis), result.getLocalTime());
    }

    @Test
    public void testFromMsgCosmosDBSaslMechsAsString() {
        // Cosmos DB may return saslSupportedMechs as a single String instead of List
        Doc msg = Doc.of("helloOk", true, "saslSupportedMechs", "SCRAM-SHA-256", "ok", 1.0);

        HelloResult result = HelloResult.fromMsg(msg);

        assertNotNull(result);
        assertNotNull(result.getSaslSupportedMechs(), "saslSupportedMechs should be converted from String to List");
        assertEquals(List.of("SCRAM-SHA-256"), result.getSaslSupportedMechs());
    }

    @Test
    public void testFromMsgCosmosDBBothTypeMismatches() {
        // Cosmos DB response with both type mismatches at once
        long epochMillis = 1711200000000L;
        Doc msg = Doc.of("helloOk", true, "isWritablePrimary", true,
                "localTime", epochMillis, "saslSupportedMechs", "SCRAM-SHA-256")
            .add("maxWireVersion", 13)
            .add("ok", 1.0);

        HelloResult result = HelloResult.fromMsg(msg);

        assertNotNull(result);
        assertEquals(new Date(epochMillis), result.getLocalTime());
        assertEquals(List.of("SCRAM-SHA-256"), result.getSaslSupportedMechs());
        assertEquals(13, result.getMaxWireVersion());
        assertTrue(result.isOk());
    }

    @Test
    public void testFromMsgNoSaslMechanisms() {
        // Cosmos DB may not return saslSupportedMechs at all
        Doc msg = Doc.of("helloOk", true, "isWritablePrimary", true, "ok", 1.0);

        HelloResult result = HelloResult.fromMsg(msg);

        assertNotNull(result);
        assertNull(result.getSaslSupportedMechs(), "Missing field should remain null");
    }

    @Test
    public void testFromMsgNullReturnsNull() {
        assertNull(HelloResult.fromMsg(null));
    }

    @Test
    public void testToMsgRoundTrip() {
        HelloResult original = new HelloResult()
            .setHelloOk(true)
            .setWritablePrimary(true)
            .setMaxBsonObjectSize(16777216)
            .setMaxMessageSizeBytes(48000000)
            .setLocalTime(new Date(1711200000000L))
            .setSaslSupportedMechs(List.of("SCRAM-SHA-256"))
            .setMinWireVersion(0)
            .setMaxWireVersion(21)
            .setOk(1.0);

        Map<String, Object> msg = original.toMsg();
        HelloResult restored = HelloResult.fromMsg(msg);

        assertEquals(original.getHelloOk(), restored.getHelloOk());
        assertEquals(original.getWritablePrimary(), restored.getWritablePrimary());
        assertEquals(original.getMaxBsonObjectSize(), restored.getMaxBsonObjectSize());
        assertEquals(original.getLocalTime(), restored.getLocalTime());
        assertEquals(original.getSaslSupportedMechs(), restored.getSaslSupportedMechs());
        assertEquals(original.getMinWireVersion(), restored.getMinWireVersion());
        assertEquals(original.getMaxWireVersion(), restored.getMaxWireVersion());
        assertTrue(restored.isOk());
    }

    @Test
    public void testFromMsgMorphiumServerBackwardCompat() {
        // Old servers send "morphiumServer" instead of "poppyDB"
        Doc msg = Doc.of("helloOk", true, "morphiumServer", true, "ok", 1.0);

        HelloResult result = HelloResult.fromMsg(msg);

        assertNotNull(result);
        assertEquals(true, result.getPoppyDB());
    }
}
