package de.caluga.poppydb.netty;

import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The hello reply advertises replica-set topology and logical sessions, which makes modern
 * drivers enable retryable writes by default - a capability PoppyDB does not have (no
 * (lsid, txnNumber) dedup, see the retryable-errors spec, issue #293). There is no standard
 * hello field to say "sessions yes, retryable writes no", so PoppyDB publishes an explicit
 * poppyCapabilities document clients and tooling can inspect.
 */
public class HelloCapabilitiesTest {

    private InMemoryDriver drv;
    private MongoCommandHandler handler;

    @BeforeEach
    public void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
        handler = new MongoCommandHandler(drv, null, null, null, new AtomicInteger(1),
                "localhost", 17017, "rs0", List.of("localhost:17017"), true, "localhost:17017",
                0, () -> null);
    }

    @AfterEach
    public void tearDown() {
        if (drv != null) {
            drv.close();
        }
    }

    @Test
    public void helloAnswerKeepsIdentityFlags() {
        Map<String, Object> answer = handler.helloAnswer();
        assertEquals(Boolean.TRUE, answer.get("poppyDB"));
        assertEquals(Boolean.TRUE, answer.get("morphiumServer"));
        assertEquals(Boolean.TRUE, answer.get("inMemoryBackend"));
        assertNotNull(answer.get("logicalSessionTimeoutMinutes"),
            "sessions stay advertised - the partial transaction support needs lsid");
    }

    @Test
    public void helloAnswerCarriesHonestCapabilities() {
        Map<String, Object> answer = handler.helloAnswer();
        @SuppressWarnings("unchecked")
        Map<String, Object> caps = (Map<String, Object>) answer.get("poppyCapabilities");
        assertNotNull(caps, "hello must carry the poppyCapabilities document");
        assertEquals(Boolean.FALSE, caps.get("retryableWrites"),
            "no (lsid, txnNumber) dedup exists - clients should run retryWrites=false");
        assertEquals(Boolean.FALSE, caps.get("journal"), "PoppyDB has no journal");
        assertEquals("snapshot", caps.get("durability"));
        assertEquals("local", caps.get("readConcern"));
        assertEquals("partial", caps.get("transactions"));
        assertEquals("simplified", caps.get("textSearch"));
        assertTrue(caps.get("version") instanceof Number);
    }
}
