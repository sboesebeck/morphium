package de.caluga.poppydb.netty;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PoppyDB has no journal. A write concern of j:true was silently accepted and acknowledged,
 * promising durability that does not exist. Like mongod without journaling, the write is
 * executed but the answer must carry a writeConcernError (code 2, BadValue).
 */
public class JournalConcernHonestyTest {

    private InMemoryDriver drv;
    private MongoCommandHandler handler;
    private final String db = "journal_test";
    private final String coll = "docs";

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
    public void journalTrueYieldsWriteConcernError() {
        Map<String, Object> answer = Doc.of("ok", 1.0, "n", 1);
        boolean async = handler.postWrite(null,
                Doc.of("$db", db, "insert", coll, "writeConcern", Doc.of("j", true)),
                "insert", answer, 1);

        assertFalse(async, "j:true must not enter the async replication wait");
        @SuppressWarnings("unchecked")
        Map<String, Object> wce = (Map<String, Object>) answer.get("writeConcernError");
        assertNotNull(wce, "j:true must be answered with a writeConcernError - PoppyDB has no journal");
        assertEquals(2, ((Number) wce.get("code")).intValue(), "mongod reports code 2 (BadValue) without journaling");
        assertEquals(1.0, ((Number) answer.get("ok")).doubleValue(),
            "the write itself is executed - only the durability promise fails, like mongod without journaling");
    }

    @Test
    public void journalFalseOrAbsentStaysClean() {
        Map<String, Object> plain = Doc.of("ok", 1.0, "n", 1);
        handler.postWrite(null, Doc.of("$db", db, "insert", coll), "insert", plain, 1);
        assertNull(plain.get("writeConcernError"));

        Map<String, Object> jFalse = Doc.of("ok", 1.0, "n", 1);
        handler.postWrite(null,
                Doc.of("$db", db, "insert", coll, "writeConcern", Doc.of("j", false)),
                "insert", jFalse, 1);
        assertNull(jFalse.get("writeConcernError"), "j:false is satisfiable - memory acknowledgment needs no journal");
    }
}
