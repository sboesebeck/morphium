package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.bson.BsonEncoder;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.GetMoreMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reply batches in server mode are capped by BYTES, not only by count - mongod limits
 * cursor batches to ~16MB so a reply never exceeds the wire message bound; PoppyDB has to
 * do the same or a find over large documents with a big batchSize builds a reply message
 * that any real driver answers by dropping the connection. The cap follows
 * maxBsonObjectSize (mongod parity at the 16MB default), the remainder stays on the
 * cursor for getMore. Embedded use (serverMode=false) is untouched: replies never become
 * wire messages there, so no byte walk is spent on them.
 */
@Tag("inmemory")
public class ReplyBatchByteCapTest {

    private final String db = "replycap_test";
    private final String coll = "cap_coll";

    private InMemoryDriver drv;

    private InMemoryDriver serverDriver() throws Exception {
        drv = new InMemoryDriver();
        drv.setServerMode(true);
        drv.connect();
        drv.setMaxBsonObjectSize(8 * 1024); // small cap so no test allocates megabytes
        return drv;
    }

    @AfterEach
    public void tearDown() {
        if (drv != null) {
            drv.close();
        }
    }

    private void seed(InMemoryDriver d, int count) throws Exception {
        List<Map<String, Object>> docs = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            docs.add(Doc.of("_id", i, "p", "x".repeat(1024)));
        }

        d.insert(db, coll, docs, null);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> cursorOf(Map<String, Object> reply) {
        assertNotNull(reply, "no reply");
        assertEquals(1.0, reply.get("ok"), "reply not ok: " + reply);
        return (Map<String, Object>) reply.get("cursor");
    }

    private int byteSize(List<Map<String, Object>> docs) {
        return docs.stream().mapToInt(BsonEncoder::documentSize).sum();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void serverModeCapsFindAndGetMoreBatchesByBytes() throws Exception {
        InMemoryDriver d = serverDriver();
        seed(d, 50);

        FindCommand find = new FindCommand(d).setDb(db).setColl(coll)
            .setFilter(Doc.of()).setBatchSize(50);
        Map<String, Object> cursor = cursorOf(d.readSingleAnswer(d.sendCommand(find)));

        List<Map<String, Object>> firstBatch = (List<Map<String, Object>>) cursor.get("firstBatch");
        assertTrue(firstBatch.size() < 50, "firstBatch must be byte-capped, got all " + firstBatch.size());
        assertTrue(byteSize(firstBatch) <= 8 * 1024, "firstBatch over the byte cap: " + byteSize(firstBatch));

        long cursorId = ((Number) cursor.get("id")).longValue();
        assertTrue(cursorId != 0L, "remainder must stay on the cursor");

        int total = firstBatch.size();

        while (cursorId != 0L) {
            GetMoreMongoCommand more = new GetMoreMongoCommand(d).setDb(db).setColl(coll)
                .setCursorId(cursorId).setBatchSize(50);
            Map<String, Object> next = cursorOf(d.readSingleAnswer(d.sendCommand(more)));
            List<Map<String, Object>> batch = (List<Map<String, Object>>) next.get("nextBatch");
            assertTrue(byteSize(batch) <= 8 * 1024, "getMore batch over the byte cap: " + byteSize(batch));
            assertFalse(batch.isEmpty(), "an open cursor must always yield at least one document");
            total += batch.size();
            cursorId = ((Number) next.get("id")).longValue();
        }

        assertEquals(50, total, "every document must arrive across the batches");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void embeddedModeStaysUncapped() throws Exception {
        drv = new InMemoryDriver();
        drv.connect(); // serverMode = false
        drv.setMaxBsonObjectSize(8 * 1024);
        seed(drv, 50);

        FindCommand find = new FindCommand(drv).setDb(db).setColl(coll)
            .setFilter(Doc.of()).setBatchSize(50);
        Map<String, Object> cursor = cursorOf(drv.readSingleAnswer(drv.sendCommand(find)));

        List<Map<String, Object>> firstBatch = (List<Map<String, Object>>) cursor.get("firstBatch");
        assertEquals(50, firstBatch.size(), "embedded replies are plain map handovers - no byte cap");
    }
}
