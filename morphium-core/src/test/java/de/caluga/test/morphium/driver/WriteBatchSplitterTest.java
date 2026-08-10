package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.bson.BsonEncoder;
import de.caluga.morphium.driver.commands.WriteBatchSplitter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Byte-aware splitting of write-command payloads (insert documents / update statements /
 * delete statements): a single OP_MSG must stay under the server's maxMessageSizeBytes,
 * which count-based batching alone cannot guarantee (1000 x 1MB documents = one ~1GB
 * message that any real MongoDB answers by closing the connection). The splitter cuts the
 * statement list into chunks under a byte budget AND the maxWriteBatchSize count limit;
 * the merger recombines the per-chunk results (summing counters, shifting writeError and
 * upserted indices so they refer to the caller's original statement positions).
 */
@Tag("inmemory")
public class WriteBatchSplitterTest {

    private Map<String, Object> docOfSize(int id, int approxBytes) {
        // payload sized so the encoded document lands near approxBytes
        return Doc.of("_id", id, "p", "x".repeat(Math.max(1, approxBytes - 30)));
    }

    @Test
    public void payloadWithinTheLimitsIsNotSplit() {
        List<Map<String, Object>> docs = List.of(docOfSize(1, 100), docOfSize(2, 100));

        assertNull(WriteBatchSplitter.split(docs, 10_000, 1000), "no split needed - null signals the fast path");
    }

    @Test
    public void singleStatementIsNeverSplit() {
        List<Map<String, Object>> docs = List.of(docOfSize(1, 5000));

        assertNull(WriteBatchSplitter.split(docs, 1000, 1000), "a single statement cannot be split further");
    }

    @Test
    public void splitsByByteBudgetKeepingOrderAndCompleteness() {
        List<Map<String, Object>> docs = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            docs.add(docOfSize(i, 400));
        }

        List<List<Map<String, Object>>> chunks = WriteBatchSplitter.split(docs, 1000, 1000);

        assertNotNull(chunks);
        assertTrue(chunks.size() > 1);
        List<Map<String, Object>> flattened = new ArrayList<>();

        for (List<Map<String, Object>> chunk : chunks) {
            assertFalse(chunk.isEmpty());
            int chunkBytes = chunk.stream().mapToInt(BsonEncoder::documentSize).sum();
            assertTrue(chunkBytes <= 1000, "chunk exceeds the byte budget: " + chunkBytes);
            flattened.addAll(chunk);
        }

        assertEquals(docs, flattened, "all statements, in original order");
    }

    @Test
    public void statementLargerThanTheBudgetGetsItsOwnChunk() {
        List<Map<String, Object>> docs = List.of(docOfSize(1, 100), docOfSize(2, 5000), docOfSize(3, 100));

        List<List<Map<String, Object>>> chunks = WriteBatchSplitter.split(docs, 1000, 1000);

        assertNotNull(chunks);
        assertEquals(List.of(docs.get(1)), chunks.get(1), "the oversized statement travels alone");
        assertEquals(3, chunks.stream().mapToInt(List::size).sum());
    }

    @Test
    public void splitsByCountLimitToo() {
        List<Map<String, Object>> docs = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            docs.add(docOfSize(i, 50));
        }

        List<List<Map<String, Object>>> chunks = WriteBatchSplitter.split(docs, 1_000_000, 4);

        assertNotNull(chunks);

        for (List<Map<String, Object>> chunk : chunks) {
            assertTrue(chunk.size() <= 4, "chunk exceeds maxWriteBatchSize: " + chunk.size());
        }

        assertEquals(10, chunks.stream().mapToInt(List::size).sum());
    }

    @Test
    public void mergeSumsCountersAndShiftsErrorIndices() {
        Map<String, Object> aggregate = new java.util.LinkedHashMap<>();

        WriteBatchSplitter.mergeInto(aggregate, Doc.of("ok", 1.0, "n", 3), 0);
        WriteBatchSplitter.mergeInto(aggregate, Doc.of("ok", 1.0, "n", 1,
                "writeErrors", List.of(Doc.of("index", 1, "code", 11000, "errmsg", "dup"))), 3);

        assertEquals(1.0, aggregate.get("ok"));
        assertEquals(4, aggregate.get("n"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> errs = (List<Map<String, Object>>) aggregate.get("writeErrors");
        assertEquals(1, errs.size());
        assertEquals(4, errs.get(0).get("index"), "index must refer to the caller's original statement position");
    }

    @Test
    public void mergeSumsUpdateCountersAndShiftsUpsertedIndices() {
        Map<String, Object> aggregate = new java.util.LinkedHashMap<>();

        WriteBatchSplitter.mergeInto(aggregate, Doc.of("ok", 1.0, "n", 2, "nModified", 2), 0);
        WriteBatchSplitter.mergeInto(aggregate, Doc.of("ok", 1.0, "n", 1, "nModified", 0,
                "upserted", List.of(Doc.of("index", 0, "_id", "xyz"))), 2);

        assertEquals(3, aggregate.get("n"));
        assertEquals(2, aggregate.get("nModified"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> upserted = (List<Map<String, Object>>) aggregate.get("upserted");
        assertEquals(2, upserted.get(0).get("index"));
    }

    @Test
    public void mergeKeepsTheFirstCommandLevelFailure() {
        Map<String, Object> aggregate = new java.util.LinkedHashMap<>();

        WriteBatchSplitter.mergeInto(aggregate, Doc.of("ok", 1.0, "n", 2), 0);
        WriteBatchSplitter.mergeInto(aggregate, Doc.of("ok", 0.0, "code", 146, "errmsg", "boom"), 2);

        assertEquals(0.0, aggregate.get("ok"), "a failed sub-batch must not be masked");
        assertEquals(146, aggregate.get("code"));
        assertEquals("boom", aggregate.get("errmsg"));
        assertEquals(2, aggregate.get("n"), "work done before the failure stays reported");
    }
}
