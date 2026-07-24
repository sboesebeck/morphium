package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.bson.BsonEncoder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Byte-aware splitting of write-command payloads (insert documents, update/delete
 * statements): a single OP_MSG must stay under the server's maxMessageSizeBytes, which
 * count-based batching alone cannot guarantee - 1000 x 1MB documents are one ~1GB message
 * that any real MongoDB answers by closing the connection. Official drivers split write
 * batches by bytes AND count; this is Morphium's equivalent, used by
 * {@link WriteMongoCommand#execute()}.
 */
public final class WriteBatchSplitter {

    private WriteBatchSplitter() {
    }

    /**
     * Cuts {@code statements} into chunks whose summed BSON size stays within
     * {@code maxBytes} and whose count stays within {@code maxCount}. Returns null when no
     * split is needed (the fast path - also for single statements, which cannot be split
     * further; a lone statement over the budget is the document size limit's business, not
     * ours). Chunks preserve order and completeness; a statement larger than the budget
     * travels in a chunk of its own.
     */
    public static List<List<Map<String, Object>>> split(List<Map<String, Object>> statements,
            int maxBytes, int maxCount) {
        if (statements == null || statements.size() <= 1) {
            return null;
        }

        if (statements.size() <= maxCount) {
            long total = 0;

            for (Map<String, Object> st : statements) {
                total += BsonEncoder.documentSizeOrZero(st);

                if (total > maxBytes) {
                    break;
                }
            }

            if (total <= maxBytes) {
                return null; // fits in one message
            }
        }

        List<List<Map<String, Object>>> chunks = new ArrayList<>();
        List<Map<String, Object>> current = new ArrayList<>();
        long currentBytes = 0;

        for (Map<String, Object> st : statements) {
            // OrZero: embedded in-memory connections accept values BSON cannot encode
            int size = BsonEncoder.documentSizeOrZero(st);

            if (!current.isEmpty() && (currentBytes + size > maxBytes || current.size() >= maxCount)) {
                chunks.add(current);
                current = new ArrayList<>();
                currentBytes = 0;
            }

            current.add(st);
            currentBytes += size;
        }

        if (!current.isEmpty()) {
            chunks.add(current);
        }

        return chunks;
    }

    /**
     * Folds one chunk's result into the aggregate: counters (n, nModified) are summed,
     * writeErrors and upserted entries get their {@code index} shifted by
     * {@code statementOffset} so they refer to the caller's original statement positions,
     * and a command-level failure (ok != 1.0) is kept - first failure wins, later chunks
     * are not supposed to run after it (ordered semantics are the caller's business).
     */
    @SuppressWarnings("unchecked")
    public static void mergeInto(Map<String, Object> aggregate, Map<String, Object> chunkResult,
            int statementOffset) {
        if (chunkResult == null) {
            return;
        }

        for (Map.Entry<String, Object> e : chunkResult.entrySet()) {
            String key = e.getKey();
            Object value = e.getValue();

            switch (key) {
                case "n":
                case "nModified":
                    int sum = value instanceof Number ? ((Number) value).intValue() : 0;
                    Object prev = aggregate.get(key);

                    if (prev instanceof Number) {
                        sum += ((Number) prev).intValue();
                    }

                    aggregate.put(key, sum);
                    break;

                case "ok":
                    // first failure wins - never overwrite a recorded ok:0 with a later ok:1
                    Object prevOk = aggregate.get("ok");

                    if (!(prevOk instanceof Number) || ((Number) prevOk).doubleValue() == 1.0) {
                        aggregate.put("ok", value);
                    }

                    break;

                case "writeErrors":
                case "upserted":
                    List<Map<String, Object>> shifted = (List<Map<String, Object>>)
                        aggregate.computeIfAbsent(key, k -> new ArrayList<Map<String, Object>>());

                    for (Map<String, Object> entry : (List<Map<String, Object>>) value) {
                        Map<String, Object> copy = new LinkedHashMap<>(entry);

                        if (copy.get("index") instanceof Number idx) {
                            copy.put("index", idx.intValue() + statementOffset);
                        }

                        shifted.add(copy);
                    }

                    break;

                default:
                    // code/codeName/errmsg of a failed chunk, $clusterTime, operationTime, ... -
                    // keep the first occurrence (for failure details) or overwrite with the
                    // latest (for timestamps); "don't overwrite failure details" is what matters
                    if (!aggregate.containsKey(key) || okIsStillGood(aggregate)) {
                        aggregate.put(key, value);
                    }
            }
        }
    }

    private static boolean okIsStillGood(Map<String, Object> aggregate) {
        Object ok = aggregate.get("ok");
        return !(ok instanceof Number) || ((Number) ok).doubleValue() == 1.0;
    }
}
