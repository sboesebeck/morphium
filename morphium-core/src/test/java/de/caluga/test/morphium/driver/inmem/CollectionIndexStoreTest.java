package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.CollectionIndexStore;
import de.caluga.morphium.driver.inmem.IndexDefinition;
import de.caluga.morphium.driver.inmem.IndexKey;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CollectionIndexStore} - structures and incremental maintenance
 * (Phase B1, no driver wiring yet).
 */
@Tag("inmemory")
public class CollectionIndexStoreTest {

    private static Map<String, Object> doc(Object id, Object... kv) {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("_id", id);
        for (int i = 0; i < kv.length; i += 2) {
            d.put((String) kv[i], kv[i + 1]);
        }
        return d;
    }

    private static IndexDefinition uniqueIndex(String name, String field) {
        Map<String, Object> indexMap = new LinkedHashMap<>();
        indexMap.put(field, 1);
        indexMap.put("$options", Map.of("name", name, "unique", true));
        return IndexDefinition.fromIndexMap(indexMap);
    }

    private static IndexDefinition sparseUniqueIndex(String name, String field) {
        Map<String, Object> indexMap = new LinkedHashMap<>();
        indexMap.put(field, 1);
        indexMap.put("$options", Map.of("name", name, "unique", true, "sparse", true));
        return IndexDefinition.fromIndexMap(indexMap);
    }

    private static IndexDefinition partialUniqueIndex(String name, String field, Map<String, Object> filter) {
        Map<String, Object> indexMap = new LinkedHashMap<>();
        indexMap.put(field, 1);
        indexMap.put("$options", Map.of("name", name, "unique", true, "partialFilterExpression", filter));
        return IndexDefinition.fromIndexMap(indexMap);
    }

    private static IndexDefinition index(String name, String field, int direction) {
        Map<String, Object> indexMap = new LinkedHashMap<>();
        indexMap.put(field, direction);
        indexMap.put("$options", Map.of("name", name));
        return IndexDefinition.fromIndexMap(indexMap);
    }

    private static IndexDefinition compoundIndex(String name, String f1, int d1, String f2, int d2) {
        Map<String, Object> indexMap = new LinkedHashMap<>();
        indexMap.put(f1, d1);
        indexMap.put(f2, d2);
        indexMap.put("$options", Map.of("name", name));
        return IndexDefinition.fromIndexMap(indexMap);
    }

    // ---------------------------------------------------------------- _id index always present

    @Test
    void idIndexAlwaysPresentAndUnique() {
        CollectionIndexStore store = new CollectionIndexStore();

        List<IndexDefinition> defs = new ArrayList<>(store.definitions());
        assertEquals(1, defs.size());
        assertEquals("_id_", defs.get(0).name());
        assertTrue(defs.get(0).unique());
    }

    @Test
    void removeIndexRefusesToRemoveIdIndex() {
        CollectionIndexStore store = new CollectionIndexStore();
        assertThrows(IllegalArgumentException.class, () -> store.removeIndex("_id_"));
    }

    // ---------------------------------------------------------------- addIndex / equalityLookup

    @Test
    void addIndexBuildsFromExistingDocsAndEqualityLookupHitsAndMisses() {
        CollectionIndexStore store = new CollectionIndexStore();
        Map<String, Object> d1 = doc(1, "u", "a");
        Map<String, Object> d2 = doc(2, "u", "b");

        store.addIndex(index("u_1", "u", 1), List.of(d1, d2));

        List<Map<String, Object>> hit = store.equalityLookup("u_1", IndexKey.of(List.of("a")));
        assertEquals(1, hit.size());
        assertTrue(hit.get(0) == d1, "lookup must return the same live document reference");

        List<Map<String, Object>> miss = store.equalityLookup("u_1", IndexKey.of(List.of("nope")));
        assertTrue(miss.isEmpty());
    }

    // ---------------------------------------------------------------- sparse unique indexes

    @Test
    void sparseUniqueIndexAllowsMultipleDocsWithoutTheField() {
        // mongorestore of a typical schema (unique+sparse email index over docs that mostly
        // lack the field) used to throw E11000 on IndexKey.MISSING here
        CollectionIndexStore store = new CollectionIndexStore();
        Map<String, Object> d1 = doc(1, "name", "a");
        Map<String, Object> d2 = doc(2, "name", "b");

        store.addIndex(sparseUniqueIndex("email_1", "email"), List.of(d1, d2));

        store.onInsert(doc(3, "name", "c"));                       // still no email - allowed
        store.onInsert(doc(4, "email", "x@y.z"));                  // first real value - allowed
        assertThrows(de.caluga.morphium.driver.MorphiumDriverException.class,
                () -> store.onInsert(doc(5, "email", "x@y.z")),    // real duplicate - rejected
                "unique must still be enforced for present values");
    }

    @Test
    void sparseUniqueIndexOnUpdateSkipsMissingKeys() {
        CollectionIndexStore store = new CollectionIndexStore();
        store.addIndex(sparseUniqueIndex("email_1", "email"), List.of());
        Map<String, Object> d1 = doc(1, "email", "a@b.c");
        Map<String, Object> d2 = doc(2, "email", "d@e.f");
        store.onInsert(d1);
        store.onInsert(d2);

        // removing the field from both must not collide on MISSING
        Map<String, Object> before1 = new LinkedHashMap<>(d1);
        d1.remove("email");
        store.onUpdate(before1, d1);
        Map<String, Object> before2 = new LinkedHashMap<>(d2);
        d2.remove("email");
        store.onUpdate(before2, d2);
    }

    @Test
    void nonSparseUniqueIndexStillCollidesOnMissing() {
        // mongod parity: without sparse, absent counts as null and collides
        CollectionIndexStore store = new CollectionIndexStore();
        store.addIndex(uniqueIndex("email_1", "email"), List.of());
        store.onInsert(doc(1, "name", "a"));
        assertThrows(de.caluga.morphium.driver.MorphiumDriverException.class,
                () -> store.onInsert(doc(2, "name", "b")));
    }

    // ------------------------------------------------- partial (partialFilterExpression) indexes

    @Test
    void partialUniqueIndexAllowsDocsOutsideTheFilter() {
        // JEF's tasks collection: {msg_id:1}, unique, partialFilterExpression
        // {msg_id:{$type:"objectId"}}. Documents whose msg_id is absent or not an ObjectId are
        // not part of the index in MongoDB, so they can never collide there.
        CollectionIndexStore store = new CollectionIndexStore();
        Map<String, Object> filter = Map.of("msg_id", Map.of("$type", "objectId"));
        store.addIndex(partialUniqueIndex("uniq_msg_id_partial", "msg_id", filter), List.of());

        store.onInsert(doc(1));                          // no msg_id at all
        store.onInsert(doc(2));                          // second one - still outside the filter
        store.onInsert(doc(3, "msg_id", "x"));           // String, not an ObjectId
        store.onInsert(doc(4, "msg_id", "x"));           // same String - also outside the filter

        MorphiumId shared = new MorphiumId();
        store.onInsert(doc(5, "msg_id", shared));        // first ObjectId - inside the filter
        assertThrows(MorphiumDriverException.class,
                () -> store.onInsert(doc(6, "msg_id", shared)),
                "unique must still be enforced for documents the filter covers");
    }

    @Test
    void partialUniqueIndexIgnoresUncoveredDocsAlreadyInTheBucket() {
        // The filter selects on a DIFFERENT field than the indexed one: an uncovered document
        // sits in the same key bucket, but must not make a covered document collide.
        CollectionIndexStore store = new CollectionIndexStore();
        Map<String, Object> filter = Map.of("active", true);
        store.addIndex(partialUniqueIndex("email_1", "email", filter), List.of());

        store.onInsert(doc(1, "email", "a@b.c", "active", false));  // not indexed by mongod
        store.onInsert(doc(2, "email", "a@b.c", "active", true));   // indexed, but alone there

        assertThrows(MorphiumDriverException.class,
                () -> store.onInsert(doc(3, "email", "a@b.c", "active", true)),
                "two covered documents on the same key must still collide");
    }

    @Test
    void partialUniqueIndexAddIndexSkipsUncoveredExistingDocs() {
        // mongorestore path: the index is built over documents that already exist
        CollectionIndexStore store = new CollectionIndexStore();
        Map<String, Object> filter = Map.of("msg_id", Map.of("$type", "objectId"));

        store.addIndex(partialUniqueIndex("uniq_msg_id_partial", "msg_id", filter),
                List.of(doc(1), doc(2), doc(3, "msg_id", "x"), doc(4, "msg_id", "x")));
    }

    @Test
    void partialUniqueIndexOnUpdateSkipsUncoveredDocs() {
        CollectionIndexStore store = new CollectionIndexStore();
        Map<String, Object> filter = Map.of("msg_id", Map.of("$type", "objectId"));
        store.addIndex(partialUniqueIndex("uniq_msg_id_partial", "msg_id", filter), List.of());

        Map<String, Object> d1 = doc(1, "msg_id", new MorphiumId());
        Map<String, Object> d2 = doc(2, "msg_id", new MorphiumId());
        store.onInsert(d1);
        store.onInsert(d2);

        // both drop out of the filter by losing msg_id - they must not collide on MISSING
        Map<String, Object> before1 = new LinkedHashMap<>(d1);
        d1.remove("msg_id");
        store.onUpdate(before1, d1);
        Map<String, Object> before2 = new LinkedHashMap<>(d2);
        d2.remove("msg_id");
        store.onUpdate(before2, d2);
    }

    @Test
    void addIndexThrowsOnPreexistingDuplicateAndRegistersNothing() {
        CollectionIndexStore store = new CollectionIndexStore();
        Map<String, Object> d1 = doc(1, "u", "dup");
        Map<String, Object> d2 = doc(2, "u", "dup");

        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class,
                () -> store.addIndex(uniqueIndex("u_1", "u"), List.of(d1, d2)));
        assertEquals(11000, ((Number) ex.getMongoCode()).intValue());
        assertTrue(ex.getMessage().contains("u_1"), "message must name the index: " + ex.getMessage());

        // the failed index must not have been registered at all
        List<IndexDefinition> defs = new ArrayList<>(store.definitions());
        assertEquals(1, defs.size());
        assertEquals("_id_", defs.get(0).name());
    }

    // ---------------------------------------------------------------- onInsert

    @Test
    void onInsertUpdatesEveryIndex() {
        CollectionIndexStore store = new CollectionIndexStore();
        store.addIndex(index("u_1", "u", 1), List.of());

        Map<String, Object> d1 = doc(1, "u", "a");
        store.onInsert(d1);

        assertEquals(List.of(d1), store.equalityLookup("_id_", IndexKey.of(List.of(1))));
        assertEquals(List.of(d1), store.equalityLookup("u_1", IndexKey.of(List.of("a"))));
    }

    @Test
    void onInsertUniqueViolationThrowsBeforeMutatingAnyIndex() {
        CollectionIndexStore store = new CollectionIndexStore();
        store.addIndex(uniqueIndex("u_1", "u"), List.of());

        Map<String, Object> existing = doc(1, "u", "dup");
        store.onInsert(existing);

        Map<String, Object> losing = doc(2, "u", "dup");
        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, () -> store.onInsert(losing));
        assertEquals(11000, ((Number) ex.getMongoCode()).intValue());
        assertTrue(ex.getMessage().contains("u_1"));

        // store must be fully consistent: losing doc absent from every index, winning doc intact
        assertEquals(List.of(existing), store.equalityLookup("u_1", IndexKey.of(List.of("dup"))));
        assertTrue(store.equalityLookup("_id_", IndexKey.of(List.of(2))).isEmpty(),
                "losing doc must not have been added to the _id index either");
    }

    // ---------------------------------------------------------------- onRemove

    @Test
    void onRemoveDropsDocFromEveryIndex() {
        CollectionIndexStore store = new CollectionIndexStore();
        store.addIndex(index("u_1", "u", 1), List.of());

        Map<String, Object> d1 = doc(1, "u", "a");
        store.onInsert(d1);
        store.onRemove(d1);

        assertTrue(store.equalityLookup("_id_", IndexKey.of(List.of(1))).isEmpty());
        assertTrue(store.equalityLookup("u_1", IndexKey.of(List.of("a"))).isEmpty());
    }

    // ---------------------------------------------------------------- onUpdate

    @Test
    void onUpdateMovesOnlyEntriesWhoseKeyChanged() {
        CollectionIndexStore store = new CollectionIndexStore();
        store.addIndex(index("a_1", "a", 1), List.of());
        store.addIndex(index("b_1", "b", 1), List.of());

        Map<String, Object> live = doc(1, "a", 1, "b", 10);
        store.onInsert(live);

        Map<String, Object> before = doc(1, "a", 1, "b", 10);
        // simulate the driver's in-place mutation: same object reference, new field value
        live.put("a", 2);

        store.onUpdate(before, live);

        assertTrue(store.equalityLookup("a_1", IndexKey.of(List.of(1))).isEmpty(), "old a-key must be gone");
        assertEquals(List.of(live), store.equalityLookup("a_1", IndexKey.of(List.of(2))), "new a-key must be present");
        assertEquals(List.of(live), store.equalityLookup("b_1", IndexKey.of(List.of(10))),
                "b index untouched since b did not change");
        assertEquals(List.of(live), store.equalityLookup("_id_", IndexKey.of(List.of(1))),
                "_id index untouched since _id did not change");
    }

    @Test
    void onUpdateValidatesNewKeyForUniqueIndexAndThrowsWithoutMutating() {
        CollectionIndexStore store = new CollectionIndexStore();
        store.addIndex(uniqueIndex("u_1", "u"), List.of());

        Map<String, Object> docA = doc(1, "u", "a");
        Map<String, Object> docB = doc(2, "u", "b");
        store.onInsert(docA);
        store.onInsert(docB);

        Map<String, Object> beforeB = doc(2, "u", "b");
        docB.put("u", "a"); // would collide with docA

        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class,
                () -> store.onUpdate(beforeB, docB));
        assertEquals(11000, ((Number) ex.getMongoCode()).intValue());

        // The store's actual guarantee after the throw: buckets are untouched. docB is still
        // registered under its OLD key "b" and NOT under the new key "a" - even though the
        // caller's in-place mutation left docB's field reading "a". Per the onUpdate contract
        // the CALLER must now revert that mutation (or validate before mutating); the store
        // itself cannot heal the doc-vs-bucket mismatch.
        assertEquals(List.of(docA), store.equalityLookup("u_1", IndexKey.of(List.of("a"))),
                "new key must hold only the pre-existing winner, not the losing doc");
        assertEquals(List.of(docB), store.equalityLookup("u_1", IndexKey.of(List.of("b"))),
                "losing doc must still be reachable under its old key - buckets untouched");
    }

    // ---------------------------------------------------------------- rangeScan / orderedScan

    private CollectionIndexStore storeWithNumericIndex(String indexName, int direction, int... values) {
        CollectionIndexStore store = new CollectionIndexStore();
        store.addIndex(index(indexName, "n", direction), List.of());
        int id = 1;
        for (int v : values) {
            store.onInsert(doc(id++, "n", v));
        }
        return store;
    }

    @Test
    void rangeScanRespectsBoundsInclusivityAndDescendingOrder() {
        CollectionIndexStore store = storeWithNumericIndex("n_1", 1, 1, 2, 3, 4, 5);

        Iterator<Map<String, Object>> asc = store.rangeScan("n_1",
                IndexKey.of(List.of(2)), true, IndexKey.of(List.of(4)), false, false);
        List<Object> ascValues = collectField(asc, "n");
        assertEquals(List.of(2, 3), ascValues);

        Iterator<Map<String, Object>> desc = store.rangeScan("n_1",
                IndexKey.of(List.of(2)), true, IndexKey.of(List.of(4)), true, true);
        List<Object> descValues = collectField(desc, "n");
        assertEquals(List.of(4, 3, 2), descValues);
    }

    @Test
    void orderedScanReturnsFullSequenceInBothDirections() {
        CollectionIndexStore store = storeWithNumericIndex("n_1", 1, 3, 1, 2);

        assertEquals(List.of(1, 2, 3), collectField(store.orderedScan("n_1", false), "n"));
        assertEquals(List.of(3, 2, 1), collectField(store.orderedScan("n_1", true), "n"));
    }

    @Test
    void rangeScanOverCompoundIndexWithPrefixBoundsScansOnlyMatchingPrefix() {
        CollectionIndexStore store = new CollectionIndexStore();
        IndexDefinition def = compoundIndex("a_1_b_1", "a", 1, "b", 1);
        store.addIndex(def, List.of());

        store.onInsert(doc(1, "a", 4, "b", 9));
        store.onInsert(doc(2, "a", 5, "b", 3));
        store.onInsert(doc(3, "a", 5, "b", 1));
        store.onInsert(doc(4, "a", 5, "b", 2));
        store.onInsert(doc(5, "a", 6, "b", 0));

        IndexKey from = IndexKey.prefixLow(def, List.of(5));
        IndexKey to = IndexKey.prefixHigh(def, List.of(5));

        Iterator<Map<String, Object>> it = store.rangeScan("a_1_b_1", from, true, to, true, false);
        List<Object> ids = collectField(it, "_id");
        assertEquals(List.of(3, 4, 2), ids, "must scan exactly the a==5 entries, ordered by b");
    }

    @Test
    void rangeScanWithPrefixBoundsCoversDescendingTrailingField() {
        // trailing field sorted descending (b: -1) - exercises the direction flip in
        // IndexKey.prefixLow/prefixHigh's sentinel padding
        CollectionIndexStore store = new CollectionIndexStore();
        IndexDefinition def = compoundIndex("a_1_b_-1", "a", 1, "b", -1);
        store.addIndex(def, List.of());

        store.onInsert(doc(1, "a", 4, "b", 9));
        store.onInsert(doc(2, "a", 5, "b", 1));
        store.onInsert(doc(3, "a", 5, "b", 3));
        store.onInsert(doc(4, "a", 5, "b", 2));
        store.onInsert(doc(5, "a", 6, "b", 0));

        IndexKey from = IndexKey.prefixLow(def, List.of(5));
        IndexKey to = IndexKey.prefixHigh(def, List.of(5));

        Iterator<Map<String, Object>> it = store.rangeScan("a_1_b_-1", from, true, to, true, false);
        List<Object> bValues = collectField(it, "b");
        assertEquals(List.of(3, 2, 1), bValues,
                "must scan exactly the a==5 entries, with b in the index's descending order");
    }

    // ---------------------------------------------------------------- misc

    @Test
    void equalityLookupOnUnknownIndexThrows() {
        CollectionIndexStore store = new CollectionIndexStore();
        assertThrows(IllegalArgumentException.class,
                () -> store.equalityLookup("does_not_exist", IndexKey.of(List.of(1))));
    }

    @Test
    void removeIndexDropsDefinition() {
        CollectionIndexStore store = new CollectionIndexStore();
        store.addIndex(index("u_1", "u", 1), List.of());
        assertEquals(2, store.definitions().size());

        store.removeIndex("u_1");

        assertEquals(1, store.definitions().size());
        assertThrows(IllegalArgumentException.class,
                () -> store.equalityLookup("u_1", IndexKey.of(List.of("a"))));
    }

    private static List<Object> collectField(Iterator<Map<String, Object>> it, String field) {
        List<Object> result = new ArrayList<>();
        while (it.hasNext()) {
            result.add(it.next().get(field));
        }
        return result;
    }
}
