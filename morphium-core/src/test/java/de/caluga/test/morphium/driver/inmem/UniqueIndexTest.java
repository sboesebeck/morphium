package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Driver-level tests for MongoDB-conform unique enforcement (Phase B1, Task 5): a duplicate key
 * WITHIN a single insert batch (never previously committed) must be reported the same way a
 * duplicate against an already-committed document is - a {@code writeErrors} entry (code
 * {@code 11000}), never a thrown exception that discards the whole batch's results. {@code
 * ordered} batches stop at the first error (earlier docs stay persisted, later docs are not even
 * attempted); {@code ordered:false} batches keep going and only the offending doc is skipped.
 *
 * <p>A full-document replacement update ($-operator-free) that would move a document onto a
 * unique key already held by a different document must still be rejected outright (error, no
 * change) - this branch (B1e) was already wired up in Task 4; the test here pins it.
 */
@Tag("inmemory")
public class UniqueIndexTest {
    private final String db = "uniqidxdb";

    private InMemoryDriver freshDriver() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        return drv;
    }

    private void createUniqueIndex(InMemoryDriver drv, String coll, String field) throws Exception {
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of(field, 1)).setUnique(true))
                .execute();
    }

    @Test
    void orderedBatchWithInternalIdDuplicate_firstPersisted_writeErrorForDup_subsequentNotPersisted() throws Exception {
        InMemoryDriver drv = freshDriver();
        String coll = "orderedIdDup";

        List<Map<String, Object>> docs = List.of(
                Doc.of("_id", 1, "v", "a"),
                Doc.of("_id", 1, "v", "b"), // intra-batch duplicate of the doc above, not pre-existing
                Doc.of("_id", 2, "v", "c"));

        List<Map<String, Object>> writeErrors = drv.insert(db, coll, docs, null, true);

        assertEquals(1, writeErrors.size(), "exactly the duplicate doc must be reported");
        assertEquals(11000, writeErrors.get(0).get("code"));

        List<Map<String, Object>> found1 = drv.find(db, coll, Doc.of("_id", 1), null, null, 0, 10);
        assertEquals(1, found1.size(), "the first doc (id=1) must be persisted");
        assertEquals("a", found1.get(0).get("v"));

        List<Map<String, Object>> found2 = drv.find(db, coll, Doc.of("_id", 2), null, null, 0, 10);
        assertTrue(found2.isEmpty(),
                "ordered: the doc AFTER the failing one must not even be attempted, so it is not persisted");
    }

    @Test
    void unorderedBatchWithInternalIdDuplicate_allButDupPersisted() throws Exception {
        InMemoryDriver drv = freshDriver();
        String coll = "unorderedIdDup";

        List<Map<String, Object>> docs = List.of(
                Doc.of("_id", 1, "v", "a"),
                Doc.of("_id", 1, "v", "b"), // intra-batch duplicate
                Doc.of("_id", 2, "v", "c"));

        List<Map<String, Object>> writeErrors = drv.insert(db, coll, docs, null, false);

        assertEquals(1, writeErrors.size(), "exactly the duplicate doc must be reported");
        assertEquals(11000, writeErrors.get(0).get("code"));

        List<Map<String, Object>> found1 = drv.find(db, coll, Doc.of("_id", 1), null, null, 0, 10);
        assertEquals(1, found1.size(), "the first doc (id=1) must be persisted");
        assertEquals("a", found1.get(0).get("v"));

        List<Map<String, Object>> found2 = drv.find(db, coll, Doc.of("_id", 2), null, null, 0, 10);
        assertEquals(1, found2.size(), "unordered: docs after the failing one must still be attempted/persisted");
    }

    @Test
    void unorderedBatchWithInternalSecondaryUniqueDuplicate_allButDupPersisted() throws Exception {
        InMemoryDriver drv = freshDriver();
        String coll = "unorderedSecondaryDup";
        createUniqueIndex(drv, coll, "email");

        List<Map<String, Object>> docs = List.of(
                Doc.of("_id", 1, "email", "a@x.de"),
                Doc.of("_id", 2, "email", "a@x.de"), // intra-batch duplicate on the unique secondary index
                Doc.of("_id", 3, "email", "b@x.de"));

        List<Map<String, Object>> writeErrors = drv.insert(db, coll, docs, null, false);

        assertEquals(1, writeErrors.size(), "exactly the duplicate doc must be reported");
        assertEquals(11000, writeErrors.get(0).get("code"));

        assertEquals(1, drv.find(db, coll, Doc.of("_id", 1), null, null, 0, 10).size());
        assertTrue(drv.find(db, coll, Doc.of("_id", 2), null, null, 0, 10).isEmpty(),
                "the doc colliding on the unique secondary index must not be persisted");
        assertEquals(1, drv.find(db, coll, Doc.of("_id", 3), null, null, 0, 10).size(),
                "unordered: a later, non-conflicting doc must still be persisted");
    }

    @Test
    void orderedBatchWithInternalSecondaryUniqueDuplicate_stopsAfterFirstError() throws Exception {
        InMemoryDriver drv = freshDriver();
        String coll = "orderedSecondaryDup";
        createUniqueIndex(drv, coll, "email");

        List<Map<String, Object>> docs = List.of(
                Doc.of("_id", 1, "email", "a@x.de"),
                Doc.of("_id", 2, "email", "a@x.de"), // intra-batch duplicate
                Doc.of("_id", 3, "email", "b@x.de"));

        List<Map<String, Object>> writeErrors = drv.insert(db, coll, docs, null, true);

        assertEquals(1, writeErrors.size());
        assertEquals(11000, writeErrors.get(0).get("code"));

        assertEquals(1, drv.find(db, coll, Doc.of("_id", 1), null, null, 0, 10).size());
        assertTrue(drv.find(db, coll, Doc.of("_id", 2), null, null, 0, 10).isEmpty());
        assertTrue(drv.find(db, coll, Doc.of("_id", 3), null, null, 0, 10).isEmpty(),
                "ordered: the doc after the failing one must not even be attempted");
    }

    @Test
    void replacementUpdateViolatingUniqueSecondaryIndex_errorNoChange() throws Exception {
        InMemoryDriver drv = freshDriver();
        String coll = "replaceViolatesUnique";
        createUniqueIndex(drv, coll, "email");

        drv.insert(db, coll, List.of(
                Doc.of("_id", 1, "email", "taken@x.de"),
                Doc.of("_id", 2, "email", "free@x.de")), null, true);

        // Full-document replacement (no $ operators) that would move id=2 onto id=1's email.
        Map<String, Object> replacement = Doc.of("email", "taken@x.de");

        boolean threw = false;
        try {
            drv.update(db, coll, Doc.of("_id", 2), null, replacement, false, false, null, null);
            fail("Expected duplicate key enforcement on a full-document replace");
        } catch (MorphiumDriverException ex) {
            threw = true;
            assertEquals(11000, ex.getMongoCode());
        }
        assertTrue(threw);

        List<Map<String, Object>> found2 = drv.find(db, coll, Doc.of("_id", 2), null, null, 0, 10);
        assertEquals(1, found2.size());
        assertEquals("free@x.de", found2.get(0).get("email"), "the document must be unchanged after the rejected replace");
    }
}
