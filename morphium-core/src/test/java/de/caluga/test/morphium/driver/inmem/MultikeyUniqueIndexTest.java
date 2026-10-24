package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-098: a unique index on an array field is unique per element, like on mongod: a value that another document holds in its array (or as a scalar) is a
 * duplicate key (11000) for an insert, a store and an update ($push, $addToSet, $each), and nothing changes. The same value twice in ONE document is fine.
 */
@Tag("inmemory")
public class MultikeyUniqueIndexTest {

    private static final String DB = "multikeydb";
    private static final String COLL = "docs";

    private InMemoryDriver drv;

    @BeforeEach
    public void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
        unique(COLL, "tags");
        drv.insert(DB, COLL, List.of(
            Doc.of("_id", "a", "tags", new ArrayList<>(List.of("a"))),
            Doc.of("_id", "b", "tags", new ArrayList<>(List.of("b")))), null, true);
    }

    @AfterEach
    public void tearDown() {
        if (drv != null) {
            drv.close();
        }
    }

    private void unique(String coll, String field) throws Exception {
        new CreateIndexesCommand(drv).setDb(DB).setColl(coll)
            .addIndex(new IndexDescription().setKey(Doc.of(field, 1)).setUnique(true))
            .execute();
    }

    @SuppressWarnings("unchecked")
    private Integer codeOfUpdate(String id, Map<String, Object> update) throws Exception {
        try {
            Map<String, Object> result = drv.update(DB, COLL, Doc.of("_id", id), null, update, false, false, null, null, null);

            if (result != null && result.get("writeErrors") instanceof List<?> errs && !errs.isEmpty()) {
                return ((Number) ((Map<String, Object>) errs.get(0)).get("code")).intValue();
            }
        } catch (MorphiumDriverException e) {
            if (e.getMongoCode() instanceof Number n) {
                return n.intValue();
            }
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private Integer codeOfInsert(Map<String, Object> doc) throws Exception {
        try {
            List<Map<String, Object>> errors = drv.insert(DB, COLL, List.of(doc), null, true);

            if (errors != null && !errors.isEmpty()) {
                return ((Number) errors.get(0).get("code")).intValue();
            }
        } catch (MorphiumDriverException e) {
            if (e.getMongoCode() instanceof Number n) {
                return n.intValue();
            }
        }

        return null;
    }

    private Object tags(String id) throws Exception {
        return drv.find(DB, COLL, Doc.of("_id", id), null, null, 0, 1).get(0).get("tags");
    }

    @Test
    public void pushOfAValueThatAnotherDocumentHoldsIsADuplicateKey() throws Exception {
        assertEquals(11000, codeOfUpdate("a", Doc.of("$push", Doc.of("tags", "b"))));
        assertEquals(List.of("a"), tags("a"));
    }

    @Test
    public void pushEachOfAValueThatAnotherDocumentHoldsIsADuplicateKey() throws Exception {
        assertEquals(11000, codeOfUpdate("a", Doc.of("$push", Doc.of("tags", Doc.of("$each", List.of("x", "b"))))));
        assertEquals(List.of("a"), tags("a"));
    }

    @Test
    public void addToSetOfAValueThatAnotherDocumentHoldsIsADuplicateKey() throws Exception {
        assertEquals(11000, codeOfUpdate("a", Doc.of("$addToSet", Doc.of("tags", "b"))));
        assertEquals(List.of("a"), tags("a"));
    }

    @Test
    public void pushOfAFreeValueWorksAndTheNewElementIsHeld() throws Exception {
        assertEquals(null, codeOfUpdate("a", Doc.of("$push", Doc.of("tags", "c"))));
        assertEquals(List.of("a", "c"), tags("a"));
        assertEquals(11000, codeOfUpdate("b", Doc.of("$push", Doc.of("tags", "c"))), "the element pushed before is held by document a");
    }

    @Test
    public void aRemovedElementIsFree() throws Exception {
        assertEquals(null, codeOfUpdate("a", Doc.of("$pull", Doc.of("tags", "a"))));
        assertEquals(null, codeOfUpdate("b", Doc.of("$push", Doc.of("tags", "a"))));
        assertEquals(List.of("b", "a"), tags("b"));
    }

    @Test
    public void aDeletedDocumentFreesItsElements() throws Exception {
        drv.delete(DB, COLL, Doc.of("_id", "a"), null, false, null, null);
        assertEquals(null, codeOfInsert(Doc.of("_id", "c", "tags", new ArrayList<>(List.of("a", "z")))));
    }

    @Test
    public void insertOfAnArrayWithAHeldElementIsADuplicateKey() throws Exception {
        assertEquals(11000, codeOfInsert(Doc.of("_id", "c", "tags", new ArrayList<>(List.of("z", "a")))));
        assertTrue(drv.find(DB, COLL, Doc.of("_id", "c"), null, null, 0, 1).isEmpty());
    }

    @Test
    public void insertOfAScalarThatAnArrayHoldsIsADuplicateKey() throws Exception {
        assertEquals(11000, codeOfInsert(Doc.of("_id", "c", "tags", "b")));
    }

    @Test
    public void theSameValueTwiceInOneDocumentIsAllowed() throws Exception {
        assertEquals(null, codeOfInsert(Doc.of("_id", "c", "tags", new ArrayList<>(List.of("q", "q")))));
        assertEquals(null, codeOfUpdate("c", Doc.of("$push", Doc.of("tags", "q"))));
    }

    @Test
    public void aBuildOverExistingDocumentsWithAnOverlapIsRejected() {
        // createIndexes only registers the index; the store is built over the documents when the collection is used next (as for scalars), so the build
        // is tested on the store
        List<Map<String, Object>> docs = List.of(
            Doc.of("_id", 1, "tags", new ArrayList<>(List.of("a", "b"))),
            Doc.of("_id", 2, "tags", new ArrayList<>(List.of("b", "c"))));
        de.caluga.morphium.driver.inmem.CollectionIndexStore store = new de.caluga.morphium.driver.inmem.CollectionIndexStore();
        de.caluga.morphium.driver.inmem.IndexDefinition def = de.caluga.morphium.driver.inmem.IndexDefinition.fromIndexMap(
            Doc.of("tags", 1, "$options", Doc.of("name", "tags_1", "unique", true)));

        MorphiumDriverException e = org.junit.jupiter.api.Assertions.assertThrows(MorphiumDriverException.class, () -> store.addIndex(def, docs));
        assertEquals(11000, ((Number) e.getMongoCode()).intValue());
        assertEquals(1, store.definitions().size(), "nothing but the _id index is registered");
    }

    @Test
    public void aRejectedFirstArrayLeavesNoPhantomElementsBehind() throws Exception {
        // an index of scalars only: the first array arrives with an update that collides and is rolled back
        unique("scalars", "tags");
        drv.insert(DB, "scalars", List.of(Doc.of("_id", "a", "tags", "a"), Doc.of("_id", "b", "tags", "b")), null, true);

        try {
            Map<String, Object> result = drv.update(DB, "scalars", Doc.of("_id", "a"), null, Doc.of("$set", Doc.of("tags", new ArrayList<>(List.of("x", "b")))), false, false, null, null, null);
            assertTrue(result != null && result.get("writeErrors") instanceof List<?> errs && !errs.isEmpty(), "the update collides with b");
        } catch (MorphiumDriverException e) {
            assertEquals(11000, ((Number) e.getMongoCode()).intValue());
        }

        assertEquals("a", drv.find(DB, "scalars", Doc.of("_id", "a"), null, null, 0, 1).get(0).get("tags"), "rolled back");
        List<Map<String, Object>> errors = drv.insert(DB, "scalars", List.of(Doc.of("_id", "c", "tags", "x")), null, true);
        assertTrue(errors == null || errors.isEmpty(), "x is held by nobody: " + errors);
        errors = drv.insert(DB, "scalars", List.of(Doc.of("_id", "d", "tags", "a")), null, true);
        assertEquals(11000, ((Number) errors.get(0).get("code")).intValue(), "a is still held by document a");
    }

    @Test
    public void anArrayOfSubDocumentsIsUniquePerElement() throws Exception {
        unique("items", "items.sku");
        drv.insert(DB, "items", List.of(Doc.of("_id", 1, "items", new ArrayList<>(List.of(Doc.of("sku", "x"))))), null, true);

        List<Map<String, Object>> errors = drv.insert(DB, "items", List.of(Doc.of("_id", 4, "items", new ArrayList<>(List.of(Doc.of("sku", "free"))))), null, true);
        assertTrue(errors == null || errors.isEmpty(), "a free sku is no duplicate: " + errors);

        errors = drv.insert(DB, "items", List.of(Doc.of("_id", 2, "items", new ArrayList<>(List.of(Doc.of("sku", "y"), Doc.of("sku", "x"))))), null, true);
        assertEquals(11000, ((Number) errors.get(0).get("code")).intValue());

        drv.update(DB, "items", Doc.of("_id", 1), null, Doc.of("$push", Doc.of("items", Doc.of("sku", "y"))), false, false, null, null, null);

        errors = drv.insert(DB, "items", List.of(Doc.of("_id", 3, "items", new ArrayList<>(List.of(Doc.of("sku", "y"))))), null, true);
        assertEquals(11000, ((Number) errors.get(0).get("code")).intValue(), "the pushed sku is held by document 1");
        assertTrue(drv.find(DB, "items", Doc.of("_id", 3), null, null, 0, 1).isEmpty());
    }
}
