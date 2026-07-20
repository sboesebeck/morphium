package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * store() is a replace-by-_id upsert. It used to work only when handed back the very same Map
 * instance that is already in the collection: storeInternal located the previous document via
 * findByFieldValue, which returns COPIES, while CollectionIndexStore removes index entries by
 * IDENTITY. So the copy never matched, the index kept a stale _id entry, and the following
 * onInsert failed with a duplicate-key error — i.e. the completely ordinary
 * "find it, change it, store it back" round-trip was broken, and the index was left holding an
 * entry for a document no longer in the collection.
 */
@Tag("inmemory")
public class InMemStoreReplaceTest {

    private InMemoryDriver drv;
    private final String db = "store_replace_test";
    private final String coll = "docs";

    @BeforeEach
    public void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
    }

    @AfterEach
    public void tearDown() {
        if (drv != null) {
            drv.close();
        }
    }

    private void store(Map<String, Object> doc) throws Exception {
        List<Map<String, Object>> l = new ArrayList<>();
        l.add(doc);
        drv.store(db, coll, l, null);
    }

    private List<Map<String, Object>> all() throws Exception {
        return drv.find(db, coll, Doc.of(), null, null, 0, 0);
    }

    @Test
    public void storeReplacesAnExistingDocumentReadBackViaFind() throws Exception {
        MorphiumId id = new MorphiumId();
        store(Doc.of("_id", id, "v", 1, "keep", "x"));

        // the ordinary round-trip: read, modify the returned (copied) document, store it back
        Map<String, Object> readBack = new java.util.HashMap<>(all().get(0));
        readBack.put("v", 2);
        store(readBack);

        List<Map<String, Object>> after = all();
        assertEquals(1, after.size(), "replace-by-_id must not add a second document");
        assertEquals(2, ((Number) after.get(0).get("v")).intValue(), "the new value must be stored");
        assertEquals(id, after.get(0).get("_id"), "_id is preserved");
    }

    @Test
    public void storeReplaceKeepsTheIdIndexQueryable() throws Exception {
        MorphiumId id = new MorphiumId();
        store(Doc.of("_id", id, "v", 1));

        Map<String, Object> readBack = new java.util.HashMap<>(all().get(0));
        readBack.put("v", 2);
        store(readBack);

        // A stale index entry pointing at the removed document would make this return the old
        // document, two documents, or nothing at all.
        List<Map<String, Object>> byId = drv.find(db, coll, Doc.of("_id", id), null, null, 0, 0);
        assertEquals(1, byId.size(), "the _id index must resolve to exactly one live document");
        assertEquals(2, ((Number) byId.get(0).get("v")).intValue());
    }

    @Test
    public void repeatedStoresOfTheSameIdKeepASingleDocument() throws Exception {
        MorphiumId id = new MorphiumId();

        for (int i = 1; i <= 5; i++) {
            Map<String, Object> doc = new java.util.HashMap<>();
            doc.put("_id", id);
            doc.put("v", i);
            store(doc);
        }

        List<Map<String, Object>> after = all();
        assertEquals(1, after.size(), "five stores of one _id must leave exactly one document");
        assertEquals(5, ((Number) after.get(0).get("v")).intValue(), "the last write wins");
    }
}
