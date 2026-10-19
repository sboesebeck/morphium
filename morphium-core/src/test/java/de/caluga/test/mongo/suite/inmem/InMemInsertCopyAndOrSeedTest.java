package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * MOR-057: the InMemoryDriver keeps its own copy of an inserted document, as a wire driver does by serialising it, and MOR-056: a single-branch {@code $or} seeds the document of
 * an upsert, also inside an {@code $and}. The cases of the contract suite pin the same behaviour against a real MongoDB; these are the edges the table has no column for.
 */
public class InMemInsertCopyAndOrSeedTest extends MorphiumInMemTestBase {
    private static final String COLL = "insert_copy_or_seed";

    private List<Map<String, Object>> all(MongoConnection con) throws Exception {
        FindCommand find = new FindCommand(con).setDb(morphium.getDatabase()).setColl(COLL).setFilter(Map.of());

        try {
            return find.execute();
        } finally {
            find.releaseConnection();
        }
    }

    @Test
    public void aChangeOfTheCallersMapAfterTheInsertDoesNotReachTheCollection() throws Exception {
        MongoConnection con = morphium.getDriver().getPrimaryConnection(null);

        try {
            Map<String, Object> inner = new LinkedHashMap<>(Doc.of("x", 1));
            List<Object> list = new ArrayList<>(List.of(1, inner));
            Map<String, Object> doc = new LinkedHashMap<>(Doc.of("_id", 1, "n", 1, "list", list));
            new InsertMongoCommand(con).setDb(morphium.getDatabase()).setColl(COLL).setDocuments(new ArrayList<>(List.of(doc))).execute();
            doc.put("n", 99);
            inner.put("x", 99);
            list.add(99);
            Map<String, Object> stored = all(con).get(0);
            assertEquals(1, stored.get("n"));
            assertEquals(List.of(1, Map.of("x", 1)), stored.get("list"));
        } finally {
            morphium.getDriver().releaseConnection(con);
        }
    }

    @Test
    public void theInsertDoesNotWriteTheGeneratedIdIntoTheCallersMap() throws Exception {
        MongoConnection con = morphium.getDriver().getPrimaryConnection(null);

        try {
            Map<String, Object> doc = new LinkedHashMap<>(Doc.of("n", 1));
            new InsertMongoCommand(con).setDb(morphium.getDatabase()).setColl(COLL).setDocuments(new ArrayList<>(List.of(doc))).execute();
            assertFalse(doc.containsKey("_id"), "a wire driver does not change the document either");
            assertEquals(1, all(con).size());
            assertEquals(false, all(con).get(0).get("_id") == null);
        } finally {
            morphium.getDriver().releaseConnection(con);
        }
    }

    @Test
    public void theStoredFieldOrderIsKept() throws Exception {
        MongoConnection con = morphium.getDriver().getPrimaryConnection(null);

        try {
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("_id", 1);
            doc.put("z", 1);
            doc.put("a", new LinkedHashMap<>(Doc.of("y", 1, "b", 2)));
            new InsertMongoCommand(con).setDb(morphium.getDatabase()).setColl(COLL).setDocuments(new ArrayList<>(List.of(doc))).execute();
            UpdateMongoCommand up = new UpdateMongoCommand(con).setDb(morphium.getDatabase()).setColl(COLL);
            up.addUpdate(Doc.of("_id", 1), Doc.of("$addToSet", Doc.of("list", Doc.of("y", 1, "b", 2))), null, false, false, null, null, null);
            up.addUpdate(Doc.of("_id", 1), Doc.of("$addToSet", Doc.of("list", Doc.of("b", 2, "y", 1))), null, false, false, null, null, null);
            up.execute();
            List<?> list = (List<?>) all(con).get(0).get("list");
            assertEquals(2, list.size(), "documents with another field order are different values, as on a real MongoDB");
        } finally {
            morphium.getDriver().releaseConnection(con);
        }
    }

    @Test
    public void aSingleBranchOrInsideAnAndSeedsToo() throws Exception {
        MongoConnection con = morphium.getDriver().getPrimaryConnection(null);

        try {
            UpdateMongoCommand up = new UpdateMongoCommand(con).setDb(morphium.getDatabase()).setColl(COLL);
            up.addUpdate(Doc.of("$and", List.of(Doc.of("_id", 1), Doc.of("$or", List.of(Doc.of("k", 5))))), Doc.of("$set", Doc.of("v", 1)), null, true, false, null, null, null);
            up.execute();
            Map<String, Object> stored = all(con).get(0);
            assertEquals(5, stored.get("k"));
            assertEquals(1, stored.get("v"));
        } finally {
            morphium.getDriver().releaseConnection(con);
        }
    }

    @Test
    public void aMultiBranchOrStillSeedsNothing() throws Exception {
        MongoConnection con = morphium.getDriver().getPrimaryConnection(null);

        try {
            UpdateMongoCommand up = new UpdateMongoCommand(con).setDb(morphium.getDatabase()).setColl(COLL);
            up.addUpdate(Doc.of("_id", 2, "$or", List.of(Doc.of("k", 5), Doc.of("k", 6))), Doc.of("$set", Doc.of("v", 1)), null, true, false, null, null, null);
            up.execute();
            assertNull(all(con).get(0).get("k"));
        } finally {
            morphium.getDriver().releaseConnection(con);
        }
    }
}
