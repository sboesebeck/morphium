package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * An {@link IndexKey} extracted from a document used to keep the document's own List/Map
 * instance as its value, while equals/hashCode are content-based - so mutating that container
 * in place (which is exactly what {@code $push}/{@code $addToSet} and dotted-path {@code $set}
 * do) changed the hash of a key already filed in the index store's HashMap. The old bucket
 * became unreachable, removal silently no-opped and the deleted document stayed referenced
 * forever: the message-bus heap leak of #303, where every message is inserted with an empty
 * {@code processed_by} list that is then pushed to before the message is deleted.
 */
@Tag("inmemory")
public class IndexKeyMutableValueLeakTest {
    private final String db = "idxkeymutdb";
    private final String coll = "idxkeymutcoll";

    private InMemoryDriver driverWithIndexOn(String field) throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of(field, 1)))
                .execute();
        return drv;
    }

    @Test
    void extractedKeyKeepsItsHashWhenTheSourceArrayIsMutated() {
        Map<String, Object> doc = Doc.of("tags", new ArrayList<>(List.of("a")));
        IndexDefinition def = IndexDefinition.fromIndexMap(Doc.of("tags", 1));

        IndexKey key = IndexKey.extract(doc, def);
        int hashWhenFiled = key.hashCode();

        ((List<Object>) doc.get("tags")).add("b");

        assertEquals(hashWhenFiled, key.hashCode(),
                "an extracted index key must not change its hash when the source document is mutated - "
                        + "it is used as a HashMap key and would become unreachable");
    }

    @Test
    void deletingADocumentWhoseIndexedArrayWasPushedToLeavesNoIndexEntries() throws Exception {
        InMemoryDriver drv = driverWithIndexOn("tags");
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("_id", "m1", "tags", new ArrayList<String>()))).execute();

        // The messaging pattern: mark the document via an array push, then delete it.
        drv.update(db, coll, Doc.of("_id", "m1"), null, Doc.of("$addToSet", Doc.of("tags", "processed")),
                false, false, null, null);
        drv.delete(db, coll, Doc.of("_id", "m1"), null, false, null, null);

        assertEquals(0, drv.getIndexStore(db, coll).totalIndexedEntries(),
                "a deleted document must not stay referenced by any index bucket");
    }

    @Test
    void deletingADocumentWhoseIndexedSubDocumentWasSetLeavesNoIndexEntries() throws Exception {
        InMemoryDriver drv = driverWithIndexOn("sub");
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("_id", "m2", "sub", Doc.of("x", 1)))).execute();

        // Dotted-path $set writes into the existing nested map - same in-place mutation.
        drv.update(db, coll, Doc.of("_id", "m2"), null, Doc.of("$set", Doc.of("sub.x", 2)),
                false, false, null, null);
        drv.delete(db, coll, Doc.of("_id", "m2"), null, false, null, null);

        assertEquals(0, drv.getIndexStore(db, coll).totalIndexedEntries(),
                "a deleted document must not stay referenced by any index bucket");
    }

    @Test
    void deletingADocumentWhoseNestedArrayWasPushedToLeavesNoIndexEntries() throws Exception {
        InMemoryDriver drv = driverWithIndexOn("sub");
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("_id", "m3", "sub", Doc.of("list", new ArrayList<String>())))).execute();

        // The indexed value is the sub-document, but the mutation happens one level below it -
        // only a deep snapshot of the key keeps the bucket reachable.
        drv.update(db, coll, Doc.of("_id", "m3"), null, Doc.of("$push", Doc.of("sub.list", "x")),
                false, false, null, null);
        drv.delete(db, coll, Doc.of("_id", "m3"), null, false, null, null);

        assertEquals(0, drv.getIndexStore(db, coll).totalIndexedEntries(),
                "a deleted document must not stay referenced by any index bucket");
    }

    @Test
    void aDocumentIsStillFoundViaTheIndexAfterItsIndexedArrayWasPushedTo() throws Exception {
        InMemoryDriver drv = driverWithIndexOn("tags");
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("_id", "m4", "tags", new ArrayList<String>()))).execute();

        drv.update(db, coll, Doc.of("_id", "m4"), null, Doc.of("$addToSet", Doc.of("tags", "processed")),
                false, false, null, null);

        // The re-filed key must describe the document's new state, not the state it was
        // inserted with - the add side of onUpdate, which the leak count alone does not cover.
        List<Map<String, Object>> found = drv.find(db, coll, Doc.of("tags", List.of("processed")), null, null, 0, 0);
        assertEquals(1, found.size(), "the document must be findable under its updated array value");
        assertEquals("m4", found.get(0).get("_id"));
    }
}
