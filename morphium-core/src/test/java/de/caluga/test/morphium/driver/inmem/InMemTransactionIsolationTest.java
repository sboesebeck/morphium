package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.DropDatabaseMongoCommand;
import de.caluga.morphium.driver.commands.RenameCollectionCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that committing an in-memory transaction only merges back the collections the
 * transaction actually wrote, so concurrent non-transactional writes to untouched collections
 * survive the commit instead of being clobbered by the start-of-transaction snapshot.
 */
@Tag("inmemory")
public class InMemTransactionIsolationTest {

    @Test
    void concurrentWriteToOtherCollectionSurvivesCommit() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            // seed both collections outside any transaction
            drv.store("testdb", "txcoll", List.of(Doc.of("_id", 1, "v", "tx-before")), null);
            drv.store("testdb", "othercoll", List.of(Doc.of("_id", 1, "v", "before")), null);

            drv.startTransaction(false);
            drv.store("testdb", "txcoll", List.of(Doc.of("_id", 2, "v", "tx-write")), null);

            // concurrent non-transactional write from another thread while tx is open
            Thread other = new Thread(() -> {
                try {
                    drv.store("testdb", "othercoll", List.of(Doc.of("_id", 2, "v", "concurrent")), null);
                } catch (MorphiumDriverException e) {
                    throw new RuntimeException(e);
                }
            });
            other.start();
            other.join();

            drv.commitTransaction();

            // tx write is visible
            assertEquals(2, drv.find("testdb", "txcoll", Doc.of(), null, null, 0, 0).size());
            // concurrent write to a collection the tx never touched MUST survive
            assertEquals(2, drv.find("testdb", "othercoll", Doc.of(), null, null, 0, 0).size());
        } finally {
            drv.shutdown(true);
        }
    }

    /**
     * dropDatabase, renameCollection and clear all mutate the driver-level `database` map
     * directly, bypassing the transaction snapshot mechanism entirely. Full transaction
     * awareness for these whole-DB/whole-collection DDL-style ops is out of scope; instead
     * they must be rejected outright whenever a transaction is active on the calling thread -
     * matching real MongoDB, which also forbids these operations inside multi-document
     * transactions. Critically, the rejection must happen BEFORE any mutation (so the data is
     * left completely intact) and must NOT corrupt the transaction context - a subsequent write
     * in the same transaction must still work, and the transaction must still be committable.
     */
    @Test
    void dropDatabaseInsideTransactionThrowsDataIntactAndTransactionStaysUsable() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            drv.store("testdb", "txcoll", List.of(Doc.of("_id", 1, "v", "before")), null);

            drv.startTransaction(false);
            DropDatabaseMongoCommand dropCmd = new DropDatabaseMongoCommand(drv).setDb("testdb");
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, () -> drv.sendCommand(dropCmd));
            assertTrue(ex.getMessage().toLowerCase().contains("dropdatabase"), "message should name the operation: " + ex.getMessage());
            assertTrue(ex.getMessage().toLowerCase().contains("transaction"), "message should mention transaction: " + ex.getMessage());

            // data intact immediately after the throw, still inside the (unaborted) transaction
            assertEquals(1, drv.find("testdb", "txcoll", Doc.of(), null, null, 0, 0).size());

            // transaction context is not corrupted: a subsequent write in the SAME transaction works
            drv.store("testdb", "txcoll", List.of(Doc.of("_id", 2, "v", "tx-write")), null);
            drv.commitTransaction();

            // commit succeeded and both original + post-rejection write are visible
            assertEquals(2, drv.find("testdb", "txcoll", Doc.of(), null, null, 0, 0).size());
        } finally {
            drv.shutdown(true);
        }
    }

    /**
     * Twin entry point for whole-DB drop: {@code drop(String, WriteConcern)} is independently
     * public and directly callable (not only reachable via DropDatabaseMongoCommand's runCommand
     * dispatch) - e.g. CanResumeChangeStreamDropTest calls {@code drv.drop(db, null)} directly,
     * and InMemAggregator casts to InMemoryDriver and calls it too. It must carry the identical
     * guard as the wire-command path above, since both are independently reachable and either one
     * left unguarded would let a caller silently corrupt the transaction snapshot.
     */
    @Test
    void directDropMethodInsideTransactionThrowsDataIntactAndTransactionStaysUsable() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            drv.store("testdb", "txcoll", List.of(Doc.of("_id", 1, "v", "before")), null);

            drv.startTransaction(false);
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, () -> drv.drop("testdb", null));
            assertTrue(ex.getMessage().toLowerCase().contains("dropdatabase"), "message should name the operation: " + ex.getMessage());
            assertTrue(ex.getMessage().toLowerCase().contains("transaction"), "message should mention transaction: " + ex.getMessage());

            // data intact immediately after the throw, still inside the (unaborted) transaction
            assertEquals(1, drv.find("testdb", "txcoll", Doc.of(), null, null, 0, 0).size());

            // transaction context is not corrupted: a subsequent write in the SAME transaction works
            drv.store("testdb", "txcoll", List.of(Doc.of("_id", 2, "v", "tx-write")), null);
            drv.commitTransaction();

            // commit succeeded and both original + post-rejection write are visible
            assertEquals(2, drv.find("testdb", "txcoll", Doc.of(), null, null, 0, 0).size());
        } finally {
            drv.shutdown(true);
        }
    }

    @Test
    void renameCollectionInsideTransactionThrowsDataIntactAndTransactionStaysUsable() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            drv.store("testdb", "txcoll", List.of(Doc.of("_id", 1, "v", "before")), null);

            drv.startTransaction(false);
            RenameCollectionCommand renameCmd = new RenameCollectionCommand(drv).setDb("testdb").setColl("txcoll").setTo("renamedcoll");
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, () -> drv.sendCommand(renameCmd));
            assertTrue(ex.getMessage().toLowerCase().contains("renamecollection"), "message should name the operation: " + ex.getMessage());
            assertTrue(ex.getMessage().toLowerCase().contains("transaction"), "message should mention transaction: " + ex.getMessage());

            // data intact: original collection still exists under its original name, unchanged;
            // target name was never created
            assertEquals(1, drv.find("testdb", "txcoll", Doc.of(), null, null, 0, 0).size());
            assertEquals(0, drv.find("testdb", "renamedcoll", Doc.of(), null, null, 0, 0).size());

            // transaction context is not corrupted: a subsequent write in the SAME transaction works
            drv.store("testdb", "txcoll", List.of(Doc.of("_id", 2, "v", "tx-write")), null);
            drv.commitTransaction();

            assertEquals(2, drv.find("testdb", "txcoll", Doc.of(), null, null, 0, 0).size());
        } finally {
            drv.shutdown(true);
        }
    }

    @Test
    void clearInsideTransactionThrowsDataIntactAndTransactionStaysUsable() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            drv.store("testdb", "txcoll", List.of(Doc.of("_id", 1, "v", "before")), null);

            drv.startTransaction(false);
            // ClearCollectionCommand.execute()/doClear() delegate to a DeleteMongoCommand and never
            // reach InMemoryDriver.runCommand(ClearCollectionCommand) - dispatch to it directly via
            // sendCommand(), exercising the exact structural-clear path being guarded here.
            ClearCollectionCommand clearCmd = new ClearCollectionCommand(drv).setDb("testdb").setColl("txcoll");
            MorphiumDriverException ex = assertThrows(MorphiumDriverException.class, () -> drv.sendCommand(clearCmd));
            assertTrue(ex.getMessage().toLowerCase().contains("clear"), "message should name the operation: " + ex.getMessage());
            assertTrue(ex.getMessage().toLowerCase().contains("transaction"), "message should mention transaction: " + ex.getMessage());

            // data intact
            assertEquals(1, drv.find("testdb", "txcoll", Doc.of(), null, null, 0, 0).size());

            // transaction context is not corrupted: a subsequent write in the SAME transaction works
            drv.store("testdb", "txcoll", List.of(Doc.of("_id", 2, "v", "tx-write")), null);
            drv.commitTransaction();

            assertEquals(2, drv.find("testdb", "txcoll", Doc.of(), null, null, 0, 0).size());
        } finally {
            drv.shutdown(true);
        }
    }

    @Test
    void transactionRemainsAbortableAfterRejectedDropDatabase() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            drv.store("testdb", "txcoll", List.of(Doc.of("_id", 1, "v", "before")), null);

            drv.startTransaction(false);
            drv.store("testdb", "txcoll", List.of(Doc.of("_id", 2, "v", "tx-write")), null);

            DropDatabaseMongoCommand dropCmd = new DropDatabaseMongoCommand(drv).setDb("testdb");
            assertThrows(MorphiumDriverException.class, () -> drv.sendCommand(dropCmd));

            // transaction context is not corrupted by the rejected drop: abort still succeeds
            drv.abortTransaction();

            // aborted, so only the pre-transaction state remains
            assertEquals(1, drv.find("testdb", "txcoll", Doc.of(), null, null, 0, 0).size());
        } finally {
            drv.shutdown(true);
        }
    }

    /**
     * Regression test for the bug fixed alongside {@code abortTransaction}: a persistent
     * {@link de.caluga.morphium.driver.inmem.CollectionIndexStore} lazily built WHILE a
     * transaction is open is built from the transaction's private snapshot - i.e. from
     * structurally-cloned document instances, not the live documents. If that transaction then
     * aborts without invalidating the store, the store keeps registering those orphaned clones
     * under their unique-index key forever (removal only matches by reference identity, so the
     * clone can never be found and evicted by any later {@code onRemove}/{@code clearCollection}
     * against the real live documents). Every subsequent insert of a brand-new, never-before-seen
     * document under that same key is then rejected as a duplicate, even though the live
     * collection is provably empty.
     *
     * <p>This is exactly the failure this test drives directly at the driver level, without
     * needing to touch a real MongoDB or start a real multi-document transaction: create a
     * unique index, insert a document, then force a duplicate-key insert to fail INSIDE a
     * transaction (which lazily builds the persistent index store from the transaction's
     * snapshot for the first time), abort, clear the collection down to zero documents, and
     * finally insert a fresh document under the very same key. Before the fix, the last insert
     * fails with a duplicate-key error against an empty collection; after the fix, it succeeds.
     */
    @Test
    void abortedTransactionDoesNotLeakStaleIndexEntriesIntoLaterInserts() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            drv.createIndex("testdb", "uniqcoll", Doc.of("k", 1), Doc.of("name", "k_1", "unique", true));

            // Insert the first, real document INSIDE a transaction that COMMITS. The commit is
            // essential: commitTransaction() invalidates the persistent index store for every
            // collection the transaction touched (existing, correct behaviour) - so after this,
            // the store for "uniqcoll" no longer exists and the NEXT access must rebuild it.
            drv.startTransaction(false);
            drv.store("testdb", "uniqcoll", List.of(Doc.of("_id", 1, "k", "SB01")), null);
            drv.commitTransaction();

            // Open a SECOND transaction and attempt to insert a duplicate under the same key.
            // Handling the unique-index check forces getIndexStore() to lazily rebuild the
            // (invalidated) persistent store for the first time since the commit above - and it
            // builds that rebuild from getCollection(), which resolves against THIS transaction's
            // private snapshot while it is open (see InMemoryDriver#getDB). The snapshot's copy of
            // the already-committed SB01 document is a structural CLONE
            // ({@link InMemoryDriver#deepCloneDatabase}), not the same object reference stored in
            // the live database. That clone gets registered into the rebuilt store's unique-index
            // bucket for key "SB01".
            drv.startTransaction(false);
            assertThrows(MorphiumDriverException.class,
                () -> drv.store("testdb", "uniqcoll", List.of(Doc.of("_id", 2, "k", "SB01")), null));

            // Abort - the transaction's own writes are discarded, but the persistent index store
            // that was just rebuilt (seeded with the CLONE of the committed SB01) is a single
            // object shared across the live database and every transaction. Before the fix,
            // nothing invalidates it here, so it survives the abort holding a reference to an
            // object that is not the one in the live collection.
            drv.abortTransaction();

            // Clear the collection down to zero documents via delete() with an empty query -
            // this is exactly the codepath Morphium.clearCollection(Class) uses in production
            // (Morphium#clearCollection -> remove(createQueryFor(cls)) ->
            // MorphiumWriterImpl#remove -> DeleteMongoCommand -> InMemoryDriver#delete), NOT the
            // dedicated ClearCollectionCommand (which already correctly invalidates the index
            // store itself and would mask this bug). The real, live SB01 document is deleted
            // here via reference-identity removal from the index store. It matches and is
            // removed correctly, because it was inserted through the FIRST (committed)
            // transaction as itself, never as a clone.
            drv.delete("testdb", "uniqcoll", Doc.of(), null, true, null, null);
            assertEquals(0, drv.find("testdb", "uniqcoll", Doc.of(), null, null, 0, 0).size(),
                "collection must be empty after clear");

            // Before this fix, a lookup ON THE INDEXED FIELD (not just a full-scan query) would
            // return the orphaned clone as a phantom document, since the stale index bucket
            // still "finds" it even though the live collection is empty - arguably the worse
            // symptom, since it surfaces through the exact codepath the index exists to serve.
            assertEquals(0, drv.find("testdb", "uniqcoll", Doc.of("k", "SB01"), null, null, 0, 0).size(),
                "indexed lookup on the unique-index field must not return the orphaned clone "
                + "as a phantom document");

            // A completely fresh insert under the SAME key, against a provably empty collection,
            // must succeed. Before the fix this throws a duplicate-key error against the orphaned
            // clone that was seeded into the store during the second (aborted) transaction's
            // rebuild and never evicted, because reference-identity removal can never match a
            // clone against the real object it was copied from.
            assertDoesNotThrow(() ->
                drv.store("testdb", "uniqcoll", List.of(Doc.of("_id", 3, "k", "SB01")), null),
                "fresh insert under a key that was only ever seen (as a clone) inside an ABORTED "
                + "transaction, against a now-empty collection, must not be rejected as a duplicate");
            assertEquals(1, drv.find("testdb", "uniqcoll", Doc.of(), null, null, 0, 0).size());
        } finally {
            drv.shutdown(true);
        }
    }

    /**
     * Regression test for the gap in the initial version of the {@code abortTransaction} fix
     * above: it only invalidated collections in
     * {@link de.caluga.morphium.driver.inmem.InMemTransactionContext#getTouchedCollections}
     * (collections the transaction WROTE to). A purely READ-ONLY transaction can just as easily
     * cause the persistent {@link de.caluga.morphium.driver.inmem.CollectionIndexStore} to be
     * lazily rebuilt from the transaction's cloned snapshot (any {@code find()} call reaches
     * {@code getIndexStore()} via {@code getDataFromIndex()}, regardless of whether an index plan
     * is ultimately used), without ever calling {@code markCollectionTouched} - so the write-only
     * {@code touchedCollections} set never records it, and the original fix silently skipped
     * invalidating it on abort.
     *
     * <p>This test drives exactly that: commit a document so the store starts fresh-buildable,
     * then open a SECOND transaction that only ever calls {@code find()} (never a write) before
     * aborting for an unrelated reason, then verify a later insert under the same key - against a
     * now-empty collection - is not rejected as a duplicate.
     */
    @Test
    void abortedReadOnlyTransactionDoesNotLeakStaleIndexEntriesEither() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            drv.createIndex("testdb", "uniqcoll", Doc.of("k", 1), Doc.of("name", "k_1", "unique", true));

            drv.startTransaction(false);
            drv.store("testdb", "uniqcoll", List.of(Doc.of("_id", 1, "k", "SB01")), null);
            drv.commitTransaction();

            // Second transaction: READ ONLY. This find() call forces getIndexStore() to lazily
            // rebuild the (invalidated-by-commit) persistent store for the first time since the
            // commit above, from getCollection() resolving against THIS transaction's private
            // snapshot - i.e. from a structurally-cloned copy of the committed SB01 document.
            // markCollectionTouched is never called anywhere on this path.
            drv.startTransaction(false);
            assertEquals(1, drv.find("testdb", "uniqcoll", Doc.of(), null, null, 0, 0).size());

            // Abort for an unrelated reason - no write ever happened in this transaction, so
            // "uniqcoll" is absent from getTouchedCollections(), but its index store was still
            // rebuilt from a clone while this transaction's snapshot was live.
            drv.abortTransaction();

            // Clear via the same production codepath as before, then insert fresh under the
            // same key against a provably empty collection.
            drv.delete("testdb", "uniqcoll", Doc.of(), null, true, null, null);
            assertEquals(0, drv.find("testdb", "uniqcoll", Doc.of(), null, null, 0, 0).size(),
                "collection must be empty after clear");

            // Before this fix, a lookup ON THE INDEXED FIELD (not just a full-scan query) would
            // return the orphaned clone as a phantom document, since the stale index bucket
            // still "finds" it even though the live collection is empty - arguably the worse
            // symptom, since it surfaces through the exact codepath the index exists to serve.
            assertEquals(0, drv.find("testdb", "uniqcoll", Doc.of("k", "SB01"), null, null, 0, 0).size(),
                "indexed lookup on the unique-index field must not return the orphaned clone "
                + "as a phantom document");

            assertDoesNotThrow(() ->
                drv.store("testdb", "uniqcoll", List.of(Doc.of("_id", 3, "k", "SB01")), null),
                "fresh insert under a key that was only ever seen (as a clone, via a read-only "
                + "find()) inside an ABORTED transaction, against a now-empty collection, must "
                + "not be rejected as a duplicate");
            assertEquals(1, drv.find("testdb", "uniqcoll", Doc.of(), null, null, 0, 0).size());
        } finally {
            drv.shutdown(true);
        }
    }
}
