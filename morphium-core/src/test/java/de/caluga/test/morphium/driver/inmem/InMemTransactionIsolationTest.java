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
}
