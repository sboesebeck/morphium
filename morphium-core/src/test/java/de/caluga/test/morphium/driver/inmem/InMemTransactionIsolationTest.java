package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
