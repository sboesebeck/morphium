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

/**
 * A {@link de.caluga.morphium.driver.inmem.CollectionIndexStore} built before a transaction
 * starts is not invalidated by {@code startTransaction()} (unlike a store built DURING one,
 * which {@code commitTransaction()}/{@code abortTransaction()} already invalidate - see
 * {@code InMemTransactionContext#getIndexStoreAccessedCollections}). Such a pre-existing store
 * was built by reading through the live database and therefore holds live document instances,
 * while every write inside the transaction mutates the transaction's cloned snapshot instead.
 * An index-backed read (equality lookup on a secondary index) inside the transaction then keeps
 * returning the pre-transaction live instance - stale relative to a full scan, which does read
 * through the transaction's snapshot - and an update whose candidate came from that stale
 * index-backed lookup mutates a live object the commit never merges back, so the write is lost.
 *
 * <p>Both symptoms are reproduced here. Note which one bites first without the fix: the update
 * itself lands on the live document, because its candidate came from the stale index-backed
 * lookup - so the transaction's own snapshot never sees the change at all, and the full-scan
 * assertion is the one that fails (`expected: <updated> but was: <created>`). The divergence
 * between an index-backed lookup and a full scan is the visible surface of that; the lost write
 * after commit is its consequence.
 */
@Tag("inmemory")
public class InMemTransactionPreExistingIndexStoreStalenessTest {
    private static final String DB = "testdb";
    private static final String COLL = "uniqcoll";

    @Test
    void preTransactionIndexStore_doesNotSeeUpdateAppliedInsideTransaction() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            new CreateIndexesCommand(drv).setDb(DB).setColl(COLL)
                    .addIndex(new IndexDescription().setKey(Doc.of("k", 1)).setUnique(true))
                    .execute();
            drv.insert(DB, COLL, List.of(Doc.of("_id", 1, "k", "key-1", "status", "created")),
                    null, true);

            // Force the persistent index store to be built now, strictly BEFORE the
            // transaction below starts. This equality lookup on the secondary "k" index is
            // exactly the read path CollectionIndexStore.equalityLookup answers.
            assertEquals("created", indexLookup(drv).get("status"));

            drv.startTransaction(false);
            drv.update(DB, COLL, Doc.of("_id", 1), null, Doc.of("$set", Doc.of("status", "updated")),
                    false, false, null, null);

            // Read-side symptom: while the transaction is still open, an index-backed lookup
            // and a full scan disagree about the very same document.
            Map<String, Object> viaIndex = indexLookup(drv);
            Map<String, Object> viaFullScan = fullScan(drv);
            assertEquals("updated", viaFullScan.get("status"),
                    "full scan reads through the transaction's snapshot and must see the update");
            assertEquals("updated", viaIndex.get("status"),
                    "index-backed lookup must agree with the full scan inside the same "
                            + "transaction instead of still returning the pre-transaction live "
                            + "document from a store built before the transaction started");

            drv.commitTransaction();

            // Write-loss symptom: after commit, the update must be visible however it is read.
            assertEquals("updated", fullScan(drv).get("status"));
            assertEquals("updated", indexLookup(drv).get("status"),
                    "the update must survive commit even when read back through the "
                            + "index-backed path");
        } finally {
            drv.shutdown(true);
        }
    }

    /**
     * Two transactions open at the same time on different threads, each with its own cloned
     * snapshot ({@code currentTransaction} is thread-local, so this is supported - see
     * {@code InMemTransactionIsolationTest}). If the shared store cache were keyed by build
     * ORDER rather than by transaction IDENTITY, the transaction that built its store first
     * would accept the second transaction's store simply because it was built later. Its
     * index-backed update would then mutate the OTHER transaction's clone: lost on its own
     * commit, and corrupting the other transaction's snapshot on the way.
     */
    @Test
    void overlappingTransactions_doNotShareEachOthersIndexStore() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            new CreateIndexesCommand(drv).setDb(DB).setColl(COLL)
                    .addIndex(new IndexDescription().setKey(Doc.of("k", 1)).setUnique(true))
                    .execute();
            drv.insert(DB, COLL, List.of(Doc.of("_id", 1, "k", "key-1", "status", "created")),
                    null, true);

            // Transaction A on this thread: opens, then builds its store from its own snapshot
            // via an index-backed read.
            drv.startTransaction(false);
            assertEquals("created", indexLookup(drv).get("status"));

            // Transaction B on another thread: opens LATER and builds a store from ITS snapshot,
            // then STAYS OPEN. B's store therefore sits in the shared cache, built after A's and
            // holding B's clones, at the moment A reaches for it below. B must not commit or
            // abort here: either would invalidate the store (see commitTransaction/
            // abortTransaction) and A would simply rebuild, hiding the very confusion under test.
            java.util.concurrent.CountDownLatch bBuiltItsStore = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.CountDownLatch aIsDone = new java.util.concurrent.CountDownLatch(1);
            Throwable[] failure = new Throwable[1];
            Thread other = new Thread(() -> {
                try {
                    drv.startTransaction(false);
                    indexLookup(drv);
                    drv.update(DB, COLL, Doc.of("_id", 1), null,
                            Doc.of("$set", Doc.of("status", "from-b")), false, false, null, null);
                    bBuiltItsStore.countDown();
                    aIsDone.await();
                    drv.abortTransaction();
                } catch (Throwable t) {
                    failure[0] = t;
                    bBuiltItsStore.countDown();
                }
            });
            other.start();
            bBuiltItsStore.await();
            if (failure[0] != null) {
                throw new AssertionError("transaction B failed", failure[0]);
            }

            // Back in A, while B is still open: an index-backed read must NOT see B's write, and
            // an index-backed update must land in A's OWN snapshot. If A reused B's store, the
            // candidate would be B's clone - A would read "from-b" here and its write would go
            // astray.
            assertEquals("created", indexLookup(drv).get("status"),
                    "transaction A must not see an uncommitted write from a concurrently open "
                            + "transaction through a shared index store");
            drv.update(DB, COLL, Doc.of("_id", 1), null,
                    Doc.of("$set", Doc.of("status", "from-a")), false, false, null, null);
            assertEquals("from-a", indexLookup(drv).get("status"),
                    "transaction A must read back its own write, not another transaction's");
            assertEquals("from-a", fullScan(drv).get("status"));

            drv.commitTransaction();
            aIsDone.countDown();
            other.join();
            if (failure[0] != null) {
                throw new AssertionError("transaction B failed", failure[0]);
            }

            assertEquals("from-a", fullScan(drv).get("status"),
                    "A committed and B aborted, so A's write is the one that must survive");
            assertEquals("from-a", indexLookup(drv).get("status"));
        } finally {
            drv.shutdown(true);
        }
    }

    /**
     * A reader outside any transaction must never see a still-open transaction's uncommitted
     * write, not even when that transaction built the shared index store first and the reader's
     * lookup is index-backed. The store built from the transaction's clones is valid only for
     * that transaction; anyone else has to get a store built from the live database.
     */
    @Test
    void nonTransactionalReader_doesNotSeeAnOpenTransactionsUncommittedWrite() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            new CreateIndexesCommand(drv).setDb(DB).setColl(COLL)
                    .addIndex(new IndexDescription().setKey(Doc.of("k", 1)).setUnique(true))
                    .execute();
            drv.insert(DB, COLL, List.of(Doc.of("_id", 1, "k", "key-1", "status", "created")),
                    null, true);

            // The transaction runs on another thread and stays open, so its store - seeded with
            // its own clones - is the one sitting in the shared cache while we read below.
            java.util.concurrent.CountDownLatch txHasWritten = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.CountDownLatch readerIsDone = new java.util.concurrent.CountDownLatch(1);
            Throwable[] failure = new Throwable[1];
            Thread tx = new Thread(() -> {
                try {
                    drv.startTransaction(false);
                    indexLookup(drv);
                    drv.update(DB, COLL, Doc.of("_id", 1), null,
                            Doc.of("$set", Doc.of("status", "uncommitted")), false, false, null, null);
                    txHasWritten.countDown();
                    readerIsDone.await();
                    drv.abortTransaction();
                } catch (Throwable t) {
                    failure[0] = t;
                    txHasWritten.countDown();
                }
            });
            tx.start();
            txHasWritten.await();
            if (failure[0] != null) {
                throw new AssertionError("the transaction thread failed", failure[0]);
            }

            // This thread has no transaction: both read paths must still show the live document.
            assertEquals("created", indexLookup(drv).get("status"),
                    "an index-backed read outside any transaction must not observe an open "
                            + "transaction's uncommitted write");
            assertEquals("created", fullScan(drv).get("status"));

            readerIsDone.countDown();
            tx.join();
            if (failure[0] != null) {
                throw new AssertionError("the transaction thread failed", failure[0]);
            }

            // The transaction aborted, so the live document is unchanged.
            assertEquals("created", indexLookup(drv).get("status"));
            assertEquals("created", fullScan(drv).get("status"));
        } finally {
            drv.shutdown(true);
        }
    }

    private Map<String, Object> indexLookup(InMemoryDriver drv) throws MorphiumDriverException {
        List<Map<String, Object>> result = drv.find(DB, COLL, Doc.of("k", "key-1"), null, null, 0, 0);
        assertEquals(1, result.size());
        return result.get(0);
    }

    private Map<String, Object> fullScan(InMemoryDriver drv) throws MorphiumDriverException {
        List<Map<String, Object>> result = drv.find(DB, COLL, Doc.of(), null, null, 0, 0);
        assertEquals(1, result.size());
        return result.get(0);
    }
}
