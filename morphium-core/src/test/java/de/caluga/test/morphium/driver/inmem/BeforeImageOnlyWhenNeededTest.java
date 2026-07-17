package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.MongoConnection;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Phase B2, Task 7: {@code updateInternal} must only take an unconditional, whole-document
 * {@code deepClone(obj)} before-image when that snapshot can actually be observed outside the
 * write lock - a change-stream subscriber exists for the namespace, or a transaction is active on
 * the thread. Every other update takes a cheap partial before-image instead
 * ({@code buildPartialBeforeImage}: an independent copy of only the top-level fields the update
 * operators are about to touch, everything else shared by reference).
 *
 * <p>Three things must hold simultaneously:
 * <ul>
 *   <li>no watchers, no transaction -&gt; zero full clones (the perf win), indexes stay correct;</li>
 *   <li>an active watcher -&gt; change events still carry a correct before AND after image (the
 *       cheap path must never leak into the async notification path);</li>
 *   <li>inside a transaction -&gt; a rejected update (unique violation) still fully reverts the
 *       in-place mutation, including fields the update touched that are NOT part of any index -
 *       this is the central risk the task brief calls out: a before-image that only carries
 *       indexed-field values would be insufficient to revert a mutation that also touched
 *       non-indexed fields.</li>
 * </ul>
 *
 * The last point is proven a second time OUTSIDE a transaction (the actual hot/no-clone path) in
 * {@link #uniqueViolationWithoutWatchersOrTransactionFullyRevertsNonIndexedFieldsToo()} - that is
 * the scenario that would silently desync index and document data if the partial before-image
 * were built incorrectly.
 */
@Tag("inmemory")
public class BeforeImageOnlyWhenNeededTest {
    private final String db = "beforeimagedb";

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
    void tenThousandUpdatesWithoutWatchersOrTransactionTakeZeroFullBeforeImageClones() throws Exception {
        InMemoryDriver drv = freshDriver();
        String coll = "noWatcherNoTx";
        createUniqueIndex(drv, coll, "email");
        int n = 10_000;

        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            docs.add(Doc.of("_id", i, "email", "user" + i + "@x.de", "counter", i));
        }
        drv.insert(db, coll, docs, null, true);
        // Sanity: no watcher registered, no transaction on this thread.
        assertFalse(drv.isTransactionInProgress());

        long clonesBefore = drv.getFullBeforeImageCloneCount();
        long start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            drv.update(db, coll, Doc.of("_id", i), null, Doc.of("$set", Doc.of("counter", i + n)),
                    false, false, null, null);
        }
        long dur = System.currentTimeMillis() - start;

        assertEquals(clonesBefore, drv.getFullBeforeImageCloneCount(),
                "an update with no watchers and no active transaction must never take a full deepClone before-image");

        // Indexes must still be correct: the unique "email" index was never touched by these
        // updates (only "counter" was $set), and the changed "counter" field is a plain secondary
        // lookup outside of any index here, so re-fetch by _id and check the value directly.
        for (int i = 0; i < n; i += 997) { // spot-check across the range, not every single doc
            Map<String, Object> doc = drv.find(db, coll, Doc.of("_id", i), null, null, 0, 1).get(0);
            assertEquals(i + n, ((Number) doc.get("counter")).intValue());
            assertEquals("user" + i + "@x.de", doc.get("email"), "untouched indexed field must be unchanged");
        }

        System.out.println("BeforeImageOnlyWhenNeededTest: " + n + " no-watcher/no-tx updates took " + dur + "ms, "
                + "fullBeforeImageCloneCount delta=" + (drv.getFullBeforeImageCloneCount() - clonesBefore));
    }

    @Test
    void updateWithActiveChangeStreamWatcherStillCarriesCorrectBeforeAndAfter() throws Exception {
        InMemoryDriver drv = freshDriver();
        String coll = "withWatcher";
        drv.insert(db, coll, List.of(Doc.of("_id", 1, "counter", 10, "tag", "pre-image")), null, true);

        MongoConnection watchConnection = drv.getPrimaryConnection(null);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Map<String, Object>> eventRef = new AtomicReference<>();
        DriverTailableIterationCallback callback = new DriverTailableIterationCallback() {
            private volatile boolean running = true;

            @Override
            public void incomingData(Map<String, Object> data, long dur) {
                eventRef.set(data);
                running = false;
                latch.countDown();
            }

            @Override
            public boolean isContinued() {
                return running;
            }
        };

        WatchCommand watch = new WatchCommand(watchConnection)
                .setDb(db)
                .setColl(coll)
                .setFullDocument(WatchCommand.FullDocumentEnum.updateLookup)
                .setFullDocumentBeforeChange(WatchCommand.FullDocumentBeforeChangeEnum.whenAvailable)
                .setBatchSize(1)
                .setMaxTimeMS(5000)
                .setCb(callback);

        Thread watcher = Thread.ofVirtual().start(() -> {
            try {
                watch.watch();
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            } finally {
                watch.releaseConnection();
            }
        });

        // Give the watch a moment to actually register before writing, matching
        // ChangeStreamInMemTest's own pattern for this race.
        Thread.sleep(100);

        long clonesBefore = drv.getFullBeforeImageCloneCount();
        drv.update(db, coll, Doc.of("_id", 1), null,
                Doc.of("$set", Doc.of("counter", 77), "$unset", Doc.of("tag", "")),
                false, false, null, null);

        assertTrue(clonesBefore < drv.getFullBeforeImageCloneCount(),
                "an update while a change-stream subscriber exists for the namespace must take the full before-image");

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("no change stream event for update");
        }
        watcher.join();

        Map<String, Object> event = eventRef.get();
        assertNotNull(event);

        Map<String, Object> updateDescription = (Map<String, Object>) event.get("updateDescription");
        assertNotNull(updateDescription);
        Map<String, Object> updatedFields = (Map<String, Object>) updateDescription.get("updatedFields");
        List<String> removedFields = (List<String>) updateDescription.get("removedFields");
        assertTrue(updatedFields.containsKey("counter"));
        assertEquals(77, ((Number) updatedFields.get("counter")).intValue());
        assertTrue(removedFields.contains("tag"));

        Map<String, Object> beforeDoc = (Map<String, Object>) event.get("fullDocumentBeforeChange");
        assertNotNull(beforeDoc);
        assertEquals(10, ((Number) beforeDoc.get("counter")).intValue());
        assertEquals("pre-image", beforeDoc.get("tag"));

        Map<String, Object> fullDoc = (Map<String, Object>) event.get("fullDocument");
        assertNotNull(fullDoc);
        assertEquals(77, ((Number) fullDoc.get("counter")).intValue());
        assertFalse(fullDoc.containsKey("tag"));
    }

    @Test
    void updateInsideTransactionFullyRevertsOnUniqueViolation() throws Exception {
        InMemoryDriver drv = freshDriver();
        String coll = "txRevert";
        createUniqueIndex(drv, coll, "email");
        drv.insert(db, coll, List.of(
                Doc.of("_id", 1, "email", "a@x.de", "tag", "keep-me"),
                Doc.of("_id", 2, "email", "b@x.de")), null, true);

        drv.startTransaction(false);
        try {
            long clonesBefore = drv.getFullBeforeImageCloneCount();
            boolean threw = false;
            try {
                // Collides with doc 2's email, and also touches a non-indexed field ("tag") and
                // introduces a brand-new field ("added") - the revert must undo all of it, not just
                // the indexed "email" field.
                drv.update(db, coll, Doc.of("_id", 1), null,
                        Doc.of("$set", Doc.of("email", "b@x.de", "added", "should-not-survive"),
                                "$unset", Doc.of("tag", "")),
                        false, false, null, null);
                fail("Expected duplicate key enforcement inside the transaction");
            } catch (MorphiumDriverException ex) {
                threw = true;
                assertEquals(11000, ex.getMongoCode());
            }
            assertTrue(threw);
            assertTrue(clonesBefore < drv.getFullBeforeImageCloneCount(),
                    "an update inside an active transaction must take the full before-image");

            Map<String, Object> doc1 = drv.find(db, coll, Doc.of("_id", 1), null, null, 0, 1).get(0);
            assertEquals("a@x.de", doc1.get("email"), "email must be fully reverted");
            assertEquals("keep-me", doc1.get("tag"), "non-indexed field must be restored by the revert");
            assertFalse(doc1.containsKey("added"), "a field newly added by the rejected update must not survive the revert");

            // The unique index itself must still be intact: doc 1 is still found under its
            // original key, and looking it up by the (rejected) new key finds only doc 2.
            assertEquals(1, drv.find(db, coll, Doc.of("email", "a@x.de"), null, null, 0, 10).size());
            List<Map<String, Object>> byTakenEmail = drv.find(db, coll, Doc.of("email", "b@x.de"), null, null, 0, 10);
            assertEquals(1, byTakenEmail.size());
            assertEquals(2, byTakenEmail.get(0).get("_id"));
        } finally {
            drv.abortTransaction();
        }
    }

    @Test
    void uniqueViolationWithoutWatchersOrTransactionFullyRevertsNonIndexedFieldsToo() throws Exception {
        InMemoryDriver drv = freshDriver();
        String coll = "hotPathRevert";
        createUniqueIndex(drv, coll, "email");
        drv.insert(db, coll, List.of(
                Doc.of("_id", 1, "email", "a@x.de", "tag", "keep-me"),
                Doc.of("_id", 2, "email", "b@x.de")), null, true);
        assertFalse(drv.isTransactionInProgress());

        long clonesBefore = drv.getFullBeforeImageCloneCount();
        boolean threw = false;
        try {
            // Same shape as the transactional test above, but on the plain no-watcher/no-tx hot
            // path this task adds: this is exactly the scenario the task brief flags as the
            // central risk - an indexed-field-only before-image would be insufficient to revert
            // the non-indexed "tag" field or drop the newly-added "added" field.
            drv.update(db, coll, Doc.of("_id", 1), null,
                    Doc.of("$set", Doc.of("email", "b@x.de", "added", "should-not-survive"),
                            "$unset", Doc.of("tag", "")),
                    false, false, null, null);
            fail("Expected duplicate key enforcement");
        } catch (MorphiumDriverException ex) {
            threw = true;
            assertEquals(11000, ex.getMongoCode());
        }
        assertTrue(threw);
        assertEquals(clonesBefore, drv.getFullBeforeImageCloneCount(),
                "the revert-capable partial before-image must not require a full deepClone either");

        Map<String, Object> doc1 = drv.find(db, coll, Doc.of("_id", 1), null, null, 0, 1).get(0);
        assertEquals("a@x.de", doc1.get("email"), "email must be fully reverted");
        assertEquals("keep-me", doc1.get("tag"), "non-indexed field must be restored by the revert");
        assertFalse(doc1.containsKey("added"), "a field newly added by the rejected update must not survive the revert");

        assertEquals(1, drv.find(db, coll, Doc.of("email", "a@x.de"), null, null, 0, 10).size());
        List<Map<String, Object>> byTakenEmail = drv.find(db, coll, Doc.of("email", "b@x.de"), null, null, 0, 10);
        assertEquals(1, byTakenEmail.size());
        assertEquals(2, byTakenEmail.get(0).get("_id"));
    }

    @Test
    void uniqueViolationOnNoClonePathRevertsInPlaceNestedMapAndArrayMutations() throws Exception {
        InMemoryDriver drv = freshDriver();
        String coll = "hotPathNestedRevert";
        createUniqueIndex(drv, coll, "email");
        // Doc 1 carries a NESTED sub-document and an existing ARRAY - the two container shapes that
        // update operators mutate IN PLACE (applySetFields walks into the existing nested Map;
        // $push appends to the existing List). buildPartialBeforeImage must deep-copy the WHOLE
        // top-level subtree these paths start under ("nested", "tags"), not just the leaf, or the
        // revert below would restore a before-image that still shares - and therefore still shows -
        // the in-place mutation. This is the exact desync the "whole top-level subtree" widening
        // exists to prevent; it is proven here directly on the no-watcher/no-tx no-clone path.
        drv.insert(db, coll, List.of(
                Doc.of("_id", 1, "email", "a@x.de",
                        "nested", Doc.of("inner", Doc.of("val", "orig")),
                        "tags", new ArrayList<>(List.of("x", "y"))),
                Doc.of("_id", 2, "email", "b@x.de")), null, true);
        assertFalse(drv.isTransactionInProgress());

        long clonesBefore = drv.getFullBeforeImageCloneCount();
        boolean threw = false;
        try {
            drv.update(db, coll, Doc.of("_id", 1), null,
                    Doc.of("$set", Doc.of("email", "b@x.de", "nested.inner.val", "mutated"),
                            "$push", Doc.of("tags", "z")),
                    false, false, null, null);
            fail("Expected duplicate key enforcement");
        } catch (MorphiumDriverException ex) {
            threw = true;
            assertEquals(11000, ex.getMongoCode());
        }
        assertTrue(threw);
        assertEquals(clonesBefore, drv.getFullBeforeImageCloneCount(),
                "the revert must go through the partial before-image (no-clone) path, not the full-clone path");

        Map<String, Object> doc1 = drv.find(db, coll, Doc.of("_id", 1), null, null, 0, 1).get(0);
        assertEquals("a@x.de", doc1.get("email"), "email must be fully reverted");

        @SuppressWarnings("unchecked")
        Map<String, Object> nested = (Map<String, Object>) doc1.get("nested");
        @SuppressWarnings("unchecked")
        Map<String, Object> inner = (Map<String, Object>) nested.get("inner");
        assertEquals("orig", inner.get("val"),
                "the in-place dotted-path mutation into the nested Map must be fully reverted");

        @SuppressWarnings("unchecked")
        List<Object> tags = (List<Object>) doc1.get("tags");
        assertEquals(List.of("x", "y"), tags,
                "the element pushed into the existing array must NOT survive the revert");

        assertEquals(1, drv.find(db, coll, Doc.of("email", "a@x.de"), null, null, 0, 10).size());
        List<Map<String, Object>> byTakenEmail = drv.find(db, coll, Doc.of("email", "b@x.de"), null, null, 0, 10);
        assertEquals(1, byTakenEmail.size());
        assertEquals(2, byTakenEmail.get(0).get("_id"));
    }
}
