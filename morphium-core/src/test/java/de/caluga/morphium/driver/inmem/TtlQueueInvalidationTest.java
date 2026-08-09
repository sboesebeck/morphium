package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test for #269: the TTL expiry queue must be rebuilt by whichever of the two lazy
 * rebuild paths runs first after an {@code invalidateTtlQueue()}.
 *
 * <p>{@code invalidateTtlQueue()} removes a collection's queue outright, and both
 * {@code sweepTtlQueue()} and {@code ttlEnqueue()} are supposed to rebuild it on miss. Before the
 * fix only the sweep did; {@code ttlEnqueue()} used {@code computeIfAbsent} and so put a fresh,
 * otherwise-EMPTY queue in place holding nothing but the document it was called for. That queue is
 * no longer absent, so the sweep's bootstrap-on-miss never fires again and every document that
 * existed before the invalidation permanently loses its expiry tracking.
 *
 * <p>Concretely for messaging: {@code Msg.deleteAt} carries
 * {@code @Index(options = "expireAfterSeconds:0")}, so this is the exact mechanism Morphium's
 * messaging uses to clean up. A single insert landing in the window between an invalidation and the
 * next sweep tick left every already-stored message un-expirable - an unbounded {@code msg}
 * collection.
 *
 * <p>Lives in the driver's own package to reach the package-private {@code runTtlSweepPass()},
 * which makes the sweep deterministic instead of racing the background scheduler.
 */
@Tag("inmemory")
public class TtlQueueInvalidationTest {
    private final String db = "ttlinvalidationdb";
    private final String coll = "ttlinvalidationcoll";

    /**
     * A driver whose background sweep is effectively disabled: {@code expireCheck} is set before
     * {@code connect()} (the period is fixed when the task is scheduled, so setting it afterwards
     * has no effect), and the one unconditional tick 100ms after scheduling is waited out here. All
     * sweeping in this test is then driven explicitly via {@link InMemoryDriver#runTtlSweepPass()}.
     */
    private InMemoryDriver quiescentDriver() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.setExpireCheck(3_600_000);
        drv.connect();
        Thread.sleep(400);
        return drv;
    }

    @Test
    void enqueueAfterInvalidationMustNotStripOlderDocsOfTheirExpiryTracking() throws Exception {
        InMemoryDriver drv = quiescentDriver();
        drv.createIndex(db, coll, Doc.of("expiresAt", 1), Doc.of("name", "ttl_1", "expireAfterSeconds", 0));

        // Five documents that are ALREADY due. Nothing removes them yet - the background sweep is
        // quiesced and runTtlSweepPass() has not been called.
        long past = System.currentTimeMillis() - 5_000L;
        List<Map<String, Object>> old = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            old.add(Doc.of("counter", i, "expiresAt", new Date(past)));
        }
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(old).execute();
        assertEquals(5, drv.find(db, coll, Doc.of(), null, null, 0, 0).size(),
                "sanity: the five due documents must still be there - no sweep has run yet");

        // Structural change that discards the queue while leaving the collection, its documents and
        // its TTL index registration fully intact: a transaction commit (commitTransaction ->
        // invalidateTtlQueue for every touched collection). The marker document deliberately has no
        // TTL field, so it neither expires nor re-creates the queue on its own way in.
        drv.startTransaction(false);
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("marker", true))).execute();
        drv.commitTransaction();

        // The race window #269 is about: one single insert of a TTL-bearing document before the
        // next sweep tick. Pre-fix this created a fresh queue holding only this document.
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("counter", 99, "expiresAt", new Date(past)))).execute();

        drv.runTtlSweepPass();

        List<Map<String, Object>> remaining = drv.find(db, coll, Doc.of(), null, null, 0, 0);
        assertEquals(1, remaining.size(),
                "every due TTL document must have expired, not just the one inserted after the "
                        + "invalidation - still present: " + remaining);
        assertTrue(Boolean.TRUE.equals(remaining.get(0).get("marker")),
                "only the non-TTL marker document may survive, but found: " + remaining.get(0));
    }

    /**
     * The bootstrap-on-miss added to {@code ttlEnqueue} scans the collection - which, at that
     * point, already contains the very document being enqueued. It must not end up queued twice.
     * A duplicate would not delete anything twice (the sweep re-checks each popped entry against
     * the live document), but it would be popped and re-checked for nothing, so the
     * {@code ttlEntriesChecked} counter is the observable that catches it.
     */
    @Test
    void bootstrapOnEnqueueMustNotDoubleQueueTheTriggeringDocument() throws Exception {
        InMemoryDriver drv = quiescentDriver();
        drv.createIndex(db, coll, Doc.of("expiresAt", 1), Doc.of("name", "ttl_1", "expireAfterSeconds", 0));

        long farFuture = System.currentTimeMillis() + 3_600_000L;
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("counter", 0, "expiresAt", new Date(farFuture)))).execute();

        drv.startTransaction(false);
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("marker", true))).execute();
        drv.commitTransaction();

        // Triggers the bootstrap-on-miss; "past" makes this document (and only this one) due.
        long past = System.currentTimeMillis() - 5_000L;
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("counter", 1, "expiresAt", new Date(past)))).execute();

        long checkedBefore = drv.ttlEntriesChecked;
        drv.runTtlSweepPass();
        long checked = drv.ttlEntriesChecked - checkedBefore;

        assertEquals(1, checked,
                "the due document must be popped and checked exactly once - more means the "
                        + "bootstrap-on-miss queued it a second time on top of its own scan");
        assertEquals(2, drv.find(db, coll, Doc.of(), null, null, 0, 0).size(),
                "only the due document may have been removed");
    }
}
