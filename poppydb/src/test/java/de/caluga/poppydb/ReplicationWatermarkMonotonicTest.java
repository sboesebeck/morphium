package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

/**
 * The applied-sequence watermark must only ever move forward.
 *
 * <p>Events do not always arrive in sequence order: on a resume, the primary's history replay
 * runs concurrently with live dispatch, so a replayed (older) event can land behind a live
 * (newer) one. The batch apply paths have always advanced {@code lastAppliedSequence} with
 * {@code Math.max}; {@code applyChangeEvent} used a plain {@code set} and therefore moved the
 * watermark BACKWARDS in exactly that situation. The next reconnect then asks the primary to
 * resume from a point this node is already past, and re-applies stale full documents over newer
 * ones - silent divergence from the primary.
 *
 * <p>Pure unit test against the package-private {@code applyEventsInOrder} seam, same approach
 * as {@link ReplicationOrderingTest}.
 */
public class ReplicationWatermarkMonotonicTest {

    /**
     * An update event. Deliberately NOT an insert: inserts are collected into bulk runs whose
     * flush advances the watermark with {@code Math.max}, which masks the plain {@code set}
     * inside applyChangeEvent. Update/replace/delete take the single-event path, where that set
     * is the only - and therefore authoritative - writer of the watermark.
     */
    private Map<String, Object> updateEvent(String db, String coll, Object id, long seq) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("_id", id);
        doc.put("value", "v" + seq);

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("_id", Doc.of("_data", String.format(Locale.ROOT, "%016x", seq)));
        event.put("operationType", "update");
        event.put("ns", Doc.of("db", db, "coll", coll));
        event.put("fullDocument", doc);
        event.put("documentKey", Doc.of("_id", id));
        return event;
    }

    /** An event outside the replicated namespace set - it takes the "skipped" branch, which
     * advances the watermark without applying anything. Update-shaped for the same reason as
     * {@link #updateEvent}: it must reach the single-event path. */
    private Map<String, Object> skippedEvent(long seq) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("_id", Doc.of("_data", String.format(Locale.ROOT, "%016x", seq)));
        event.put("operationType", "update");
        event.put("ns", Doc.of("db", "config", "coll", "transactions"));
        event.put("fullDocument", Doc.of("_id", seq));
        event.put("documentKey", Doc.of("_id", seq));
        return event;
    }

    @Test
    public void anOutOfOrderEventMustNotMoveTheWatermarkBackwards() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        try {
            ReplicationManager rm = new ReplicationManager(drv, "127.0.0.1", 1);
            // applied in sequence order first
            rm.applyEventsInOrder(List.of(updateEvent("wm", "docs", 1, 10)));
            assertEquals(10, rm.getLastAppliedSequence(), "watermark must follow the applied event");

            // a replayed, older event arrives after a newer one (resume racing live dispatch)
            rm.applyEventsInOrder(List.of(updateEvent("wm", "docs", 2, 4)));
            assertEquals(10, rm.getLastAppliedSequence(),
                    "an older event must not drag the watermark back - the node is past that point");

            // ...and the stream continues forward from where it really was
            rm.applyEventsInOrder(List.of(updateEvent("wm", "docs", 3, 11)));
            assertEquals(11, rm.getLastAppliedSequence());
        } finally {
            drv.close();
        }
    }

    @Test
    public void skippedNamespacesMustNotMoveTheWatermarkBackwardsEither() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        try {
            ReplicationManager rm = new ReplicationManager(drv, "127.0.0.1", 1);
            rm.applyEventsInOrder(List.of(updateEvent("wm", "docs", 1, 20)));
            assertEquals(20, rm.getLastAppliedSequence());

            // the skip branch advances the watermark too - it must be just as monotonic
            rm.applyEventsInOrder(List.of(skippedEvent(7)));
            assertEquals(20, rm.getLastAppliedSequence(),
                    "a skipped out-of-order event must not move the watermark back");

            rm.applyEventsInOrder(List.of(skippedEvent(21)));
            assertEquals(21, rm.getLastAppliedSequence(),
                    "a skipped event still advances the watermark when it is ahead");
        } finally {
            drv.close();
        }
    }
}
