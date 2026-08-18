package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The pre-image ({@code fullDocumentBeforeChange}) is a second, complete copy of the changed
 * document inside the same change event. It is only ever delivered to a subscription that asked
 * for it - every other subscription strips it again before the callback (see
 * ChangeStreamSubscription.applyFullDocumentBeforeChange). Buffering it unconditionally therefore
 * doubles the replay-buffer cost of every update for a payload nobody reads, which on an
 * update-heavy, large-document workload collapses the resume window (genios ACC, 2026-08-18:
 * 267MB buffered in 687 events = a 5 second window). See GitHub issue #313.
 */
@Tag("inmemory")
public class ChangeStreamPreImageBufferingTest {

    private static final String DB = "preimage";
    private static final String COLL = "coll";
    private static final int PAYLOAD_CHARS = 200_000;
    /** estimateBsonSize() bills strings at 1.5 bytes/char. */
    private static final long PAYLOAD_ESTIMATE = (long) (PAYLOAD_CHARS * 1.5);

    private static InMemoryDriver freshDriver() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        return drv;
    }

    /** Same big payload, different counter - so the delta itself stays small and only the
     *  full-document copies dominate the event's size. */
    private static Map<String, Object> doc(String payload, int counter) {
        return Doc.of("_id", "m1", "payload", payload, "counter", counter);
    }

    @Test
    public void updateDoesNotBufferPreImageWhenNoSubscriptionWantsIt() throws Exception {
        InMemoryDriver drv = freshDriver();

        try {
            String payload = "x".repeat(PAYLOAD_CHARS);
            drv.store(DB, COLL, List.of(doc(payload, 1)), null);
            long afterInsert = drv.getChangeStreamHistoryBytes();
            // storing the same _id again goes out as an update event
            drv.store(DB, COLL, List.of(doc(payload, 2)), null);
            long updateEventBytes = drv.getChangeStreamHistoryBytes() - afterInsert;
            assertTrue(updateEventBytes < PAYLOAD_ESTIMATE * 1.5,
                       "update event must not buffer a second full copy of the document as pre-image - "
                       + "buffered " + updateEventBytes + " bytes for a ~" + PAYLOAD_ESTIMATE + " byte payload");
        } finally {
            drv.close();
        }
    }

    @Test
    public void preImageIsStillDeliveredWhenSubscriptionRequestsIt() throws Exception {
        InMemoryDriver drv = freshDriver();

        try {
            LinkedBlockingQueue<Map<String, Object>> events = new LinkedBlockingQueue<>();
            WatchCommand wc = new WatchCommand(drv).setDb(DB).setColl(COLL)
            .setFullDocumentBeforeChange(WatchCommand.FullDocumentBeforeChangeEnum.whenAvailable)
            .setCb(new DriverTailableIterationCallback() {
                @Override
                public void incomingData(Map<String, Object> data, long dur) {
                    events.add(data);
                }
                @Override
                public boolean isContinued() {
                    return true;
                }
            });
            drv.runCommand(wc);
            drv.store(DB, COLL, List.of(doc("small", 1)), null);
            drv.store(DB, COLL, List.of(doc("small", 2)), null);
            Map<String, Object> update = null;

            for (int i = 0; i < 4 && update == null; i++) {
                Map<String, Object> evt = events.poll(5, TimeUnit.SECONDS);
                assertNotNull(evt, "expected an update event, got none");

                if ("update".equals(evt.get("operationType"))) {
                    update = evt;
                }
            }

            assertNotNull(update, "no update event delivered");
            @SuppressWarnings("unchecked")
            Map<String, Object> before = (Map<String, Object>) update.get("fullDocumentBeforeChange");
            assertNotNull(before, "a subscription asking for the pre-image must still receive it");
            assertTrue(Integer.valueOf(1).equals(before.get("counter")),
                       "pre-image must carry the pre-update state, was " + before.get("counter"));
        } finally {
            drv.close();
        }
    }
}
