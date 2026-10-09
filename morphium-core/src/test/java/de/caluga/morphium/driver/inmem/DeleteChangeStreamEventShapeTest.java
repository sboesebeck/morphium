package de.caluga.morphium.driver.inmem;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.commands.DeleteMongoCommand;
import de.caluga.morphium.driver.commands.WatchCommand;

/**
 * A delete change-stream event must carry the deleted document's identity (documentKey) but
 * NOT a fullDocument after-image - measured against mongod: a delete has no after-image, so
 * MongoDB only sends {@code {_id, operationType:"delete", ns, documentKey, clusterTime,
 * txnNumber, lsid}}.
 *
 * <p>Previously the live (now removed) document was handed in as the event's "doc", so the
 * unconditional fullDocument attachment carried a full copy of the deleted document on every
 * delete. On a delete-heavy workload (messaging's delete-after-processing, where every processed
 * message is removed) that is a full duplicate of the bytes already buffered for the insert -
 * it inflated the replay buffer and, worst, every watch cursor's queued bytes, making the byte
 * budgets in WatchCursorManager / ReplicationManager trip earlier than their payload warranted.
 * Dropping it here halves the size of delete events and matches the spec.
 */
@Tag("core")
public class DeleteChangeStreamEventShapeTest {

    private static final String DB = "delete_evt_db";
    private static final String COLL = "probe";
    /** insert + delete */
    private static final int EXPECTED_EVENTS = 2;

    @Test
    public void deleteEventHasDocumentKeyButNoFullDocument() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        List<Map<String, Object>> events = new CopyOnWriteArrayList<>();

        try {
            var con = drv.getPrimaryConnection(null);
            WatchCommand w = new WatchCommand(con).setDb(DB).setColl(COLL)
                .setCb(new DriverTailableIterationCallback() {
                    @Override
                    public void incomingData(Map<String, Object> data, long dur) {
                        events.add(data);
                    }

                    @Override
                    public boolean isContinued() {
                        // Must go false once the expected events are in - a callback that answers
                        // true forever keeps the watch loop and this subscription alive past the
                        // test ("Keeping eventDispatcher alive ..." on close()).
                        return events.size() < EXPECTED_EVENTS;
                    }
                });
            Thread watcher = new Thread(() -> {
                try {
                    drv.watch(w);
                } catch (Exception ignored) {
                }
            });
            watcher.setDaemon(true);
            watcher.start();
            Thread.sleep(300);

            drv.store(DB, COLL, new ArrayList<>(List.of(Doc.of("_id", 7, "payload", "x".repeat(512)))), null);

            long inserted = 0;
            for (long i = 0; i < 5000 && events.size() < 1; i++) {
                Thread.sleep(5);
                inserted = i;
            }
            assertThat(events).as("insert must be observed (waited %d iters)", inserted).hasSizeGreaterThanOrEqualTo(1);

            new DeleteMongoCommand(drv).setDb(DB).setColl(COLL)
                .addDelete(Doc.of("_id", 7), 1, null, null)
                .execute();

            for (long i = 0; i < 5000 && events.size() < EXPECTED_EVENTS; i++) {
                Thread.sleep(5);
            }

            assertThat(events).as("insert + delete must both be observed").hasSize(EXPECTED_EVENTS);

            Map<String, Object> deleteEvent = events.get(1);
            assertThat(deleteEvent.get("operationType"))
                .as("second event is the delete").isEqualTo("delete");
            assertThat(deleteEvent.get("documentKey"))
                .as("delete carries the deleted document's key").isNotNull();
            assertThat(((Map<?, ?>) deleteEvent.get("documentKey")).get("_id"))
                .as("documentKey._id is the deleted id").isEqualTo(7);
            assertThat(deleteEvent.get("fullDocument"))
                .as("delete must NOT carry a fullDocument after-image (mongod does not)").isNull();
        } finally {
            drv.close();
        }
    }
}