package de.caluga.morphium.driver.inmem;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.commands.WatchCommand;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A full-document replacement (an update whose update-document carries no $ operators, i.e. what
 * a client's replaceOne sends) must reach change-stream watchers as operationType "replace" -
 * measured against mongod: replace carries the new fullDocument and deliberately NO
 * updateDescription, since a replacement has no meaningful per-field delta.
 *
 * Regression for #288: the replacement branch applied the change, updated the index store and
 * re-queued the TTL entry, then `continue`d past the notification code - the document changed
 * silently, invisible to every watcher (messaging, cache sync, PoppyDB replication).
 */
@Tag("core")
public class ReplaceChangeStreamEventTest {

    private static final String DB = "replace_evt_db";
    private static final String COLL = "probe";
    /** insert + operator update + replacement */
    private static final int EXPECTED_EVENTS = 3;

    @Test
    public void replacementEmitsReplaceEventOperatorUpdateEmitsUpdate() throws Exception {
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
                        // Must go false once the expected events are in: a callback that answers
                        // "true" forever keeps the watch loop - and with it the driver's event
                        // dispatcher and this subscription - alive past the test ("Keeping
                        // eventDispatcher alive - 1 active subscription(s) remain" on close()).
                        // In a full-suite run that leak is what turns a tight heap into an OOM.
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

            drv.store(DB, COLL, new ArrayList<>(List.of(Doc.of("_id", 1, "a", 1))), null);
            // update WITH operator - mongod: "update" plus updateDescription
            drv.update(DB, COLL, Doc.of("_id", 1), null, Doc.of("$set", Doc.of("a", 2)), false, false, null, null);
            // update WITHOUT operators == replaceOne - mongod: "replace", no updateDescription
            drv.update(DB, COLL, Doc.of("_id", 1), null, Doc.of("b", 3), false, false, null, null);

            long deadline = System.currentTimeMillis() + 5000;
            while (events.size() < EXPECTED_EVENTS && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            // let the watch loop observe isContinued() == false and unwind before asserting,
            // so the subscription is gone even if an assertion below fails
            watcher.join(5000);

            assertThat(events).as("insert, operator update and replacement must all be delivered")
                .hasSize(EXPECTED_EVENTS);

            assertThat(events.get(0).get("operationType")).isEqualTo("insert");

            assertThat(events.get(1).get("operationType")).as("$set is an operator update").isEqualTo("update");
            assertThat(events.get(1)).as("an operator update reports its per-field delta")
                .containsKey("updateDescription");

            Map<String, Object> replaceEvt = events.get(2);
            assertThat(replaceEvt.get("operationType"))
                .as("an update without $ operators is a replacement, not an update (#288)")
                .isEqualTo("replace");
            assertThat(replaceEvt).as("mongod sends no updateDescription for a replacement")
                .doesNotContainKey("updateDescription");

            // and the replacement really replaced rather than merged
            var after = drv.find(DB, COLL, Doc.of("_id", 1), null, null, 0, 1);
            assertThat(after).hasSize(1);
            assertThat(after.get(0)).containsEntry("b", 3).doesNotContainKey("a");
        } finally {
            drv.close();
        }
    }

    /**
     * The other half of #288: morphium.store() on an existing document goes out on the wire as
     * an update WITH $set (see StoreMongoCommand), so mongod reports operationType "update" with
     * an updateDescription. The in-memory store() path used to report "replace" for the same
     * call - same API, different event type depending on the backend.
     */
    @Test
    public void storeOnExistingDocumentEmitsUpdateLikeMongod() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        List<Map<String, Object>> events = new CopyOnWriteArrayList<>();

        try {
            var con = drv.getPrimaryConnection(null);
            WatchCommand w = new WatchCommand(con).setDb(DB).setColl("store_probe")
                .setCb(new DriverTailableIterationCallback() {
                    @Override
                    public void incomingData(Map<String, Object> data, long dur) {
                        events.add(data);
                    }
                    @Override
                    public boolean isContinued() {
                        return events.size() < 2;
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

            drv.store(DB, "store_probe", new ArrayList<>(List.of(Doc.of("_id", 1, "a", 1))), null);
            // store of an EXISTING document - the ORM's store() sends {$set: doc} to mongod
            drv.store(DB, "store_probe", new ArrayList<>(List.of(Doc.of("_id", 1, "a", 2))), null);

            long deadline = System.currentTimeMillis() + 5000;
            while (events.size() < 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            watcher.join(5000);

            assertThat(events).as("insert and re-store must both be delivered").hasSize(2);
            assertThat(events.get(0).get("operationType")).isEqualTo("insert");

            Map<String, Object> updateEvt = events.get(1);
            assertThat(updateEvt.get("operationType"))
                .as("store() on an existing document is a $set update on the wire, not a replaceOne (#288)")
                .isEqualTo("update");
            assertThat(updateEvt).as("mongod reports the per-field delta for that update")
                .containsKey("updateDescription");
            @SuppressWarnings("unchecked")
            Map<String, Object> updated =
                (Map<String, Object>) ((Map<String, Object>) updateEvt.get("updateDescription")).get("updatedFields");
            assertThat(updated).containsEntry("a", 2);
        } finally {
            drv.close();
        }
    }
}
