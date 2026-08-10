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
                        return true;
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
            while (events.size() < 3 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }

            assertThat(events).as("insert, operator update and replacement must all be delivered")
                .hasSize(3);

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
}
