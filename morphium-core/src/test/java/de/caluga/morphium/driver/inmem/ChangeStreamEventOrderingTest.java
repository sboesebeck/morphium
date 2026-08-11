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
 * Change-stream events must reach a subscriber in the order the writes happened - mongod
 * guarantees per-cursor ordering, so the in-memory driver has to as well.
 *
 * <p>Regression for the client-mode dispatcher: each event used to be submitted as its own task
 * to a cached thread pool, which does not preserve submission order - under CPU contention two
 * back-to-back events could be delivered swapped (first seen as a spurious
 * ReplaceChangeStreamEventTest failure on the loaded test runner: the $set "update" and the
 * subsequent "replace" arrived inverted).
 */
@Tag("core")
public class ChangeStreamEventOrderingTest {

    private static final String DB = "cs_order_db";
    private static final String COLL = "probe";
    /** insert + WRITES updates */
    private static final int WRITES = 500;
    private static final int EXPECTED_EVENTS = WRITES + 1;

    @Test
    public void eventsArriveInWriteOrder() throws Exception {
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
                        // terminate the watch loop once everything arrived - see
                        // ReplaceChangeStreamEventTest for why leaking the subscription is not ok
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

            drv.store(DB, COLL, new ArrayList<>(List.of(Doc.of("_id", 1, "seq", 0))), null);
            for (int i = 1; i <= WRITES; i++) {
                drv.update(DB, COLL, Doc.of("_id", 1), null, Doc.of("$set", Doc.of("seq", i)), false, false, null, null);
            }

            long deadline = System.currentTimeMillis() + 15000;
            while (events.size() < EXPECTED_EVENTS && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            watcher.join(5000);

            assertThat(events).as("every write must be delivered").hasSize(EXPECTED_EVENTS);
            assertThat(events.get(0).get("operationType")).isEqualTo("insert");

            List<Integer> received = new ArrayList<>();
            for (int i = 1; i < events.size(); i++) {
                Map<String, Object> evt = events.get(i);
                assertThat(evt.get("operationType")).as("event %d", i).isEqualTo("update");
                @SuppressWarnings("unchecked")
                Map<String, Object> updated =
                    (Map<String, Object>) ((Map<String, Object>) evt.get("updateDescription")).get("updatedFields");
                received.add(((Number) updated.get("seq")).intValue());
            }

            List<Integer> expected = new ArrayList<>();
            for (int i = 1; i <= WRITES; i++) {
                expected.add(i);
            }
            assertThat(received)
                .as("update events must arrive in write order (mongod guarantees per-cursor ordering)")
                .containsExactlyElementsOf(expected);
        } finally {
            drv.close();
        }
    }
}
