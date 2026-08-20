package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A driver instance that is shut down and then reconnected must be fully functional again -
 * {@code shutdown(true)} is the documented way to clean up after a test, so recycling the instance
 * is a supported thing to do.
 *
 * <p>The change-stream event dispatcher used to be the exception: unlike the other executors it was
 * {@code final} and never re-created, so a driver that shut down while no subscription was active
 * (the only case in which the dispatcher is stopped at all) came back up with a dead dispatcher and
 * dropped every client-mode change stream event from then on - one WARN per lost event.
 */
@Tag("inmemory")
public class ReconnectAfterShutdownTest {
    private final String db = "reconnectdb";
    private final String coll = "reconnectcoll";
    private final AtomicBoolean keepWatching = new AtomicBoolean(true);
    private InMemoryDriver drv;

    @AfterEach
    void tearDown() {
        keepWatching.set(false);

        if (drv != null) {
            drv.shutdown(true);
            drv = null;
        }
    }

    @Test
    void changeStreamEventsMustStillBeDeliveredAfterAShutdownAndReconnect() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
        // No watch has ever been started, so there is no active subscription - exactly the
        // condition under which shutdown() stops the event dispatcher.
        drv.shutdown(true);
        drv.connect();

        AtomicInteger delivered = new AtomicInteger();
        WatchCommand cmd = new WatchCommand(drv).setDb(db).setColl(coll).setMaxTimeMS(200)
        .setCb(new DriverTailableIterationCallback() {
            @Override
            public void incomingData(Map<String, Object> data, long dur) {
                delivered.incrementAndGet();
            }
            @Override
            public boolean isContinued() {
                return keepWatching.get();
            }
        });
        drv.runCommand(cmd);
        Thread.sleep(300);
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
        .setDocuments(List.of(Doc.of("counter", 1))).execute();
        long until = System.currentTimeMillis() + 5_000;

        while (System.currentTimeMillis() < until && delivered.get() == 0) {
            Thread.sleep(50);
        }

        assertTrue(delivered.get() > 0,
            "the insert after the reconnect must reach the change stream - it did not, so the "
            + "event dispatcher stayed shut down from the first shutdown() and every event since "
            + "has been dropped");
    }
}
