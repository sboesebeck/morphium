package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression test for #325: a change stream must not consume a thread of the driver's shared
 * scheduler for its whole lifetime.
 *
 * <p>The watch loop parks in {@code monitor.await(...)} until the stream ends. Submitted to
 * {@code exec} - a {@code ScheduledThreadPoolExecutor} that never grows past its core size - that
 * makes the number of concurrent change streams a hard, silent ceiling: watch number
 * {@code poolSize + 1} never starts, so its {@code replayHistory} never runs, and the TTL sweep
 * sharing the same pool stops node-wide until some stream ends.
 *
 * <p>Both tests pin the pool explicitly via {@code inmemory.scheduledThreads} rather than relying
 * on {@code DEFAULT_EXEC_THREADS} ({@code max(20, 2*cores)}), which would mean 40+ long-lived
 * streams on a many-core CI box.
 *
 * <p>Lives in the driver's own package for symmetry with the other driver-level tests; it only
 * uses public API.
 */
@Tag("inmemory")
public class WatchThreadStarvationTest {
    /** Small enough to be quick, larger than 1 so the starvation is a ceiling and not a fluke. */
    private static final int POOL = 4;
    /** Deliberately past the pool size: the last five are the ones that never started. */
    private static final int WATCHES = POOL + 5;
    private static final int EVENTS = 3;

    private final String db = "watchstarvationdb";
    private final String coll = "watched";
    private final String ttlColl = "ttlwatched";

    private final AtomicBoolean keepWatching = new AtomicBoolean(true);
    private final List<WatchCommand> openCommands = new ArrayList<>();
    private InMemoryDriver drv;
    private String previousPoolProperty;

    @AfterEach
    void tearDown() {
        keepWatching.set(false);

        if (drv != null) {
            drv.shutdown(true);
            drv = null;
        }

        openCommands.clear();

        if (previousPoolProperty == null) {
            System.clearProperty("inmemory.scheduledThreads");
        } else {
            System.setProperty("inmemory.scheduledThreads", previousPoolProperty);
        }
    }

    /**
     * The pool size is read in the field initializer of {@code exec}, so the property has to be in
     * place before the driver is constructed.
     */
    private InMemoryDriver pinnedPoolDriver(int poolSize, int expireCheckMs) {
        previousPoolProperty = System.setProperty("inmemory.scheduledThreads", String.valueOf(poolSize));
        InMemoryDriver d = new InMemoryDriver();
        d.setExpireCheck(expireCheckMs);
        d.connect();
        return d;
    }

    /**
     * Opens one change stream through the server path ({@code runCommand(WatchCommand)}) that
     * resumes from token 0, i.e. asks for the complete history to be replayed. The returned counter
     * is what the replay actually delivered.
     */
    private AtomicInteger openWatch(String collection) throws Exception {
        AtomicInteger delivered = new AtomicInteger();
        WatchCommand cmd = new WatchCommand(drv)
        .setDb(db)
        .setColl(collection)
        .setMaxTimeMS(200)
        .setResumeAfter(Doc.of("_data", String.format(Locale.ROOT, "%016x", 0L)))
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
        openCommands.add(cmd);
        drv.runCommand(cmd);
        return delivered;
    }

    private void awaitOrTimeout(java.util.function.BooleanSupplier done, long timeoutMs) throws Exception {
        long until = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < until && !done.getAsBoolean()) {
            Thread.sleep(50);
        }
    }

    @Test
    void everyWatchMustReplayItsHistoryEvenWithMoreStreamsThanSchedulerThreads() throws Exception {
        drv = pinnedPoolDriver(POOL, 3_600_000);

        for (int i = 0; i < EVENTS; i++) {
            new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(Doc.of("counter", i))).execute();
        }

        List<AtomicInteger> delivered = new ArrayList<>();

        for (int i = 0; i < WATCHES; i++) {
            delivered.add(openWatch(coll));
        }

        awaitOrTimeout(() -> delivered.stream().allMatch(d -> d.get() >= EVENTS), 15_000);
        long starved = delivered.stream().filter(d -> d.get() < EVENTS).count();
        assertEquals(0, starved,
            "every one of the " + WATCHES + " streams asked to resume from the start of history, so each "
            + "must have replayed " + EVENTS + " events - " + starved + " got fewer, which means their "
            + "watch task never ran: with a " + POOL + "-thread scheduler the first " + POOL
            + " streams park every thread they have. Delivered per stream: "
            + delivered.stream().map(d -> String.valueOf(d.get())).toList());
    }

    @Test
    void ttlExpiryMustKeepRunningWhileManyWatchesAreOpen() throws Exception {
        drv = pinnedPoolDriver(POOL, 100);
        drv.createIndex(db, ttlColl, Doc.of("expiresAt", 1), Doc.of("name", "ttl_1", "expireAfterSeconds", 0));

        for (int i = 0; i < WATCHES; i++) {
            openWatch(coll);
        }

        // Let the watch loops settle into their wait before the TTL document goes in, so the
        // document really is swept by a scheduler that is already carrying every stream.
        Thread.sleep(500);
        new InsertMongoCommand(drv).setDb(db).setColl(ttlColl)
        .setDocuments(List.of(Doc.of("counter", 0, "expiresAt", new Date(System.currentTimeMillis() - 5_000L))))
        .execute();
        awaitOrTimeout(() -> {
            try {
                return drv.find(db, ttlColl, Doc.of(), null, null, 0, 0).isEmpty();
            } catch (Exception e) {
                return false;
            }
        }, 15_000);
        assertEquals(0, drv.find(db, ttlColl, Doc.of(), null, null, 0, 0).size(),
            "the document was already due when it was inserted and the sweep runs every 100ms, so it must "
            + "be gone - it is still there because all " + WATCHES + " open change streams hold the "
            + POOL + " scheduler threads the TTL sweep needs");
    }
}
