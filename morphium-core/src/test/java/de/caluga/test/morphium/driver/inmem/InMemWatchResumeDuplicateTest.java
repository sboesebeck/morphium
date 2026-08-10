package de.caluga.test.morphium.driver.inmem;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.test.mongo.suite.base.TestUtils;

/**
 * A watch that resumes with a resumeAfter token registers its subscription BEFORE replaying the
 * event history (deliberately - the reverse order would open a loss window between history
 * snapshot and live stream). A write landing in that window used to be delivered twice: once by
 * the live dispatch to the already-registered subscription and once by the replay, whose token
 * range covers it. Real mongod cannot do this (oplog-cursor resume is snapshot-consistent), so
 * the in-memory driver must suppress the duplicate.
 *
 * <p>The test makes the race deterministic by performing the write inside the WatchCommand's
 * registrationCallback, which runs exactly between subscription registration and history replay.
 */
@Tag("inmemory")
public class InMemWatchResumeDuplicateTest {

    private static final String DB = "watch_resume_dup";
    private static final String COLL = "events";

    private InMemoryDriver drv;

    @BeforeEach
    void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
    }

    @AfterEach
    void tearDown() {
        drv.close();
    }

    private void insert(String id) throws Exception {
        drv.insert(DB, COLL, List.of(Doc.of("_id", id)), null, true);
    }

    @Test
    void resumedWatchDeliversRaceWindowEventExactlyOnce() throws Exception {
        // Phase 1: capture a resume token from a first watch seeing document "a".
        AtomicReference<Map<String, Object>> tokenOfA = new AtomicReference<>();
        CountDownLatch aSeen = new CountDownLatch(1);
        CountDownLatch registered = new CountDownLatch(1);
        AtomicBoolean firstRunning = new AtomicBoolean(true);
        MongoConnection con1 = drv.getPrimaryConnection(null);
        WatchCommand first = new WatchCommand(con1).setDb(DB).setColl(COLL)
            .setMaxTimeMS(300)
            .setRegistrationCallback(registered::countDown)
            .setCb(new DriverTailableIterationCallback() {
            @Override
            public void incomingData(Map<String, Object> data, long dur) {
                @SuppressWarnings("unchecked")
                Map<String, Object> token = (Map<String, Object>) data.get("_id");
                tokenOfA.set(token);
                firstRunning.set(false);
                aSeen.countDown();
            }
            @Override
            public boolean isContinued() {
                return firstRunning.get();
            }
        });
        Thread w1 = Thread.ofVirtual().start(() -> {
            try {
                first.watch();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                first.releaseConnection();
            }
        });
        registered.await(5, TimeUnit.SECONDS);
        insert("a");
        aSeen.await(5, TimeUnit.SECONDS);
        w1.join(5000);
        assertNotNull(tokenOfA.get(), "first watch must have captured a resume token");

        // Phase 2: history beyond the token, written while nobody watches.
        insert("b");
        insert("c");

        // Phase 3: resume from token "a"; the registration callback writes "d" exactly inside
        // the register-before-replay window, so "d" is both in the replayed history AND
        // dispatched live to the already-registered subscription.
        List<Object> deliveredIds = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean secondRunning = new AtomicBoolean(true);
        MongoConnection con2 = drv.getPrimaryConnection(null);
        WatchCommand second = new WatchCommand(con2).setDb(DB).setColl(COLL)
            .setMaxTimeMS(300)
            .setResumeAfter(tokenOfA.get())
            .setRegistrationCallback(() -> {
            try {
                insert("d");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        })
            .setCb(new DriverTailableIterationCallback() {
            @Override
            public void incomingData(Map<String, Object> data, long dur) {
                @SuppressWarnings("unchecked")
                Map<String, Object> key = (Map<String, Object>) data.get("documentKey");
                deliveredIds.add(key == null ? null : key.get("_id"));
            }
            @Override
            public boolean isContinued() {
                return secondRunning.get();
            }
        });
        Thread w2 = Thread.ofVirtual().start(() -> {
            try {
                second.watch();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                second.releaseConnection();
            }
        });

        TestUtils.waitForConditionToBecomeTrue(5000,
            "resumed watch did not deliver b, c and d: " + deliveredIds,
            () -> deliveredIds.contains("b") && deliveredIds.contains("c") && deliveredIds.contains("d"));
        // Settle: a duplicate "d" arrives via the async live dispatch shortly after the replay.
        Thread.sleep(800);
        secondRunning.set(false);
        w2.join(5000);

        assertEquals(1, deliveredIds.stream().filter("b"::equals).count(), "b delivered once: " + deliveredIds);
        assertEquals(1, deliveredIds.stream().filter("c"::equals).count(), "c delivered once: " + deliveredIds);
        assertEquals(1, deliveredIds.stream().filter("d"::equals).count(),
            "the race-window event 'd' must be delivered exactly once: " + deliveredIds);
    }
}
