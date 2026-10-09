package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.poppydb.netty.ReplicationFlowControl;
import de.caluga.poppydb.netty.ReplicationFlowControlSettings;

/**
 * Spec section 5, E2E: replication flow control on a REAL PoppyDB primary with a real
 * {@link ReplicationManager} as the secondary (pattern of {@link ReplicationDeadWatchGateTest})
 * - the braking and the parking live in the primary's server-side {@code WatchCursorManager}
 * and {@code MongoCommandHandler}, so only a real server reproduces the chain end to end.
 *
 * <p>The primary runs as a STATIC two-seed replica set so it carries a replication coordinator:
 * the handler only brakes writers on a current primary WITH a coordinator (the same guard
 * postWrite uses), and PoppyDB creates the coordinator only for seed lists of two or more
 * members. Only the first seed exists as a real node; static (non-election) mode never
 * contacts the second seed.
 *
 * <p>Every test asserts its preconditions, not only the outcome: the sync thread parked in the
 * snapshot phase, the watch reader blocked in byte-budget backpressure, the replication cursor
 * established on the primary, and - where the outcome depends on the brake - the gate engaged
 * with a write actually parked on the wire.
 */
@Tag("server")
public class ReplicationFlowControlE2ETest {

    private static final String DB = "flowcontroltest";
    private static final String COLL = "objs";
    private static final String RS_NAME = "rsFlowControlE2E";
    private static final int PRE_DOCS = 10;

    private final List<PoppyDB> nodes = new ArrayList<>();
    private ReplicationManager rm;
    private InMemoryDriver local;

    @AfterEach
    public void tearDown() {
        if (rm != null) {
            try {
                rm.stop();
            } catch (Exception ignored) {
            }
        }

        if (local != null) {
            try {
                local.close();
            } catch (Exception ignored) {
            }
        }

        for (int i = nodes.size() - 1; i >= 0; i--) {
            try {
                nodes.get(i).shutdown();
            } catch (Exception ignored) {
            }
        }

        nodes.clear();
    }

    private int nextPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /**
     * A real primary that can meaningfully brake: static replica set with two seeds, of which
     * only this node exists. The dummy second seed exists solely to make PoppyDB create the
     * {@link ReplicationCoordinator}; in static mode nothing ever contacts it.
     */
    private PoppyDB startFlowControlPrimary(int port) throws Exception {
        PoppyDB srv = new PoppyDB(port, "localhost", 20, 5);
        srv.configureReplicaSet(RS_NAME,
                List.of("localhost:" + port, "localhost:" + (port + 1)), null, false, null);
        nodes.add(srv);
        srv.start();
        long deadline = System.currentTimeMillis() + 10_000;

        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("localhost", port), 250);
                return srv;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }

                Thread.sleep(50);
            }
        }
    }

    private boolean poll(long timeoutMs, Callable<Boolean> condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) {
                return true;
            }

            Thread.sleep(100);
        }

        return Boolean.TRUE.equals(condition.call());
    }

    private Morphium writerFor(int port, String db) {
        return writerFor(port, db, 10);
    }

    private Morphium writerFor(int port, String db, int maxConnections) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.clusterSettings().setHostSeed("localhost:" + port);
        cfg.connectionSettings().setDatabase(db);
        cfg.connectionSettings().setMaxConnections(maxConnections);
        cfg.cacheSettings().setBufferedWritesEnabled(false);
        return new Morphium(cfg);
    }

    private int primaryWatchCursorCount(PoppyDB primary) {
        return (int) primary.getCursorManagerForTest().getStats().get("watchCursors");
    }

    private ReplicationFlowControl flowControl(PoppyDB primary) {
        return primary.getCursorManagerForTest().replicationFlowControl();
    }

    private Map<String, Object> gateSnapshot(PoppyDB primary) {
        return flowControl(primary).statusSnapshot();
    }

    /**
     * Park a secondary in its snapshot phase and block its reader (small event-queue budget):
     * the primary's replication cursor fills, the gate closes at high water, and the burst
     * writer's inserts PARK on the wire instead of overflowing the cursor into a kill. After
     * the pause is released the secondary drains, the gate opens, and every document arrives.
     */
    @Test
    public void writersAreBrakedInsteadOfKillingTheReplicationWatch() throws Exception {
        int port = nextPort();
        PoppyDB primary = startFlowControlPrimary(port);
        // 64KB cursor budget: high water (50% = 32KB) crosses after ~32 blocked 1KB events,
        // while a kill needs the full 64KB, so a working brake has a wide margin and a dead
        // brake proves itself by killing the cursor.
        primary.setCursorQueueByteBudget(64 * 1024);
        primary.getCursorManagerForTest().setReplicationFlowControlSettings(
                new ReplicationFlowControlSettings(true, 50, 25, 3_000));

        Morphium writer = writerFor(port, DB);
        try {
            for (int i = 0; i < PRE_DOCS; i++) {
                primary.getDriver().store(DB, COLL, List.of(Doc.of("_id", "pre-" + i, "v", i)), null);
            }

            local = new InMemoryDriver();
            local.connect();
            rm = new ReplicationManager(local, "localhost", port);
            // Small event queue on the secondary: the reader blocks after ~4KB of buffered
            // events while the apply gate is closed - the backpressure the scenario needs.
            rm.setEventQueueByteBudget(4096);
            rm.armTestPauseInShortcutForTest();
            rm.start();

            assertTrue(poll(10_000, () -> rm.getConsistencyShortcutAttemptsForTest() >= 1),
                    "precondition: the sync thread must be parked inside the snapshot phase");
            assertTrue(poll(10_000, () -> primaryWatchCursorCount(primary) >= 1),
                    "precondition: the replication watch cursor must be established on the primary");
            assertTrue(primary.isPrimary(), "precondition: the node must hold the primary role");
            long generationBefore = rm.watchGeneration.get();

            // 1KB inserts from a writer on its own connection, every write timed.
            List<Long> writeDurationsMs = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> writerFailure = new AtomicReference<>();
            Thread writerThread = new Thread(() -> {
                try {
                    for (int i = 0; i < 80; i++) {
                        long t0 = System.currentTimeMillis();
                        writer.storeMap(COLL, Doc.of("_id", "burst-" + i, "payload", "x".repeat(1024)));
                        writeDurationsMs.add(System.currentTimeMillis() - t0);
                    }
                } catch (Throwable t) {
                    writerFailure.set(t);
                }
            }, "flowctl-burst-a");
            writerThread.start();

            // Chain: reader blocks, cursor crosses high water, gate closes, the writer's next
            // write parks instead of executing - the events that would overflow never land.
            AtomicReference<Long> engageWallMs = new AtomicReference<>();
            assertTrue(poll(30_000, () -> {
                if (!Boolean.TRUE.equals(gateSnapshot(primary).get("engaged"))) {
                    return false;
                }
                engageWallMs.compareAndSet(null, System.currentTimeMillis());
                return true;
            }), "the gate must engage while the secondary's reader is blocked (got "
                    + gateSnapshot(primary) + ")");
            assertTrue(rm.getEventQueueBytePressureCount() >= 1,
                    "precondition: the reader must be blocked in byte-budget backpressure");
            assertTrue(poll(10_000, () -> (Long) gateSnapshot(primary).get("parkedWrites") >= 1L),
                    "the writer's write must actually be parked on the wire, not executed and killed");

            // Give the parked write a real wait before the pause is released.
            while (System.currentTimeMillis() - engageWallMs.get() < 2_000) {
                Thread.sleep(50);
            }

            assertTrue(primaryWatchCursorCount(primary) == 1,
                    "while the gate brakes the writer, the replication cursor must still be alive");
            assertEquals(generationBefore, rm.watchGeneration.get(),
                    "no re-registration may happen while the gate brakes the writer");

            rm.releaseTestPauseInShortcutForTest();

            writerThread.join(45_000);
            assertFalse(writerThread.isAlive(), "the burst writer must finish once the gate opens");
            if (writerFailure.get() != null) {
                throw new AssertionError("the burst writer failed", writerFailure.get());
            }

            // Writer latency: the write that parked lasted the whole brake window. The median
            // over all samples is dominated by the ~80 fast pre-engage writes; the max is the
            // parked one, so max-vs-median is the before-vs-after comparison.
            long maxLatencyMs = Collections.max(writeDurationsMs);
            double medianLatencyMs = median(writeDurationsMs);
            assertTrue(maxLatencyMs >= 500,
                    "a parked write must have waited at least half a second (slowest write: "
                            + maxLatencyMs + " ms)");
            assertTrue(maxLatencyMs > 5 * Math.max(10, medianLatencyMs),
                    "the slowest write (" + maxLatencyMs + " ms) must be measurably slower than "
                            + "the fast-write median (" + medianLatencyMs + " ms)");

            // The pause released: the secondary drains, the gate opens, everything arrives.
            assertTrue(poll(60_000, () -> rm.isInitialSyncComplete()
                    && local.count(DB, COLL, Doc.of(), null, null) == PRE_DOCS + 80),
                    "the secondary must converge to every document (pre + burst), got "
                            + local.count(DB, COLL, Doc.of(), null, null) + " of "
                            + (PRE_DOCS + 80));
            assertTrue(poll(20_000, () -> {
                Map<String, Object> snapshot = gateSnapshot(primary);
                return Boolean.FALSE.equals(snapshot.get("engaged"))
                        && ((Long) snapshot.get("parkedWrites")) == 0L;
            }), "the gate must open once the secondary has drained below low water");
            assertEquals(1, primaryWatchCursorCount(primary),
                    "the replication cursor must never have been killed");
            assertEquals(generationBefore, rm.watchGeneration.get(),
                    "no kill and no re-registration at any point");
        } finally {
            writer.close();
        }
    }

    /**
     * A secondary that NEVER drains: parked writes are released after max-wait (3s), and their
     * events re-fill the cursor until the existing per-cursor byte budget kills it - flow
     * control shifts the kill, it does not remove it for a truly dead secondary. The primary
     * stays writable throughout.
     */
    @Test
    public void aDeadSecondaryReleasesWritersAfterMaxWait() throws Exception {
        int port = nextPort();
        PoppyDB primary = startFlowControlPrimary(port);
        // 12KB cursor budget: high water (50% = 6KB) is crossed after ~6 blocked 1KB events;
        // a dozen parked writes expiring together at max-wait re-fill it past 12KB -> kill.
        primary.setCursorQueueByteBudget(12 * 1024);
        primary.getCursorManagerForTest().setReplicationFlowControlSettings(
                new ReplicationFlowControlSettings(true, 50, 25, 3_000));

        Morphium writer = writerFor(port, DB);
        List<Morphium> stampede = new ArrayList<>();
        try {
            for (int i = 0; i < PRE_DOCS; i++) {
                primary.getDriver().store(DB, COLL, List.of(Doc.of("_id", "pre-" + i, "v", i)), null);
            }

            local = new InMemoryDriver();
            local.connect();
            rm = new ReplicationManager(local, "localhost", port);
            rm.setEventQueueByteBudget(4096);
            rm.armTestPauseInShortcutForTest();
            rm.start();

            assertTrue(poll(10_000, () -> rm.getConsistencyShortcutAttemptsForTest() >= 1),
                    "precondition: the sync thread must be parked inside the snapshot phase");
            assertTrue(poll(10_000, () -> primaryWatchCursorCount(primary) >= 1),
                    "precondition: the replication watch cursor must be established on the primary");

            // Phase 1: one writer inserts until the gate closes (its write after high water
            // parks and only completes by max-wait).
            AtomicBoolean gateEngaged = new AtomicBoolean(false);
            AtomicReference<Throwable> phase1Failure = new AtomicReference<>();
            Thread phase1 = new Thread(() -> {
                try {
                    for (int i = 0; i < 60 && !gateEngaged.get(); i++) {
                        writer.storeMap(COLL, Doc.of("_id", "b1-" + i, "payload", "x".repeat(1024)));
                    }
                } catch (Throwable t) {
                    phase1Failure.set(t);
                }
            }, "flowctl-burst-b1");
            phase1.start();

            assertTrue(poll(30_000, () -> Boolean.TRUE.equals(gateSnapshot(primary).get("engaged"))),
                    "the gate must engage once the secondary stops consuming");
            assertTrue(poll(10_000, () -> (Long) gateSnapshot(primary).get("parkedWrites") >= 1L),
                    "phase 1 must leave a write parked");
            gateEngaged.set(true);
            phase1.join(10_000);
            if (phase1Failure.get() != null) {
                throw new AssertionError("the phase-1 writer failed", phase1Failure.get());
            }

            // Phase 2: 14 writers, each its own connection so every write parks as its own
            // token. ALL expire together at max-wait; their events overflow the 12KB cursor.
            int stampedeSize = 14;
            ExecutorService pool = Executors.newFixedThreadPool(stampedeSize);
            List<Future<Long>> futures = new ArrayList<>();
            for (int k = 0; k < stampedeSize; k++) {
                Morphium c = writerFor(port, DB, 1);
                stampede.add(c);
                final int id = k;
                futures.add(pool.submit(() -> {
                    long t0 = System.currentTimeMillis();
                    c.storeMap(COLL, Doc.of("_id", "b2-" + id, "payload", "x".repeat(1024)));
                    return System.currentTimeMillis() - t0;
                }));
            }
            pool.shutdown();

            // Every stampede write completes by max-wait plus slack (they expire at 3s, or the
            // removeWatchCursor at the kill releases the rest).
            List<Long> stampedeDurations = new ArrayList<>();
            for (Future<Long> f : futures) {
                stampedeDurations.add(f.get(10, TimeUnit.SECONDS));
            }
            assertTrue(Collections.max(stampedeDurations) <= 7_000,
                    "every parked write must complete within max-wait plus slack (worst was "
                            + Collections.max(stampedeDurations) + " ms)");

            // The dead secondary's writes were released by the max-wait path, not by the gate.
            assertTrue(poll(10_000, () -> (Long) gateSnapshot(primary).get("releasedByTimeout") >= 1L),
                    "the reader never drains, so writes must be released by max-wait");

            // ... and their re-fill eventually hits the per-cursor budget: the existing kill
            // path bounds a truly dead secondary (count 0 persists - the secondary is still
            // paused and blocked, so nothing re-registers for ~30s).
            assertTrue(poll(30_000, () -> primaryWatchCursorCount(primary) == 0),
                    "the existing kill path must eventually remove the dead secondary's cursor");
            assertTrue(poll(10_000, () -> Boolean.FALSE.equals(gateSnapshot(primary).get("engaged"))),
                    "killing the cursor removes the member and opens the gate");

            // Primary stays writable after the kill - a fresh write executes immediately.
            long t0 = System.currentTimeMillis();
            writer.storeMap(COLL, Doc.of("_id", "afterkill", "v", 1));
            assertTrue(System.currentTimeMillis() - t0 < 2_000,
                    "the primary must remain writable after the kill path removed the cursor");

            // Clean teardown: release the pause, let the secondary re-sync (new watch + snapshot).
            rm.releaseTestPauseInShortcutForTest();
            assertTrue(poll(45_000, rm::isInitialSyncComplete),
                    "after the pause is released the secondary must re-sync for a clean teardown");
        } finally {
            for (Morphium c : stampede) {
                try {
                    c.close();
                } catch (Exception ignored) {
                }
            }
            writer.close();
        }
    }

    /**
     * The regression switch: replication-flow-control=false restores today's behaviour - no
     * braking at all (engagements 0, nothing ever parked), the blocked reader's cursor is
     * killed by the byte budget as before, and the secondary still recovers via
     * resume/286/resync once the pause is released.
     */
    @Test
    public void disabledFlowControlKeepsTheKillBehaviour() throws Exception {
        int port = nextPort();
        PoppyDB primary = startFlowControlPrimary(port);
        primary.setCursorQueueByteBudget(64 * 1024);
        primary.getCursorManagerForTest().setReplicationFlowControlSettings(
                new ReplicationFlowControlSettings(false, 50, 25, 3_000));

        Morphium writer = writerFor(port, DB);
        try {
            for (int i = 0; i < PRE_DOCS; i++) {
                primary.getDriver().store(DB, COLL, List.of(Doc.of("_id", "pre-" + i, "v", i)), null);
            }

            local = new InMemoryDriver();
            local.connect();
            rm = new ReplicationManager(local, "localhost", port);
            rm.setEventQueueByteBudget(4096);
            rm.armTestPauseInShortcutForTest();
            rm.start();

            assertTrue(poll(10_000, () -> rm.getConsistencyShortcutAttemptsForTest() >= 1),
                    "precondition: the sync thread must be parked inside the snapshot phase");
            assertTrue(poll(10_000, () -> primaryWatchCursorCount(primary) >= 1),
                    "precondition: the replication watch cursor must be established on the primary");

            // A burst large enough to overflow the 64KB cursor with a blocked reader.
            List<Long> durationsMs = new CopyOnWriteArrayList<>();
            for (int i = 0; i < 120; i++) {
                long t0 = System.currentTimeMillis();
                writer.storeMap(COLL, Doc.of("_id", "burst-" + i, "payload", "x".repeat(1024)));
                durationsMs.add(System.currentTimeMillis() - t0);
            }

            // Nothing ever parked: the switch is off, writes run at full speed.
            assertEquals(0L, gateSnapshot(primary).get("engagements"),
                    "a disabled gate must never engage");
            assertEquals(0L, gateSnapshot(primary).get("parkedWrites"),
                    "a disabled gate must never park a write");
            assertTrue(Collections.max(durationsMs) < 2_000,
                    "with flow control off, no write may wait at the gate (worst was "
                            + Collections.max(durationsMs) + " ms)");

            // The blocked reader's cursor is killed by the byte budget exactly as before the
            // feature existed (count stays 0 - the secondary is still paused and blocked).
            assertTrue(poll(40_000, () -> primaryWatchCursorCount(primary) == 0),
                    "with flow control off, the per-cursor byte budget must kill the cursor as today");

            // Recovery unchanged: resume/replay or 286 + full re-sync, then exact convergence.
            rm.releaseTestPauseInShortcutForTest();
            assertTrue(poll(60_000, () -> rm.isInitialSyncComplete()
                    && local.count(DB, COLL, Doc.of(), null, null) == PRE_DOCS + 120),
                    "the secondary must still converge via resume/286/resync, got "
                            + local.count(DB, COLL, Doc.of(), null, null) + " of "
                            + (PRE_DOCS + 120));
        } finally {
            writer.close();
        }
    }

    private static double median(List<Long> values) {
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int n = sorted.size();
        return n % 2 == 0
                ? (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0
                : sorted.get(n / 2);
    }
}