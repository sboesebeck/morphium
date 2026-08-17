package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.test.mongo.suite.data.UncachedObject;

/**
 * Fail-closed destructive resync (D2) + shortcut namespace union (D4) - 2026-08-14
 * empty-node-wipe fix, task 3.
 *
 * <p>Reproduces, at the {@link ReplicationManager} level (no election/RS machinery needed - a
 * ReplicationManager talks to a fixed {@code primaryHost:primaryPort} regardless of RS/leader
 * state), the exact kill chain from the bug report: a follower holding real data reconnects to
 * whatever now answers at that address, and if that "primary" turns out to be a freshly
 * restarted, empty process, its change-stream sequence counter necessarily starts fresh (0-ish) -
 * behind the sequence our local data was last known to reflect. Before the fix, the follower
 * would trust that empty state and wipe its own data to match it. The fix refuses instead.
 *
 * <p>The "primary restarted empty" step is reproduced literally: a standalone primary is fed data
 * and live-replicates it to a manually-wired {@link ReplicationManager}; the connection is then
 * severed ({@link ReplicationManager#pauseReplicationForTest()}), the primary process is shut
 * down (destroying its in-memory state), and a brand-new, empty {@code PoppyDB} is started on the
 * SAME port before the connection is healed again ({@link ReplicationManager#resumeReplicationForTest()}).
 * The follower's next reconnect necessarily hits the primary's shrunk replay buffer ("resume
 * window lost"), which is exactly the fallback branch the bug report's log lines show.
 *
 * <ul>
 *   <li>{@link #refusesWhenReconnectedPrimaryIsBehind()} - case (a): the freshly-restarted primary
 *       is empty AND behind (sequence 0-ish) - the follower must refuse, keep its data, log an
 *       ERROR, and surface the refusal in stats.</li>
 *   <li>{@link #proceedsWhenReconnectedPrimaryIsEmptyButCaughtUp()} - case (b): the
 *       freshly-restarted primary is also empty, but its sequence has been advanced (by other
 *       writes then a drop) past the follower's local sequence - a legitimate post-dropDatabase
 *       shape. The resync must proceed exactly as before this fix.</li>
 *   <li>{@link #shortcutNotTakenWhenLocalHasExtraNamespace()} - case (c) / D4: a follower whose
 *       local state has an extra namespace the (still fully caught-up, never-restarted) primary
 *       does not must NOT take the consistency shortcut - the namespace comparison must be a
 *       union (local-only namespaces count as mismatch), not an intersection that could miss
 *       this and leave the extra namespace behind forever.</li>
 * </ul>
 */
@Tag("server")
public class ReplicationFailClosedTest {

    private static final Logger log = LoggerFactory.getLogger(ReplicationFailClosedTest.class);

    private static final String DB = "failclosedtest";
    private static final String COLL = "objs";
    private static final int DOCS = 20;

    /** Started nodes, shut down in reverse start order on teardown. */
    private final List<PoppyDB> nodes = new ArrayList<>();
    /** Extra ReplicationManager instances (RM-replacement tests use more than one) to stop on teardown. */
    private final List<ReplicationManager> extraReplicationManagers = new ArrayList<>();
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
        for (int i = extraReplicationManagers.size() - 1; i >= 0; i--) {
            try {
                extraReplicationManagers.get(i).stop();
            } catch (Exception ignored) {
            }
        }
        extraReplicationManagers.clear();
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

    // ---- bootstrap helpers (pattern of ReplicationResumeTest / FastResyncTest) --------------

    private int nextPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /** Starts a standalone (no RS config -> immediately primary) PoppyDB node, tracked for teardown. */
    private PoppyDB startStandalonePrimary(int port) throws Exception {
        PoppyDB srv = new PoppyDB(port, "localhost", 20, 5);
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

    private long localCount() throws Exception {
        return local.count(DB, COLL, Doc.of(), null, null);
    }

    /** Count of "Starting change stream watch on primary..." lines captured so far - one per watch registration. */
    private long registrationLogCount(ListAppender<ILoggingEvent> appender) {
        return appender.list.stream()
            .filter(ev -> ev.getFormattedMessage().contains("Starting change stream watch on primary"))
            .count();
    }

    private void writeDocs(Morphium writer, int count, String prefix) {
        List<UncachedObject> batch = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            batch.add(new UncachedObject(prefix + "-" + i, i));
        }
        writer.storeList(batch, COLL);
    }

    private Morphium writerFor(int port, String db) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.clusterSettings().setHostSeed("localhost:" + port);
        cfg.connectionSettings().setDatabase(db);
        cfg.connectionSettings().setMaxConnections(10);
        cfg.cacheSettings().setBufferedWritesEnabled(false);
        return new Morphium(cfg);
    }

    /**
     * Copies the follower's CURRENT {@code DB.COLL} documents, verbatim, into a target node's
     * driver - the same doc {@code Map}s {@link InMemoryDriver#find} returns, re-inserted as-is.
     * Unlike two independent inserts built from scratch (which do not reliably dbHash-match -
     * apparently not just field content but some internal representation detail differs), this
     * guarantees a genuine dbHash match: it is a literal copy of the exact bytes replication
     * already produced, the same technique {@code performInitialSync}'s own {@code syncCollection}
     * uses to seed a follower from a primary.
     */
    private void copyLocalDataInto(PoppyDB target) throws Exception {
        List<Map<String, Object>> docs = local.find(DB, COLL, Doc.of(), null, null, 0, 1000);
        GenericCommand cmd = new GenericCommand(target.getDriver());
        cmd.setDb(DB);
        cmd.setColl(COLL);
        cmd.setCmdData(Doc.of("insert", COLL, "$db", DB, "documents", docs));
        target.getDriver().runCommand(cmd);
    }

    /**
     * Inserts {@code count} documents with DETERMINISTIC {@code _id}s (unlike {@link #writeDocs},
     * whose {@link UncachedObject}s get a fresh random {@code MorphiumId} on every call) directly
     * into a target node's driver - bypassing Morphium/the wire protocol, same pattern as the
     * dbHash-comparison tests elsewhere in this file.
     */
    private void insertFixedDocs(PoppyDB target, int count, String prefix) throws Exception {
        List<Map<String, Object>> docs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            docs.add(Doc.of("_id", prefix + "-" + i, "value", i));
        }
        GenericCommand cmd = new GenericCommand(target.getDriver());
        cmd.setDb(DB);
        cmd.setColl(COLL);
        cmd.setCmdData(Doc.of("insert", COLL, "$db", DB, "documents", docs));
        target.getDriver().runCommand(cmd);
    }

    /**
     * Common setup for cases (a) and (b): a standalone primary fed {@link #DOCS} documents,
     * live-replicated to a manually-wired {@link ReplicationManager}, its replay buffer then
     * shrunk so a subsequent gap cannot be resumed from the buffer (forcing the primary to
     * answer "resume window lost" rather than silently truncating - the same trick
     * {@code ReplicationResumeTest#bufferMissTriggersResync} uses).
     *
     * @return the follower's lastAppliedSequence right after live replication converged (S in
     *         the class javadoc / bug report).
     */
    private long bootstrapFollowerWithData(int port1) throws Exception {
        PoppyDB primary = startStandalonePrimary(port1);

        local = new InMemoryDriver();
        local.connect();
        rm = new ReplicationManager(local, "localhost", port1);
        rm.start();
        assertTrue(poll(30_000, rm::isInitialSyncComplete), "initial (trivially empty) sync must complete");

        Morphium writer = writerFor(port1, DB);
        try {
            writeDocs(writer, DOCS, "pre");
            assertTrue(poll(30_000, () -> localCount() == DOCS),
                "follower must live-replicate the batch (got " + localCount() + ")");
        } finally {
            writer.close();
        }
        assertTrue(poll(5_000, () -> rm.getLastAppliedSequence() > 0),
            "lastAppliedSequence must have advanced past 0 via live replication");
        long s = rm.getLastAppliedSequence();

        // Shrink the buffer so the upcoming gap cannot be resumed from it.
        primary.getDriver().setChangeStreamHistoryLimit(2);
        return s;
    }

    // ---- case (a): reconnected primary is behind -> refuse -----------------------------------

    @Test
    public void refusesWhenReconnectedPrimaryIsBehind() throws Exception {
        int port1 = nextPort();
        long s = bootstrapFollowerWithData(port1);
        assertEquals(0, rm.getRefusedResyncCount(), "no refusal should have happened yet");

        ch.qos.logback.classic.Logger rmLogger =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ReplicationManager.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        rmLogger.addAppender(appender);

        try {
            // Sever, kill the primary (destroying its state), and put a brand-new EMPTY PoppyDB
            // on the SAME port - "a freshly restarted node" the follower will reconnect to.
            rm.pauseReplicationForTest();
            Thread.sleep(500);
            nodes.get(0).shutdown();
            nodes.remove(0);
            startStandalonePrimary(port1); // fresh, empty, sequence starts near 0

            rm.resumeReplicationForTest();

            assertTrue(poll(30_000, () -> rm.getRefusedResyncCount() >= 1),
                "reconnecting to a behind/empty primary must be refused (refusedResyncCount="
                    + rm.getRefusedResyncCount() + ")");
            assertTrue(poll(5_000, rm::isRefusingDestructiveResync),
                "the refusal state must be currently active");

            // Data must be untouched - the whole point of the fix.
            assertEquals(DOCS, localCount(),
                "local data must survive a refused resync against a regressed primary");
            assertFalse(rm.isInitialSyncComplete(),
                "a refused resync must not be reported as a completed sync");

            List<ILoggingEvent> errors = appender.list.stream()
                .filter(ev -> ev.getLevel() == Level.ERROR)
                .collect(Collectors.toList());
            assertTrue(errors.stream().anyMatch(ev -> ev.getFormattedMessage().contains("refusing full re-sync")
                    && ev.getFormattedMessage().contains("possible restarted/stale primary")),
                "an ERROR log must document the refusal: " + errors.stream()
                    .map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList()));

            Map<String, Object> stats = rm.getStats();
            assertEquals(Boolean.TRUE, stats.get("refusingDestructiveResync"),
                "getStats() must surface the active refusal");
            assertTrue((Long) stats.get("refusedResyncCount") >= 1,
                "getStats() must surface the refusal count");

            // Pacing sanity check (task-3 review, issue 1): while refusing, the watch
            // register/teardown cycle must be paced (REFUSAL_WATCH_PACE_MS), not spinning hot.
            // A ~6s window at the 2s pace should see roughly 3 registrations; assert well under a
            // spin's ~1400/s rate (thousands in this window) without pinning to an exact count.
            long registrationsBefore = registrationLogCount(appender);
            long windowStart = System.currentTimeMillis();
            Thread.sleep(6_000);
            long registrationsDuringWindow = registrationLogCount(appender) - registrationsBefore;
            long windowMs = System.currentTimeMillis() - windowStart;
            assertTrue(registrationsDuringWindow < 20,
                "watch registrations while refusing must be paced, not spinning (got "
                    + registrationsDuringWindow + " registrations in " + windowMs + "ms)");
            assertTrue(rm.isRefusingDestructiveResync(),
                "still refusing after the pacing-measurement window (primary was never caught up)");

            // Recoverability (the brief's explicit requirement): once the SAME reconnected
            // process genuinely catches up - its own sequence overtakes the follower's local
            // sequence via real writes - the refusal must lift on its own and the follower must
            // resync normally, with no operator intervention beyond writes actually happening.
            PoppyDB caughtUpPrimary = nodes.get(0);
            Morphium catchUpWriter = writerFor(port1, DB);
            try {
                writeDocs(catchUpWriter, (int) s + 50, "catchup");
            } finally {
                catchUpWriter.close();
            }

            assertTrue(poll(60_000, () -> rm.isInitialSyncComplete() && !rm.isRefusingDestructiveResync()),
                "the follower must eventually resync once the primary genuinely caught up "
                    + "(isInitialSyncComplete=" + rm.isInitialSyncComplete()
                    + ", refusing=" + rm.isRefusingDestructiveResync() + ")");
            assertTrue(poll(30_000, () -> localCount() == caughtUpPrimary.getDriver()
                    .count(DB, COLL, Doc.of(), null, null)),
                "the recovered follower must converge to the caught-up primary's data (local="
                    + localCount() + ")");

            log.info("case (a) converged: local sequence was {}, refusedResyncCount={}",
                s, rm.getRefusedResyncCount());
        } finally {
            rmLogger.detachAppender(appender);
        }
    }

    // ---- case (b): reconnected primary is empty but caught up -> resync proceeds ------------

    @Test
    public void proceedsWhenReconnectedPrimaryIsEmptyButCaughtUp() throws Exception {
        int port1 = nextPort();
        long s = bootstrapFollowerWithData(port1);

        rm.pauseReplicationForTest();
        Thread.sleep(500);
        nodes.get(0).shutdown();
        nodes.remove(0);
        PoppyDB freshPrimary = startStandalonePrimary(port1);

        // Legitimate post-dropDatabase shape: advance the fresh primary's own sequence counter
        // well past the follower's local sequence (via unrelated writes), then drop that data -
        // final state is empty, but the sequence keeps counting up, unlike a genuinely-behind
        // primary.
        Morphium bumpWriter = writerFor(port1, "bumpdb");
        try {
            writeDocs(bumpWriter, (int) Math.max(50, s + 100), "bump");
        } finally {
            bumpWriter.close();
        }
        GenericCommand dropBump = new GenericCommand(freshPrimary.getDriver());
        dropBump.setDb("bumpdb");
        dropBump.setCmdData(Doc.of("dropDatabase", 1, "$db", "bumpdb"));
        freshPrimary.getDriver().runCommand(dropBump);

        rm.resumeReplicationForTest();

        assertTrue(poll(30_000, () -> rm.isInitialSyncComplete() && localCount() == 0),
            "a legitimately caught-up (even if empty) primary must sync normally (got count="
                + localCount() + ", initialSyncComplete=" + rm.isInitialSyncComplete() + ")");
        assertEquals(0, rm.getRefusedResyncCount(),
            "a caught-up primary must never trigger the destructive-resync refusal");
        assertFalse(rm.isRefusingDestructiveResync(), "must not be left in a refusing state");
        assertFalse(rm.wasLastSyncShortcut(),
            "namespaces genuinely differed (primary emptied, local still had data) - must be a full sync");

        log.info("case (b) converged: local sequence was {}", s);
    }

    // ---- case (c) / D4: shortcut must not be taken when local has an extra namespace ---------

    @Test
    public void shortcutNotTakenWhenLocalHasExtraNamespace() throws Exception {
        int port1 = nextPort();
        long s = bootstrapFollowerWithData(port1);
        PoppyDB primary = nodes.get(0);

        // Test-only backdoor: write straight into the follower's InMemoryDriver, bypassing
        // replication, to create a namespace the (still perfectly healthy, never-restarted)
        // primary does not have (same technique as FastResyncTest#fallbackOnDivergence, but a
        // whole extra NAMESPACE rather than an extra document in an existing one).
        GenericCommand inject = new GenericCommand(local);
        inject.setDb("extradb");
        inject.setColl("extracoll");
        inject.setCmdData(Doc.of(
            "insert", "extracoll", "$db", "extradb",
            "documents", List.of(Doc.of("_id", "extra-doc", "note", "local-only"))));
        local.runCommand(inject);
        assertEquals(1, local.count("extradb", "extracoll", Doc.of(), null, null),
            "the injected extra namespace must be present before the forced resync");

        // Force a fresh sync cycle against the SAME still-alive primary (never restarted, so its
        // sequence only ever increases - the D2 guard must never fire here, isolating this test
        // to the shortcut's namespace comparison alone): sever, write a gap the shrunk buffer
        // cannot cover, heal.
        rm.pauseReplicationForTest();
        Thread.sleep(500);
        Morphium writer = writerFor(port1, DB);
        try {
            writeDocs(writer, DOCS, "gap");
        } finally {
            writer.close();
        }
        Thread.sleep(300);
        rm.resumeReplicationForTest();

        assertTrue(poll(30_000, () -> rm.isInitialSyncComplete() && localCount() == 2 * DOCS),
            "follower must converge to both batches after the forced resync (got count="
                + localCount() + ")");
        assertEquals(0, rm.getRefusedResyncCount(),
            "the still-live, never-restarted primary must never trigger the D2 refusal");
        assertFalse(rm.wasLastSyncShortcut(),
            "a follower with a local-only extra namespace must NOT take the consistency shortcut "
                + "(union comparison, not intersection)");

        // The extra namespace must be gone - proof the mismatch was actually detected and acted
        // on, not silently waved through by an intersection-only comparison.
        assertTrue(poll(15_000, () -> local.count("extradb", "extracoll", Doc.of(), null, null) == 0),
            "the local-only extra namespace must be wiped by the (legitimate) full resync");

        log.info("case (c) converged: local sequence was {}", s);
    }

    // ---- issue 2 (task-3 review): sequence carry-over across RM replacement (leader change) --
    //
    // PoppyDB#startReplicationToLeader constructs a brand-new ReplicationManager on every leader
    // change, reusing the SAME persistent local driver (only the RM wrapper is replaced - the
    // production analogue of what these two tests build by hand: rm1 against primary A is
    // stop()ped, and a fresh rm is built against a different primary, sharing the same `local`
    // driver). A fresh instance's own lastAppliedSequence starts at 0, which - absent the carry-
    // over - would make the destructive-resync guard vacuously pass on every leader change; these
    // tests exercise ReplicationManager#carryOverLastAppliedSequence directly, the same call
    // PoppyDB now makes before starting the replacement.
    //
    // 2026-08-14 production-CI fix (I-2): both tests below now use the two-arg,
    // primary-identity-aware carryOverLastAppliedSequence(seq, sourceAddress) - portA/portB/portC
    // are all DIFFERENT addresses, exactly the shape a real leader change has in production. See
    // carryOverRefusesOnlyWhenReplacementLeaderIsTheSameAddressRegressed below for the mirror that
    // covers the SAME-address case (the actual kill chain).

    @Test
    public void carryOverAllowsNormalSyncWhenReplacementLeaderIsCaughtUp() throws Exception {
        int portA = nextPort();
        int portB = nextPort();
        startStandalonePrimary(portA);

        local = new InMemoryDriver();
        local.connect();
        ReplicationManager rm1 = new ReplicationManager(local, "localhost", portA);
        extraReplicationManagers.add(rm1);
        rm1.start();
        assertTrue(poll(30_000, rm1::isInitialSyncComplete), "rm1 initial sync must complete");

        Morphium writerA = writerFor(portA, DB);
        try {
            writeDocs(writerA, DOCS, "pre");
            assertTrue(poll(30_000, () -> localCount() == DOCS),
                "rm1 must live-replicate the batch (got " + localCount() + ")");
        } finally {
            writerA.close();
        }
        assertTrue(poll(5_000, () -> rm1.getLastAppliedSequence() > 0),
            "rm1 lastAppliedSequence must have advanced past 0");
        long predecessorSeq = rm1.getLastAppliedSequence();
        String predecessorAddress = rm1.getLeaderAddress();

        // Simulate PoppyDB#startReplicationToLeader tearing down the old RM on a leader change.
        rm1.stop();

        // The "new leader": a DIFFERENT standalone primary, fed enough writes that its own
        // sequence is comfortably >= predecessorSeq - a legitimate, caught-up new leader.
        startStandalonePrimary(portB);
        Morphium writerB = writerFor(portB, DB);
        try {
            writeDocs(writerB, (int) predecessorSeq + 50, "leaderb");
        } finally {
            writerB.close();
        }

        // Replacement RM, same local driver, carrying the predecessor's (sequence, source
        // address) forward exactly as PoppyDB#startReplicationToLeader now does. Different
        // address (portA vs portB) - per the identity-aware overload, this does NOT arm the
        // guard; the outcome (sync succeeds) is unchanged from before I-2 either way here since
        // the new leader is caught up regardless, but the MECHANISM is now adopt-at-registration,
        // not a guard pass.
        rm = new ReplicationManager(local, "localhost", portB);
        rm.carryOverLastAppliedSequence(predecessorSeq, predecessorAddress);
        rm.start();

        assertTrue(poll(30_000, rm::isInitialSyncComplete),
            "a caught-up replacement leader must sync normally despite the carried-over sequence");
        assertEquals(0, rm.getRefusedResyncCount(),
            "a caught-up replacement leader must never trigger the destructive-resync refusal");
        assertFalse(rm.isRefusingDestructiveResync());

        log.info("carry-over caught-up case converged: predecessorSeq={}, final local count={}",
            predecessorSeq, localCount());
    }

    /**
     * I-2 (production-CI fix, superseding the old {@code carryOverRefusesWhenReplacementLeaderIsRegressed}):
     * a genuine leader change to a DIFFERENT primary, even one whose own counter is far below the
     * predecessor's, must NOT be refused - the carried sequence lives in an unrelated, foreign
     * number space and must not arm the guard at all. This is exactly the CI incident: node1
     * carried 227951 from its old leader; the new leader's own counter was 213896 (lower, but a
     * completely different and entirely legitimate primary) - the old code refused for 40+
     * minutes; the fix adopts the new primary's own base at registration and lets dbHash/the
     * consistency shortcut decide. Here that means a full resync legitimately proceeds (the new,
     * near-empty primary's data does not match local's) and local converges to ITS (near-empty)
     * state - the wipe is correct in this case, because a genuinely different, currently-elected
     * leader's state is exactly what a follower is supposed to converge to. Protecting against a
     * WRONGLY-elected empty leader is the election layer's job (Tasks 1/2/4), not this guard's -
     * see the class-level javadoc on {@code ReplicationManager#carryOverLastAppliedSequence(long, String)}.
     */
    @Test
    public void carryOverAdoptsFreshBaseWhenReplacementLeaderIsDifferentEvenIfItsCounterIsLower() throws Exception {
        int portA = nextPort();
        int portC = nextPort();
        startStandalonePrimary(portA);

        local = new InMemoryDriver();
        local.connect();
        ReplicationManager rm1 = new ReplicationManager(local, "localhost", portA);
        extraReplicationManagers.add(rm1);
        rm1.start();
        assertTrue(poll(30_000, rm1::isInitialSyncComplete), "rm1 initial sync must complete");

        Morphium writerA = writerFor(portA, DB);
        try {
            writeDocs(writerA, DOCS, "pre");
            assertTrue(poll(30_000, () -> localCount() == DOCS),
                "rm1 must live-replicate the batch (got " + localCount() + ")");
        } finally {
            writerA.close();
        }
        assertTrue(poll(5_000, () -> rm1.getLastAppliedSequence() > 0),
            "rm1 lastAppliedSequence must have advanced past 0");
        long predecessorSeq = rm1.getLastAppliedSequence();
        String predecessorAddress = rm1.getLeaderAddress();

        rm1.stop();

        // The "new leader": a genuinely DIFFERENT (different port/address) standalone primary,
        // fresh and empty - its own counter is near 0, far below predecessorSeq. In production
        // this is an ordinary leader change to a new, currently-quiet leader, not a restart of
        // the same node.
        startStandalonePrimary(portC);

        rm = new ReplicationManager(local, "localhost", portC);
        rm.carryOverLastAppliedSequence(predecessorSeq, predecessorAddress);
        rm.start();

        assertTrue(poll(30_000, rm::isInitialSyncComplete),
            "a genuinely different replacement leader must sync normally - never blocked by a "
                + "carried sequence earned against a different primary");
        assertEquals(0, rm.getRefusedResyncCount(),
            "a different replacement leader must never trip the destructive-resync guard, "
                + "regardless of its own counter being lower than the predecessor's");
        assertFalse(rm.isRefusingDestructiveResync());
        assertTrue(poll(10_000, () -> localCount() == 0),
            "local must legitimately converge to the new (empty) leader's real state - this guard "
                + "is not the barrier against a wrongly-elected leader, the election layer is");

        log.info("I-2 different-leader-lower-counter case converged: predecessorSeq={}", predecessorSeq);
    }

    /**
     * I-2's mirror: the SAME leader address restarting empty/stale, reached via the
     * RM-REPLACEMENT path (not the intra-RM {@code triggerResync()} path already covered by
     * {@link #refusesWhenReconnectedPrimaryIsBehind()}) - this is the true kill chain
     * {@code EmptyNodeRestartWipeTest} guards end-to-end, and must still refuse after I-2.
     */
    @Test
    public void carryOverRefusesOnlyWhenReplacementLeaderIsTheSameAddressRegressed() throws Exception {
        int portA = nextPort();
        startStandalonePrimary(portA);

        local = new InMemoryDriver();
        local.connect();
        ReplicationManager rm1 = new ReplicationManager(local, "localhost", portA);
        extraReplicationManagers.add(rm1);
        rm1.start();
        assertTrue(poll(30_000, rm1::isInitialSyncComplete), "rm1 initial sync must complete");

        Morphium writerA = writerFor(portA, DB);
        try {
            writeDocs(writerA, DOCS, "pre");
            assertTrue(poll(30_000, () -> localCount() == DOCS),
                "rm1 must live-replicate the batch (got " + localCount() + ")");
        } finally {
            writerA.close();
        }
        assertTrue(poll(5_000, () -> rm1.getLastAppliedSequence() > 0),
            "rm1 lastAppliedSequence must have advanced past 0");
        long predecessorSeq = rm1.getLastAppliedSequence();
        String predecessorAddress = rm1.getLeaderAddress();

        rm1.stop();
        nodes.get(0).shutdown(); // kill the SAME node's process (destroying its in-memory state)
        nodes.remove(0);
        // ... and put a brand-new, empty PoppyDB back on the EXACT SAME port - "the same node
        // restarted empty", reached this time via a fresh ReplicationManager (RM replacement),
        // not via the original RM's own reconnect/triggerResync loop.
        startStandalonePrimary(portA);

        rm = new ReplicationManager(local, "localhost", portA);
        assertEquals(predecessorAddress, rm.getLeaderAddress(),
            "test setup: the replacement RM must target the exact same address as the predecessor");
        rm.carryOverLastAppliedSequence(predecessorSeq, predecessorAddress);
        rm.start();

        assertTrue(poll(30_000, () -> rm.getRefusedResyncCount() >= 1),
            "a same-address regressed replacement leader must still be refused (refusedResyncCount="
                + rm.getRefusedResyncCount() + ")");
        assertFalse(rm.isInitialSyncComplete(), "a refused replacement must not report a completed sync");
        assertEquals(DOCS, localCount(),
            "local data carried over from the predecessor RM must survive a refused replacement resync");

        log.info("I-2 same-address-regressed case converged: predecessorSeq={}, refusedResyncCount={}",
            predecessorSeq, rm.getRefusedResyncCount());
    }

    // ---- issue 1 (2nd review pass): carry-over must survive a failed replication start --------
    //
    // PoppyDB#startReplicationToLeader(String, long) is private and tightly coupled to the
    // election/leader-discovery machinery (primary/leaderId guards, the retry-scheduler chain),
    // and reproducing a genuine SYNCHRONOUS throw from newReplicationManager.start() realistically
    // needs an auth/TLS connect mismatch (a plain unreachable port is documented elsewhere in this
    // class as SWALLOWED by PooledDriver.connect(), not thrown - see
    // scheduleReplicationLivenessProbe's javadoc). Driving that end-to-end through a real 3-node
    // election, on a schedule precise enough to fail exactly the FIRST attempt and succeed the
    // retry, would be disproportionate machinery for covering one fallback decision. Per the
    // review's own escape hatch, these two tests instead exercise PoppyDB#carryOverSequenceFor -
    // the pure decision function startReplicationToLeader delegates to - directly and in
    // isolation: no network, no election, no started server at all.

    @Test
    public void carryOverSequenceFallsBackToPersistedWatermarkWhenNoPredecessor() {
        PoppyDB node = new PoppyDB();
        nodes.add(node); // shutdown() on a never-started instance is a safe no-op

        // The exact bug scenario: a PREVIOUS attempt persisted a real predecessor's position into
        // the durable watermark, then (in production) newReplicationManager.start() threw, so
        // replicationManager is null going into the retry - predecessor == null here mirrors that.
        node.setLastKnownAppliedSequenceForTest(777);

        assertEquals(777, node.carryOverSequenceFor(null),
            "a failed-start retry (predecessor == null) must fall back to the persisted "
                + "watermark, not silently reset to 0");
    }

    @Test
    public void carryOverSequenceReadsLivePredecessorWhenPresent() throws Exception {
        PoppyDB node = new PoppyDB();
        nodes.add(node);

        // A stale watermark from an even earlier attempt must NOT shadow a real, live
        // predecessor - the live value always wins when one is available.
        node.setLastKnownAppliedSequenceForTest(1);

        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            ReplicationManager predecessor = new ReplicationManager(drv, "localhost", 1);
            // Seeds lastAppliedSequence without ever calling start()/connecting anywhere - the
            // decision function only reads getLastAppliedSequence(), so no live connection is
            // needed to exercise it.
            predecessor.carryOverLastAppliedSequence(500);

            assertEquals(500, node.carryOverSequenceFor(predecessor),
                "a live predecessor's own position must be used, not the stale watermark");
        } finally {
            drv.close();
        }
    }

    // ---- I-2 (production-CI fix): PoppyDB#carryOverSourceFor, the companion to
    // carryOverSequenceFor - same pure/isolated/no-network shape as the two tests above.

    @Test
    public void carryOverSourceFallsBackToPersistedWatermarkWhenNoPredecessor() {
        PoppyDB node = new PoppyDB();
        nodes.add(node);

        node.setLastKnownAppliedSequenceSourceForTest("localhost:9999");

        assertEquals("localhost:9999", node.carryOverSourceFor(null),
            "a failed-start retry (predecessor == null) must fall back to the persisted source "
                + "watermark, exactly mirroring carryOverSequenceFor's own fallback");
    }

    @Test
    public void carryOverSourceReadsLivePredecessorWhenPresent() throws Exception {
        PoppyDB node = new PoppyDB();
        nodes.add(node);

        node.setLastKnownAppliedSequenceSourceForTest("localhost:1111"); // stale, must not shadow

        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            ReplicationManager predecessor = new ReplicationManager(drv, "localhost", 2222);

            assertEquals("localhost:2222", node.carryOverSourceFor(predecessor),
                "a live predecessor's own leader address must be used, not the stale watermark");
        } finally {
            drv.close();
        }
    }

    // ---- I-1 (final review): adopt the synced primary's own base, don't max() with a stale one -

    /**
     * Sequences are PRIMARY-LOCAL (see {@code tryConsistencyShortcut}'s own javadoc). Before this
     * fix, a successful sync/shortcut reseeded {@code lastAppliedSequence} via
     * {@code Math.max(current, lastKnownPrimarySequence)} - so a follower that had accumulated a
     * HIGH sequence N against an old primary kept N even after successfully converging against a
     * brand-new primary whose own counter is comfortably below N (only reachable via the
     * consistency SHORTCUT: the D2 guard would otherwise refuse a full sync against a primary
     * whose counter is behind local - so "successful sync with a lower primary counter" and
     * "guard-gated full sync" are mutually exclusive by construction; the shortcut is the only
     * path that bypasses the guard entirely). Every later reconnect then sent
     * {@code resumeAfter=N}, which the new (low-counter) primary could never satisfy -> "resume
     * window lost" -> a dbHash mismatch as soon as one real write happened (breaking the
     * shortcut) -> the D2 guard comparing the new primary's still-low counter against the STALE
     * inherited N -> refusing an entirely legitimate resync, unbounded on a quiet cluster.
     *
     * <p>Reproduced here by CHURNING the original primary (delete + re-insert the same content)
     * so its own counter inflates well past what recreating that exact content needs, then
     * replacing it with a brand-new primary fed the identical content in a single write (same
     * deterministic {@code _id}s via {@link #insertFixedDocs} - the dbHash comparison, unlike
     * {@link #writeDocs}'s random {@code MorphiumId}s, needs byte-for-byte identical documents to
     * match). Verified red against the reverted {@code Math.max(...)} before landing the
     * {@code set(...)} fix (manual step, not committed - the assertion on
     * {@code getLastAppliedSequence() < n} failed, and the final legitimate-resync poll timed out
     * with the follower stuck refusing).
     */
    @Test
    public void adoptsNewPrimaryBaseAfterSuccessfulShortcutSoLaterResyncsAreNotRefused() throws Exception {
        int port1 = nextPort();
        PoppyDB primaryA = startStandalonePrimary(port1);

        local = new InMemoryDriver();
        local.connect();
        rm = new ReplicationManager(local, "localhost", port1);
        rm.start();
        assertTrue(poll(30_000, rm::isInitialSyncComplete), "initial (trivially empty) sync must complete");

        insertFixedDocs(primaryA, DOCS, "chk");
        assertTrue(poll(30_000, () -> localCount() == DOCS),
            "follower must live-replicate the batch (got " + localCount() + ")");

        // Churn: delete and re-insert the SAME content so primaryA's own counter advances well
        // past what a single write needs, while the final DATA (all dbHash compares) is
        // unchanged.
        GenericCommand delAll = new GenericCommand(primaryA.getDriver());
        delAll.setDb(DB);
        delAll.setColl(COLL);
        delAll.setCmdData(Doc.of("delete", COLL, "$db", DB,
            "deletes", List.of(Doc.of("q", Doc.of(), "limit", 0))));
        primaryA.getDriver().runCommand(delAll);
        assertTrue(poll(10_000, () -> primaryA.getDriver().count(DB, COLL, Doc.of(), null, null) == 0),
            "churn delete must land on the primary");
        insertFixedDocs(primaryA, DOCS, "chk");
        // The primary's writes are done synchronously above, so this is the sequence the
        // follower has to reach for the churn to be fully applied.
        long primarySeqAfterChurn = primaryA.getDriver().getChangeStreamSequence();

        // Waiting on localCount() alone does NOT establish that: it is already true while the
        // churn is still unapplied, because the follower then simply still holds the FIRST
        // batch - same count, wrong state. Capturing n there yields the un-inflated 20 (the
        // later adoption assertion then compares 20 < 20), or freezes the follower mid-churn
        // so a deleted document is lost for good and the final convergence can never reach
        // 2*DOCS. Both failure modes were seen on the loaded CI runner.
        assertTrue(poll(30_000, () -> rm.getLastAppliedSequence() >= primarySeqAfterChurn
                && localCount() == DOCS),
            "follower must fully apply the churn before N is captured (primary seq="
                + primarySeqAfterChurn + ", applied=" + rm.getLastAppliedSequence()
                + ", count=" + localCount() + ")");
        long n = rm.getLastAppliedSequence(); // the OLD primary's high, churn-inflated counter

        primaryA.getDriver().setChangeStreamHistoryLimit(2);
        rm.pauseReplicationForTest();
        Thread.sleep(500);
        nodes.get(0).shutdown();
        nodes.remove(0);

        // A brand-new primary whose own counter starts near 0, fed the EXACT SAME final content
        // in one write (no churn), copied verbatim from the follower's current data (see
        // copyLocalDataInto's javadoc for why a verbatim copy, not an independently-reconstructed
        // insert, is what reliably dbHash-matches) - comfortably below N either way.
        PoppyDB primaryB = startStandalonePrimary(port1);
        copyLocalDataInto(primaryB);

        // isInitialSyncComplete() is ALREADY true at this point (stale from the trivial bootstrap
        // sync at the very top of this test, never reset) - polling it directly would pass
        // instantly without waiting for a real cycle against primaryB at all. Wait for the
        // MONOTONIC shortcut-attempt counter to advance instead - the only reliable "a genuinely
        // NEW sync decision cycle has run" signal (a boolean flip false->true here is real but
        // racy: the whole reconnect+resume-window-lost+shortcut cycle can complete faster than a
        // 100ms poll interval, so a poll might only ever observe the post-cycle `true`, identical
        // to the pre-cycle stale `true`).
        int shortcutAttemptsBeforeResume = rm.getConsistencyShortcutAttemptsForTest();
        rm.resumeReplicationForTest();

        assertTrue(poll(30_000, () -> rm.getConsistencyShortcutAttemptsForTest() > shortcutAttemptsBeforeResume
                && rm.isInitialSyncComplete()),
            "a genuinely new sync cycle must run and complete against primaryB (shortcut attempts "
                + "before=" + shortcutAttemptsBeforeResume + ", now=" + rm.getConsistencyShortcutAttemptsForTest()
                + ", isInitialSyncComplete=" + rm.isInitialSyncComplete() + ")");
        assertTrue(rm.wasLastSyncShortcut(),
            "test setup: this sync must take the consistency shortcut (identical data, D2 guard "
                + "bypassed) to reproduce a successful sync while the new primary's own counter "
                + "is far below N=" + n);
        assertEquals(0, rm.getRefusedResyncCount(), "the initial shortcut sync itself must never be refused");

        // The core I-1 assertion: lastAppliedSequence must have been ADOPTED from the new
        // primary's own (low) base, not left at the old primary's inflated N via Math.max.
        assertTrue(rm.getLastAppliedSequence() < n,
            "lastAppliedSequence must adopt the new primary's own (lower) base after a successful "
                + "sync, not stay pinned at the old primary's unrelated, inflated sequence space "
                + "(N=" + n + ", got " + rm.getLastAppliedSequence() + ")");

        // The actual regression: force a SECOND, entirely legitimate resync against the SAME
        // still-alive (never regressed) primary B - a real gap it cannot buffer from. Before the
        // fix this refused forever, because lastAppliedSequence (still pinned at N) could never
        // be <= primary B's real, much lower counter.
        primaryB.getDriver().setChangeStreamHistoryLimit(2);
        rm.pauseReplicationForTest();
        Thread.sleep(500);
        insertFixedDocs(primaryB, DOCS, "gap2");
        Thread.sleep(300);
        rm.resumeReplicationForTest();

        assertTrue(poll(30_000, () -> rm.isInitialSyncComplete() && localCount() == 2 * DOCS),
            "a legitimate resync against the SAME (never-regressed) primary must proceed, not be "
                + "refused forever due to a stale, unrelated old-primary sequence (got count="
                + localCount() + ", refusedResyncCount=" + rm.getRefusedResyncCount() + ")");
        assertEquals(0, rm.getRefusedResyncCount(),
            "a legitimate resync must never trip the D2 guard once the base has been correctly adopted");

        log.info("I-1 regression converged: N={}, final lastAppliedSequence={}",
            n, rm.getLastAppliedSequence());
    }
}
