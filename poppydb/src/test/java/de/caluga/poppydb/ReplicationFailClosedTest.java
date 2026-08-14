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
}
