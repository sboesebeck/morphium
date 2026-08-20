package de.caluga.poppydb;

import de.caluga.morphium.driver.Doc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * #317: exactly one dump at a time, whoever triggers it. The on-demand {@code dumpNow} trigger,
 * the periodic scheduler and the final dump on shutdown share one guard - two of them writing
 * the same {@code <db>.morphium.gz.tmp} files concurrently would rename an interleaved result
 * into place. The trigger itself must not wait for the dump it starts.
 *
 * <p>The dump write is blocked from inside via the {@code writeDumpFiles()} seam, so "a dump is
 * currently running" is a state the test controls instead of a race it has to hit.
 */
@Tag("server")
public class DumpGuardTest {

    /** PoppyDB whose dump write can be held open, and which counts how often it was entered. */
    private static class BlockingDumpPoppyDB extends PoppyDB {
        private final AtomicInteger dumpCalls = new AtomicInteger();
        private final AtomicInteger dumpsCompleted = new AtomicInteger();
        private volatile CountDownLatch entered = new CountDownLatch(1);
        private volatile CountDownLatch release = new CountDownLatch(0);

        BlockingDumpPoppyDB(int port) {
            super(port, "127.0.0.1", 100, 10);
        }

        /** When set, the blocked dump has already created a temp file - the state a real dump is
         * in while it is writing, and the one an interrupt has to survive without damage. */
        private volatile File tempFileWhileBlocked = null;

        @Override
        int writeDumpFiles() throws IOException {
            dumpCalls.incrementAndGet();

            File tmp = tempFileWhileBlocked;

            if (tmp != null) {
                // half-written dump on disk, exactly like a real dump that got this far
                java.nio.file.Files.writeString(tmp.toPath(), "half written dump");
            }

            entered.countDown();

            try {
                if (!release.await(30, TimeUnit.SECONDS)) {
                    throw new IOException("test dump was never released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted", e);
            }

            try {
                return super.writeDumpFiles();
            } finally {
                dumpsCompleted.incrementAndGet();
            }
        }

        /** Waits until no dump is writing any more - a dump thread still running into the
         * @TempDir teardown would race the directory deletion. */
        void awaitNoDumpWriting() throws InterruptedException {
            for (int i = 0; i < 200 && dumpsCompleted.get() < dumpCalls.get(); i++) {
                Thread.sleep(50);
            }

            assertEquals(dumpCalls.get(), dumpsCompleted.get(),
                    "a dump was still writing - it would race the temp directory teardown");
        }

        /** Makes the next dump block until {@link #releaseDump()}. */
        void blockNextDump() {
            entered = new CountDownLatch(1);
            release = new CountDownLatch(1);
        }

        void releaseDump() {
            release.countDown();
        }

        boolean awaitDumpEntered() throws InterruptedException {
            return entered.await(10, TimeUnit.SECONDS);
        }
    }

    private BlockingDumpPoppyDB server;

    @AfterEach
    public void tearDown() {
        if (server != null) {
            // never leave a dump blocked - shutdown waits for the guard
            server.releaseDump();

            try {
                server.awaitNoDumpWriting();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            try {
                server.shutdown();
            } catch (Exception ignored) {
            }

            server = null;
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private BlockingDumpPoppyDB serverWithDumpDir(File dumpDir) throws Exception {
        BlockingDumpPoppyDB srv = new BlockingDumpPoppyDB(freePort());
        srv.setDumpDirectory(dumpDir);
        srv.getDriver().store("guarded", "docs", List.of(Doc.of("_id", 1, "v", "x")), null);
        return srv;
    }

    @Test
    public void triggerReturnsBeforeTheDumpFinishedAndThenWritesIt(@TempDir Path dumpDir) throws Exception {
        server = serverWithDumpDir(dumpDir.toFile());
        server.blockNextDump();

        assertTrue(server.triggerDumpNow(), "the first trigger must start a dump");
        assertTrue(server.awaitDumpEntered(), "the dump must have started on its own thread");
        // the trigger returned while the dump is still inside writeDumpFiles() - that is the point
        assertFalse(new File(dumpDir.toFile(), "guarded.morphium.gz").exists(),
                "nothing may be written yet while the dump is held");

        server.releaseDump();

        File dumpFile = new File(dumpDir.toFile(), "guarded.morphium.gz");

        for (int i = 0; i < 100 && !dumpFile.exists(); i++) {
            Thread.sleep(50);
        }

        assertTrue(dumpFile.exists(), "the started dump must eventually write its file: " + dumpFile);

        for (int i = 0; i < 200 && ((Number) server.getDumpStatus().get("lastDumpMs")).longValue() == 0; i++) {
            Thread.sleep(50);
        }

        assertTrue(((Number) server.getDumpStatus().get("lastDumpMs")).longValue() > 0,
                "the completed dump must be visible in dumpStatus: " + server.getDumpStatus());
    }

    @Test
    public void aSecondTriggerWhileADumpRunsStartsNothing(@TempDir Path dumpDir) throws Exception {
        server = serverWithDumpDir(dumpDir.toFile());
        server.blockNextDump();

        assertTrue(server.triggerDumpNow());
        assertTrue(server.awaitDumpEntered());

        assertFalse(server.triggerDumpNow(), "a second trigger must answer alreadyRunning");
        assertFalse(server.triggerDumpNow(), "and stay that way for repeated triggers");
        assertEquals(-1, server.dumpNow(), "the synchronous variant must skip as well, not block");
        assertEquals(1, server.dumpCalls.get(), "only the first trigger may have started a dump");

        server.releaseDump();
        server.awaitNoDumpWriting();
    }

    @Test
    public void afterCompletionAnotherDumpCanBeTriggered(@TempDir Path dumpDir) throws Exception {
        server = serverWithDumpDir(dumpDir.toFile());
        server.blockNextDump();
        assertTrue(server.triggerDumpNow());
        assertTrue(server.awaitDumpEntered());
        server.releaseDump();

        File dumpFile = new File(dumpDir.toFile(), "guarded.morphium.gz");

        for (int i = 0; i < 100 && !dumpFile.exists(); i++) {
            Thread.sleep(50);
        }

        assertTrue(dumpFile.exists(), "first dump must have completed");
        // guard released -> the next trigger starts a dump again
        boolean started = false;

        for (int i = 0; i < 100 && !started; i++) {
            started = server.triggerDumpNow();

            if (!started) {
                Thread.sleep(50);   // the first dump's thread may not have released the guard yet
            }
        }

        assertTrue(started, "after completion a new dump must be startable");

        for (int i = 0; i < 100 && server.dumpCalls.get() < 2; i++) {
            Thread.sleep(50);   // the started dump runs on its own thread
        }

        assertEquals(2, server.dumpCalls.get(), "the second trigger must have run a second dump");
    }

    @Test
    public void scheduledDumpIsSkippedWhileAManualDumpRuns(@TempDir Path dumpDir) throws Exception {
        server = serverWithDumpDir(dumpDir.toFile());
        server.setDumpIntervalMs(100);
        // armed BEFORE the scheduler exists - a tick landing between start() and blockNextDump()
        // would otherwise sail through the un-armed latch and make this test flaky
        server.blockNextDump();
        server.start();

        assertTrue(server.triggerDumpNow(), "manual trigger must win the guard");
        assertTrue(server.awaitDumpEntered());

        // several scheduler ticks are due in this window - every one of them must skip
        Thread.sleep(700);
        assertEquals(1, server.dumpCalls.get(),
                "the periodic dump must be skipped, not queued, while a manual dump runs");

        server.releaseDump();

        // and the scheduler picks up again once the guard is free
        for (int i = 0; i < 100 && server.dumpCalls.get() < 2; i++) {
            Thread.sleep(50);
        }

        assertTrue(server.dumpCalls.get() >= 2, "the scheduler must resume after the manual dump");
        server.awaitNoDumpWriting();
    }

    /**
     * The gap the guard alone does not close: if shutdown gives up waiting, it goes on to reset
     * the driver - a dump still snapshotting would then rename EMPTY databases over the last
     * good dump files. Shutdown must therefore interrupt the straggler, and no dump may start
     * once shutdown has begun.
     *
     * <p>The blocked dump holds a half-written temp file while the interrupt lands, so this also
     * pins that an interrupted dump never replaces the good file with what it had written so
     * far. That an interrupt aborts the REAL write cleanly (channel-backed streams, temp
     * cleanup) is pinned one layer down, in
     * {@code InMemDumpAtomicWriteTest.anInterruptedWriteLeavesThePreviousDumpIntact} - this test
     * cannot show it, because its dump is blocked in the seam rather than inside the driver.
     */
    @Test
    public void shutdownInterruptsADumpThatOutstaysTheWaitAndStartsNoNewOne(@TempDir Path dumpDir) throws Exception {
        server = serverWithDumpDir(dumpDir.toFile());
        server.finalDumpWaitMs = 300;   // no need to sit through the production 10s here
        server.start();
        // a first, complete dump - this is the "last good dump" that must survive
        assertTrue(server.dumpNow() > 0, "the initial dump must write something");
        File dumpFile = new File(dumpDir.toFile(), "guarded.morphium.gz");
        assertTrue(dumpFile.exists(), "precondition: a good dump exists");
        byte[] good = java.nio.file.Files.readAllBytes(dumpFile.toPath());

        server.tempFileWhileBlocked = new File(dumpDir.toFile(), "guarded.morphium.gz.tmp");
        server.blockNextDump();
        assertTrue(server.triggerDumpNow(), "a dump must be running when shutdown starts");
        assertTrue(server.awaitDumpEntered());
        assertTrue(server.tempFileWhileBlocked.exists(),
                "precondition: the interrupted dump has a half-written temp file on disk");

        assertFalse(server.triggerDumpNow(), "guard: no second dump while one runs");
        Thread shutdown = new Thread(server::shutdown, "test-shutdown");
        shutdown.start();
        shutdown.join(30_000);
        assertFalse(shutdown.isAlive(), "shutdown must not hang on the running dump");

        assertFalse(server.triggerDumpNow(), "no dump may be started once shutdown has begun");
        assertEquals(-1, server.dumpNow(),
                "the synchronous entry point must refuse during shutdown too - the driver is "
                + "about to be reset");
        // the interrupted dump must not have replaced the good dump - neither with a post-reset
        // (empty) one nor with the half-written temp file it was holding
        assertArrayEquals(good, java.nio.file.Files.readAllBytes(dumpFile.toPath()),
                "the last good dump must survive the interrupted dump untouched");
        server = null;   // already shut down
    }

    @Test
    public void withoutADumpDirectoryNothingIsTriggered() throws Exception {
        server = new BlockingDumpPoppyDB(freePort());

        assertFalse(server.triggerDumpNow(), "no dump directory - nothing to trigger");
        assertEquals(0, server.dumpCalls.get());
    }
}
