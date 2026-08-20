package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the crash-safety contract of the dump write path (#317): a dump is written to a sibling
 * temp file and only moved over the final {@code <db>.morphium.gz} once it is complete, so a
 * process/machine death mid-write can never leave a truncated dump behind with the last good one
 * already gone.
 *
 * <p>The failure case is simulated deterministically rather than by killing a JVM: the temp path
 * the writer must use is occupied by a directory, which makes the write fail exactly where a
 * crash would - after the previous dump would have been truncated by the old
 * {@code new FileOutputStream(finalFile)} implementation.
 */
@Tag("inmemory")
public class InMemDumpAtomicWriteTest {

    private InMemoryDriver driverWith(String db, String coll, int docs) throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        for (int i = 0; i < docs; i++) {
            drv.store(db, coll, List.of(Doc.of("_id", i, "v", "value-" + i)), null);
        }

        return drv;
    }

    @Test
    public void dumpToFileLeavesNoTempFileBehind(@TempDir Path tempDir) throws Exception {
        InMemoryDriver drv = driverWith("atomicdump", "docs", 5);

        try {
            File dir = tempDir.toFile();
            File target = new File(dir, "atomicdump.morphium.gz");
            drv.dumpToFile("atomicdump", target);

            assertTrue(target.exists(), "dump file must exist: " + target);
            assertFalse(new File(dir, "atomicdump.morphium.gz.tmp").exists(),
                    "the temp file must be gone after a successful dump");
            assertEquals(1, dir.listFiles().length, "only the dump file may remain: "
                    + List.of(dir.list()));
        } finally {
            drv.close();
        }
    }

    @Test
    public void dumpToFileWritesThroughATempFileNotTheFinalName(@TempDir Path tempDir) throws Exception {
        InMemoryDriver drv = driverWith("atomicdump", "docs", 3);

        try {
            File dir = tempDir.toFile();
            File target = new File(dir, "atomicdump.morphium.gz");
            // A pre-existing dump from an earlier run - it must survive a failed write untouched.
            drv.dumpToFile("atomicdump", target);
            byte[] previous = Files.readAllBytes(target.toPath());

            // Occupy the temp path with a (non-empty, hence non-removable) directory: the write
            // now fails where a crash would, i.e. before anything reached the final file.
            Path tmp = new File(dir, "atomicdump.morphium.gz.tmp").toPath();
            Files.createDirectory(tmp);
            Files.writeString(tmp.resolve("blocker"), "x");

            assertThrows(IOException.class, () -> drv.dumpToFile("atomicdump", target),
                    "a failed dump write must be reported, not swallowed");

            assertTrue(target.exists(), "the previous dump must still exist");
            assertArrayEquals(previous, Files.readAllBytes(target.toPath()),
                    "the previous dump must be byte-identical - not truncated by the failed write");

            // and it must still be readable/restorable, not just the same size
            Files.walk(tmp).sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            InMemoryDriver restored = new InMemoryDriver();
            restored.connect();

            try {
                restored.restoreFromFile(target);
                List<Map<String, Object>> docs = restored.find("atomicdump", "docs", Doc.of(), null, null, 0, 0);
                assertEquals(3, docs.size(), "the surviving dump must still restore completely");
            } finally {
                restored.close();
            }
        } finally {
            drv.close();
        }
    }

    /**
     * The shutdown path interrupts a dump that outstays its wait, rather than letting it write
     * into a driver that is about to be reset. That only helps if an interrupted write actually
     * aborts cleanly: {@code Files.newOutputStream} is channel-backed, so a write on an
     * interrupted thread fails with {@code ClosedByInterruptException} - which must land in the
     * same cleanup path as any other failure, leaving the previous dump untouched and no temp
     * file behind.
     */
    @Test
    public void anInterruptedWriteLeavesThePreviousDumpIntact(@TempDir Path tempDir) throws Exception {
        // enough data that the gzip stream actually reaches the channel during the write
        InMemoryDriver drv = driverWith("atomicdump", "docs", 400);

        try {
            File dir = tempDir.toFile();
            File target = new File(dir, "atomicdump.morphium.gz");
            drv.dumpToFile("atomicdump", target);
            byte[] previous = Files.readAllBytes(target.toPath());

            Thread.currentThread().interrupt();   // what shutdown does to a straggling dump

            boolean aborted = false;
            try {
                drv.dumpToFile("atomicdump", target);
            } catch (IOException expected) {
                aborted = true;
            } finally {
                Thread.interrupted();   // clear the flag for the rest of the suite
            }
            assertTrue(aborted, "the interrupt must abort the write - that is what shutdown "
                    + "relies on when it interrupts a dump that outstayed its wait");

            assertTrue(target.exists(), "the previous dump must still be there");
            assertFalse(new File(dir, "atomicdump.morphium.gz.tmp").exists(),
                    "an aborted write must not leave its temp file behind");

            // whatever happened, the file on disk is a COMPLETE dump - either the old one or a
            // fully written new one, never a torso
            InMemoryDriver restored = new InMemoryDriver();
            restored.connect();

            try {
                restored.restoreFromFile(target);
                assertEquals(400, restored.find("atomicdump", "docs", Doc.of(), null, null, 0, 0).size(),
                        "the dump on disk must be complete and restorable");
            } finally {
                restored.close();
            }

            assertArrayEquals(previous, Files.readAllBytes(target.toPath()),
                    "the aborted write must not have touched the previous dump at all");
        } finally {
            drv.close();
        }
    }

    /** A hard crash mid-dump leaves a .tmp behind. It must never be restored, and startup -
     * i.e. the restore pass - is where it gets removed. */
    @Test
    public void staleTempFilesAreIgnoredAndRemovedOnRestore(@TempDir Path tempDir) throws Exception {
        InMemoryDriver drv = driverWith("atomicdump", "docs", 4);

        try {
            File dir = tempDir.toFile();
            drv.dumpAllToDirectory(dir);
            // what a crash mid-write leaves behind - and a leftover for a database that is gone
            Files.writeString(new File(dir, "atomicdump.morphium.gz.tmp").toPath(), "half written");
            Files.writeString(new File(dir, "vanished.morphium.gz.tmp").toPath(), "half written");

            InMemoryDriver restored = new InMemoryDriver();
            restored.connect();

            try {
                InMemoryDriver.DirectoryRestoreResult res = restored.restoreAllFromDirectoryResult(dir);

                assertTrue(res.isComplete(), "the truncated temp files must not count as dumps: "
                        + res.getFailedFiles());
                assertEquals(4, restored.find("atomicdump", "docs", Doc.of(), null, null, 0, 0).size(),
                        "the good dump must be restored");
                assertFalse(new File(dir, "atomicdump.morphium.gz.tmp").exists(),
                        "the stale temp file must be removed on restore");
                assertFalse(new File(dir, "vanished.morphium.gz.tmp").exists(),
                        "including one whose database no longer exists");
                assertTrue(new File(dir, "atomicdump.morphium.gz").exists(),
                        "the real dump must be untouched by the cleanup");
            } finally {
                restored.close();
            }
        } finally {
            drv.close();
        }
    }

    @Test
    public void dumpAllToDirectorySurvivesAFailedWriteOfOneDatabase(@TempDir Path tempDir) throws Exception {
        InMemoryDriver drv = driverWith("dbone", "docs", 2);
        drv.store("dbtwo", "docs", List.of(Doc.of("_id", 1, "v", "two")), null);

        try {
            File dir = tempDir.toFile();
            // the driver also carries its internal databases (admin/local/...), so only the
            // two under test are pinned here
            assertTrue(drv.dumpAllToDirectory(dir) >= 2, "both databases must be dumped");
            byte[] one = Files.readAllBytes(new File(dir, "dbone.morphium.gz").toPath());
            byte[] two = Files.readAllBytes(new File(dir, "dbtwo.morphium.gz").toPath());

            // add data, then block one database's temp path so its rewrite fails
            drv.store("dbone", "docs", List.of(Doc.of("_id", 99, "v", "new")), null);
            drv.store("dbtwo", "docs", List.of(Doc.of("_id", 99, "v", "new")), null);
            Path blocked = new File(dir, "dbone.morphium.gz.tmp").toPath();
            Files.createDirectory(blocked);
            Files.writeString(blocked.resolve("blocker"), "x");

            assertThrows(IOException.class, () -> drv.dumpAllToDirectory(dir));

            assertArrayEquals(one, Files.readAllBytes(new File(dir, "dbone.morphium.gz").toPath()),
                    "the database whose write failed must keep its previous dump unchanged");
            // dbtwo is dumped after dbone in listDatabases() order or before it - either way its
            // file must be either the old or a complete new dump, never a truncated one
            byte[] twoNow = Files.readAllBytes(new File(dir, "dbtwo.morphium.gz").toPath());
            InMemoryDriver restored = new InMemoryDriver();
            restored.connect();

            try {
                restored.restoreFromFile(new File(dir, "dbtwo.morphium.gz"));
                int expected = java.util.Arrays.equals(two, twoNow) ? 1 : 2;
                assertEquals(expected, restored.find("dbtwo", "docs", Doc.of(), null, null, 0, 0).size(),
                        "every dump file must be complete, whether it is the old or the new one");
            } finally {
                restored.close();
            }
        } finally {
            drv.close();
        }
    }
}
