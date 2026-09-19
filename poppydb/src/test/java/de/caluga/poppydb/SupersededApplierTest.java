package de.caluga.poppydb;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * #323, part 3: a superseded {@link ReplicationManager} must be refused at the point of writing.
 *
 * <p>Parts 1 and 2 (cooperative cancellation, and closing the in-flight sync connection on
 * {@code stop()}) narrow the window in which an abandoned sync thread can still write, and the
 * straggler test covers them. They cannot close it: every check is a check, and the thread can be
 * descheduled between passing one and reaching the write it guards. A collection read that finally
 * returns after {@code stop()} came and went needs no network for its local insert, so nothing
 * external stops it either.
 *
 * <p>This is the check that sits at the write, where there is no window left to lose. The node
 * already knows which manager is current - {@code PoppyDB.replicationManager} - so the manager
 * simply asks before every local apply, and a manager that has been replaced fails instead of
 * quietly writing documents from the old primary into data that now belongs to its successor.
 */
@Tag("server")
public class SupersededApplierTest {

    private InMemoryDriver drv;

    @AfterEach
    public void tearDown() {
        if (drv != null) {
            drv.close();
        }
    }

    private ReplicationManager managerFor(InMemoryDriver driver) {
        return new ReplicationManager(driver, "127.0.0.1", 1);
    }

    /**
     * Drives the apply choke point the way the initial-sync insert does, without needing a primary
     * to read from: the straggler's local write is a plain insert into the local driver.
     */
    private void applyInsert(ReplicationManager rm, String db, String coll, String id) throws Exception {
        java.lang.reflect.Method m = ReplicationManager.class.getDeclaredMethod(
                "runLocalApplyCommand", de.caluga.morphium.driver.commands.GenericCommand.class, String.class);
        m.setAccessible(true);
        var cmd = new de.caluga.morphium.driver.commands.GenericCommand(drv);
        cmd.setDb(db);
        cmd.setColl(coll);
        cmd.setCmdData(Doc.of("insert", coll, "$db", db, "documents", List.of(Doc.of("_id", id))));

        try {
            m.invoke(rm, cmd, "test insert of " + id);
        } catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }

            throw e;
        }
    }

    @Test
    public void aSupersededManagerCannotWriteIntoItsSuccessorsData() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();

        ReplicationManager superseded = managerFor(drv);
        ReplicationManager current = managerFor(drv);

        // The node's view: `current` is the one in charge now.
        superseded.setStillCurrentApplier(() -> false);
        current.setStillCurrentApplier(() -> true);

        // The straggler resurfaces with its read result in hand and tries to write it.
        MorphiumDriverException ex = assertThrows(MorphiumDriverException.class,
                () -> applyInsert(superseded, "payload", "c", "from-the-old-primary"));
        assertTrue(ex.getMessage().contains("superseded"),
                "the refusal must say why, so it is not mistaken for a transport failure: "
                + ex.getMessage());

        // The successor's own apply is untouched.
        applyInsert(current, "payload", "c", "from-the-new-primary");

        List<Map<String, Object>> docs = drv.find("payload", "c", Doc.of(), null, null, 0, 0);
        assertEquals(1, docs.size(), "only the current manager's document may be there");
        assertEquals("from-the-new-primary", docs.get(0).get("_id"),
                "a stale foreign document surviving here is the silent divergence #323 is about");
    }

    @Test
    public void aManagerWithNoNodeAroundItAppliesAsBefore() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();

        // Most tests build a ReplicationManager without a PoppyDB around it; the default must not
        // turn those into refusals.
        ReplicationManager standalone = managerFor(drv);
        applyInsert(standalone, "payload", "c", "d1");

        assertEquals(1, drv.find("payload", "c", Doc.of(), null, null, 0, 0).size(),
                "negative control: the guard defaults to allowing the apply");
    }

    /** Invokes a private method and unwraps what it actually threw, so a NPE cannot pass for a refusal. */
    private Throwable invokeAndCatch(ReplicationManager rm, String method, Class<?>[] sig, Object[] args) {
        try {
            java.lang.reflect.Method m = ReplicationManager.class.getDeclaredMethod(method, sig);
            m.setAccessible(true);
            m.invoke(rm, args);
            return null;
        } catch (java.lang.reflect.InvocationTargetException e) {
            return e.getCause();
        } catch (Exception e) {
            throw new AssertionError("could not invoke " + method, e);
        }
    }

    @Test
    public void theChangeStreamBulkInsertIsCoveredToo() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();

        ReplicationManager superseded = managerFor(drv);
        superseded.setStillCurrentApplier(() -> false);

        // applyBulkInserts reaches the driver directly, NOT through runLocalApplyCommand - and it
        // is the most frequent write a manager makes, so a guard that misses it guards very little.
        // A review caught exactly that: the first version of this fix claimed a single choke point
        // that was not one.
        List<Map<String, Object>> events = List.of(Doc.of(
                "operationType", "insert",
                "fullDocument", Doc.of("_id", "from-the-old-primary"),
                "sequence", 1L));

        Throwable t = invokeAndCatch(superseded, "applyBulkInserts",
                new Class<?>[] {String.class, List.class},
                new Object[] {"payload.c", events});

        assertTrue(t instanceof MorphiumDriverException,
                "the bulk insert must be refused, got: " + t);
        assertTrue(t.getMessage().contains("superseded"), t.getMessage());
        assertEquals(0, drv.find("payload", "c", Doc.of(), null, null, 0, 0).size(),
                "nothing from the superseded manager may have landed");
    }

    @Test
    public void thePreSyncClearIsRefusedBeforeItTouchesAnything() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
        new InsertMongoCommand(drv).setDb("admin").setColl("system.users")
                .setDocuments(List.of(Doc.of("_id", "root"))).execute();

        // The case the second review named: a node whose listDatabases() yields only
        // admin/local/config makes NO guarded call in clearLocalDatabases()'s loop, so the two
        // admin drops below it were the first thing that touched data. A fresh driver carries an
        // empty `test`, and leaving it there is how the first version of this test fooled itself -
        // the loop threw on `test` and the test called that a pass, while the drops were never
        // reached at all.
        drv.drop("test", null);
        assertTrue(drv.listDatabases().stream().noneMatch(d ->
                        !"admin".equals(d) && !"local".equals(d) && !"config".equals(d)),
                "precondition: no user database may remain: " + drv.listDatabases());

        ReplicationManager superseded = managerFor(drv);
        superseded.setStillCurrentApplier(() -> false);

        Throwable t = invokeAndCatch(superseded, "clearLocalDatabases", new Class<?>[] {}, new Object[] {});

        assertTrue(t instanceof MorphiumDriverException,
                "the clear must be refused, got: " + t);
        assertEquals(1, drv.find("admin", "system.users", Doc.of(), null, null, 0, 0).size(),
                "the successor's user collection must survive - losing it locks the cluster out");

        // The refusal now comes from the check at the top of clearLocalDatabases (fourth review,
        // H1: without it a superseded manager marked the node's store as emptied although it drops
        // nothing). The per-drop checks below it stay as defence in depth, for a manager that is
        // superseded partway through a clear it was entitled to start.
        assertTrue(t.getMessage().contains("pre-sync"),
                "the refusal must name a pre-sync operation: " + t.getMessage());
    }

    /**
     * H2 from the second review: index replication is a local write too, and the first sweep missed
     * it because it goes through {@code localDriver.getPrimaryConnection(null)} rather than a
     * {@code localDriver.<mutator>} call. A superseded manager here would put the old primary's
     * indexes on its successor's data or drop the successor's - and a stray TTL index outlives the
     * manager that created it, expiring documents that are not its to expire.
     */
    @Test
    public void theIndexSyncIsCoveredToo() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
        new InsertMongoCommand(drv).setDb("payload").setColl("c")
                .setDocuments(List.of(Doc.of("_id", "d1"))).execute();

        ReplicationManager superseded = managerFor(drv);
        superseded.setStillCurrentApplier(() -> false);

        Throwable t = invokeAndCatch(superseded, "applyIndexDiff",
                new Class<?>[] {String.class, String.class, List.class},
                new Object[] {"payload", "c", List.of()});

        assertTrue(t instanceof MorphiumDriverException,
                "the index sync must be refused, got: " + t);
        assertTrue(t.getMessage().contains("index sync"), t.getMessage());
    }

    /**
     * H1 from the fourth review, and a bug I introduced while fixing a different one. Moving the
     * cleared-store notice to the start of {@code clearLocalDatabases()} was right for an IO
     * failure mid-loop - part of the store really is gone by then - and wrong for a superseded
     * manager, which throws at the first guarded call having dropped nothing. Since only a
     * completed sync lifts the mark, that locked a node holding perfectly good data out of both
     * dumping and candidacy for the life of the process. My own comment named "a superseded
     * manager" as the reason to move it forward.
     */
    @Test
    public void aSupersededManagerMustNotMarkTheStoreAsCleared() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();

        ReplicationManager superseded = managerFor(drv);
        superseded.setStillCurrentApplier(() -> false);
        java.util.concurrent.atomic.AtomicInteger marked = new java.util.concurrent.atomic.AtomicInteger();
        superseded.setOnLocalDataCleared(marked::incrementAndGet);

        Throwable t = invokeAndCatch(superseded, "clearLocalDatabases", new Class<?>[] {}, new Object[] {});

        assertTrue(t instanceof MorphiumDriverException, "the clear must be refused, got: " + t);
        assertEquals(0, marked.get(),
                "a manager that drops nothing must not tell the node its store was emptied");
    }

    @Test
    public void aCurrentManagerDoesMarkTheStoreAsCleared() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();

        ReplicationManager current = managerFor(drv);
        current.setStillCurrentApplier(() -> true);
        java.util.concurrent.atomic.AtomicInteger marked = new java.util.concurrent.atomic.AtomicInteger();
        current.setOnLocalDataCleared(marked::incrementAndGet);

        invokeAndCatch(current, "clearLocalDatabases", new Class<?>[] {}, new Object[] {});

        assertEquals(1, marked.get(),
                "negative control: the notice must still fire for the manager actually doing the work");
    }

    @Test
    public void theRefusalNamesTheOperationSoItIsNotMistakenForATransportError() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
        ReplicationManager superseded = managerFor(drv);
        superseded.setStillCurrentApplier(() -> false);

        Throwable t = invokeAndCatch(superseded, "assertStillCurrentApplier",
                new Class<?>[] {String.class}, new Object[] {"some local write"});

        assertTrue(t instanceof MorphiumDriverException, "got: " + t);
        assertTrue(t.getMessage().contains("some local write") && t.getMessage().contains("#323"),
                "the message must name the operation and the reason: " + t.getMessage());
    }
}
