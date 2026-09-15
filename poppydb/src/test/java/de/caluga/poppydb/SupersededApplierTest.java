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

    @Test
    public void theRefusalCoversEveryLocalApplyNotJustTheSnapshotInsert() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
        new InsertMongoCommand(drv).setDb("payload").setColl("c")
                .setDocuments(List.of(Doc.of("_id", "pre-existing"))).execute();

        ReplicationManager superseded = managerFor(drv);
        superseded.setStillCurrentApplier(() -> false);

        // The same choke point carries the pre-sync dropDatabase and the change-stream applies.
        // A superseded manager dropping the successor's database would be worse than a stray
        // insert, so the guard has to sit below all of them, not at the snapshot insert.
        java.lang.reflect.Method m = ReplicationManager.class.getDeclaredMethod(
                "runLocalApplyCommand", de.caluga.morphium.driver.commands.GenericCommand.class, String.class);
        m.setAccessible(true);
        var drop = new de.caluga.morphium.driver.commands.GenericCommand(drv);
        drop.setDb("payload");
        drop.setColl(null);
        drop.setCmdData(Doc.of("dropDatabase", 1, "$db", "payload"));

        assertThrows(java.lang.reflect.InvocationTargetException.class,
                () -> m.invoke(superseded, drop, "pre-sync dropDatabase of payload"));
        assertEquals(1, drv.find("payload", "c", Doc.of(), null, null, 0, 0).size(),
                "the successor's data must still be there");
    }
}
