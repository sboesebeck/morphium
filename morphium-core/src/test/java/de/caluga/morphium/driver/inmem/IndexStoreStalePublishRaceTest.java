package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.commands.auth.CreateUserAdminCommand;
import de.caluga.morphium.driver.inmem.auth.UserDocuments;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Issue #290: {@code getIndexStore} may be entered without the collection lock (explain /
 * slow-query paths), which opens a publish race with every store-invalidating mutation - a store
 * built from the pre-mutation document list can be published AFTER the mutation's invalidate, and
 * then serves stale data to all later readers until the next invalidate. The same shape exists
 * for whole-DB drops, which remove the affected stores without going through
 * {@code invalidateIndexStore}.
 *
 * <p>The race window (between the documents snapshot in {@code buildIndexStore} and the publish
 * in {@code getIndexStore}) is made deterministic here by overriding the package-private
 * {@code buildIndexStore} to run the concurrent mutation after the snapshot is taken but before
 * the caller publishes it. Lives in the driver's own package to reach
 * {@code getIndexStore}/{@code buildIndexStore}.
 */
@Tag("inmemory")
public class IndexStoreStalePublishRaceTest {

    private static final String USERS_DB = "admin";
    private static final String USERS_COLLECTION = "system.users";
    private static final String USER_NAME = "bob";

    private RacingDriver drv;

    /**
     * Lets the test inject a mutation into the window between {@code buildIndexStore}'s document
     * snapshot and the publish in {@code getIndexStore} - exactly where a concurrent thread's
     * write+invalidate lands in the real race. Fires only for the given namespace, and only once
     * (the hook's own write paths trigger builds too and must not recurse).
     */
    private static class RacingDriver extends InMemoryDriver {
        private final String targetDb;
        private final String targetColl;
        volatile Runnable betweenSnapshotAndPublish;

        RacingDriver(String targetDb, String targetColl) {
            this.targetDb = targetDb;
            this.targetColl = targetColl;
        }

        @Override
        CollectionIndexStore buildIndexStore(String db, String collection) throws MorphiumDriverException {
            CollectionIndexStore store = super.buildIndexStore(db, collection);
            if (targetDb.equals(db) && targetColl.equals(collection)) {
                Runnable hook = betweenSnapshotAndPublish;
                if (hook != null) {
                    betweenSnapshotAndPublish = null;
                    hook.run();
                }
            }
            return store;
        }
    }

    @AfterEach
    void tearDown() {
        if (drv != null) {
            drv.close();
        }
    }

    /** Driver whose next admin.system.users store build races the issue's createUser writer. */
    private RacingDriver driverRacingCreateUser() throws Exception {
        RacingDriver racing = new RacingDriver(USERS_DB, USERS_COLLECTION);
        racing.connect();
        // The concurrent writer from the issue: createUser adds to admin.system.users directly
        // and calls invalidateIndexStore - racing the build our test thread has in flight.
        racing.betweenSnapshotAndPublish = () -> {
            CreateUserAdminCommand cmd = new CreateUserAdminCommand(null).setUserName(USER_NAME).setPwd("pw");
            cmd.setDb(USERS_DB);
            Map<String, Object> result = racing.readSingleAnswer(racing.runCommand(cmd));
            if (!Double.valueOf(1.0).equals(result.get("ok"))) {
                throw new IllegalStateException("createUser failed: " + result);
            }
        };
        return racing;
    }

    @Test
    void storePublishedPastConcurrentInvalidateMustNotServeStaleData() throws Exception {
        drv = driverRacingCreateUser();

        // Thread A from the issue: enters getIndexStore lock-free, snapshots the (still empty)
        // collection, and publishes - while the hook's createUser lands in between.
        drv.getIndexStore(USERS_DB, USERS_COLLECTION);

        CollectionIndexStore published = drv.getIndexStore(USERS_DB, USERS_COLLECTION);
        assertTrue(published.containsId(UserDocuments.userId(USERS_DB, USER_NAME)),
                "the index store visible after the concurrent invalidate must contain the concurrently created user");
    }

    @Test
    void duplicateIdCheckMustSeeUserWrittenConcurrentlyWithStoreBuild() throws Exception {
        drv = driverRacingCreateUser();

        drv.getIndexStore(USERS_DB, USERS_COLLECTION);

        // Worst case from the issue: the generic insert's duplicate-_id check runs against the
        // stale store, misses the concurrently created user and admits a second document with
        // the same _id.
        String bobId = UserDocuments.userId(USERS_DB, USER_NAME);
        assertThrows(MorphiumDriverException.class,
                () -> drv.insert(USERS_DB, USERS_COLLECTION, List.of(Doc.of("_id", bobId)), null),
                "inserting a document with the _id of the concurrently created user must be rejected as a duplicate");
    }

    @Test
    void dropDatabaseDuringBuildMustNotResurrectDroppedDocuments() throws Exception {
        String db = "racedb";
        String coll = "stuff";
        drv = new RacingDriver(db, coll);
        drv.connect();
        drv.insert(db, coll, List.of(Doc.of("_id", "doc1")), null);
        // Structural invalidate so the racing getIndexStore below has to build from scratch.
        new CreateIndexesCommand(drv).setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of("counter", 1)))
                .execute();

        // The whole-DB drop removes the collection's store WITHOUT invalidateIndexStore - a
        // build racing it must not re-publish the pre-drop snapshot afterwards.
        drv.betweenSnapshotAndPublish = () -> drv.drop(db, null);
        drv.getIndexStore(db, coll);

        assertFalse(drv.getIndexStore(db, coll).containsId("doc1"),
                "the index store visible after a concurrent dropDatabase must not contain pre-drop documents");
    }
}
