package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.ListIndexesCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

/**
 * Index replication (#258): replica-set replication used to copy documents only - a secondary
 * (and any node promoted after failover) had none of the primary's user-defined indexes, so
 * unique constraints went unenforced and TTL indexes never expired anything there.
 *
 * Pure seam test around {@link ReplicationManager#syncIndexesFrom} /
 * {@link ReplicationManager#applyIndexDiff} against two local {@link InMemoryDriver}s (the
 * "primary" is just used as the listIndexes source) - no network, no full replica set, same
 * pattern as {@link ReplicationOrderingTest}.
 */
public class IndexReplicationTest {

    private static final String DB = "idxrepl";
    private static final String COLL = "docs";

    private InMemoryDriver source;
    private InMemoryDriver local;
    private ReplicationManager rm;

    @BeforeEach
    public void setup() throws Exception {
        source = new InMemoryDriver();
        source.connect();
        local = new InMemoryDriver();
        local.connect();
        rm = new ReplicationManager(local, "127.0.0.1", 1);
    }

    @AfterEach
    public void tearDown() {
        source.close();
        local.close();
    }

    private void createIndex(InMemoryDriver drv, String db, String coll, Map<String, Object> key, Map<String, Object> options) throws Exception {
        var con = drv.getPrimaryConnection(null);
        try {
            new CreateIndexesCommand(con).setDb(db).setColl(coll).addIndex(key, options).execute();
        } finally {
            drv.releaseConnection(con);
        }
    }

    private void insertDoc(InMemoryDriver drv, String db, String coll) throws Exception {
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(Doc.of("_id", new MorphiumId(), "user_id", 1, "archived", false)))
            .execute();
    }

    private List<IndexDescription> listIndexes(InMemoryDriver drv, String db, String coll) throws Exception {
        return new ListIndexesCommand(drv.getPrimaryConnection(null)).setDb(db).setColl(coll).execute();
    }

    private Optional<IndexDescription> byName(List<IndexDescription> indexes, String name) {
        return indexes.stream().filter(i -> name.equals(i.getName())).findFirst();
    }

    @Test
    public void syncCreatesMissingIndexesWithOptions() throws Exception {
        insertDoc(source, DB, COLL);
        insertDoc(local, DB, COLL);
        createIndex(source, DB, COLL, Doc.of("user_id", 1),
                    Doc.of("name", "uniq_user", "unique", true));
        createIndex(source, DB, COLL, Doc.of("created", 1),
                    Doc.of("name", "ttl_created", "expireAfterSeconds", 60));
        createIndex(source, DB, COLL, Doc.of("archived", 1),
                    Doc.of("name", "partial_arch", "partialFilterExpression", Doc.of("archived", false)));

        rm.syncIndexesFrom(source);

        List<IndexDescription> localIdx = listIndexes(local, DB, COLL);
        IndexDescription uniq = byName(localIdx, "uniq_user").orElse(null);
        assertNotNull(uniq, "unique index must be replicated: " + localIdx);
        assertEquals(Boolean.TRUE, uniq.getUnique(), "unique option must survive replication");

        IndexDescription ttl = byName(localIdx, "ttl_created").orElse(null);
        assertNotNull(ttl, "TTL index must be replicated");
        assertEquals(60, ttl.getExpireAfterSeconds(), "expireAfterSeconds must survive replication");

        IndexDescription partial = byName(localIdx, "partial_arch").orElse(null);
        assertNotNull(partial, "partial index must be replicated");
        assertEquals(Doc.of("archived", false), partial.getPartialFilterExpression(),
                     "partialFilterExpression must survive replication");
    }

    @Test
    public void syncDropsIndexesAbsentOnPrimaryButKeepsId() throws Exception {
        insertDoc(source, DB, COLL);
        insertDoc(local, DB, COLL);
        // stale local index that the primary does not have (e.g. dropped there while we were down)
        createIndex(local, DB, COLL, Doc.of("stale", 1), Doc.of("name", "stale_idx"));

        rm.syncIndexesFrom(source);

        List<IndexDescription> localIdx = listIndexes(local, DB, COLL);
        assertTrue(byName(localIdx, "stale_idx").isEmpty(),
                   "index absent on primary must be dropped locally: " + localIdx);
        assertTrue(localIdx.stream().anyMatch(i -> i.getKey() != null && i.getKey().containsKey("_id")),
                   "_id index must never be dropped: " + localIdx);
    }

    @Test
    public void syncIsIdempotent() throws Exception {
        insertDoc(source, DB, COLL);
        insertDoc(local, DB, COLL);
        createIndex(source, DB, COLL, Doc.of("user_id", 1), Doc.of("name", "uniq_user", "unique", true));

        rm.syncIndexesFrom(source);
        rm.syncIndexesFrom(source);

        List<IndexDescription> localIdx = listIndexes(local, DB, COLL);
        assertEquals(1, localIdx.stream().filter(i -> "uniq_user".equals(i.getName())).count(),
                     "second sync must not duplicate or fail: " + localIdx);
    }

    @Test
    public void syncCoversAllDatabasesAndSkipsSystemDbs() throws Exception {
        insertDoc(source, "app_one", "a");
        insertDoc(source, "app_two", "b");
        insertDoc(local, "app_one", "a");
        insertDoc(local, "app_two", "b");
        createIndex(source, "app_one", "a", Doc.of("f1", 1), Doc.of("name", "idx_one"));
        createIndex(source, "app_two", "b", Doc.of("f2", 1), Doc.of("name", "idx_two"));
        // system database content must not be touched/replicated
        insertDoc(source, "admin", "system.users");

        rm.syncIndexesFrom(source);

        assertTrue(byName(listIndexes(local, "app_one", "a"), "idx_one").isPresent());
        assertTrue(byName(listIndexes(local, "app_two", "b"), "idx_two").isPresent());
    }

    @Test
    public void syncHandlesEmptyCollectionOnPrimary() throws Exception {
        // collection exists on the primary only through its index - no documents yet
        createIndex(source, DB, "empty_coll", Doc.of("f", 1), Doc.of("name", "idx_empty"));

        rm.syncIndexesFrom(source);

        assertTrue(byName(listIndexes(local, DB, "empty_coll"), "idx_empty").isPresent(),
                   "index on an empty primary collection must still be replicated");
    }
}
