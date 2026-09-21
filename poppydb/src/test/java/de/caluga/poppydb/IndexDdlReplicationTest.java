package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.commands.DropIndexesCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.ListIndexesCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

/**
 * Index DDL as a replicated event (#386): an index created on the primary less than one periodic
 * index-sync interval (30s) before a leader change was lost cluster-wide - the new primary had
 * never received it (index replication was periodic only, the change stream carried no index
 * DDL), and the followers dropped it while aligning to the new primary. MongoDB replicates
 * index builds and drops through the oplog; the equivalent here is a {@code createIndexes} /
 * {@code dropIndexes} change stream event (MongoDB's expanded-event shape:
 * {@code operationDescription.indexes}) that the replication manager applies in order with the
 * data.
 *
 * Seam test around {@link ReplicationManager#applyEventsInOrder} against two local
 * {@link InMemoryDriver}s, same pattern as {@link IndexReplicationTest} /
 * {@link ReplicationOrderingTest}: the "primary" emits the events, a resumed watch reads them
 * back from its replay buffer (which is also what a reconnecting follower does), and they are fed
 * to the manager on the "secondary". No network, no replica set.
 */
public class IndexDdlReplicationTest {

    private static final String DB = "idxddl";
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

    private void createIndex(InMemoryDriver drv, Map<String, Object> key, Map<String, Object> options) throws Exception {
        var con = drv.getPrimaryConnection(null);
        try {
            new CreateIndexesCommand(con).setDb(DB).setColl(COLL).addIndex(key, options).execute();
        } finally {
            drv.releaseConnection(con);
        }
    }

    private void dropIndex(InMemoryDriver drv, String name) throws Exception {
        var con = drv.getPrimaryConnection(null);
        try {
            new DropIndexesCommand(con).setDb(DB).setColl(COLL).setIndex(name).execute();
        } finally {
            drv.releaseConnection(con);
        }
    }

    private void insertDoc(InMemoryDriver drv) throws Exception {
        new InsertMongoCommand(drv).setDb(DB).setColl(COLL)
            .setDocuments(List.of(Doc.of("_id", new MorphiumId(), "ended_on", 1)))
            .execute();
    }

    private List<IndexDescription> listIndexes(InMemoryDriver drv) throws Exception {
        return new ListIndexesCommand(drv.getPrimaryConnection(null)).setDb(DB).setColl(COLL).execute();
    }

    private Optional<IndexDescription> byName(List<IndexDescription> indexes, String name) {
        return indexes.stream().filter(i -> name.equals(i.getName())).findFirst();
    }

    /**
     * Reads back every event the source emitted after {@code afterToken} through a RESUMED
     * cluster-wide watch - the same registration the replication manager uses over the wire
     * ({@code db=admin, coll=1}), with or without {@code showExpandedEvents}. Resuming from a
     * recorded token instead of racing a live registration against the write keeps the test
     * deterministic, and it proves the event is in the replay buffer a reconnecting follower
     * resumes from. Returns once {@code expected} events arrived or the wait ran out.
     */
    private List<Map<String, Object>> eventsAfter(long afterToken, boolean expanded, int expected) throws Exception {
        List<Map<String, Object>> events = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean running = new AtomicBoolean(true);
        long deadline = System.currentTimeMillis() + 3000;
        MongoConnection con = source.getPrimaryConnection(null);
        Thread watcher = Thread.ofVirtual().start(() -> {
            WatchCommand cmd = new WatchCommand(con).setDb("admin").setColl("1").setMaxTimeMS(100)
                .setShowExpandedEvents(expanded)
                .setResumeAfter(Doc.of("_data", String.format(Locale.ROOT, "%016x", afterToken)))
                .setCb(new DriverTailableIterationCallback() {
                    @Override
                    public void incomingData(Map<String, Object> data, long dur) {
                        events.add(data);
                    }

                    @Override
                    public boolean isContinued() {
                        return running.get() && System.currentTimeMillis() < deadline;
                    }
                });
            try {
                cmd.watch();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        while (events.size() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }

        // give a possible surplus event a moment to show up before the negative assertions
        Thread.sleep(200);
        running.set(false);
        watcher.join(5000);
        source.releaseConnection(con);
        return new ArrayList<>(events);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> describedIndexes(Map<String, Object> event) {
        Map<String, Object> description = (Map<String, Object>) event.get("operationDescription");
        assertNotNull(description, "expanded DDL event must carry operationDescription: " + event);
        return (List<Map<String, Object>>) description.get("indexes");
    }

    @Test
    public void createIndexesIsAnExpandedChangeStreamEventWithTheFullSpec() throws Exception {
        insertDoc(source);
        long before = source.getChangeStreamSequence();
        createIndex(source, Doc.of("ended_on", 1), Doc.of("name", "ended_on_ttl", "expireAfterSeconds", 259200));

        List<Map<String, Object>> events = eventsAfter(before, true, 1);

        assertEquals(1, events.size(), "exactly the createIndexes event: " + events);
        Map<String, Object> event = events.get(0);
        assertEquals("createIndexes", event.get("operationType"));
        assertEquals(Doc.of("db", DB, "coll", COLL), event.get("ns"));
        List<Map<String, Object>> indexes = describedIndexes(event);
        assertEquals(1, indexes.size(), indexes.toString());
        assertEquals(Doc.of("ended_on", 1), indexes.get(0).get("key"));
        assertEquals("ended_on_ttl", indexes.get(0).get("name"));
        assertEquals(259200, indexes.get(0).get("expireAfterSeconds"),
                     "the full index spec (TTL/unique/partial options) must travel with the event");
    }

    @Test
    public void secondaryAppliesCreateIndexesEventWithOptions() throws Exception {
        insertDoc(source);
        insertDoc(local);
        long before = source.getChangeStreamSequence();
        createIndex(source, Doc.of("ended_on", 1), Doc.of("name", "ended_on_ttl", "expireAfterSeconds", 259200));

        rm.applyEventsInOrder(eventsAfter(before, true, 1));

        IndexDescription ttl = byName(listIndexes(local), "ended_on_ttl").orElse(null);
        assertNotNull(ttl, "createIndexes event must create the index on the secondary: " + listIndexes(local));
        assertEquals(259200, ttl.getExpireAfterSeconds(), "expireAfterSeconds must survive the event");
    }

    @Test
    public void secondaryAppliesDropIndexesEvent() throws Exception {
        insertDoc(source);
        insertDoc(local);
        createIndex(source, Doc.of("ended_on", 1), Doc.of("name", "ended_on_ttl"));
        createIndex(local, Doc.of("ended_on", 1), Doc.of("name", "ended_on_ttl"));
        long before = source.getChangeStreamSequence();
        dropIndex(source, "ended_on_ttl");

        List<Map<String, Object>> events = eventsAfter(before, true, 1);
        assertEquals(1, events.size(), "exactly the dropIndexes event: " + events);
        assertEquals("dropIndexes", events.get(0).get("operationType"));
        assertEquals("ended_on_ttl", describedIndexes(events.get(0)).get(0).get("name"));

        rm.applyEventsInOrder(events);

        List<IndexDescription> localIdx = listIndexes(local);
        assertTrue(byName(localIdx, "ended_on_ttl").isEmpty(),
                   "dropIndexes event must drop the index on the secondary: " + localIdx);
        assertTrue(localIdx.stream().anyMatch(i -> i.getKey() != null && i.getKey().containsKey("_id")),
                   "_id index must never be dropped: " + localIdx);
    }

    @Test
    public void replayedCreateIndexesEventIsIdempotent() throws Exception {
        insertDoc(source);
        insertDoc(local);
        long before = source.getChangeStreamSequence();
        createIndex(source, Doc.of("ended_on", 1), Doc.of("name", "ended_on_ttl", "expireAfterSeconds", 259200));
        List<Map<String, Object>> events = eventsAfter(before, true, 1);

        // a resume after a reconnect can deliver the event a second time
        rm.applyEventsInOrder(new ArrayList<>(events));
        rm.applyEventsInOrder(new ArrayList<>(events));

        List<IndexDescription> localIdx = listIndexes(local);
        assertEquals(1, localIdx.stream().filter(i -> "ended_on_ttl".equals(i.getName())).count(),
                     "second apply must neither duplicate nor fail: " + localIdx);
    }

    @Test
    public void indexDdlIsInvisibleWithoutShowExpandedEvents() throws Exception {
        // MongoDB semantics, and what keeps messaging/cache change streams unaffected: without
        // showExpandedEvents a watch sees the data events only, never the DDL.
        insertDoc(source);
        long before = source.getChangeStreamSequence();
        createIndex(source, Doc.of("ended_on", 1), Doc.of("name", "ended_on_ttl"));
        insertDoc(source);

        List<Map<String, Object>> events = eventsAfter(before, false, 1);

        assertEquals(1, events.size(), "the insert only, no DDL: " + events);
        assertEquals("insert", events.get(0).get("operationType"));
    }
}
