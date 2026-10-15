package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-030: an upsert whose filter does not match inserts a NEW document. If the document to insert has an {@code _id} that already exists (pinned in
 * the update or seeded from an equality on {@code _id} in the filter), a real MongoDB raises E11000. The InMemoryDriver used to treat the insert
 * like {@code store()} and silently replaced the existing document: a second contender "stole" a held lease.
 */
public class InMemUpsertDuplicateKeyTest extends MorphiumInMemTestBase {

    @Entity(translateCamelCase = false)
    public static class Held {
        @Id
        public String id;
        public String owner;
        public String payload;
    }

    private void seed(String id, String owner) {
        Held h = new Held();
        h.id = id;
        h.owner = owner;
        h.payload = "original";
        morphium.store(h);
    }

    /** _id = k AND owner = me: seeds _id from the filter and does not match if somebody else holds k */
    private Query<Held> conditional(String id, String me) {
        Query<Held> q = morphium.createQueryFor(Held.class).f("_id").eq(id);
        q.or(morphium.createQueryFor(Held.class).f("owner").eq(me), morphium.createQueryFor(Held.class).f("owner").eq("nobody"));
        return q;
    }

    private static boolean isDuplicateKey(Throwable t) {
        for (; t != null; t = t.getCause()) {
            if (t instanceof MorphiumDriverException mde && mde.getMongoCode() != null && ((Number) mde.getMongoCode()).intValue() == 11000) {
                return true;
            }
        }

        return false;
    }

    private void assertUntouched(String id, String owner) {
        Held h = morphium.createQueryFor(Held.class).f("_id").eq(id).get();
        assertNotNull(h);
        assertEquals(owner, h.owner);
        assertEquals("original", h.payload);
        assertEquals(1, morphium.createQueryFor(Held.class).countAll());
    }

    @Test
    public void findOneAndUpdateWithAnIdFromTheFilterRaisesDuplicateKey() {
        seed("job", "A");
        Throwable t = assertThrows(RuntimeException.class, () -> conditional("job", "B").findOneAndUpdate(UtilsMap.of("$set", UtilsMap.of("owner", "B", "payload", "stolen")), true, true));
        assertTrue(isDuplicateKey(t), String.valueOf(t));
        assertUntouched("job", "A");
    }

    @Test
    public void theDuplicateKeyErrorIsAMorphiumDriverExceptionWithTheNumericMongoCode() {
        seed("job", "A");
        MorphiumDriverException e = assertThrows(MorphiumDriverException.class, () -> conditional("job", "B").findOneAndUpdate(UtilsMap.of("$set", UtilsMap.of("owner", "B")), true, true));
        assertEquals(11000, ((Number) e.getMongoCode()).intValue());
        assertTrue(e.getMessage().contains("E11000") && e.getMessage().contains("duplicate key"), e.getMessage());
    }

    @Test
    public void anIdPinnedBySetOnInsertRaisesDuplicateKey() {
        seed("job", "A");
        Query<Held> unrelated = morphium.createQueryFor(Held.class).f("owner").eq("somebody else");
        Throwable t = assertThrows(RuntimeException.class, () -> unrelated.findOneAndUpdate(UtilsMap.of("$set", UtilsMap.of("owner", "B"), "$setOnInsert", UtilsMap.of("_id", "job")), true, true));
        assertTrue(isDuplicateKey(t), String.valueOf(t));
        assertUntouched("job", "A");
    }

    @Test
    public void theUpdateCommandPathRaisesDuplicateKeyToo() {
        seed("job", "A");
        Throwable t = assertThrows(RuntimeException.class, () -> conditional("job", "B").set(UtilsMap.of("owner", "B", "payload", "stolen"), true, false));
        assertTrue(isDuplicateKey(t), String.valueOf(t));
        assertUntouched("job", "A");
    }

    @Test
    public void aMatchingFilterUpdatesTheExistingDocumentInPlace() {
        seed("job", "A");
        Held h = conditional("job", "A").findOneAndUpdate(UtilsMap.of("$set", UtilsMap.of("owner", "A", "payload", "renewed")), true, true);
        assertNotNull(h);
        assertEquals("renewed", h.payload);
        assertEquals(1, morphium.createQueryFor(Held.class).countAll());
    }

    @Test
    public void aFilterThatSeedsNoIdInsertsANewDocument() {
        seed("job", "A");
        Query<Held> other = morphium.createQueryFor(Held.class).f("owner").eq("C");
        Held h = other.findOneAndUpdate(UtilsMap.of("$set", UtilsMap.of("payload", "new")), true, true);
        assertNotNull(h);
        assertTrue(!"job".equals(h.id), "a new id is generated: " + h.id);
        assertEquals("C", h.owner);
        assertEquals(2, morphium.createQueryFor(Held.class).countAll());
    }

    @Test
    public void anInsertingUpsertWithoutReturnNewStillReturnsNull() {
        Query<Held> q = morphium.createQueryFor(Held.class).f("_id").eq("fresh");
        assertNull(q.findOneAndUpdate(UtilsMap.of("$set", UtilsMap.of("owner", "A")), true, false));
        assertEquals("A", morphium.createQueryFor(Held.class).f("_id").eq("fresh").get().owner);
    }

    @Test
    public void storeOfAnExistingIdStillReplacesTheDocument() {
        seed("job", "A");
        Held h = new Held();
        h.id = "job";
        h.owner = "B";
        h.payload = "replaced";
        morphium.store(h);
        Held read = morphium.createQueryFor(Held.class).f("_id").eq("job").get();
        assertEquals("B", read.owner);
        assertEquals("replaced", read.payload);
        assertEquals(1, morphium.createQueryFor(Held.class).countAll());
    }

    @Test
    public void contendersRaceForOneIdAndExactlyOneWins() throws Exception {
        int contenders = 8;
        ExecutorService pool = Executors.newFixedThreadPool(contenders);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<String>> results = new ArrayList<>();

        for (int i = 0; i < contenders; i++) {
            String me = "C" + i;
            results.add(pool.submit(() -> {
                go.await();

                try {
                    Query<Held> q = morphium.createQueryFor(Held.class).f("_id").eq("race");
                    q.or(morphium.createQueryFor(Held.class).f("owner").eq(me), morphium.createQueryFor(Held.class).f("owner").eq("nobody"));
                    return q.findOneAndUpdate(Map.of("$set", Map.of("owner", me)), true, true).owner;
                } catch (RuntimeException e) {
                    assertTrue(isDuplicateKey(e), "unexpected failure: " + e);
                    return null;
                }
            }));
        }

        go.countDown();
        List<String> winners = new ArrayList<>();

        for (Future<String> f : results) {
            String w = f.get(30, TimeUnit.SECONDS);

            if (w != null) {
                winners.add(w);
            }
        }

        pool.shutdown();
        assertEquals(1, winners.size(), "exactly one contender may win: " + winners);
        assertEquals(1, morphium.createQueryFor(Held.class).countAll());
        assertEquals(winners.get(0), morphium.createQueryFor(Held.class).f("_id").eq("race").get().owner);
    }
}
