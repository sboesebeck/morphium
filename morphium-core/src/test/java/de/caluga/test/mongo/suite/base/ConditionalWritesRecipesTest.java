package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.query.ReturnDocument;
import de.caluga.morphium.query.UpdateOutcome;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MOR-100: the recipes of docs/howtos/conditional-writes.md, run on a real MongoDB and on the InMemoryDriver, so that the page does not describe what the code does not do. */
@Tag("core")
public class ConditionalWritesRecipesTest extends MultiDriverTestBase {

    @Entity
    public static class Done {
        @Id
        public String key;
        public Instant startedAt;
    }

    @Entity
    public static class Job {
        @Id
        public String id;
        public String state;
        public int attempts;
    }

    @Entity
    public static class Outcome {
        @Id
        public String id;
        public String state;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void runOnceTheFirstCallerInsertsTheSecondFindsIt(Morphium m) throws Exception {
        try (m) {
            Instant first = Instant.parse("2026-10-23T10:00:00Z");
            UpdateOutcome<Done> a = m.createQueryFor(Done.class).f("key").eq("req-1").modify().setOnInsert("startedAt", first).upsert().tryFindOneAndUpdate(ReturnDocument.AFTER);
            assertTrue(a.wasInserted());
            UpdateOutcome<Done> b = m.createQueryFor(Done.class).f("key").eq("req-1").modify().setOnInsert("startedAt", first.plusSeconds(60)).upsert().tryFindOneAndUpdate(ReturnDocument.AFTER);
            assertFalse(b.wasInserted(), "the second caller finds the document: UPDATED, not a duplicate key (the filter has no guard)");
            assertEquals(UpdateOutcome.Kind.UPDATED, b.getKind());
            assertEquals(first, b.getDocument().startedAt, "setOnInsert did not run again");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aJobClaimTakesAnIdleJobOnceAndAnOpenToClosedTransitionIsIdempotent(Morphium m) throws Exception {
        try (m) {
            Job j = new Job();
            j.id = "j1";
            j.state = "IDLE";
            m.store(j);

            UpdateOutcome<Job> c1 = m.createQueryFor(Job.class).f("id").eq("j1").f("state").eq("IDLE")
                    .modify().set("state", "RUNNING").inc("attempts", 1).tryFindOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(UpdateOutcome.Kind.UPDATED, c1.getKind());
            assertEquals("RUNNING", c1.getDocument().state);
            assertEquals(1, c1.getDocument().attempts);

            UpdateOutcome<Job> c2 = m.createQueryFor(Job.class).f("id").eq("j1").f("state").eq("IDLE")
                    .modify().set("state", "RUNNING").inc("attempts", 1).tryFindOneAndUpdate(ReturnDocument.AFTER);
            assertEquals(UpdateOutcome.Kind.NOT_MATCHED, c2.getKind(), "somebody else holds the job: nothing to claim");
            assertEquals(1, m.createQueryFor(Job.class).f("id").eq("j1").get().attempts);

            Outcome o = new Outcome();
            o.id = "o1";
            o.state = "OPEN";
            m.store(o);
            assertEquals(UpdateOutcome.Kind.UPDATED, m.createQueryFor(Outcome.class).f("id").eq("o1").f("state").eq("OPEN").modify().set("state", "CLOSED").tryFindOneAndUpdate(ReturnDocument.AFTER).getKind());
            assertEquals(UpdateOutcome.Kind.NOT_MATCHED, m.createQueryFor(Outcome.class).f("id").eq("o1").f("state").eq("OPEN").modify().set("state", "CLOSED").tryFindOneAndUpdate(ReturnDocument.AFTER).getKind(), "closing twice is a no-op");
            assertNotNull(m.createQueryFor(Outcome.class).f("id").eq("o1").get());
        }
    }

    @Entity
    public static class Case {
        @Id
        public String id;
        public String unit;
        public String state;
        public List<String> waiting;
    }

    /** create or join: one OPEN case per unit (a partial unique index), a loser of the insert race repeats once and joins the case of the winner */
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void createOrJoinRepeatsOnceOnADuplicateKey(Morphium m) throws Exception {
        try (m) {
            m.dropCollection(Case.class);
            m.ensureIndex(Case.class, m.getMapper().getCollectionName(Case.class), Map.of("unit", 1), Map.of("unique", true, "partialFilterExpression", Map.of("state", "OPEN")));

            for (int round = 0; round < 3; round++) {
                String unit = "unit-" + round;
                ExecutorService pool = Executors.newFixedThreadPool(8);
                CountDownLatch go = new CountDownLatch(1);
                List<Future<Integer>> futures = new ArrayList<>();

                for (int i = 0; i < 8; i++) {
                    String me = "req-" + i;
                    futures.add(pool.submit(() -> {
                        go.await();
                        int attempts = 0;

                        while (true) {
                            attempts++;
                            UpdateOutcome<Case> o = m.createQueryFor(Case.class).f("unit").eq(unit).f("state").eq("OPEN")
                                    .modify().setOnInsert("id", java.util.UUID.randomUUID().toString()).setOnInsert("unit", unit).setOnInsert("state", "OPEN").addToSet("waiting", me)
                                    .upsert().tryFindOneAndUpdate(ReturnDocument.AFTER);

                            if (!o.isDuplicateKey()) {
                                return attempts;
                            }

                            if (attempts > 1) {
                                throw new IllegalStateException("a second duplicate key: the document of the winner must match the filter");
                            }
                        }
                    }));
                }

                go.countDown();

                for (Future<Integer> f : futures) {
                    assertTrue(f.get(60, TimeUnit.SECONDS) <= 2, "one repetition at most");
                }

                pool.shutdown();
                List<Case> open = m.createQueryFor(Case.class).f("unit").eq(unit).f("state").eq("OPEN").asList();
                assertEquals(1, open.size(), "one OPEN case per unit, round " + round);
                assertEquals(8, open.get(0).waiting.size(), "every request joined the case");
            }
        }
    }
}
