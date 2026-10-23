package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumStorageAdapter;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * MOR-059: a write of {@code Query}/{@code Morphium} that the server rejects raises a {@link MorphiumDriverException}; the switch and the per-query override restore the
 * old behaviour; async callbacks get {@code onOperationError}; caches and events still follow what was applied.
 */
@Tag("core")
public class RaiseOnWriteErrorsTest extends MultiDriverTestBase {

    @Entity
    public static class Keyed {
        @Id
        public String id;
        public String code;
        public int counter;
    }

    @Entity
    public static class Versioned {
        @Id
        public String id;
        public String code;
        @de.caluga.morphium.annotations.Version
        public Long version;
    }

    private void setup(Morphium m, String... codes) throws Exception {
        m.dropCollection(Keyed.class);
        m.ensureIndex(Keyed.class, m.getMapper().getCollectionName(Keyed.class), Map.of("code", 1), Map.of("unique", true));
        m.ensureIndex(Keyed.class, m.getMapper().getCollectionName(Keyed.class), Map.of("counter", 1), Map.of("unique", true));

        for (int i = 0; i < codes.length; i++) {
            Keyed k = new Keyed();
            k.id = "k" + i;
            k.code = codes[i];
            k.counter = i;
            m.store(k);
        }
    }

    private Query<Keyed> byId(Morphium m, String id) {
        return m.createQueryFor(Keyed.class).f("id").eq(id);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aDuplicateKeyOfSetRaisesAndTheDocumentIsUnchanged(Morphium m) throws Exception {
        try (m) {
            setup(m, "a", "b");
            MorphiumDriverException e = assertThrows(MorphiumDriverException.class, () -> byId(m, "k0").set("code", "b"));
            assertTrue(e.isDuplicateKey(), String.valueOf(e));
            assertTrue(m.getDriver() instanceof InMemoryDriver || e.getWriteErrors() != null, "the write errors of the server are on the exception");
            assertEquals("a", byId(m, "k0").get().code);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aSuccessfulWriteStillReturnsTheReply(Morphium m) throws Exception {
        try (m) {
            setup(m, "a", "b");
            Map<String, Object> reply = byId(m, "k0").set("code", "c");
            assertEquals(1, ((Number) reply.get("n")).intValue());
            assertEquals("c", byId(m, "k0").get().code);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theOverrideOfTheQueryReturnsTheReplyAsBefore(Morphium m) throws Exception {
        try (m) {
            assumeFalse(m.getDriver() instanceof InMemoryDriver, "the InMemoryDriver throws the unique violation itself");
            setup(m, "a", "b");
            Map<String, Object> reply = byId(m, "k0").setRaiseOnWriteErrors(false).set("code", "b");
            assertNotNull(reply.get("writeErrors"), "the reply of the server is returned and nobody looks at it: " + reply);
            assertEquals("a", byId(m, "k0").get().code);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    @SuppressWarnings("deprecation")
    public void theConfigurationSwitchAndTheQueryOverrideCombine(Morphium m) throws Exception {
        try (m) {
            assumeFalse(m.getDriver() instanceof InMemoryDriver, "the InMemoryDriver throws the unique violation itself");
            setup(m, "a", "b");
            m.getConfig().writerSettings().disableRaiseOnWriteErrors();

            try {
                assertNotNull(byId(m, "k0").set("code", "b").get("writeErrors"), "switched off: the old behaviour");
                assertThrows(MorphiumDriverException.class, () -> byId(m, "k0").setRaiseOnWriteErrors(true).set("code", "b"), "the query asks for raising");
            } finally {
                m.getConfig().writerSettings().enableRaiseOnWriteErrors();
            }

            assertThrows(MorphiumDriverException.class, () -> byId(m, "k0").set("code", "b"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aMultiUpdateKeepsTheDocumentsBeforeTheErrorAndSaysSo(Morphium m) throws Exception {
        try (m) {
            assumeFalse(m.getDriver() instanceof InMemoryDriver, "the InMemoryDriver reports a violated unique index as an exception of the first failing document, so a multi update does not reach the second one");
            setup(m, "a", "b", "c");
            MorphiumDriverException e = assertThrows(MorphiumDriverException.class, () -> m.createQueryFor(Keyed.class).set("code", "same", false, true));
            assertTrue(e.isDuplicateKey());
            assertTrue(e.getMessage().contains("not rolled back"), e.getMessage());
            assertEquals(1, m.createQueryFor(Keyed.class).f("code").eq("same").countAll(), "ordered: the first document was updated, the second was rejected, the third not tried");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theAsyncCallbackGetsTheErrorAndNeverTheSuccess(Morphium m) throws Exception {
        try (m) {
            setup(m, "a", "b");
            AtomicInteger succeeded = new AtomicInteger();
            AtomicInteger failed = new AtomicInteger();
            AtomicReference<Throwable> error = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            AsyncOperationCallback<Keyed> cb = new AsyncOperationCallback<>() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query<Keyed> q, long duration, List<Keyed> result, Keyed entity, Object... param) {
                    succeeded.incrementAndGet();
                    done.countDown();
                }

                @Override
                public void onOperationError(AsyncOperationType type, Query<Keyed> q, long duration, String message, Throwable t, Keyed entity, Object... param) {
                    failed.incrementAndGet();
                    error.set(t);
                    done.countDown();
                }
            };
            byId(m, "k0").set("code", "b", false, false, cb);
            assertTrue(done.await(30, TimeUnit.SECONDS));
            Thread.sleep(200);
            assertEquals(0, succeeded.get());
            assertEquals(1, failed.get(), "once, not retried");
            assertTrue(error.get() instanceof MorphiumDriverException, String.valueOf(error.get()));
            assertTrue(((MorphiumDriverException) error.get()).isDuplicateKey());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void thePostUpdateEventStillFiresForWhatWasApplied(Morphium m) throws Exception {
        try (m) {
            assumeFalse(m.getDriver() instanceof InMemoryDriver, "the InMemoryDriver reports a violated unique index as an exception of the first failing document, so a multi update does not reach the second one");
            setup(m, "a", "b", "c");
            AtomicInteger postUpdates = new AtomicInteger();
            m.addListener(new MorphiumStorageAdapter<Keyed>() {
                @Override
                public void postUpdate(Morphium mo, Class<? extends Keyed> cls, Enum updateType) {
                    postUpdates.incrementAndGet();
                }
            });
            assertThrows(MorphiumDriverException.class, () -> m.createQueryFor(Keyed.class).set("code", "same", false, true));
            assertEquals(1, postUpdates.get(), "the first document was updated: the cluster has to learn about it (caches)");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aMissingMatchIsNotAnError(Morphium m) throws Exception {
        try (m) {
            setup(m, "a");
            Map<String, Object> reply = byId(m, "nothing").set("code", "z");
            assertEquals(0, ((Number) reply.get("n")).intValue());
            assertNull(byId(m, "nothing").get());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theEntityVariantsRaiseWithTheWriteErrorsOfTheServer(Morphium m) throws Exception {
        try (m) {
            setup(m, "a", "b");
            Keyed k0 = byId(m, "k0").get();
            MorphiumDriverException inc = assertThrows(MorphiumDriverException.class, () -> m.inc(k0, "counter", 1));
            assertTrue(inc.isDuplicateKey(), "inc(entity) used to say 'Update failed': " + inc);
            assertThrows(MorphiumDriverException.class, () -> m.dec(k0, "counter", -1));
            MorphiumDriverException set = assertThrows(MorphiumDriverException.class, () -> m.setInEntity(k0, "code", "b"));
            assertTrue(set.isDuplicateKey(), String.valueOf(set));
            assertEquals("a", byId(m, "k0").get().code);
            assertEquals(0, byId(m, "k0").get().counter);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void unsetInEntityRaises(Morphium m) throws Exception {
        try (m) {
            m.dropCollection(Keyed.class);
            m.ensureIndex(Keyed.class, m.getMapper().getCollectionName(Keyed.class), Map.of("code", 1), Map.of("unique", true));
            Keyed a = new Keyed();
            a.id = "a";
            a.code = "x";
            m.store(a);
            Keyed b = new Keyed();
            b.id = "b";
            m.store(b);
            MorphiumDriverException e = assertThrows(MorphiumDriverException.class, () -> m.unsetInEntity(a, "code"), "the unset code is null, like the one of b");
            assertTrue(e.isDuplicateKey(), String.valueOf(e));
            assertEquals("x", byId(m, "a").get().code);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void updateUsingFieldsRaisesTheDuplicateKey(Morphium m) throws Exception {
        try (m) {
            setup(m, "a", "b");
            Keyed k0 = byId(m, "k0").get();
            k0.code = "b";
            MorphiumDriverException e = assertThrows(MorphiumDriverException.class, () -> m.updateUsingFields(k0, "code"));
            assertTrue(e.isDuplicateKey(), String.valueOf(e));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aDuplicateKeyOnAVersionedStoreIsNotAVersionMismatch(Morphium m) throws Exception {
        try (m) {
            m.dropCollection(Versioned.class);
            m.ensureIndex(Versioned.class, m.getMapper().getCollectionName(Versioned.class), Map.of("code", 1), Map.of("unique", true));
            Versioned a = new Versioned();
            a.id = "a";
            a.code = "a";
            m.store(a);
            Versioned b = new Versioned();
            b.id = "b";
            b.code = "b";
            m.store(b);
            Versioned loaded = m.createQueryFor(Versioned.class).f("id").eq("a").get();
            loaded.code = "b";
            MorphiumDriverException e = assertThrows(MorphiumDriverException.class, () -> m.store(loaded), "used to surface as a VersionMismatchException");
            assertTrue(e.isDuplicateKey(), String.valueOf(e));
            assertEquals("a", m.createQueryFor(Versioned.class).f("id").eq("a").get().code);

            Versioned again = m.createQueryFor(Versioned.class).f("id").eq("a").get();
            again.code = "b";
            Exception list = assertThrows(Exception.class, () -> m.storeList(java.util.List.of(again), m.getMapper().getCollectionName(Versioned.class), false));
            assertTrue(list instanceof MorphiumDriverException mde && mde.isDuplicateKey() || list.getCause() instanceof MorphiumDriverException, String.valueOf(list));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anOrderedUpdateCommandStopsAtTheFirstRejectedStatement(Morphium m) throws Exception {
        try (m) {
            setup(m, "a", "b", "c");
            var con = m.getDriver().getPrimaryConnection(null);

            try {
                de.caluga.morphium.driver.commands.UpdateMongoCommand cmd = new de.caluga.morphium.driver.commands.UpdateMongoCommand(con).setDb(m.getConfig().connectionSettings().getDatabase())
                        .setColl(m.getMapper().getCollectionName(Keyed.class));
                cmd.addUpdate(Map.of("_id", "k0"), Map.of("$set", Map.of("counter", 10)), null, false, false, null, null, null);
                cmd.addUpdate(Map.of("_id", "k1"), Map.of("$set", Map.of("code", "a")), null, false, false, null, null, null); // duplicate key
                cmd.addUpdate(Map.of("_id", "k2"), Map.of("$set", Map.of("counter", 30)), null, false, false, null, null, null);

                try {
                    Map<String, Object> reply = cmd.execute();
                    List<?> errors = (List<?>) reply.get("writeErrors");
                    assertNotNull(errors, String.valueOf(reply));
                    assertEquals(1, errors.size());
                    assertEquals(1, ((Number) ((Map<?, ?>) errors.get(0)).get("index")).intValue());
                } catch (MorphiumDriverException e) {
                    // the InMemoryDriver reports a violated unique index as an exception, a real MongoDB in the reply; the state afterwards is the same
                    assertTrue(m.getDriver() instanceof InMemoryDriver && e.isDuplicateKey(), String.valueOf(e));
                }

                assertEquals(10, byId(m, "k0").get().counter, "the statement before the error stays applied");
                assertEquals(2, byId(m, "k2").get().counter, "ordered: the statement after the error was not executed");
            } finally {
                m.getDriver().releaseConnection(con);
            }
        }
    }
}
