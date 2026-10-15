package de.caluga.test.mongo.suite.repro;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.FindAndModifyMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.repro.ReproSupport.Outcome;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reproduction of hypothesis 5 of the Morphium update-path research (docs/research/reproduction-results.md):
 * <ol>
 * <li>a conditional upsert ({@code upsert=true}, filter {@code _id = X AND (expiresAt < now OR owner = me)})
 * against an existing document whose filter does not match throws a {@link MorphiumDriverException} with
 * Mongo code 11000 (duplicate key), because the upsert tries to insert a second document with the same
 * {@code _id};</li>
 * <li>(MOR-003, fixed: see conditionalUpsertViaQueryKeepsFilterId) a compound filter no longer collides on the generated _id;</li>
 * <li>an upsert that inserts, with {@code returnNew=false}, returns {@code null};</li>
 * <li>the {@code @Version} trap: a document created through {@code $setOnInsert} without a version field
 * makes a later {@code store()} of the loaded entity fail with a duplicate key on {@code _id}, because
 * version 0/null is classified as "new".</li>
 * </ol>
 * The tests assert the CURRENT behaviour of both the InMemoryDriver and a real MongoDB (selected by the
 * driver of the injected Morphium instance) and log every observed outcome as {@code REPRO-RESULT}.
 */
@Tag("core")
public class ConditionalUpsertReproductionTest extends MultiDriverTestBase {

    @Entity(translateCamelCase = false)
    public static class Lease {
        @Id
        public String id;
        public String owner;
        public Date expiresAt;
    }

    @Entity(translateCamelCase = false)
    public static class VersionedDoc {
        @Id
        public String id;
        public String payload;
        @Version
        public Long entityVersion;
    }

    @Entity(translateCamelCase = false)
    public static class PrimitiveVersionedDoc {
        @Id
        public String id;
        public String payload;
        @Version
        public long entityVersion;
    }

    private static boolean isInMem(Morphium m) {
        return m.getDriver() instanceof InMemoryDriver;
    }

    /** filter: _id = leaseId AND (expiresAt < now OR owner = me) */
    private Query<Lease> acquireFilter(Morphium m, String leaseId, String me) {
        Query<Lease> q = m.createQueryFor(Lease.class).f("_id").eq(leaseId);
        Query<Lease> expired = m.createQueryFor(Lease.class).f("expiresAt").lt(new Date());
        Query<Lease> mine = m.createQueryFor(Lease.class).f("owner").eq(me);
        q.or(expired, mine);
        return q;
    }

    private Map<String, Object> acquireUpdate(String me, long ttlMs) {
        return UtilsMap.of("$set", UtilsMap.of("owner", me, "expiresAt", new Date(System.currentTimeMillis() + ttlMs)));
    }

    /** The update that carries the pinned id; makes Query#findOneAndUpdate skip its generated-id logic. */
    private Map<String, Object> acquireUpdatePinned(String leaseId, String me, long ttlMs) {
        return UtilsMap.of("$set", UtilsMap.of("owner", me, "expiresAt", new Date(System.currentTimeMillis() + ttlMs)),
                           "$setOnInsert", UtilsMap.of("_id", leaseId));
    }

    /** The path applications use today: a hand-built FindAndModifyMongoCommand with the query object of the Morphium query. */
    private Lease viaRawCommand(Morphium m, Query<Lease> filter, Map<String, Object> update, boolean upsert, boolean returnNew) throws Exception {
        MongoConnection con = m.getDriver().getPrimaryConnection(null);
        FindAndModifyMongoCommand cmd = null;
        try {
            cmd = new FindAndModifyMongoCommand(con).setDb(m.getConfig().connectionSettings().getDatabase())
                    .setColl(filter.getCollectionName()).setQuery(Doc.of(filter.toQueryObject()))
                    .setUpdate(Doc.of(update)).setUpsert(upsert).setNewFlag(returnNew);
            Map<String, Object> ret = cmd.execute();
            return ret == null ? null : m.getMapper().deserialize(Lease.class, ret);
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            } else {
                m.getDriver().releaseConnection(con);
            }
        }
    }

    private void assertDuplicateKey(Morphium morphium, Outcome<?> o) {
        MorphiumDriverException mde = o.find(MorphiumDriverException.class);
        assertNotNull(mde, "expected MorphiumDriverException in chain: " + o.describe());
        ReproSupport.report(morphium.getDriver().getName(), "H5 held lease details",
                            "class=" + mde.getClass().getName() + " mongoCode=" + mde.getMongoCode()
                                + " (" + (mde.getMongoCode() == null ? null : mde.getMongoCode().getClass().getSimpleName()) + ")"
                                + " mongoReason=" + mde.getMongoReason() + " writeErrors=" + mde.getWriteErrors()
                                + " thrownDirectly=" + (o.error == mde) + " message=" + mde.getMessage());
        assertNotNull(mde.getMongoCode(), o.describe());
        assertEquals(11000, ((Number) mde.getMongoCode()).intValue(), o.describe());
    }

    /**
     * Conditional upsert with the _id pinned in the update ({@code $setOnInsert._id}): the lease pattern as the
     * applications need it. Held lease -> MorphiumDriverException with numeric Mongo code 11000, thrown directly
     * (no wrapper), on a real MongoDB and on the InMemoryDriver.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void conditionalUpsertOnHeldLease(Morphium morphium) throws Exception {
        try (morphium) {
            String driver = morphium.getDriver().getName();

            // 1) A acquires: no document yet -> upsert inserts
            Outcome<Lease> first = ReproSupport.attempt(() -> acquireFilter(morphium, "job", "A").findOneAndUpdate(acquireUpdatePinned("job", "A", 60_000), true, true));
            ReproSupport.report(driver, "H5 first acquire (insert, pinned _id)", first.describe());
            assertFalse(first.failed(), first.describe());
            assertEquals("A", first.value.owner);
            assertEquals("job", first.value.id);

            // 2) B tries while A holds it: filter does not match, _id exists -> upsert would insert duplicate _id
            Outcome<Lease> second = ReproSupport.attempt(() -> acquireFilter(morphium, "job", "B").findOneAndUpdate(acquireUpdatePinned("job", "B", 60_000), true, true));
            ReproSupport.report(driver, "H5 held lease (non-matching filter, upsert=true)", second.describe());
            ReproSupport.report(driver, "H5 held lease: raw documents afterwards", String.valueOf(morphium.createQueryFor(Lease.class).asMapList()));
            assertTrue(second.failed(), "expected a failure, got " + second.describe());
            assertDuplicateKey(morphium, second);
            assertTrue(second.describe().contains("E11000") && second.describe().contains("duplicate key"), second.describe());

            // the document of A must be untouched
            Lease still = morphium.createQueryFor(Lease.class).f("_id").eq("job").get();
            assertEquals("A", still.owner);
            assertEquals(1, morphium.createQueryFor(Lease.class).countAll());

            // 3) A renews (owner matches): works and keeps one document
            Outcome<Lease> renew = ReproSupport.attempt(() -> acquireFilter(morphium, "job", "A").findOneAndUpdate(acquireUpdatePinned("job", "A", 120_000), true, true));
            ReproSupport.report(driver, "H5 renew by owner", renew.describe());
            assertFalse(renew.failed(), renew.describe());
            assertEquals("A", renew.value.owner);

            // 4) expired lease can be taken over by B
            Lease l = morphium.createQueryFor(Lease.class).f("_id").eq("job").get();
            l.expiresAt = new Date(System.currentTimeMillis() - 1000);
            morphium.store(l);
            Outcome<Lease> takeover = ReproSupport.attempt(() -> acquireFilter(morphium, "job", "B").findOneAndUpdate(acquireUpdatePinned("job", "B", 60_000), true, true));
            ReproSupport.report(driver, "H5 takeover of expired lease", takeover.describe());
            assertFalse(takeover.failed(), takeover.describe());
            assertEquals("B", takeover.value.owner);
            assertEquals(1, morphium.createQueryFor(Lease.class).countAll());
        }
    }

    /** Same pattern through a hand-built FindAndModifyMongoCommand (what applications do today). */
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void conditionalUpsertOnHeldLeaseViaRawCommand(Morphium morphium) throws Exception {
        try (morphium) {
            String driver = morphium.getDriver().getName();
            Outcome<Lease> first = ReproSupport.attempt(
                    () -> viaRawCommand(morphium, acquireFilter(morphium, "job", "A"), acquireUpdate("A", 60_000), true, true));
            ReproSupport.report(driver, "H5 raw command: first acquire", first.describe());
            assertFalse(first.failed(), first.describe());
            assertEquals("A", first.value.owner);
            assertEquals("job", first.value.id, "the filter's _id equality is used for the inserted document");

            Outcome<Lease> second = ReproSupport.attempt(
                    () -> viaRawCommand(morphium, acquireFilter(morphium, "job", "B"), acquireUpdate("B", 60_000), true, true));
            ReproSupport.report(driver, "H5 raw command: held lease", second.describe());
            ReproSupport.report(driver, "H5 raw command: raw documents after held lease", String.valueOf(morphium.createQueryFor(Lease.class).asMapList()));
            assertTrue(second.failed(), second.describe());
            assertDuplicateKey(morphium, second);
            assertEquals(1, morphium.createQueryFor(Lease.class).countAll());
        }
    }

    /**
     * MOR-003 (fixed): with a compound filter ({@code _id = X AND (... OR ...)}) the query object is
     * {@code {$and: [...]}}. {@code Query#findOneAndUpdate} used to look for the _id at top level only, added a generated
     * {@code $setOnInsert._id} and a real MongoDB rejected the very first insert with code 66 (immutable _id), while the
     * InMemoryDriver stored the document under the generated id. Now the _id reachable through {@code $and} counts as
     * pinned (the server seeds it from the filter, see docs/research/upsert-id-seeding-probe.md): the first acquire inserts
     * the document under "job", and a second contender gets the duplicate key error of a held lease on a real MongoDB.
     * The InMemoryDriver behaves the same since MOR-030 (it used to overwrite the held lease).
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void conditionalUpsertViaQueryKeepsFilterId(Morphium morphium) throws Exception {
        try (morphium) {
            String driver = morphium.getDriver().getName();
            ReproSupport.report(driver, "H5 filter shape", String.valueOf(acquireFilter(morphium, "job", "A").toQueryObject()));
            Outcome<Lease> first = ReproSupport.attempt(() -> acquireFilter(morphium, "job", "A").findOneAndUpdate(acquireUpdate("A", 60_000), true, true));
            ReproSupport.report(driver, "H5 Query path: first acquire", first.describe());
            assertFalse(first.failed(), first.describe());
            assertEquals("job", first.value.id);
            assertEquals("A", first.value.owner);
            assertEquals(1, morphium.createQueryFor(Lease.class).countAll());

            Outcome<Lease> second = ReproSupport.attempt(() -> acquireFilter(morphium, "job", "B").findOneAndUpdate(acquireUpdate("B", 60_000), true, true));
            ReproSupport.report(driver, "H5 Query path: second acquire", second.describe());
            ReproSupport.report(driver, "H5 Query path: raw documents afterwards", String.valueOf(morphium.createQueryFor(Lease.class).asMapList()));
            assertEquals(1, morphium.createQueryFor(Lease.class).countAll());

            assertTrue(second.failed(), second.describe());
            assertDuplicateKey(morphium, second);
            assertEquals("A", morphium.createQueryFor(Lease.class).f("_id").eq("job").get().owner);
        }
    }

    /** Same conditional upsert without upsert flag: no match is a plain null, no exception. */
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void conditionalUpdateWithoutUpsertReturnsNullOnNoMatch(Morphium morphium) throws Exception {
        try (morphium) {
            Lease l = new Lease();
            l.id = "job";
            l.owner = "A";
            l.expiresAt = new Date(System.currentTimeMillis() + 60_000);
            morphium.store(l);
            Outcome<Lease> o = ReproSupport.attempt(() -> acquireFilter(morphium, "job", "B").findOneAndUpdate(acquireUpdate("B", 1000), false, true));
            ReproSupport.report(morphium.getDriver().getName(), "H5 non-matching filter, upsert=false", o.describe());
            assertFalse(o.failed(), o.describe());
            assertNull(o.value);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void upsertInsertWithReturnNewFalseReturnsNull(Morphium morphium) throws Exception {
        try (morphium) {
            String driver = morphium.getDriver().getName();
            Query<Lease> q = morphium.createQueryFor(Lease.class).f("_id").eq("fresh");
            Outcome<Lease> o = ReproSupport.attempt(() -> q.findOneAndUpdate(acquireUpdate("A", 60_000), true, false));
            ReproSupport.report(driver, "H5 upsert insert, returnNew=false", o.describe());
            assertFalse(o.failed(), o.describe());
            assertNull(o.value, "returnNew=false on an insert returns null");
            List<Map<String, Object>> raw = morphium.createQueryFor(Lease.class).asMapList();
            ReproSupport.report(driver, "H5 upsert insert, returnNew=false: raw documents", String.valueOf(raw));
            assertEquals(1, raw.size());
            assertEquals("fresh", raw.get(0).get("_id"));
            assertEquals("A", raw.get(0).get("owner"));

            // second call on the now existing document returns the OLD state with returnNew=false
            Query<Lease> q2 = morphium.createQueryFor(Lease.class).f("_id").eq("fresh");
            Outcome<Lease> again = ReproSupport.attempt(() -> q2.findOneAndUpdate(acquireUpdate("B", 60_000), true, false));
            assertFalse(again.failed(), again.describe());
            assertEquals("A", again.value.owner, "old document is returned");
        }
    }

    // ---------------------------------------------------------------- @Version trap

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void versionTrapAfterSetOnInsertWithoutVersion(Morphium morphium) throws Exception {
        try (morphium) {
            String driver = morphium.getDriver().getName();
            Query<VersionedDoc> q = morphium.createQueryFor(VersionedDoc.class).f("_id").eq("d1");
            Outcome<VersionedDoc> up = ReproSupport.attempt(() -> q.findOneAndUpdate(
                    UtilsMap.of("$setOnInsert", UtilsMap.of("payload", "p")), true, true));
            ReproSupport.report(driver, "H5 version trap: upsert without version", up.describe());
            assertFalse(up.failed(), up.describe());
            assertNull(up.value.entityVersion, "no version was written, Java field stays null");
            Map<String, Object> raw = morphium.createQueryFor(VersionedDoc.class).asMapList().get(0);
            ReproSupport.report(driver, "H5 version trap: raw document", String.valueOf(raw));
            assertFalse(raw.containsKey("entityVersion"), "stored document has no version field: " + raw);

            VersionedDoc loaded = morphium.createQueryFor(VersionedDoc.class).f("_id").eq("d1").get();
            loaded.payload = "changed";
            Outcome<Boolean> st = ReproSupport.attempt(() -> {
                morphium.store(loaded);
                return Boolean.TRUE;
            });
            ReproSupport.report(driver, "H5 version trap: store() of the loaded entity", st.describe());
            assertTrue(st.failed(), "store() of an entity with version null is classified as new -> insert -> duplicate _id; got " + st.describe());
            MorphiumDriverException mde = st.find(MorphiumDriverException.class);
            ReproSupport.report(driver, "H5 version trap: store() failure details",
                                mde == null ? "no MorphiumDriverException in chain" : "mongoCode=" + mde.getMongoCode());
            assertTrue(ReproSupport.describe(st.error).toLowerCase().contains("duplicate")
                           || ReproSupport.describe(st.error).contains("11000"),
                       "expected a duplicate key failure: " + st.describe());
            // the stored document is unchanged
            assertEquals("p", morphium.createQueryFor(VersionedDoc.class).f("_id").eq("d1").get().payload);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void versionTrapPrimitiveLongVersion(Morphium morphium) throws Exception {
        try (morphium) {
            String driver = morphium.getDriver().getName();
            Query<PrimitiveVersionedDoc> q = morphium.createQueryFor(PrimitiveVersionedDoc.class).f("_id").eq("d1");
            Outcome<PrimitiveVersionedDoc> up = ReproSupport.attempt(() -> q.findOneAndUpdate(
                    UtilsMap.of("$setOnInsert", UtilsMap.of("payload", "p")), true, true));
            assertFalse(up.failed(), up.describe());
            assertEquals(0L, up.value.entityVersion);
            PrimitiveVersionedDoc loaded = morphium.createQueryFor(PrimitiveVersionedDoc.class).f("_id").eq("d1").get();
            loaded.payload = "changed";
            Outcome<Boolean> st = ReproSupport.attempt(() -> {
                morphium.store(loaded);
                return Boolean.TRUE;
            });
            ReproSupport.report(driver, "H5 version trap (primitive long): store()", st.describe());
            assertTrue(st.failed(), st.describe());
        }
    }

    /** Control: initialising the version in $setOnInsert (what applications do by hand) avoids the trap. */
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void versionInitialisedInSetOnInsertAvoidsTrap(Morphium morphium) throws Exception {
        try (morphium) {
            String driver = morphium.getDriver().getName();
            Query<VersionedDoc> q = morphium.createQueryFor(VersionedDoc.class).f("_id").eq("d2");
            Outcome<VersionedDoc> up = ReproSupport.attempt(() -> q.findOneAndUpdate(
                    UtilsMap.of("$setOnInsert", UtilsMap.of("payload", "p", "entityVersion", 1L)), true, true));
            assertFalse(up.failed(), up.describe());
            assertEquals(1L, up.value.entityVersion);
            VersionedDoc loaded = morphium.createQueryFor(VersionedDoc.class).f("_id").eq("d2").get();
            loaded.payload = "changed";
            Outcome<Boolean> st = ReproSupport.attempt(() -> {
                morphium.store(loaded);
                return Boolean.TRUE;
            });
            ReproSupport.report(driver, "H5 version control (version=1 in $setOnInsert): store()", st.describe());
            assertFalse(st.failed(), st.describe());
            VersionedDoc after = morphium.createQueryFor(VersionedDoc.class).f("_id").eq("d2").get();
            assertEquals("changed", after.payload);
            assertEquals(2L, after.entityVersion);
        }
    }
}
