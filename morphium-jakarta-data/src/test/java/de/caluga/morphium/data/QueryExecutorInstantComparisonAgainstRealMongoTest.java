package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.data.QueryDescriptor.Combinator;
import de.caluga.morphium.data.QueryDescriptor.Condition;
import de.caluga.morphium.data.QueryDescriptor.Operator;
import de.caluga.morphium.data.QueryDescriptor.Prefix;
import de.caluga.morphium.data.QueryDescriptor.ReturnType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces, against a REAL MongoDB (not {@code InMemoryDriver}), the exact effect a consumer
 * reported: a derived repository method (e.g. {@code findBy...LessThan(Instant)}) comparing an
 * {@code Instant} field silently returns zero matches when
 * {@code ObjectMappingSettings#setUseBsonDateForJavaTime(true)} is set, while the equivalent
 * comparison built via the type-safe {@code query.f(...)} API — the same mechanism {@code
 * @Query}(JDQL) methods use, see {@link JdqlMethodBridge#applyCondition} — matches correctly.
 * <p>
 * <b>Why this test must NOT run against {@code InMemoryDriver}:</b> the in-memory driver does not
 * normalise values on its write path (morphium #336), so a stored {@code Instant} and a raw,
 * unmapped {@code Instant} filter value are compared as identical Java objects regardless of any
 * mapping gap in {@code QueryExecutor}. Both the failure this test pins and the fix for it are
 * only observable through the real BSON encoding path ({@code BsonEncoder}), which only the wire
 * driver against a real {@code mongod} exercises.
 * <p>
 * Requires a MongoDB replica set reachable at {@code localhost:27117} with replica set name
 * {@code rs0} by default (overridable via {@code MONGO_TEST_HOST}/{@code MONGO_TEST_PORT}/
 * {@code MONGO_TEST_REPLICA_SET}), started e.g. via
 * {@code docker run -d -p 27117:27017 mongo:7.0 --replSet rs0 --bind_ip_all} followed by
 * {@code rs.initiate()}.
 */
@Tag("external")
class QueryExecutorInstantComparisonAgainstRealMongoTest {

    private static final String MONGO_TEST_HOST = System.getProperty(
            "morphium.test.mongoHost", System.getenv().getOrDefault("MONGO_TEST_HOST", "localhost"));
    private static final int MONGO_TEST_PORT = Integer.parseInt(System.getProperty(
            "morphium.test.mongoPort", System.getenv().getOrDefault("MONGO_TEST_PORT", "27117")));
    private static final String MONGO_TEST_REPLICA_SET = System.getProperty(
            "morphium.test.mongoReplicaSet", System.getenv().getOrDefault("MONGO_TEST_REPLICA_SET", "rs0"));

    private static Morphium morphium;

    @Entity(collectionName = "job_lease_repro")
    static class JobLease {
        @Id
        private String id;
        private String leaseName;
        private Instant expiresAt;

        JobLease() {}

        JobLease(String id, String leaseName, Instant expiresAt) {
            this.id = id;
            this.leaseName = leaseName;
            this.expiresAt = expiresAt;
        }
    }

    static class JobLeaseRepositoryImpl extends AbstractMorphiumRepository<JobLease, String> {
        JobLeaseRepositoryImpl(Morphium morphium) {
            super(new RepositoryMetadata(JobLease.class, String.class, "id"));
            setMorphium(morphium);
        }
    }

    private JobLeaseRepositoryImpl repo;

    @BeforeAll
    static void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings().setDatabase("morphium_jdata_instant_repro");
        cfg.clusterSettings().addHostToSeed(MONGO_TEST_HOST, MONGO_TEST_PORT);
        cfg.clusterSettings().setRequiredReplicaSetName(MONGO_TEST_REPLICA_SET);
        cfg.driverSettings().setDriverName("PooledDriver");
        // The exact opt-in the reported bug depends on: without it, Instant already stays on the
        // legacy sub-document format everywhere and this test would pass for the wrong reason.
        cfg.objectMappingSettings().setUseBsonDateForJavaTime(true);
        morphium = new Morphium(cfg);
    }

    @AfterAll
    static void tearDown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    @BeforeEach
    void initRepo() {
        morphium.clearCollection(JobLease.class);
        repo = new JobLeaseRepositoryImpl(morphium);
    }

    @Test
    @DisplayName("LT on an Instant field matches a native-BSON-date-stored value, via a derived query")
    void derivedLessThanMatchesNativeBsonDateStoredInstant() {
        Instant expired = Instant.now().minusSeconds(3600);
        Instant stillValid = Instant.now().plusSeconds(3600);

        // store() is the ObjectMapper-driven path: with the flag on, this writes expiresAt as a
        // native BSON Date (0x09) on disk, exactly as InstantMapper#marshall does with
        // useBsonDate=true. This is the actual on-disk shape the reported bug compares against.
        morphium.store(new JobLease("expired-1", "relay", expired));
        morphium.store(new JobLease("valid-1", "relay", stillValid));

        // Sanity check first, via the type-safe API (Pfad A from the report): this must already
        // work, or the test setup itself — not QueryExecutor — is broken.
        List<JobLease> viaFieldApi = morphium.createQueryFor(JobLease.class)
                .f("expiresAt").lt(Instant.now())
                .asList();
        assertThat(viaFieldApi)
                .as("sanity check: query.f(...).lt(Instant) must already match the expired lease "
                        + "-- MongoFieldImpl.checkValue routes it through the same custom mapper "
                        + "store() uses, so this path was never affected by the reported bug")
                .extracting(l -> l.id)
                .containsExactly("expired-1");

        // The reported bug: the equivalent derived method, e.g.
        // "findByLeaseNameAndExpiresAtLessThan(String, Instant)", built by QueryExecutor.execute
        // via buildRawCondition. Before the fix, args[cond.paramIndex()] reached BsonEncoder
        // unmapped and was encoded in Instant's legacy sub-document format regardless of the
        // flag -- a different BSON type than the native date expiresAt was stored as, so this
        // query matched nothing at all.
        QueryDescriptor descriptor = new QueryDescriptor(
                Prefix.FIND,
                List.of(
                        new Condition("leaseName", Operator.EQ, 0),
                        new Condition("expiresAt", Operator.LT, 1)),
                Combinator.AND,
                List.of(),
                ReturnType.LIST);

        @SuppressWarnings("unchecked")
        List<JobLease> viaDerivedMethod = (List<JobLease>) QueryExecutor.execute(
                descriptor, new Object[]{"relay", Instant.now()}, repo);

        assertThat(viaDerivedMethod)
                .as("a derived findBy...LessThan(Instant) query must match the same document the "
                        + "type-safe API matches above -- both compare the same on-disk field "
                        + "against 'now', so a discrepancy here is exactly the reported mapping gap")
                .extracting(l -> l.id)
                .containsExactly("expired-1");
    }

    @Test
    @DisplayName("IN on a List<Instant> maps every element, not the collection as a whole")
    void derivedInMapsEveryElementOfAnInstantList() {
        Instant stored = Instant.parse("2025-06-15T10:30:45.123Z");
        morphium.store(new JobLease("in-1", "relay", stored));

        // Deliberately NOT the exact same Instant instance/precision path as store() took --
        // this is the caller's own value, constructed independently, the way a real IN-list
        // argument would be. If mapCollection() mapped the List's own class (never a registered
        // custom mapper) instead of each element, this element would stay in Instant's legacy
        // sub-document format and never match the native date stored above.
        QueryDescriptor descriptor = new QueryDescriptor(
                Prefix.FIND,
                List.of(new Condition("expiresAt", Operator.IN, 0)),
                Combinator.AND,
                List.of(),
                ReturnType.LIST);

        @SuppressWarnings("unchecked")
        List<JobLease> result = (List<JobLease>) QueryExecutor.execute(
                descriptor, new Object[]{List.of(stored)}, repo);

        assertThat(result)
                .as("IN must map each Instant element of the list individually")
                .extracting(l -> l.id)
                .containsExactly("in-1");
    }
}
