package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.query.ReturnDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MOR-021: an {@link AtomicUpdate} runs inside a Morphium transaction like the raw commands do. The case is a conditional update WITHOUT upsert that claims the right to
 * write under a fencing token (it changes the lease document, so a concurrent takeover has to wait for the commit). Needs a replica set:
 * {@code MORPHIUM_TEST_MONGO_RS=host:port} with the set name {@code rs0} (or {@code MORPHIUM_TEST_MONGO_RS_NAME}); skipped without it.
 */
@Tag("external")
class AtomicUpdateTransactionTest {

    @Entity
    static class Lease {
        @Id
        private String id;
        private String leaseName;
        private String holder;
        private long fencingToken;
        private Date lastWriteClaimedAt;

        Lease() {}

        Lease(String leaseName, String holder, long fencingToken) {
            this.id = leaseName;
            this.leaseName = leaseName;
            this.holder = holder;
            this.fencingToken = fencingToken;
        }
    }

    static class LeaseRepo extends AbstractMorphiumRepository<Lease, String> {
        LeaseRepo(Morphium morphium) {
            super(new RepositoryMetadata(Lease.class, String.class, "id"));
            setMorphium(morphium);
        }
    }

    private static final String CLAIM = new AtomicUpdateSpec("leaseName = :leaseName AND fencingToken = :token", List.of("lastWriteClaimedAt=:now"), List.of(), List.of(), false, false,
            ReturnDocument.AFTER, "", "leaseName:0,token:1,now:2").encode();
    private static final String TAKEOVER = new AtomicUpdateSpec("leaseName = :leaseName", List.of("holder=:holder"), List.of(), List.of("fencingToken=1"), false, false,
            ReturnDocument.AFTER, "", "leaseName:0,holder:1").encode();

    private Morphium m;
    private Morphium other;
    private LeaseRepo repo;
    private LeaseRepo otherRepo;

    private static Morphium connect(String database) {
        String[] hp = System.getenv("MORPHIUM_TEST_MONGO_RS").split(":");
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase(database);
        cfg.addHostToSeed(hp[0], Integer.parseInt(hp[1]));
        cfg.setRequiredReplicaSetName(System.getenv().getOrDefault("MORPHIUM_TEST_MONGO_RS_NAME", "rs0"));
        cfg.setDriverName("PooledDriver");
        return new Morphium(cfg);
    }

    @BeforeEach
    void connect() {
        Assumptions.assumeTrue(System.getenv("MORPHIUM_TEST_MONGO_RS") != null, "needs MORPHIUM_TEST_MONGO_RS=host:port of a replica set");
        String database = "mor021tx_" + System.nanoTime();
        m = connect(database);
        other = connect(database);
        repo = new LeaseRepo(m);
        otherRepo = new LeaseRepo(other);
        m.store(new Lease("job", "A", 5));
    }

    @AfterEach
    void close() {
        if (m != null) {
            m.close();
        }

        if (other != null) {
            other.close();
        }
    }

    private Lease read() {
        return m.findById(Lease.class, "job", null);
    }

    @Test
    @DisplayName("the claim is part of the transaction: an abort takes it back, a commit keeps it")
    void abortAndCommit() throws Exception {
        m.startTransaction();
        assertThat(JdqlMethodBridge.executeAtomicBoolean(repo, CLAIM, new Object[] {"job", 5L, new Date(1000)})).isTrue();
        m.abortTransaction();
        assertThat(read().lastWriteClaimedAt).as("aborted").isNull();

        m.startTransaction();
        assertThat(JdqlMethodBridge.executeAtomicBoolean(repo, CLAIM, new Object[] {"job", 5L, new Date(2000)})).isTrue();
        m.commitTransaction();
        assertThat(read().lastWriteClaimedAt).as("committed").isEqualTo(new Date(2000));
    }

    @Test
    @DisplayName("a stale token is false and leaves the transaction usable: no error, the commit works")
    void staleTokenKeepsTheTransaction() throws Exception {
        m.startTransaction();
        assertThat(JdqlMethodBridge.executeAtomicBoolean(repo, CLAIM, new Object[] {"job", 4L, new Date(1000)})).isFalse();
        m.store(new Lease("other", "A", 1));
        m.commitTransaction();

        assertThat(read().lastWriteClaimedAt).isNull();
        assertThat(m.findById(Lease.class, "other", null)).isNotNull();
    }

    @Test
    @DisplayName("a takeover from another instance waits for the commit of the claiming transaction")
    void takeoverWaitsForTheCommit() throws Exception {
        m.startTransaction();
        assertThat(JdqlMethodBridge.executeAtomicBoolean(repo, CLAIM, new Object[] {"job", 5L, new Date(1000)})).isTrue();

        CompletableFuture<Long> takeover = CompletableFuture.supplyAsync(() -> JdqlMethodBridge.executeAtomicCount(otherRepo, TAKEOVER, new Object[] {"job", "B"}));

        assertThatThrownBy(() -> takeover.get(1500, TimeUnit.MILLISECONDS)).as("the takeover does not get through while the transaction is open").isInstanceOf(TimeoutException.class);
        assertThat(read().holder).isEqualTo("A");

        m.commitTransaction();

        assertThat(takeover.get(10, TimeUnit.SECONDS)).isEqualTo(1);
        Lease after = read();
        assertThat(after.holder).isEqualTo("B");
        assertThat(after.fencingToken).isEqualTo(6);
        assertThat(after.lastWriteClaimedAt).isEqualTo(new Date(1000));
    }
}
