package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.SafetyLevel;
import de.caluga.morphium.annotations.WriteSafety;
import de.caluga.morphium.bulk.MorphiumBulkContext;
import de.caluga.morphium.driver.BulkWriteException;
import de.caluga.morphium.driver.WriteConcernNotSatisfiedException;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * MOR-059 / GitHub #408: a write that is applied but does not satisfy its write concern raises a {@link WriteConcernNotSatisfiedException} (through the writer, not only
 * in the normaliser), and the data IS written. Needs a replica set: the entity asks for an acknowledgement of two nodes, a single-node replica set cannot give it.
 */
@Tag("core")
public class WriteConcernErrorTest extends MultiDriverTestBase {

    @Entity
    @WriteSafety(level = SafetyLevel.WAIT_FOR_SLAVE, timeout = 300)
    public static class Guarded {
        @Id
        public String id;
        public String code;
        public int counter;
    }

    private void setup(Morphium m) throws Exception {
        assumeTrue(m.getDriver().isReplicaSet(), "needs a replica set");
        assumeTrue(m.getDriver().getHostSeed().size() < 2, "the replica set must not be able to satisfy w:2");
        m.dropCollection(Guarded.class);
        // the document is stored without waiting for the second node: the stored entity is not what is tested
        Guarded g = new Guarded();
        g.id = "a";
        g.code = "x";

        try {
            m.store(g);
        } catch (de.caluga.morphium.driver.MorphiumDriverException e) {
            // the insert has the same problem; the document is there
        }
    }

    private Query<Guarded> byId(Morphium m) {
        return m.createQueryFor(Guarded.class).f("id").eq("a");
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aQuerySetThatMissesItsWriteConcernRaisesTheSubtypeAndTheDataIsWritten(Morphium m) throws Exception {
        try (m) {
            setup(m);
            WriteConcernNotSatisfiedException e = assertThrows(WriteConcernNotSatisfiedException.class, () -> byId(m).set("code", "y"));
            assertTrue(e.getMessage().contains("WAS APPLIED"), e.getMessage());
            assertNotNull(e.getMongoCode());
            assertFalse(e.isDuplicateKey());
            assertEquals("y", byId(m).get().code, "the write is applied");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theQueryOverrideSuppressesIt(Morphium m) throws Exception {
        try (m) {
            setup(m);
            Map<String, Object> reply = byId(m).setRaiseOnWriteErrors(false).set("code", "z");
            assertNotNull(reply.get("writeConcernError"), String.valueOf(reply));
            assertEquals("z", byId(m).get().code);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theEntityIncRaisesTheSubtypeToo(Morphium m) throws Exception {
        try (m) {
            setup(m);
            Guarded g = byId(m).get();
            assertThrows(WriteConcernNotSatisfiedException.class, () -> m.inc(g, "counter", 1));
            assertEquals(1, byId(m).get().counter, "applied");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void aBulkReportsItAsABulkWriteExceptionWithTheCountsOfWhatWasApplied(Morphium m) throws Exception {
        try (m) {
            setup(m);
            MorphiumBulkContext<Guarded> bulk = m.createBulkRequestContext(Guarded.class, true);
            bulk.addSetRequest(byId(m), "counter", 7, false, false);
            BulkWriteException e = assertThrows(BulkWriteException.class, bulk::runBulk);
            assertEquals(1, e.getFailures().size());
            assertTrue(e.getFailures().get(0) instanceof WriteConcernNotSatisfiedException, String.valueOf(e.getFailures().get(0)));
            assertEquals(1, ((Number) e.getPartialResult().get("num_matched")).intValue());
            assertEquals(7, byId(m).get().counter);
        }
    }
}
