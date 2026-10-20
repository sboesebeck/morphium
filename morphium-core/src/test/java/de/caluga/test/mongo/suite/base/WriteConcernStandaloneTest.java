package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.SafetyLevel;
import de.caluga.morphium.annotations.WriteSafety;
import de.caluga.morphium.config.CollectionCheckSettings.CappedCheck;
import de.caluga.morphium.config.CollectionCheckSettings.IndexCheck;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for WriteConcern downgrade on standalone MongoDB.
 * Verifies that @WriteSafety levels requiring a replica set are gracefully
 * downgraded to w:1 when running on a standalone MongoDB instance.
 */
@Tag("core")
public class WriteConcernStandaloneTest {

    private Morphium morphium;

    @AfterEach
    void tearDown() {
        if (morphium != null) {
            morphium.close();
            morphium = null;
        }
    }

    private Morphium createMorphium(boolean replicaSet) {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.driverSettings().setDriverName(InMemoryDriver.driverName);
        cfg.connectionSettings().setDatabase("wc_test");
        cfg.authSettings().setMongoAuthDb(null).setMongoLogin(null).setMongoPassword(null);
        cfg.clusterSettings().setHostSeed(new ArrayList<>());
        cfg.collectionCheckSettings()
           .setCappedCheck(CappedCheck.WARN_ON_STARTUP)
           .setIndexCheck(IndexCheck.WARN_ON_STARTUP);
        Morphium m = new Morphium(cfg);
        // Set replica set state on the driver (the actual source of truth used by getWriteConcernForClass)
        m.getDriver().setReplicaSet(replicaSet);
        return m;
    }

    @Test
    void standaloneDowngradesWaitForSlave() {
        morphium = createMorphium(false);
        WriteConcern wc = morphium.getWriteConcernForClass(WaitForSlaveEntity.class);
        assertNotNull(wc);
        assertEquals(1, wc.getW(), "WAIT_FOR_SLAVE (w:2) should be downgraded to w:1 on standalone");
    }

    @Test
    void standaloneDowngradesMajority() {
        morphium = createMorphium(false);
        WriteConcern wc = morphium.getWriteConcernForClass(MajorityEntity.class);
        assertNotNull(wc);
        assertEquals(1, wc.getW(), "MAJORITY should be downgraded to w:1 on standalone");
    }

    @Test
    void standaloneDowngradesWaitForAllSlaves() {
        morphium = createMorphium(false);
        WriteConcern wc = morphium.getWriteConcernForClass(WaitForAllSlavesEntity.class);
        assertNotNull(wc);
        assertEquals(1, wc.getW(), "WAIT_FOR_ALL_SLAVES (w:3) should be downgraded to w:1 on standalone");
    }

    @Test
    void standaloneKeepsBasic() {
        morphium = createMorphium(false);
        WriteConcern wc = morphium.getWriteConcernForClass(BasicEntity.class);
        assertNotNull(wc);
        assertEquals(1, wc.getW(), "BASIC (w:1) should remain w:1 on standalone");
    }

    @Test
    void replicaSetKeepsWaitForSlave() {
        morphium = createMorphium(true);
        WriteConcern wc = morphium.getWriteConcernForClass(WaitForSlaveEntity.class);
        assertNotNull(wc);
        assertEquals(2, wc.getW(), "WAIT_FOR_SLAVE (w:2) should remain w:2 on replica set");
    }

    /**
     * Stub with set size and reachable count decoupled - the InMemoryDriver can not tell a
     * one-member set from a larger set reduced to one reachable member, which is exactly the
     * distinction that decides between progress and a wtimeout stall.
     */
    private Morphium createMorphiumWithTopology(int replicaSetSize, int reachable) {
        Morphium m = createMorphium(true);
        InMemoryDriver stub = new InMemoryDriver() {
            @Override
            public boolean isReplicaSet() {
                return true;
            }

            @Override
            public int getReplicaSetSize() {
                return replicaSetSize;
            }

            @Override
            public int getNumReachableHosts() {
                return reachable;
            }
        };
        m.setDriver(stub);
        return m;
    }

    @Test
    void oneMemberSetUsesMajorityForWaitForAllSlaves() {
        // a genuine one-node set: "all copies" is w:1, which is acknowledged before the majority
        // commit point moves - the entity's own majority reads then miss the write. w:majority is
        // the same single member, journaled, and therefore visible to a majority read.
        morphium = createMorphiumWithTopology(1, 1);
        WriteConcern wc = morphium.getWriteConcernForClass(WaitForAllSlavesEntity.class);
        assertNotNull(wc);
        assertTrue(wc.getW() < 0, "WAIT_FOR_ALL_SLAVES on a one-member set must become w:majority, got w=" + wc.getW());
        assertEquals("majority", wc.asMap().get("w"));
    }

    @Test
    void largerSetReducedToOneReachableMemberKeepsW1ForWaitForAllSlaves() {
        // three configured members, one reachable (PSA with the secondary gone, or inside the
        // detection window): a server-side majority can not be satisfied, w:majority would only
        // run into wtimeout without the write becoming any more visible. The documented
        // "reachable count" behaviour must hold here.
        morphium = createMorphiumWithTopology(3, 1);
        WriteConcern wc = morphium.getWriteConcernForClass(WaitForAllSlavesEntity.class);
        assertNotNull(wc);
        assertEquals(1, wc.getW(), "a reduced cluster must keep w = reachable count, not majority");
        assertEquals(1, wc.asMap().get("w"));
    }

    @Test
    void replicaSetWithSeveralReachableMembersKeepsCountingForWaitForAllSlaves() {
        morphium = createMorphiumWithTopology(3, 3);
        WriteConcern wc = morphium.getWriteConcernForClass(WaitForAllSlavesEntity.class);
        assertNotNull(wc);
        assertEquals(3, wc.getW(), "with several reachable members w still follows their count");
    }

    @Test
    void unknownTopologyCountsAsOneMemberSet() {
        // no seed, no hello: set size and reachable count are both 0 (the InMemoryDriver with an
        // empty seed; a wire driver always has a seed and sees a hello at connect). Nothing larger
        // can ever be waited for, so this is the one-member case and gets majority - the
        // reduced-cluster path needs a known set size larger than one.
        morphium = createMorphium(true);
        assertEquals(0, morphium.getDriver().getNumReachableHosts(), "precondition: empty seed, nothing reachable");
        assertEquals(0, morphium.getDriver().getReplicaSetSize(), "precondition: empty seed, size unknown");
        WriteConcern wc = morphium.getWriteConcernForClass(WaitForAllSlavesEntity.class);
        assertNotNull(wc);
        assertEquals("majority", wc.asMap().get("w"));
    }

    @Test
    void noAnnotationReturnsNull() {
        morphium = createMorphium(false);
        WriteConcern wc = morphium.getWriteConcernForClass(NoAnnotationEntity.class);
        assertNull(wc, "Entity without @WriteSafety should return null");
    }

    @Test
    void standaloneResetsNegativeTimeout() {
        morphium = createMorphium(false);
        WriteConcern wc = morphium.getWriteConcernForClass(NegativeTimeoutEntity.class);
        assertNotNull(wc);
        assertEquals(0, wc.getWtimeout(), "Negative timeout should be reset to 0 on standalone");
    }

    // --- Test entity classes ---

    @Entity
    @WriteSafety(level = SafetyLevel.WAIT_FOR_SLAVE)
    static class WaitForSlaveEntity {
        @Id
        String id;
    }

    @Entity
    @WriteSafety(level = SafetyLevel.MAJORITY)
    static class MajorityEntity {
        @Id
        String id;
    }

    @Entity
    @WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
    static class WaitForAllSlavesEntity {
        @Id
        String id;
    }

    @Entity
    @WriteSafety(level = SafetyLevel.BASIC)
    static class BasicEntity {
        @Id
        String id;
    }

    @Entity
    static class NoAnnotationEntity {
        @Id
        String id;
    }

    @Entity
    @WriteSafety(level = SafetyLevel.BASIC, timeout = -1)
    static class NegativeTimeoutEntity {
        @Id
        String id;
    }
}
