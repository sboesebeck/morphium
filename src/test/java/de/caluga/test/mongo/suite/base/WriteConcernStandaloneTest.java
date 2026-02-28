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
