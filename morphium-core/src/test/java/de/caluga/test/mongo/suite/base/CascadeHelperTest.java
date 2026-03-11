package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.CascadeHelper;
import de.caluga.morphium.annotations.CascadeAware;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Reference;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.MorphiumId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for @CascadeAware marker annotation detection and CascadeHelper ThreadLocal cleanup.
 */
public class CascadeHelperTest {

    private AnnotationAndReflectionHelper arHelper;

    @BeforeEach
    void setUp() {
        arHelper = new AnnotationAndReflectionHelper(true);
    }

    @Test
    void testCascadeAwareDetected() {
        assertTrue(arHelper.isAnnotationPresentInHierarchy(WithCascadeDelete.class, CascadeAware.class));
    }

    @Test
    void testCascadeAwareNotOnPlainEntity() {
        assertFalse(arHelper.isAnnotationPresentInHierarchy(PlainEntity.class, CascadeAware.class));
    }

    @Test
    void testCascadeAwareNotOnNormalReference() {
        assertFalse(arHelper.isAnnotationPresentInHierarchy(WithNormalReference.class, CascadeAware.class));
    }

    @Test
    void testCascadeAwareDetectedForOrphanRemoval() {
        assertTrue(arHelper.isAnnotationPresentInHierarchy(WithOrphanRemoval.class, CascadeAware.class));
    }

    @Test
    void testClearPendingOrphansDoesNotThrowWhenEmpty() {
        // Should not throw even when nothing is pending
        assertDoesNotThrow(() -> CascadeHelper.clearPendingOrphans(new Object()));
        assertDoesNotThrow(() -> CascadeHelper.clearPendingOrphans(null));
    }

    // Test entities

    @Entity
    @NoCache
    static class PlainEntity {
        @Id MorphiumId id;
        String name;
    }

    @Entity
    @NoCache
    @CascadeAware
    static class WithCascadeDelete {
        @Id MorphiumId id;
        @Reference(cascadeDelete = true)
        PlainEntity child;
    }

    @Entity
    @NoCache
    static class WithNormalReference {
        @Id MorphiumId id;
        @Reference
        PlainEntity child;
    }

    @Entity
    @NoCache
    @CascadeAware
    static class WithOrphanRemoval {
        @Id MorphiumId id;
        @Reference(orphanRemoval = true)
        List<PlainEntity> children;
    }
}
