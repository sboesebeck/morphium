package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.CascadeHelper;
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
 * Unit tests for CascadeHelper caching and ThreadLocal cleanup.
 */
public class CascadeHelperTest {

    private AnnotationAndReflectionHelper arHelper;

    @BeforeEach
    void setUp() {
        arHelper = new AnnotationAndReflectionHelper(true);
        CascadeHelper.clearCaches();
    }

    @Test
    void testHasCascadeDeleteCached() {
        assertTrue(CascadeHelper.hasCascadeDelete(arHelper, WithCascadeDelete.class));
        // Second call should hit cache — same result
        assertTrue(CascadeHelper.hasCascadeDelete(arHelper, WithCascadeDelete.class));
    }

    @Test
    void testHasCascadeDeleteFalseForPlainEntity() {
        assertFalse(CascadeHelper.hasCascadeDelete(arHelper, PlainEntity.class));
    }

    @Test
    void testHasCascadeDeleteFalseForNormalReference() {
        assertFalse(CascadeHelper.hasCascadeDelete(arHelper, WithNormalReference.class));
    }

    @Test
    void testHasOrphanRemovalCached() {
        assertTrue(CascadeHelper.hasOrphanRemoval(arHelper, WithOrphanRemoval.class));
        assertTrue(CascadeHelper.hasOrphanRemoval(arHelper, WithOrphanRemoval.class));
    }

    @Test
    void testHasOrphanRemovalFalseForPlainEntity() {
        assertFalse(CascadeHelper.hasOrphanRemoval(arHelper, PlainEntity.class));
    }

    @Test
    void testClearCachesResetsResults() {
        // Populate cache
        assertTrue(CascadeHelper.hasCascadeDelete(arHelper, WithCascadeDelete.class));
        assertTrue(CascadeHelper.hasOrphanRemoval(arHelper, WithOrphanRemoval.class));

        // Clear
        CascadeHelper.clearCaches();

        // Should still return same results (re-computed from annotations)
        assertTrue(CascadeHelper.hasCascadeDelete(arHelper, WithCascadeDelete.class));
        assertTrue(CascadeHelper.hasOrphanRemoval(arHelper, WithOrphanRemoval.class));
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
    static class WithOrphanRemoval {
        @Id MorphiumId id;
        @Reference(orphanRemoval = true)
        List<PlainEntity> children;
    }
}
