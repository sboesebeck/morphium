package de.caluga.test.morphium;

import de.caluga.morphium.ClassGraphCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the write-once semantics of {@link ClassGraphCache#preRegister(String, java.util.List)}:
 * a pre-registered entry must not be overwritten by a later preRegister, keeping the cache
 * immutable after initialization (addresses the Copilot review note on PR #200).
 */
@Tag("core")
public class ClassGraphCachePreRegisterTest {

    // unique, made-up annotation names so we never hit a real ClassGraph scan
    private static final String ANNO = "de.caluga.test.NoSuchAnnotation_PreRegisterTest";
    private static final String ANNO_EMPTY = "de.caluga.test.NoSuchAnnotation_PreRegisterEmptyTest";

    @BeforeEach
    @AfterEach
    public void reset() {
        ClassGraphCache.invalidate();
    }

    @Test
    public void preRegisterSeedsTheCache() {
        ClassGraphCache.preRegister(ANNO, List.of("com.example.A", "com.example.B"));
        assertEquals(List.of("com.example.A", "com.example.B"),
                ClassGraphCache.getClassesWithAnnotation(ANNO));
    }

    @Test
    public void secondPreRegisterDoesNotOverwrite() {
        ClassGraphCache.preRegister(ANNO, List.of("com.example.A"));
        // a later call must NOT replace the existing entry (write-once)
        ClassGraphCache.preRegister(ANNO, List.of("com.example.X", "com.example.Y"));
        assertEquals(List.of("com.example.A"),
                ClassGraphCache.getClassesWithAnnotation(ANNO),
                "an already-registered entry must be kept unchanged");
    }

    @Test
    public void emptyPreRegisterSkipsScanAndReturnsEmpty() {
        ClassGraphCache.preRegister(ANNO_EMPTY, List.of());
        assertTrue(ClassGraphCache.getClassesWithAnnotation(ANNO_EMPTY).isEmpty(),
                "an empty pre-registration must return empty without a live scan");
    }

    @Test
    public void preRegisterRejectsNullArguments() {
        assertThrows(NullPointerException.class, () -> ClassGraphCache.preRegister(null, List.of()));
        assertThrows(NullPointerException.class, () -> ClassGraphCache.preRegister(ANNO, null));
    }
}
