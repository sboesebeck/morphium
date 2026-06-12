package de.caluga.test.morphium;

import de.caluga.morphium.ClassGraphCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pins the contract of {@link ClassGraphCache#preRegisterClassesWithAnnotation(String, List)}:
 * pre-registrations win over a live scan, follow last-write-wins, accept an empty list, and
 * survive {@link ClassGraphCache#invalidate()} (addresses the review feedback on PR #200).
 */
@Tag("core")
public class ClassGraphCachePreRegisterTest {

    // unique, made-up annotation names so we never trigger a real ClassGraph scan
    private static final String ANNO = "de.caluga.test.NoSuchAnnotation_PreRegisterTest";
    private static final String ANNO_EMPTY = "de.caluga.test.NoSuchAnnotation_PreRegisterEmptyTest";

    @BeforeEach
    @AfterEach
    public void reset() {
        ClassGraphCache.invalidate();
        ClassGraphCache.clearPreRegistrations();
    }

    @Test
    public void preRegisterSeedsTheLookup() {
        ClassGraphCache.preRegisterClassesWithAnnotation(ANNO, List.of("com.example.A", "com.example.B"));
        assertEquals(List.of("com.example.A", "com.example.B"),
                ClassGraphCache.getClassesWithAnnotation(ANNO));
    }

    @Test
    public void lastPreRegisterWins() {
        ClassGraphCache.preRegisterClassesWithAnnotation(ANNO, List.of("com.example.A"));
        ClassGraphCache.preRegisterClassesWithAnnotation(ANNO, List.of("com.example.X", "com.example.Y"));
        assertEquals(List.of("com.example.X", "com.example.Y"),
                ClassGraphCache.getClassesWithAnnotation(ANNO),
                "a later pre-registration must replace the previous one (last-write-wins)");
    }

    @Test
    public void emptyPreRegisterSkipsScanAndReturnsEmpty() {
        ClassGraphCache.preRegisterClassesWithAnnotation(ANNO_EMPTY, List.of());
        assertTrue(ClassGraphCache.getClassesWithAnnotation(ANNO_EMPTY).isEmpty(),
                "an empty pre-registration must return empty without a live scan");
    }

    @Test
    public void preRegistrationSurvivesInvalidate() {
        ClassGraphCache.preRegisterClassesWithAnnotation(ANNO, List.of("com.example.A"));
        // invalidate() drops the scan caches but must keep build-time pre-registrations,
        // otherwise the next lookup would fall back to a live scan (empty in a native image).
        ClassGraphCache.invalidate();
        assertEquals(List.of("com.example.A"),
                ClassGraphCache.getClassesWithAnnotation(ANNO),
                "pre-registered entries must survive invalidate()");
    }

    @Test
    public void clearPreRegistrationsDropsThem() {
        ClassGraphCache.preRegisterClassesWithAnnotation(ANNO_EMPTY, List.of());
        ClassGraphCache.clearPreRegistrations();
        // ANNO_EMPTY was made-up and not on the classpath, so after clearing, the lookup falls
        // back to a live scan that finds nothing — still empty, but no longer pinned.
        assertTrue(ClassGraphCache.getClassesWithAnnotation(ANNO_EMPTY).isEmpty());
    }

    @Test
    public void preRegisterRejectsNullArguments() {
        assertThrows(NullPointerException.class,
                () -> ClassGraphCache.preRegisterClassesWithAnnotation(null, List.of()));
        assertThrows(NullPointerException.class,
                () -> ClassGraphCache.preRegisterClassesWithAnnotation(ANNO, null));
    }
}
