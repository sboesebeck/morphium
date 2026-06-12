package de.caluga.morphium;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Caches ClassGraph classpath scan results. The classpath doesn't change at runtime,
 * so scanning once and reusing the results avoids repeated ~100-500ms scans per
 * Morphium instance creation.
 */
public final class ClassGraphCache {

    private static final Logger log = LoggerFactory.getLogger(ClassGraphCache.class);

    private static volatile List<String> allClassNames;
    private static volatile ScanResult cachedScanResult;
    private static final Map<String, List<String>> classesWithAnnotation = new ConcurrentHashMap<>();
    private static final Map<String, List<String>> subclassesOf = new ConcurrentHashMap<>();
    /**
     * Build-time pre-registered class names per annotation. Unlike {@link #classesWithAnnotation}
     * (a scan cache cleared by {@link #invalidate()}), this map survives invalidation and always
     * takes precedence over a live scan. This follows the same pattern as the
     * {@code preRegisteredTypeIds} hook in {@code AnnotationAndReflectionHelper} (a separate map
     * that survives cache invalidation), and keeps a Quarkus native image scan-free even after a
     * cache invalidation, where a live scan would silently find nothing.
     */
    private static final Map<String, List<String>> preRegisteredClassesWithAnnotation = new ConcurrentHashMap<>();
    private static final Object lock = new Object();

    private ClassGraphCache() {}

    private static ScanResult getScanResult() {
        if (cachedScanResult == null) {
            synchronized (lock) {
                if (cachedScanResult == null) {
                    cachedScanResult = new ClassGraph()
                            .enableAnnotationInfo()
                            .enableClassInfo()
                            .scan();
                }
            }
        }
        return cachedScanResult;
    }

    /**
     * Get class names annotated with the given annotation.
     *
     * <p>A build-time pre-registration via {@link #preRegisterClassesWithAnnotation(String, List)}
     * always wins and is returned without a live scan. Otherwise the result is taken from (or
     * computed into) the scan cache on first lookup.
     */
    public static List<String> getClassesWithAnnotation(String annotationName) {
        List<String> preRegistered = preRegisteredClassesWithAnnotation.get(annotationName);
        if (preRegistered != null) {
            return preRegistered;
        }
        return classesWithAnnotation.computeIfAbsent(annotationName, name -> {
            ClassInfoList list = getScanResult().getClassesWithAnnotation(name);
            return List.copyOf(list.getNames());
        });
    }

    /**
     * Get class names that are subclasses of the given class.
     * Results are cached after first lookup.
     */
    public static List<String> getSubclassesOf(String className) {
        return subclassesOf.computeIfAbsent(className, name -> {
            ClassInfoList list = getScanResult().getSubclasses(name);
            return List.copyOf(list.getNames());
        });
    }

    /**
     * Get full ClassInfo for a specific annotation — for callers that need
     * annotation parameter values (e.g., AnnotationAndReflectionHelper).
     */
    public static ClassInfoList getClassInfoWithAnnotation(String annotationName) {
        return getScanResult().getClassesWithAnnotation(annotationName);
    }

    /**
     * Get ClassInfo for subclasses — for callers that need class metadata.
     */
    public static ClassInfoList getSubclassInfoOf(String className) {
        return getScanResult().getSubclasses(className);
    }

    /**
     * Pre-register the class names annotated with a given annotation, collected at build time.
     *
     * <p>A pre-registration always takes precedence over a live ClassGraph scan in
     * {@link #getClassesWithAnnotation(String)} and, unlike the scan cache, <strong>survives
     * {@link #invalidate()}</strong> — it is re-applied on the next lookup. This follows the same
     * pattern as the {@code registerTypeIds()} hook in {@code AnnotationAndReflectionHelper} (a
     * separate map that survives cache invalidation) and keeps a Quarkus native image scan-free
     * even across invalidations, where a live scan would silently find nothing.
     *
     * <p>Note the empty-list semantics differ from {@code registerTypeIds()}, which only skips the
     * scan when its map is non-empty: here an empty list is valid and intentional — it pins an
     * empty result and skips the live scan (e.g. an app with no
     * {@code @Capped}/{@code @Driver}/{@code @Messaging} classes).
     *
     * <p>Last call wins: a later pre-registration for the same annotation replaces the previous
     * one.
     *
     * <p>Note: this hook only covers the name-based {@link #getClassesWithAnnotation(String)}
     * path. {@link #getClassInfoWithAnnotation(String)} (used by the startup index check) and the
     * {@code subclassesOf} cache still require a live scan; native-image callers that must avoid
     * any scan should account for those paths separately.
     *
     * @param annotationName fully qualified annotation class name (must not be null)
     * @param classNames     list of class names to register (must not be null; may be empty)
     */
    public static void preRegisterClassesWithAnnotation(String annotationName, List<String> classNames) {
        Objects.requireNonNull(annotationName, "annotationName must not be null");
        Objects.requireNonNull(classNames, "classNames must not be null");
        preRegisteredClassesWithAnnotation.put(annotationName, List.copyOf(classNames));
        if (classNames.isEmpty()) {
            log.debug("preRegisterClassesWithAnnotation: empty class list for annotation {} — "
                    + "live scan skipped, no classes registered for it", annotationName);
        }
    }

    /**
     * Force cache invalidation. Useful for testing or hot-reload scenarios.
     *
     * <p>Clears the scan result and the scan caches. Build-time pre-registrations made via
     * {@link #preRegisterClassesWithAnnotation(String, List)} are intentionally <strong>kept</strong>,
     * so they keep taking effect after a re-scan. Use {@link #clearPreRegistrations()} to drop them.
     */
    public static void invalidate() {
        synchronized (lock) {
            if (cachedScanResult != null) {
                cachedScanResult.close();
                cachedScanResult = null;
            }
            classesWithAnnotation.clear();
            subclassesOf.clear();
            allClassNames = null;
        }
    }

    /**
     * Drop all build-time pre-registrations. Mainly for tests and hot-reload scenarios where the
     * set of entities changes; ordinary {@link #invalidate()} deliberately keeps them.
     */
    public static void clearPreRegistrations() {
        preRegisteredClassesWithAnnotation.clear();
    }
}
