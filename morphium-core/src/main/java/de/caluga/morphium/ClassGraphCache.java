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
     * Results are cached after first lookup.
     */
    public static List<String> getClassesWithAnnotation(String annotationName) {
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
     * Pre-populate the annotation cache with a known list of class names.
     *
     * <p>Must be called <em>before</em> the first call to
     * {@link #getClassesWithAnnotation(String)} for the same annotation name.
     * When an entry is already present in the cache the pre-registered list wins
     * (the {@code computeIfAbsent} in {@code getClassesWithAnnotation} will find
     * the existing entry and skip the ClassGraph scan entirely).
     *
     * <p>Calling this method with an empty list is valid and intentional: it puts
     * an empty entry into the cache, which causes {@code getClassesWithAnnotation}
     * to return the empty list without triggering a live ClassGraph scan.
     *
     * <p>Primary use-case: Quarkus native image. ClassGraph finds nothing at
     * runtime because there is no live classpath; this method lets the
     * quarkus-morphium extension inject the Jandex-discovered class names that
     * were collected at build time.
     *
     * @param annotationName fully qualified annotation class name (must not be null)
     * @param classNames     list of class names to register (must not be null)
     */
    public static void preRegister(String annotationName, List<String> classNames) {
        Objects.requireNonNull(annotationName, "annotationName must not be null");
        Objects.requireNonNull(classNames, "classNames must not be null");
        if (classNames.isEmpty()) {
            log.warn("preRegister called with empty class list for annotation {} — "
                    + "ClassGraph scan will be skipped but no classes are registered", annotationName);
        }
        classesWithAnnotation.put(annotationName, List.copyOf(classNames));
    }

    /**
     * Force cache invalidation. Useful for testing or hot-reload scenarios.
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
}
