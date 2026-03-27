package de.caluga.morphium;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Caches ClassGraph classpath scan results. The classpath doesn't change at runtime,
 * so scanning once and reusing the results avoids repeated ~100-500ms scans per
 * Morphium instance creation.
 */
public final class ClassGraphCache {

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
