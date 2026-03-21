package de.caluga.morphium;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Package-private utility that centralises all ClassGraph access behind an availability check.
 * <p>
 * When ClassGraph is not on the classpath (e.g. in a Quarkus native-image build), {@link #isAvailable()}
 * will return {@code false}. Call sites that perform ClassGraph-based scans should guard those calls with
 * {@link #isAvailable()} and, if it returns {@code false}, skip scanning (typically returning empty results).
 * {@link #warnIfUnavailable()} can be used to emit a one-time warning in that case.
 * <p>
 * All ClassGraph types are referenced only within this class. No other production class
 * imports ClassGraph directly, ensuring that ClassGraph can be absent from the classpath
 * without triggering {@code NoClassDefFoundError} at class-load time.
 */
final class ClassGraphHelper {
    private static final Logger log = LoggerFactory.getLogger(ClassGraphHelper.class);
    private static volatile boolean available;
    private static final AtomicBoolean warned = new AtomicBoolean(false);

    static {
        available = checkClassGraphPresent();
    }

    private ClassGraphHelper() {}

    static boolean isAvailable() {
        if (available) {
            return true;
        }
        // Re-check: the TCCL may have changed since the static initializer ran
        // (e.g. Quarkus sets its classloader after the extension class is loaded).
        available = checkClassGraphPresent();
        return available;
    }

    private static boolean checkClassGraphPresent() {
        // Try the thread context classloader first (important for Quarkus / isolated classloaders),
        // then fall back to this class's defining classloader.
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        if (tccl != null) {
            try {
                Class.forName("io.github.classgraph.ClassGraph", false, tccl);
                return true;
            } catch (ClassNotFoundException ignored) {
            }
        }
        try {
            Class.forName("io.github.classgraph.ClassGraph", false, ClassGraphHelper.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException ignored) {
        }
        return false;
    }

    static void warnIfUnavailable() {
        if (!isAvailable() && warned.compareAndSet(false, true)) {
            log.warn("ClassGraph not on classpath and no entities pre-registered. "
                + "Runtime classpath scanning is disabled. "
                + "Use EntityRegistry.preRegisterEntities() or add ClassGraph to the classpath.");
        }
    }

    // ------------------------------------------------------------------
    // Scan methods — all ClassGraph types are confined to this class
    // ------------------------------------------------------------------

    /**
     * Scans the classpath for classes annotated with {@code @Entity} or {@code @Embedded}
     * and builds a typeId→FQCN map (used by {@link AnnotationAndReflectionHelper#init()}).
     *
     * @return mutable map of typeId→FQCN; empty if ClassGraph is unavailable
     */
    static Map<String, String> scanEntityTypeIds() {
        Map<String, String> result = new HashMap<>();
        if (!isAvailable()) return result;

        try (var scanResult = new io.github.classgraph.ClassGraph()
                .enableAnnotationInfo()
                .scan()) {
            var entities = scanResult.getClassesWithAnnotation(Entity.class.getName());
            entities.addAll(scanResult.getClassesWithAnnotation(Embedded.class.getName()));
            log.info("Found {} entities in classpath", entities.size());

            for (String cn : entities.getNames()) {
                var ci = scanResult.getClassInfo(cn);
                var an = ci.getAnnotationInfo();
                for (var ai : an) {
                    String name = ai.getName();
                    if (name.equals(Entity.class.getName()) || name.equals(Embedded.class.getName())) {
                        for (var param : ai.getParameterValues()) {
                            if (param.getName().equals("typeId")) {
                                result.put(param.getValue().toString(), cn);
                            }
                            result.put(cn, cn);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("ClassGraph scan for entity typeIds failed", e);
        }
        return result;
    }

    /**
     * Scans the classpath for classes annotated with {@code @Entity} and builds a
     * collectionName→Class map (used by {@link ObjectMapperImpl}).
     *
     * @param collectionNameResolver function to resolve collection name for a class
     * @return mutable map of collectionName→Class; empty if ClassGraph is unavailable
     */
    static Map<String, Class<?>> scanEntityCollections(java.util.function.Function<Class<?>, String> collectionNameResolver) {
        Map<String, Class<?>> result = new HashMap<>();
        if (!isAvailable()) return result;

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = ClassGraphHelper.class.getClassLoader();
        }

        try (var scanResult = new io.github.classgraph.ClassGraph()
                .enableAnnotationInfo()
                .overrideClassLoaders(cl)
                .scan()) {
            var entities = scanResult.getClassesWithAnnotation(Entity.class.getName());
            log.debug("Found {} entities in classpath", entities.size());

            for (String cn : entities.getNames()) {
                try {
                    Class<?> c = AnnotationAndReflectionHelper.classForName(cn);
                    result.put(collectionNameResolver.apply(c), c);
                } catch (ClassNotFoundException e) {
                    log.error("Could not get class / collection {}", cn);
                }
            }
        } catch (Exception e) {
            // swallow (e.g. ClassGraphException in restricted environments)
        }
        return result;
    }

    /**
     * Scans the classpath for classes annotated with the given annotation.
     * Filters out JDK/test framework packages.
     *
     * @return list of classes; empty if ClassGraph is unavailable
     */
    static List<Class<?>> scanForAnnotatedClasses(String annotationFqcn) {
        List<Class<?>> result = new ArrayList<>();
        if (!isAvailable()) return result;

        try (var scanResult = new io.github.classgraph.ClassGraph()
                .enableAnnotationInfo()
                .enableClassInfo()
                .scan()) {
            var classInfoList = scanResult.getClassesWithAnnotation(annotationFqcn);
            for (String cn : classInfoList.getNames()) {
                try {
                    if (cn.startsWith("sun.") || cn.startsWith("com.sun.")
                            || cn.startsWith("org.assertj.") || cn.startsWith("javax.")) {
                        continue;
                    }
                    result.add(AnnotationAndReflectionHelper.classForName(cn));
                } catch (Exception e) {
                    log.error("Could not load class {}", cn, e);
                }
            }
        } catch (Exception e) {
            log.error("ClassGraph scan for @{} failed", annotationFqcn, e);
        }
        return result;
    }

    /**
     * Scans for classes with the given annotation and applies a ClassGraph-native filter.
     * Used by the deprecated {@code checkIndices(ClassInfoFilter)} method.
     *
     * @param annotationFqcn the annotation to scan for
     * @param filter         a ClassGraph {@code ClassInfoFilter} (passed as Object to avoid ClassGraph import at call site)
     * @return list of class names matching the filter
     */
    static List<String> scanForAnnotatedClassNamesFiltered(String annotationFqcn, Object filter) {
        List<String> result = new ArrayList<>();
        if (!isAvailable()) return result;

        try (var scanResult = new io.github.classgraph.ClassGraph()
                .enableAnnotationInfo()
                .enableClassInfo()
                .scan()) {
            var classInfoList = scanResult.getClassesWithAnnotation(annotationFqcn);
            if (filter instanceof io.github.classgraph.ClassInfoList.ClassInfoFilter f) {
                classInfoList = classInfoList.filter(f);
            }
            result.addAll(classInfoList.getNames());
        } catch (Exception e) {
            log.error("ClassGraph scan for @{} failed", annotationFqcn, e);
        }
        return result;
    }

    /**
     * Scans for classes with the {@code @Messaging} annotation and returns the class
     * whose {@code name()} matches the given messaging implementation name.
     *
     * @return the messaging class, or {@code null} if not found
     */
    static Class<?> scanForMessagingImpl(String msgImplName) {
        if (!isAvailable()) return null;

        try (var scanResult = new io.github.classgraph.ClassGraph()
                .enableAllInfo()
                .scan()) {
            var entities = scanResult.getClassesWithAnnotation(
                    de.caluga.morphium.annotations.Messaging.class.getName());
            log.debug("Found {} messaging implementations in classpath", entities.size());

            for (String cn : entities.getNames()) {
                try {
                    Class<?> c = AnnotationAndReflectionHelper.classForName(cn);
                    var ann = (de.caluga.morphium.annotations.Messaging) c.getAnnotation(
                            de.caluga.morphium.annotations.Messaging.class);
                    if (ann.name().equals(msgImplName)) {
                        log.info("Using Messaging {}: {}", ann.name(), ann.description());
                        return c;
                    }
                } catch (Exception e) {
                    log.error("Error handling messaging implementation {}", cn, e);
                }
            }
        } catch (Exception e) {
            log.error("Could not scan for Messaging implementations", e);
        }
        return null;
    }

    /**
     * Scans for classes with the {@code @Driver} annotation and returns the class
     * whose {@code name()} matches the given driver name.
     *
     * @return the driver class, or {@code null} if not found
     */
    @SuppressWarnings("rawtypes")
    static Class<?> scanForDriverImpl(String driverName) {
        if (!isAvailable()) return null;

        try (var scanResult = new io.github.classgraph.ClassGraph()
                .enableAllInfo()
                .scan()) {
            var entities = scanResult.getClassesWithAnnotation(
                    de.caluga.morphium.annotations.Driver.class.getName());
            log.debug("Found {} drivers in classpath", entities.size());

            for (String cn : entities.getNames()) {
                try {
                    Class c = AnnotationAndReflectionHelper.classForName(cn);
                    if (java.lang.reflect.Modifier.isAbstract(c.getModifiers())) {
                        continue;
                    }
                    var driverAnnotation = (de.caluga.morphium.annotations.Driver) c.getAnnotation(
                            de.caluga.morphium.annotations.Driver.class);
                    if (driverAnnotation.name().equals(driverName)) {
                        return c;
                    }
                } catch (Throwable e) {
                    log.error("Could not load driver {}", driverName, e);
                }
            }
        } catch (Exception e) {
            log.error("ClassGraph scan for drivers failed", e);
        }
        return null;
    }
}
