package de.caluga.morphium;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Central pre-registration API for entity classes.
 * <p>
 * Frameworks (Quarkus, Spring Boot, etc.) call {@link #preRegisterEntities(Collection)}
 * <b>before</b> creating any {@link Morphium} instance. When pre-registered entities exist,
 * ClassGraph classpath scanning is skipped entirely — making ClassGraph an optional
 * dependency at runtime.
 * <p>
 * Standalone users who have ClassGraph on the classpath need not call this class at all;
 * the existing scan-based discovery remains as a transparent fallback.
 */
public final class EntityRegistry {
    private static final Logger log = LoggerFactory.getLogger(EntityRegistry.class);

    private static volatile Map<String, String> preRegisteredTypeIds;      // typeId → FQCN
    private static volatile Set<Class<?>> preRegisteredEntities;           // all entity classes

    private EntityRegistry() {}

    /**
     * Pre-register entity classes discovered at build time (or by a framework scanner).
     * Must be called before the first {@link Morphium} instance is created.
     */
    public static synchronized void preRegisterEntities(Collection<Class<?>> entityClasses) {
        if (entityClasses == null) {
            throw new IllegalArgumentException("entityClasses must not be null");
        }
        Map<String, String> typeIds = new ConcurrentHashMap<>();
        Set<Class<?>> entities = Collections.newSetFromMap(new ConcurrentHashMap<>());

        for (Class<?> cls : entityClasses) {
            if (cls == null) {
                continue;
            }

            // Use hierarchy-aware lookup to match AnnotationAndReflectionHelper behaviour.
            // This ensures subclasses that inherit @Entity/@Embedded from a superclass are registered.
            Entity entityAnn = findAnnotationInHierarchy(cls, Entity.class);
            Embedded embeddedAnn = findAnnotationInHierarchy(cls, Embedded.class);

            // Only register classes that are actually annotated with @Entity or @Embedded.
            // Unannotated classes would disable ClassGraph scanning without providing mappings.
            if (entityAnn == null && embeddedAnn == null) {
                log.debug("Skipping unannotated class in pre-registration: {}", cls.getName());
                continue;
            }
            entities.add(cls);

            String fqcn = cls.getName();

            if (entityAnn != null) {
                if (!entityAnn.typeId().equals(".")) {
                    typeIds.put(entityAnn.typeId(), fqcn);
                }
                typeIds.put(fqcn, fqcn);
            }

            if (embeddedAnn != null) {
                if (!embeddedAnn.typeId().equals(".")) {
                    typeIds.put(embeddedAnn.typeId(), fqcn);
                }
                typeIds.put(fqcn, fqcn);
            }
        }

        preRegisteredTypeIds = typeIds;
        preRegisteredEntities = entities;
        log.info("Pre-registered {} entity classes with {} type IDs", entities.size(), typeIds.size());
    }

    /**
     * Pre-register entity classes by fully-qualified class name.
     * Useful when the framework only has class names (e.g. from Jandex index).
     */
    public static synchronized void preRegisterEntityNames(Collection<String> classNames) {
        if (classNames == null) {
            throw new IllegalArgumentException("classNames must not be null");
        }
        List<Class<?>> classes = new ArrayList<>();
        for (String cn : classNames) {
            if (cn == null) {
                log.warn("Encountered null class name while pre-registering entity names – skipping");
                continue;
            }
            try {
                classes.add(AnnotationAndReflectionHelper.classForName(cn));
            } catch (ClassNotFoundException e) {
                log.warn("Could not load pre-registered entity class: {}", cn);
            }
        }
        preRegisterEntities(classes);
    }

    /**
     * Returns {@code true} if entities have been pre-registered via
     * {@link #preRegisterEntities(Collection)} or {@link #preRegisterEntityNames(Collection)}.
     */
    public static boolean hasPreRegisteredEntities() {
        return preRegisteredEntities != null && !preRegisteredEntities.isEmpty();
    }

    /**
     * Clears all pre-registered data. Useful for hot-reload scenarios (e.g. Quarkus dev mode).
     */
    public static synchronized void clear() {
        preRegisteredTypeIds = null;
        preRegisteredEntities = null;
    }

    /**
     * Returns the pre-registered typeId→FQCN map, or an empty map if nothing was registered.
     * The returned map is unmodifiable.
     */
    public static Map<String, String> getPreRegisteredTypeIds() {
        Map<String, String> snapshot = preRegisteredTypeIds;
        if (snapshot == null || snapshot.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(snapshot);
    }

    /**
     * Returns the set of pre-registered entity classes, or an empty set if nothing was registered.
     * The returned set is unmodifiable.
     */
    public static Set<Class<?>> getPreRegisteredEntities() {
        Set<Class<?>> snapshot = preRegisteredEntities;
        if (snapshot == null || snapshot.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(snapshot);
    }

    /**
     * Walks the class hierarchy (superclasses and interfaces) looking for the given annotation.
     * This mirrors the logic in {@link AnnotationAndReflectionHelper#getAnnotationFromHierarchy}
     * without requiring an instance of that helper.
     */
    private static <T extends Annotation> T findAnnotationInHierarchy(Class<?> cls, Class<T> annotationClass) {
        // Direct check first
        T ann = cls.getAnnotation(annotationClass);
        if (ann != null) return ann;

        // Walk superclass chain
        Class<?> tmp = cls.getSuperclass();
        while (tmp != null && !tmp.equals(Object.class)) {
            ann = tmp.getAnnotation(annotationClass);
            if (ann != null) return ann;
            tmp = tmp.getSuperclass();
        }

        // Check interfaces (breadth-first)
        ArrayDeque<Class<?>> interfaces = new ArrayDeque<>();
        Collections.addAll(interfaces, cls.getInterfaces());
        while (!interfaces.isEmpty()) {
            Class<?> iface = interfaces.pollFirst();
            if (iface != null) {
                ann = iface.getAnnotation(annotationClass);
                if (ann != null) return ann;
                Collections.addAll(interfaces, iface.getInterfaces());
            }
        }
        return null;
    }
}
