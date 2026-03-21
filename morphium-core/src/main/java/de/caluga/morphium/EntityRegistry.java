package de.caluga.morphium;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            entities.add(cls);

            Entity entityAnn = cls.getAnnotation(Entity.class);
            Embedded embeddedAnn = cls.getAnnotation(Embedded.class);

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
        List<Class<?>> classes = new ArrayList<>();
        for (String cn : classNames) {
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

    // Package-private getters for AnnotationAndReflectionHelper / ObjectMapperImpl

    static Map<String, String> getPreRegisteredTypeIds() {
        return preRegisteredTypeIds;
    }

    static Set<Class<?>> getPreRegisteredEntities() {
        return preRegisteredEntities;
    }
}
