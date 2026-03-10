package de.caluga.morphium;

import de.caluga.morphium.annotations.CascadeAware;
import de.caluga.morphium.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Helper class for cascade operations on @Reference fields.
 * Handles cascade delete and orphan removal with identity-based cycle detection.
 * Entities must be annotated with {@link CascadeAware} for cascade operations to take effect.
 */
public class CascadeHelper {
    private static final Logger log = LoggerFactory.getLogger(CascadeHelper.class);

    // Cycle detection for cascade delete
    private static final ThreadLocal<Set<Object>> deletingObjects =
        ThreadLocal.withInitial(() -> Collections.newSetFromMap(new IdentityHashMap<>()));

    // Pre→post state transfer for orphan removal
    private static final ThreadLocal<Map<Object, List<OrphanCandidate>>> pendingOrphans =
        ThreadLocal.withInitial(IdentityHashMap::new);

    // Track classes already checked for missing @CascadeAware (warn once per class)
    private static final Set<Class<?>> warnedClasses = ConcurrentHashMap.newKeySet();

    record OrphanCandidate(Class<?> type, Object id, String collection) {}

    /**
     * Scans entity for @Reference fields with cascadeDelete=true and deletes referenced objects.
     * Uses identity-based cycle detection to prevent infinite recursion on circular references.
     * Should be called BEFORE the parent entity is removed.
     */
    public static void cascadeDelete(MorphiumBase morphium, Object entity) {
        if (entity == null) return;

        AnnotationAndReflectionHelper arHelper = morphium.getARHelper();
        Object realEntity = arHelper.getRealObject(entity);
        if (realEntity == null) return;

        // Skip entirely if class is not marked @CascadeAware
        if (!arHelper.isAnnotationPresentInHierarchy(realEntity.getClass(), CascadeAware.class)) {
            warnIfCascadeFieldsPresent(arHelper, realEntity.getClass());
            return;
        }

        Set<Object> inProgress = deletingObjects.get();
        if (!inProgress.add(entity)) return; // Cycle detected, skip

        boolean isTopLevel = (inProgress.size() == 1);
        try {
            for (Field fld : arHelper.getAllFields(realEntity.getClass())) {
                if (!fld.isAnnotationPresent(Reference.class)) continue;
                Reference ref = fld.getAnnotation(Reference.class);
                if (!ref.cascadeDelete()) continue;

                fld.setAccessible(true);
                Object value;
                try {
                    value = fld.get(realEntity);
                } catch (IllegalAccessException e) {
                    log.error("Cannot access field " + fld.getName() + " for cascade delete", e);
                    continue;
                }
                if (value == null) continue;

                if (value instanceof Collection<?> coll) {
                    for (Object item : new ArrayList<>(coll)) {
                        if (item != null) deleteReferenced(morphium, item);
                    }
                } else if (value instanceof Map<?, ?> map) {
                    for (Object item : new ArrayList<>(map.values())) {
                        if (item != null) deleteReferenced(morphium, item);
                    }
                } else {
                    deleteReferenced(morphium, value);
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("Cascade delete processed for {}", realEntity.getClass().getSimpleName());
            }
        } finally {
            inProgress.remove(entity);
            if (isTopLevel) deletingObjects.remove();
        }
    }

    private static void deleteReferenced(MorphiumBase morphium, Object ref) {
        Object resolved = morphium.getARHelper().getRealObject(ref);
        if (resolved == null) return;

        try {
            Object id = morphium.getARHelper().getId(resolved);
            if (id != null) {
                morphium.delete(resolved); // Triggers cascade recursively if target also has cascadeDelete
            }
        } catch (Exception e) {
            log.warn("Could not cascade-delete referenced object: " + e.getMessage());
        }
    }

    /**
     * Called BEFORE store. Loads old version from DB and records reference IDs
     * for fields with orphanRemoval=true.
     * Does nothing for inserts (entity ID is null).
     */
    public static void collectOrphanCandidates(MorphiumBase morphium, Object entity) {
        if (entity == null) return;

        AnnotationAndReflectionHelper arHelper = morphium.getARHelper();
        Object realEntity = arHelper.getRealObject(entity);
        if (realEntity == null) return;

        // Skip entirely if class is not marked @CascadeAware
        if (!arHelper.isAnnotationPresentInHierarchy(realEntity.getClass(), CascadeAware.class)) {
            warnIfCascadeFieldsPresent(arHelper, realEntity.getClass());
            return;
        }

        Object entityId;
        try {
            entityId = arHelper.getId(realEntity);
        } catch (Exception e) {
            return; // No @Id field
        }
        if (entityId == null) return; // Insert, not update — nothing to orphan-check

        // Load old version from DB
        @SuppressWarnings("unchecked")
        Object oldEntity = morphium.findById(realEntity.getClass(), entityId);
        if (oldEntity == null) return; // New entity despite having ID set (e.g. manual ID)

        List<OrphanCandidate> candidates = new ArrayList<>();

        for (Field fld : arHelper.getAllFields(realEntity.getClass())) {
            if (!fld.isAnnotationPresent(Reference.class)) continue;
            Reference ref = fld.getAnnotation(Reference.class);
            if (!ref.orphanRemoval()) continue;

            fld.setAccessible(true);
            try {
                Set<Object> oldIds = extractReferenceIds(arHelper, fld.get(oldEntity));
                Set<Object> newIds = extractReferenceIds(arHelper, fld.get(realEntity));

                Class<?> refType = getReferencedType(fld);
                String coll = ref.targetCollection().equals(".") ? null : ref.targetCollection();
                for (Object orphanId : oldIds) {
                    if (!newIds.contains(orphanId)) {
                        candidates.add(new OrphanCandidate(refType, orphanId, coll));
                    }
                }
            } catch (IllegalAccessException e) {
                log.error("Cannot access field " + fld.getName() + " for orphan removal", e);
            }
        }

        if (!candidates.isEmpty()) {
            pendingOrphans.get().put(entity, candidates);
        }
    }

    /**
     * Called AFTER store. Deletes orphaned references collected in pre-phase.
     */
    public static void deleteOrphans(MorphiumBase morphium, Object entity) {
        if (entity == null) return;

        List<OrphanCandidate> orphans = pendingOrphans.get().remove(entity);
        if (orphans == null || orphans.isEmpty()) {
            cleanupPendingOrphans();
            return;
        }

        for (OrphanCandidate oc : orphans) {
            try {
                Object orphan;
                if (oc.collection() != null) {
                    orphan = morphium.findById(oc.type(), oc.id(), oc.collection());
                } else {
                    orphan = morphium.findById(oc.type(), oc.id());
                }
                if (orphan != null) {
                    morphium.delete(orphan);
                    if (log.isDebugEnabled()) {
                        log.debug("Orphan removed: {} with id {}", oc.type().getSimpleName(), oc.id());
                    }
                }
            } catch (Exception e) {
                log.warn("Could not delete orphaned reference: " + e.getMessage());
            }
        }

        cleanupPendingOrphans();
    }

    private static void cleanupPendingOrphans() {
        if (pendingOrphans.get().isEmpty()) {
            pendingOrphans.remove();
        }
    }

    /**
     * Discards pending orphan candidates for the given entity without deleting anything.
     * Called when the store operation fails, to prevent ThreadLocal leaks.
     */
    public static void clearPendingOrphans(Object entity) {
        if (entity == null) return;
        pendingOrphans.get().remove(entity);
        cleanupPendingOrphans();
    }

    /**
     * Warns once per class if @Reference(cascadeDelete/orphanRemoval) fields exist
     * but @CascadeAware is missing. The field scan happens at most once per class.
     */
    private static void warnIfCascadeFieldsPresent(AnnotationAndReflectionHelper arHelper, Class<?> cls) {
        if (!warnedClasses.add(cls)) return; // already checked

        for (Field fld : arHelper.getAllFields(cls)) {
            if (!fld.isAnnotationPresent(Reference.class)) continue;
            Reference ref = fld.getAnnotation(Reference.class);
            if (ref.cascadeDelete() || ref.orphanRemoval()) {
                log.warn("{} has @Reference fields with cascadeDelete/orphanRemoval but is missing "
                    + "@CascadeAware — cascade operations will be skipped. "
                    + "Add @CascadeAware to the class to enable them.", cls.getName());
                return;
            }
        }
    }

    /**
     * Extracts reference IDs from a field value (single object, collection, or map).
     */
    static Set<Object> extractReferenceIds(AnnotationAndReflectionHelper arHelper, Object value) {
        if (value == null) return Collections.emptySet();

        Set<Object> ids = new HashSet<>();

        if (value instanceof Collection<?> coll) {
            for (Object item : coll) {
                if (item != null) {
                    addIdIfPresent(arHelper, item, ids);
                }
            }
        } else if (value instanceof Map<?, ?> map) {
            for (Object item : map.values()) {
                if (item != null) {
                    addIdIfPresent(arHelper, item, ids);
                }
            }
        } else {
            addIdIfPresent(arHelper, value, ids);
        }

        return ids;
    }

    private static void addIdIfPresent(AnnotationAndReflectionHelper arHelper, Object obj, Set<Object> ids) {
        try {
            Object resolved = arHelper.getRealObject(obj);
            if (resolved != null) {
                Object id = arHelper.getId(resolved);
                if (id != null) {
                    ids.add(id);
                }
            }
        } catch (Exception e) {
            // Ignore — could be a non-entity value
        }
    }

    /**
     * Determines the referenced entity type from a field.
     * For Collection<T> or Map<K,T>, returns T. Otherwise returns the field type.
     */
    static Class<?> getReferencedType(Field fld) {
        Type genericType = fld.getGenericType();
        if (genericType instanceof ParameterizedType pt) {
            Type[] typeArgs = pt.getActualTypeArguments();
            if (Collection.class.isAssignableFrom(fld.getType()) && typeArgs.length == 1) {
                if (typeArgs[0] instanceof Class<?> clz) return clz;
            } else if (Map.class.isAssignableFrom(fld.getType()) && typeArgs.length == 2) {
                if (typeArgs[1] instanceof Class<?> clz) return clz;
            }
        }
        return fld.getType();
    }
}
