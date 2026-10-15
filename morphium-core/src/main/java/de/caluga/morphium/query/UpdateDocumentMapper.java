package de.caluga.morphium.query;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Reference;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Prepares an update document for the driver: the values of every operator map ({@code $set}, {@code $setOnInsert},
 * {@code $push}, {@code $addToSet}, ...) are marshalled with {@link de.caluga.morphium.objectmapping.MorphiumObjectMapper#marshallUpdateValue(Object)}.
 * <p>
 * Contract (MOR-002):
 * <ul>
 * <li>An update document without any {@code $}-operator key (a replacement document) is returned as it is.</li>
 * <li>Field names are NOT translated here (MOR-041), keys pass through unchanged.</li>
 * <li>The mapping is idempotent and never modifies the caller's map; containers are copied.</li>
 * <li>A single value for {@code $push}, {@code $addToSet}, {@code $pull} and {@code $pullAll} is an array element and is
 * marshalled like one: an entity/embedded value carries {@code class_name} like the elements {@code store()} writes,
 * a {@code MorphiumId} becomes an {@code ObjectId}.</li>
 * <li>An entity for a field that is a {@code @Reference} is rejected: it would be embedded as a full document into a field that
 * the mapper expects to hold an id.</li>
 * </ul>
 * Package-private on purpose: the later update builder (MOR-006) calls it from {@code Query}.
 */
final class UpdateDocumentMapper {
    private static final Set<String> ELEMENT_OPERATORS = Set.of("$push", "$addToSet", "$pull", "$pullAll");
    private static final Set<String> REFERENCE_CHECKED_OPERATORS = Set.of("$set", "$setOnInsert", "$push", "$addToSet");

    private final Morphium morphium;
    private final Class<?> type;

    UpdateDocumentMapper(Morphium morphium, Class<?> type) {
        this.morphium = morphium;
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> map(Map<String, Object> update) {
        if (update == null || update.keySet().stream().noneMatch(k -> k != null && k.startsWith("$"))) {
            return update;
        }

        Map<String, Object> ret = new LinkedHashMap<>();

        for (Map.Entry<String, Object> op : update.entrySet()) {
            String operator = op.getKey();
            Object fields = op.getValue();

            if (operator == null || !operator.startsWith("$") || !(fields instanceof Map)) {
                ret.put(operator, fields);
                continue;
            }

            Map<String, Object> mapped = new LinkedHashMap<>();

            for (Map.Entry<String, Object> f : ((Map<String, Object>) fields).entrySet()) {
                if (REFERENCE_CHECKED_OPERATORS.contains(operator)) {
                    rejectEntityForReference(operator, f.getKey(), f.getValue());
                }

                mapped.put(f.getKey(), mapValue(operator, f.getValue()));
            }

            ret.put(operator, mapped);
        }

        return ret;
    }

    private Object mapValue(String operator, Object value) {
        if (ELEMENT_OPERATORS.contains(operator) && value != null && !(value instanceof Collection) && !(value instanceof Map)
                && !value.getClass().isArray()) {
            // a single array element: marshal it as a one element list to get the element form (class_name for entities,
            // ObjectId for a MorphiumId)
            return ((List<?>) morphium.getMapper().marshallUpdateValue(java.util.Collections.singletonList(value))).get(0);
        }

        return morphium.getMapper().marshallUpdateValue(value);
    }

    private void rejectEntityForReference(String operator, String fieldName, Object value) {
        if (fieldName == null || !containsEntity(value)) {
            return;
        }

        Field fld = morphium.getARHelper().getField(type, fieldName);

        if (fld != null && fld.isAnnotationPresent(Reference.class)) {
            throw new IllegalArgumentException("Field '" + fieldName + "' of " + type.getName() + " is a @Reference: " + operator
                                               + " would embed the entity as a document. Pass the id of the referenced entity (or a MorphiumReference) instead.");
        }
    }

    private boolean containsEntity(Object value) {
        if (value == null) {
            return false;
        }

        if (value instanceof Map) {
            for (Object v : ((Map<?, ?>) value).values()) {
                if (containsEntity(v)) {
                    return true;
                }
            }

            return false;
        }

        if (value instanceof Collection) {
            for (Object v : (Collection<?>) value) {
                if (containsEntity(v)) {
                    return true;
                }
            }

            return false;
        }

        if (value.getClass().isArray() && !value.getClass().getComponentType().isPrimitive()) {
            List<Object> l = new ArrayList<>();
            for (int i = 0; i < java.lang.reflect.Array.getLength(value); i++) {
                l.add(java.lang.reflect.Array.get(value, i));
            }
            return containsEntity(l);
        }

        return morphium.getARHelper().isEntity(value);
    }
}
