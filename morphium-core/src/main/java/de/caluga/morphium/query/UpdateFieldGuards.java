package de.caluga.morphium.query;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.MorphiumStorageListener.UpdateTypes;
import de.caluga.morphium.annotations.Reference;
import de.caluga.morphium.annotations.encryption.Encrypted;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The refusals that every update entry point shares (MOR-049): the update document mapper ({@code findOneAndUpdate(Map)}, the update builder) and the writers behind
 * {@code Query.set}, {@code push} and {@code pushAll}. An entity for a {@code @Reference} field would be embedded as a full document, and a plaintext value for an
 * {@code @Encrypted} field bypasses the encryption and cannot be read back. Both are an {@link IllegalArgumentException} before anything is written.
 */
public final class UpdateFieldGuards {
    private UpdateFieldGuards() {
    }

    /** @param target the field that {@code fieldName} names, or {@code null} for an unknown or a nested one: nothing is checked then */
    public static void rejectEncrypted(Class<?> type, Field target, String fieldName, String operator) {
        if (target != null && target.isAnnotationPresent(Encrypted.class)) {
            throw new IllegalArgumentException("Field '" + fieldName + "' of " + type.getName() + " is @Encrypted: " + operator
                                               + " would write a plaintext value that bypasses the encryption and cannot be read back. Store the entity instead.");
        }
    }

    /** @param target the field that {@code fieldName} names, or {@code null} for an unknown or a nested one: nothing is checked then */
    public static void rejectEntityForReference(AnnotationAndReflectionHelper helper, Class<?> type, Field target, String fieldName, String operator, Object value) {
        if (target != null && target.isAnnotationPresent(Reference.class) && containsEntity(helper, value)) {
            throw new IllegalArgumentException("Field '" + fieldName + "' of " + type.getName() + " is a @Reference: " + operator
                                               + " would embed the entity as a document. Pass the id of the referenced entity (or a MorphiumReference) instead.");
        }
    }

    /**
     * The guard of the writer path ({@code Query.set}, {@code push}, {@code pushAll}, {@code pull}, ...), called on the calling thread before the value is marshalled:
     * the refusals of the update document mapper for the operator that {@code type} stands for.
     */
    public static void guardWriterUpdate(AnnotationAndReflectionHelper helper, Class<?> type, Field target, String fieldName, UpdateTypes updateType, Object value) {
        String operator = switch (updateType) {
            case PUSH -> "$push";
            case ADD_TO_SET -> "$addToSet";
            case PULL -> "$pull";
            default -> "$set";
        };
        rejectEncrypted(type, target, fieldName, operator);

        if (!operator.equals("$pull")) {
            rejectEntityForReference(helper, type, target, fieldName, operator, value);
        }
    }

    private static boolean containsEntity(AnnotationAndReflectionHelper helper, Object value) {
        if (value == null) {
            return false;
        }

        if (value instanceof Map) {
            for (Object v : ((Map<?, ?>) value).values()) {
                if (containsEntity(helper, v)) {
                    return true;
                }
            }

            return false;
        }

        if (value instanceof Collection) {
            for (Object v : (Collection<?>) value) {
                if (containsEntity(helper, v)) {
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

            return containsEntity(helper, l);
        }

        return helper.isEntity(value);
    }
}
