package de.caluga.morphium.query;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Reference;
import de.caluga.morphium.annotations.encryption.Encrypted;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Prepares an update document for the driver: the keys of every operator map are translated to the names the fields are stored under, and
 * the values are marshalled with {@link de.caluga.morphium.objectmapping.MorphiumObjectMapper#marshallUpdateValue(Object)}.
 * <p>
 * Contract (MOR-002, MOR-041):
 * <ul>
 * <li>An update document without any {@code $}-operator key (a replacement document) is returned as it is.</li>
 * <li>With {@code translateKeys} the key of every operator map, including {@code $unset} and the key and the target of {@code $rename}, is
 * resolved with {@link de.caluga.morphium.AnnotationAndReflectionHelper#resolveFieldPath(Class, String)}: Java names become stored names,
 * stored names, {@code @Aliases}, unknown, positional and {@code $}-prefixed keys stay as they are. Without it the keys are used as given.</li>
 * <li>Two keys that resolve to the same name are merged if their values are equal and rejected otherwise.</li>
 * <li>The mapping is idempotent and never modifies the caller's map; containers are copied.</li>
 * <li>The values are marshalled in the shape {@code store()} writes at their position: the elements of {@code $push}, {@code $addToSet} and {@code $pullAll}
 * and the value of a positional target ({@code list.$}, {@code list.0}, {@code list.$[x]}) with {@link de.caluga.morphium.objectmapping.MorphiumObjectMapper#marshallUpdateElement(Object)},
 * the operand of {@code $pull} with {@link de.caluga.morphium.objectmapping.MorphiumObjectMapper#marshallUpdatePullOperand(Object)}, every other value as a whole field
 * with {@link de.caluga.morphium.objectmapping.MorphiumObjectMapper#marshallUpdateValue(Object)}. An update does not know the declared type of the field, so an entity/embedded
 * value always carries {@code class_name} (an entity keeps its {@code _id}), and a {@code MorphiumId} in a container becomes an {@code ObjectId}.</li>
 * <li>An entity for a field that is a {@code @Reference} is rejected: it would be embedded as a full document into a field that
 * the mapper expects to hold an id.</li>
 * <li>A write to an {@code @Encrypted} field is rejected (independent of {@code translateKeys}): the plaintext value would bypass the
 * encryption and make every later read of the document fail. {@code $unset} and {@code $rename} are allowed.</li>
 * </ul>
 * Package-private on purpose: the later update builder (MOR-006) calls it from {@code Query}.
 */
final class UpdateDocumentMapper {
    private static final Set<String> REFERENCE_CHECKED_OPERATORS = Set.of("$set", "$setOnInsert", "$push", "$addToSet");
    private static final Set<String> ENCRYPTION_CHECKED_OPERATORS = Set.of("$set", "$setOnInsert", "$push", "$addToSet", "$pull", "$pullAll", "$inc", "$mul", "$min", "$max", "$bit");

    private final Morphium morphium;
    private final Class<?> type;
    private final boolean translateKeys;

    UpdateDocumentMapper(Morphium morphium, Class<?> type, boolean translateKeys) {
        this.morphium = morphium;
        this.type = type;
        this.translateKeys = translateKeys;
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
            Map<String, String> origin = new LinkedHashMap<>(); // mapped key -> key as given, to name both on a collision

            for (Map.Entry<Object, Object> f : ((Map<Object, Object>) fields).entrySet()) {
                String given = keyOf(operator, f.getKey());
                rejectEncrypted(operator, given);

                if (REFERENCE_CHECKED_OPERATORS.contains(operator)) {
                    rejectEntityForReference(operator, given, f.getValue());
                }

                String key = translateKeys ? morphium.getARHelper().resolveFieldPath(type, given) : given;
                Object value = operator.equals("$rename") ? renameTarget(f.getValue()) : mapValue(operator, given, f.getValue());

                if (mapped.containsKey(key)) {
                    if (!Objects.deepEquals(mapped.get(key), value)) {
                        throw new IllegalArgumentException("The keys '" + origin.get(key) + "' and '" + given + "' of " + operator + " both denote the field '" + key + "' of "
                                                           + type.getName() + " but have different values");
                    }

                    continue;
                }

                mapped.put(key, value);
                origin.put(key, given);
            }

            ret.put(operator, mapped);
        }

        return ret;
    }

    private String keyOf(String operator, Object key) {
        if (key instanceof Enum) {
            return ((Enum<?>) key).name();
        }

        if (!(key instanceof String)) {
            throw new IllegalArgumentException("The key of " + operator + " must be a String or an enum, got " + key);
        }

        return (String) key;
    }

    private Object renameTarget(Object target) {
        if (translateKeys && target instanceof String) {
            return morphium.getARHelper().resolveFieldPath(type, (String) target);
        }

        return target;
    }

    private static final Set<String> ELEMENT_OPERATORS = Set.of("$push", "$addToSet", "$pullAll");

    private Object mapValue(String operator, String key, Object value) {
        // an update does not know the declared type of the field. The position of a value follows the operator: the elements of $push/$addToSet/$pullAll and the
        // operand of $pull are container members (store() writes them with serialize(): {"value": scalar} for a custom-mapped scalar), every other operator
        // sets a whole field. A positional target (list.$, list.0, list.$[x], list.$[]) addresses one element, whatever the operator is.
        if (operator.equals("$pull")) {
            return morphium.getMapper().marshallUpdatePullOperand(value);
        }

        if (ELEMENT_OPERATORS.contains(operator) || isElementTarget(key)) {
            return morphium.getMapper().marshallUpdateElement(value);
        }

        return morphium.getMapper().marshallUpdateValue(value);
    }

    /** true if the path ends in a positional segment: {@code $}, {@code $[]}, {@code $[x]} or an array index */
    static boolean isElementTarget(String key) {
        int dot = key.lastIndexOf('.');
        String last = dot < 0 ? key : key.substring(dot + 1);

        if (last.equals("$") || last.equals("$[]") || (last.startsWith("$[") && last.endsWith("]"))) {
            return true;
        }

        return !last.isEmpty() && dot >= 0 && last.chars().allMatch(Character::isDigit);
    }

    private Field fieldOf(String key) {
        if (key == null || key.isEmpty() || key.indexOf('.') >= 0 || key.charAt(0) == '$') {
            return null;
        }

        try {
            return morphium.getARHelper().getField(type, key);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private void rejectEncrypted(String operator, String fieldName) {
        if (!ENCRYPTION_CHECKED_OPERATORS.contains(operator)) {
            return;
        }

        Field fld = fieldOf(fieldName);

        if (fld != null && fld.isAnnotationPresent(Encrypted.class)) {
            throw new IllegalArgumentException("Field '" + fieldName + "' of " + type.getName() + " is @Encrypted: " + operator
                                               + " would write a plaintext value that bypasses the encryption and cannot be read back. Store the entity instead.");
        }
    }

    private void rejectEntityForReference(String operator, String fieldName, Object value) {
        if (fieldName == null || !containsEntity(value)) {
            return;
        }

        Field fld = fieldOf(fieldName);

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
