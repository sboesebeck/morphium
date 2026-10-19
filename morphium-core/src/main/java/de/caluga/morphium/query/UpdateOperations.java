package de.caluga.morphium.query;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The operators of an update document under construction: {@code operator -> (field -> value)}, in insertion order. Pure data structure without any
 * Morphium dependency. The keys are the names the fields are stored under and the values are already marshalled, because the {@link UpdateBuilder}
 * maps every call through the {@link UpdateDocumentMapper} before it gets here; therefore the rules below are applied to what is really sent.
 * <p>
 * Rules (violations throw {@link IllegalArgumentException} and leave the operations unchanged):
 * <ul>
 * <li>The same field twice in one operator: equal values (deep equality) merge, different values are rejected. {@code $inc} is the exception: a
 * second increment of a field is ambiguous (one increment or two) and always rejected.</li>
 * <li>Two operations on the same path, or on a path and one of its children ({@code a} and {@code a.b}), are rejected - within one operator and across
 * operators. MongoDB answers such a document with error 40 ("conflict"), and so does the InMemoryDriver (MOR-051). Paths are compared segment by
 * segment ({@code ab} is not a child of {@code a}, {@code a.0} and {@code a.1} are siblings). Pairs that contain a positional or {@code $} segment are
 * not judged: what they address depends on the document.</li>
 * <li>The target of a {@code $rename} counts as a path as well: it must not be, contain or be contained in any other path of the update, the source of its own rename included.</li>
 * <li>{@code null} is a value for {@code $set}, {@code $setOnInsert}, {@code $min} and {@code $max}, not for {@code $inc} and {@code $mul}.</li>
 * </ul>
 */
final class UpdateOperations {
    private final Map<String, Map<String, Object>> operators = new LinkedHashMap<>();
    /** the stored names that a {@code $rename} writes to: a path of its own for the rules below, although it is a value and not a key */
    private final List<String> renameTargets = new ArrayList<>();

    void add(String operator, String key, Object value) {
        if (operator == null || !operator.startsWith("$")) {
            throw new IllegalArgumentException("Not an update operator: " + operator);
        }

        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("The field name of " + operator + " must not be null or empty");
        }

        if (value == null && (operator.equals("$inc") || operator.equals("$mul"))) {
            throw new IllegalArgumentException("The amount of " + operator + " for '" + key + "' must not be null");
        }

        String renameTarget = null;

        if (operator.equals("$rename")) {
            if (!(value instanceof String target) || target.isEmpty()) {
                throw new IllegalArgumentException("The new name of $rename for '" + key + "' must be a non-empty String, not " + value);
            }

            renameTarget = target;
        }

        Map<String, Object> fields = operators.get(operator);

        if (fields != null && fields.containsKey(key)) {
            if (operator.equals("$inc")) {
                throw new IllegalArgumentException("'" + key + "' is incremented twice in one update (" + operator + "); add the amounts up before the call");
            }

            if (Objects.deepEquals(fields.get(key), value)) {
                return;
            }

            throw new IllegalArgumentException("'" + key + "' is given twice with different values for " + operator + ": " + fields.get(key) + " and " + value);
        }

        checkConflicts(operator, key);

        if (renameTarget != null) {
            if (conflicts(key, renameTarget)) {
                throw new IllegalArgumentException("$rename of '" + key + "' to '" + renameTarget + "': the field cannot be renamed onto itself or onto a path of its own");
            }

            checkConflicts("$rename (target)", renameTarget);
        }

        operators.computeIfAbsent(operator, k -> new LinkedHashMap<>()).put(key, value);

        if (renameTarget != null) {
            renameTargets.add(renameTarget);
        }
    }

    /** @throws IllegalArgumentException if the path conflicts with a key of any operator or with the target of a rename */
    private void checkConflicts(String operator, String path) {
        for (Map.Entry<String, Map<String, Object>> op : operators.entrySet()) {
            for (String other : op.getValue().keySet()) {
                if (conflicts(path, other)) {
                    throw new IllegalArgumentException("'" + path + "' (" + operator + ") conflicts with '" + other + "' (" + op.getKey()
                                                       + "): MongoDB rejects an update that touches a path and the same path or one of its children twice");
                }
            }
        }

        for (String target : renameTargets) {
            if (conflicts(path, target)) {
                throw new IllegalArgumentException("'" + path + "' (" + operator + ") conflicts with the new name '" + target + "' of a $rename: MongoDB rejects an update that touches a path and the same path or one of its children twice");
            }
        }
    }

    private static boolean conflicts(String a, String b) {
        if (a.equals(b)) {
            return true;
        }

        List<String> sa = segments(a);
        List<String> sb = segments(b);

        for (String s : sa) {
            if (s.startsWith("$")) {
                return false;
            }
        }

        for (String s : sb) {
            if (s.startsWith("$")) {
                return false;
            }
        }

        int n = Math.min(sa.size(), sb.size());

        for (int i = 0; i < n; i++) {
            if (!sa.get(i).equals(sb.get(i))) {
                return false;
            }
        }

        return true; // one is a prefix of the other, segment by segment
    }

    private static List<String> segments(String path) {
        List<String> r = new ArrayList<>();
        int start = 0;

        for (int i = 0; i <= path.length(); i++) {
            if (i == path.length() || path.charAt(i) == '.') {
                r.add(path.substring(start, i));
                start = i + 1;
            }
        }

        return r;
    }

    boolean isEmpty() {
        return operators.isEmpty();
    }

    /** true if the update contains any operator but $setOnInsert, i.e. it changes an existing document */
    boolean hasModifyingOperator() {
        for (String op : operators.keySet()) {
            if (!op.equals("$setOnInsert")) {
                return true;
            }
        }

        return false;
    }

    /** true if the update consists of $setOnInsert only: a no-op for an existing document */
    boolean hasOnlySetOnInsert() {
        return operators.size() == 1 && operators.containsKey("$setOnInsert");
    }

    /** true if any operator holds exactly this (stored) field name */
    boolean touches(String key) {
        for (Map<String, Object> fields : operators.values()) {
            if (fields.containsKey(key)) {
                return true;
            }
        }

        return renameTargets.contains(key);
    }

    /** a deep copy of the operator maps (the maps, not the values), in insertion order */
    Map<String, Object> toOperatorMap() {
        Map<String, Object> copy = new LinkedHashMap<>();

        for (Map.Entry<String, Map<String, Object>> op : operators.entrySet()) {
            copy.put(op.getKey(), new LinkedHashMap<>(op.getValue()));
        }

        return copy;
    }
}
