package de.caluga.morphium.driver.inmem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parsed, immutable view of an InMemoryDriver index descriptor.
 *
 * <p>Index descriptors are stored internally as a single {@code Map<String, Object>} where every
 * entry is a field name mapped to its sort direction ({@code 1} or {@code -1}), except for the
 * reserved {@code $options} entry which carries index-level metadata (e.g. {@code unique},
 * {@code name}, {@code expireAfterSeconds}). {@link #fromIndexMap(Map)} turns that raw shape into
 * a proper value object, preserving field order (compound indexes are order-sensitive) and
 * skipping {@code $options} when enumerating fields.
 *
 * <p>This class only parses the descriptor - it does not touch the driver's index storage or
 * enforce anything. Consumers ({@code CollectionIndexStore}, added in a later phase) use it to
 * drive extraction ({@link IndexKey#extract(Map, IndexDefinition)}) and ordering
 * ({@link IndexKey#comparator(IndexDefinition)}).
 */
public final class IndexDefinition {
    private final List<String> fields;
    private final Map<String, Integer> directions;
    private final boolean unique;
    private final Long expireAfterSeconds;
    private final String name;

    private IndexDefinition(List<String> fields, Map<String, Integer> directions, boolean unique,
            Long expireAfterSeconds, String name) {
        this.fields = fields;
        this.directions = directions;
        this.unique = unique;
        this.expireAfterSeconds = expireAfterSeconds;
        this.name = name;
    }

    /**
     * Parses a stored index descriptor map into an {@link IndexDefinition}.
     *
     * @param indexMap field-&gt;direction entries plus an optional {@code $options} entry
     *                 (a {@code Map} with keys such as {@code unique}, {@code name},
     *                 {@code expireAfterSeconds}). Field iteration order is preserved.
     */
    @SuppressWarnings("unchecked")
    public static IndexDefinition fromIndexMap(Map<String, Object> indexMap) {
        Map<String, Integer> directions = new LinkedHashMap<>();
        Map<String, Object> options = null;

        for (Map.Entry<String, Object> entry : indexMap.entrySet()) {
            if ("$options".equals(entry.getKey())) {
                Object value = entry.getValue();
                if (value instanceof Map) {
                    options = (Map<String, Object>) value;
                }
                continue;
            }

            Object rawDirection = entry.getValue();
            int direction = (rawDirection instanceof Number) ? ((Number) rawDirection).intValue() : 1;
            directions.put(entry.getKey(), direction);
        }

        boolean unique = false;
        Long expireAfterSeconds = null;
        String name = null;

        if (options != null) {
            Object uniqueOption = options.get("unique");
            unique = Boolean.TRUE.equals(uniqueOption) || "true".equalsIgnoreCase(String.valueOf(uniqueOption));

            Object expireOption = options.get("expireAfterSeconds");
            if (expireOption instanceof Number) {
                expireAfterSeconds = ((Number) expireOption).longValue();
            }

            Object nameOption = options.get("name");
            if (nameOption != null) {
                name = nameOption.toString();
            }
        }

        List<String> orderedFields = Collections.unmodifiableList(new ArrayList<>(directions.keySet()));
        return new IndexDefinition(orderedFields, directions, unique, expireAfterSeconds, name);
    }

    /**
     * Indexed field names/paths, in declaration order. Compound indexes preserve this order since
     * it is significant for prefix matching and key comparison.
     */
    public List<String> fields() {
        return fields;
    }

    /**
     * Sort direction ({@code 1} or {@code -1}) for the given field.
     *
     * @throws IllegalArgumentException if {@code field} is not part of this index
     */
    public int direction(String field) {
        Integer direction = directions.get(field);
        if (direction == null) {
            throw new IllegalArgumentException("Field '" + field + "' is not part of this index: " + fields);
        }
        return direction;
    }

    public boolean unique() {
        return unique;
    }

    /** TTL, in seconds, or {@code null} if this is not a TTL index. */
    public Long expireAfterSeconds() {
        return expireAfterSeconds;
    }

    /** Index name, or {@code null} if {@code $options} carried none. */
    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return "IndexDefinition{fields=" + fields + ", directions=" + directions + ", unique=" + unique
                + ", expireAfterSeconds=" + expireAfterSeconds + ", name=" + name + '}';
    }
}
