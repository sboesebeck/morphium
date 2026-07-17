package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.driver.MorphiumId;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable holder for the values an {@link IndexDefinition}'s fields extract to for one
 * document, in the same field order as the definition. Two {@code IndexKey}s are equal iff their
 * value lists are equal, which makes them usable directly as map keys for an in-memory index
 * structure.
 *
 * <h2>MISSING sentinel</h2>
 * MongoDB treats a field that is entirely absent from a document the same as a field explicitly
 * set to {@code null} for index/equality purposes - both sort together and are considered equal
 * to each other. To make that identity explicit (and avoid ambiguity with a value that is
 * genuinely, intentionally {@code null} in some other sense), extraction never stores Java
 * {@code null} in a key. Instead both cases are encoded as the same {@link #MISSING} sentinel
 * object. Callers must compare against {@code IndexKey.MISSING} by reference (as {@link #equals}
 * does), never against {@code null}.
 */
public final class IndexKey {
    /**
     * Sentinel used in place of Java {@code null} for a field that is either absent from the
     * document or explicitly set to {@code null}. See the class Javadoc for why both cases share
     * one sentinel.
     */
    public static final Object MISSING = new Object() {
        @Override
        public String toString() {
            return "IndexKey.MISSING";
        }
    };

    /**
     * Sentinel that compares below every other value (including {@link #MISSING}), regardless of
     * field type. Only used to build synthetic range bounds via {@link #prefixLow}/
     * {@link #prefixHigh} - never produced by {@link #extract}.
     */
    private static final Object NEGATIVE_INFINITY = new Object() {
        @Override
        public String toString() {
            return "IndexKey.NEGATIVE_INFINITY";
        }
    };

    /**
     * Sentinel that compares above every other value, regardless of field type. Only used to build
     * synthetic range bounds via {@link #prefixLow}/{@link #prefixHigh} - never produced by
     * {@link #extract}.
     */
    private static final Object POSITIVE_INFINITY = new Object() {
        @Override
        public String toString() {
            return "IndexKey.POSITIVE_INFINITY";
        }
    };

    private final List<Object> values;

    private IndexKey(List<Object> values) {
        this.values = values;
    }

    /**
     * Wraps an explicit list of already-extracted values as an {@link IndexKey}. Each value is
     * passed through {@link #normalizeIdValue} first, so a caller building a key from a raw query
     * value (e.g. a {@code MorphiumId}) still lands in the same bucket as a document whose stored
     * {@code _id} is the driver's internal {@code ObjectId} representation.
     */
    public static IndexKey of(List<Object> values) {
        List<Object> normalized = new ArrayList<>(values.size());
        for (Object v : values) {
            normalized.add(normalizeIdValue(v));
        }
        return new IndexKey(Collections.unmodifiableList(normalized));
    }

    /**
     * Extracts one value per field of {@code def} (in field order) from {@code doc}, walking
     * dotted paths (e.g. {@code "a.b.c"}) the same way a plain nested-map lookup would. Absent
     * fields and explicit {@code null}s both become {@link #MISSING}.
     *
     * <p>Arrays are not expanded into multiple keys: if a path's <em>terminal</em> segment
     * resolves to a {@code List}, that list itself becomes the extracted value, exactly as
     * MongoDB stores a scalar. A {@code List} encountered <em>mid-path</em> (e.g. {@code "a.b"}
     * where {@code a} is an array of sub-documents) is NOT traversed - the walk stops and the
     * field extracts as {@link #MISSING}. Per-element multikey indexing (one index entry per
     * array element) is out of scope here.
     * // Phase B follow-up: multikey indexes
     */
    public static IndexKey extract(Map<String, Object> doc, IndexDefinition def) {
        List<Object> values = new ArrayList<>(def.fields().size());
        for (String field : def.fields()) {
            values.add(extractValue(doc, field));
        }
        return new IndexKey(Collections.unmodifiableList(values));
    }

    @SuppressWarnings("unchecked")
    private static Object extractValue(Map<String, Object> doc, String path) {
        Object current = doc;

        for (String segment : path.split("\\.")) {
            if (!(current instanceof Map)) {
                // Either we walked off into a scalar, or hit a List along the path.
                // Phase B follow-up: multikey indexes - arrays would need to fan out into one
                // index entry per element here; for now we just stop and treat it as missing.
                return MISSING;
            }

            Map<String, Object> map = (Map<String, Object>) current;
            if (!map.containsKey(segment)) {
                return MISSING;
            }
            current = map.get(segment);
        }

        return current == null ? MISSING : normalizeIdValue(current);
    }

    /**
     * {@code MorphiumId} (Morphium's own {@code _id} wrapper type) and {@code ObjectId} (the raw
     * BSON type the in-memory driver actually stores documents' {@code _id} under - see {@code
     * InMemoryDriver.find}'s {@code !internal} branch, which only converts to {@code MorphiumId}
     * on the way OUT to external callers) represent the same value but are different classes with
     * no cross-type {@code equals()}. {@code QueryHelper.matchesQuery} already treats them as
     * interchangeable by comparing {@code toString()} (both render the same 24-hex-digit form).
     * {@link IndexKey} must do the same normalization - otherwise a query built with a {@code
     * MorphiumId} (as every ORM-level {@code _id} lookup is) would never hash/compare equal to a
     * stored document's {@code ObjectId}, silently missing every {@code _id} index lookup.
     */
    private static Object normalizeIdValue(Object v) {
        return (v instanceof MorphiumId || v instanceof ObjectId) ? v.toString() : v;
    }

    /**
     * Builds a comparator over {@link IndexKey}s produced from {@code def}, comparing field by
     * field (in {@code def}'s order) and applying each field's direction ({@code 1}/{@code -1}).
     *
     * <p>Per-field, single-value comparison mirrors the cross-type semantics
     * {@code QueryHelper} uses when comparing query operands:
     * <ul>
     *   <li>{@link #MISSING} sorts before every other value (ascending sense, before direction
     *       is applied) - matching MongoDB's null/missing-sorts-first behaviour;</li>
     *   <li>numbers are unified via {@link Number#doubleValue()} so {@code int}/{@code long}/
     *       {@code double} compare purely by magnitude, never by type;</li>
     *   <li>temporal values are normalised via {@code QueryHelper.toTemporalNumber} - raw
     *       {@code java.time} objects and their serialised Map forms ({@code {sec,n}} for
     *       LocalDateTime, {@code {type:"instant",seconds,nanos}} for Instant) all compare
     *       chronologically as epoch nanos;</li>
     *   <li>same-family {@link Comparable} values (e.g. two Strings) compare naturally;</li>
     *   <li>otherwise-incomparable types fall back to ordering by runtime type name, so the
     *       comparator stays a total order instead of throwing {@link ClassCastException}.</li>
     * </ul>
     */
    public static Comparator<IndexKey> comparator(IndexDefinition def) {
        List<String> fields = def.fields();
        return (left, right) -> {
            for (int i = 0; i < fields.size(); i++) {
                int direction = def.direction(fields.get(i));
                int cmp = compareSingleValue(left.values.get(i), right.values.get(i)) * direction;
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareSingleValue(Object a, Object b) {
        if (a == NEGATIVE_INFINITY || b == NEGATIVE_INFINITY || a == POSITIVE_INFINITY || b == POSITIVE_INFINITY) {
            return compareInfinitySentinel(a, b);
        }
        if (a == MISSING && b == MISSING) {
            return 0;
        }
        if (a == MISSING) {
            return -1;
        }
        if (b == MISSING) {
            return 1;
        }

        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }

        // Temporal types, same normalisation QueryHelper uses for $lt/$gt: raw java.time
        // objects and their serialised forms ({sec,n} / {type:"instant",seconds,nanos} Maps)
        // all unify to epoch nanos so date/TTL index keys order chronologically.
        Long aTemporal = QueryHelper.toTemporalNumber(a);
        Long bTemporal = QueryHelper.toTemporalNumber(b);
        if (aTemporal != null && bTemporal != null) {
            return Long.compare(aTemporal, bTemporal);
        }

        if (a instanceof Comparable && b instanceof Comparable) {
            try {
                return ((Comparable) a).compareTo(b);
            } catch (ClassCastException ignored) {
                // fall through to the type-name fallback below
            }
        }

        // Incomparable/mixed types: fall back to a stable, deterministic order by runtime type
        // name rather than throwing, so the comparator remains total.
        return a.getClass().getName().compareTo(b.getClass().getName());
    }

    private static int compareInfinitySentinel(Object a, Object b) {
        if (a == b) {
            return 0;
        }
        if (a == NEGATIVE_INFINITY || b == POSITIVE_INFINITY) {
            return -1;
        }
        return 1;
    }

    /**
     * Builds a synthetic {@link IndexKey} that sorts at or below every real key extracted via
     * {@code def} whose leading fields equal {@code prefixValues}, in {@code def}'s TreeMap
     * iteration order (i.e. after each field's direction has been applied by
     * {@link #comparator}). Trailing fields beyond {@code prefixValues} are padded with a
     * sentinel chosen per-field so the direction multiplication still yields the lowest possible
     * position - see {@link #POSITIVE_INFINITY}/{@link #NEGATIVE_INFINITY}.
     *
     * <p>Intended as the inclusive lower bound for a prefix range scan over a compound index, e.g.
     * {@code rangeScan(name, IndexKey.prefixLow(def, List.of(5)), true,
     * IndexKey.prefixHigh(def, List.of(5)), true, false)} to scan every entry whose first field is
     * {@code 5}.
     *
     * @throws IllegalArgumentException if {@code prefixValues} has more values than {@code def}
     *                                  has fields
     */
    public static IndexKey prefixLow(IndexDefinition def, List<Object> prefixValues) {
        return buildPrefixBound(def, prefixValues, true);
    }

    /**
     * Same as {@link #prefixLow}, but builds the upper bound instead - sorts at or above every
     * real key sharing the given prefix.
     *
     * @throws IllegalArgumentException if {@code prefixValues} has more values than {@code def}
     *                                  has fields
     */
    public static IndexKey prefixHigh(IndexDefinition def, List<Object> prefixValues) {
        return buildPrefixBound(def, prefixValues, false);
    }

    private static IndexKey buildPrefixBound(IndexDefinition def, List<Object> prefixValues, boolean low) {
        List<String> fields = def.fields();
        if (prefixValues.size() > fields.size()) {
            throw new IllegalArgumentException("Prefix has " + prefixValues.size()
                    + " values but the index only has " + fields.size() + " fields: " + fields);
        }
        List<Object> values = new ArrayList<>(fields.size());
        values.addAll(prefixValues);

        for (int i = prefixValues.size(); i < fields.size(); i++) {
            int direction = def.direction(fields.get(i));
            // Want the padded field to end up "lowest" (for prefixLow) or "highest" (for
            // prefixHigh) once the comparator multiplies by direction. For direction == 1 that
            // means using the sentinel directly; for direction == -1 it flips.
            boolean useRawLow = low == (direction == 1);
            values.add(useRawLow ? NEGATIVE_INFINITY : POSITIVE_INFINITY);
        }

        return new IndexKey(Collections.unmodifiableList(values));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexKey)) {
            return false;
        }
        return values.equals(((IndexKey) o).values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }

    @Override
    public String toString() {
        return "IndexKey" + values;
    }
}
