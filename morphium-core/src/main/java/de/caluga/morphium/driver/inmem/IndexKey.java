package de.caluga.morphium.driver.inmem;

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

    private final List<Object> values;

    private IndexKey(List<Object> values) {
        this.values = values;
    }

    /** Wraps an explicit list of already-extracted values as an {@link IndexKey}. */
    public static IndexKey of(List<Object> values) {
        return new IndexKey(Collections.unmodifiableList(new ArrayList<>(values)));
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

        return current == null ? MISSING : current;
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
