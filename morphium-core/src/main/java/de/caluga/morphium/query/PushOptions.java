package de.caluga.morphium.query;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The modifiers of a {@code $push} with {@code $each}: {@code $slice}, {@code $position} and {@code $sort}, for
 * {@link UpdateBuilder#pushEach(String, java.util.Collection, PushOptions)}.
 * <pre>{@code
 * // keep the last 10 events, newest last
 * query.modify().pushEach("events", newEvents, PushOptions.options().slice(-10)).findOneAndUpdate(ReturnDocument.AFTER);
 * // the elements are documents: sort the whole array by a field of the elements
 * query.modify().pushEach("lines", lines, PushOptions.options().sortBy("qty", true))...
 * }</pre>
 * MongoDB applies the modifiers in the order {@code $each}, {@code $position}, {@code $sort}, {@code $slice}. The field names of {@link #sortBy(String, boolean)} are the names of the
 * fields of the elements as they are stored; they are used as given (the builder does not know the type of the elements).
 * The object is mutable and not thread safe.
 *
 * @apiNote incubating in 7.0, may change in a later 7.x release
 */
public final class PushOptions {
    private Integer slice;
    private Integer position;
    private Integer wholeSort;
    private Map<String, Object> fieldSort;

    private PushOptions() {
    }

    public static PushOptions options() {
        return new PushOptions();
    }

    /** {@code $slice}: after the push keep the first {@code n} elements (positive), the last {@code -n} elements (negative) or none (0) */
    public PushOptions slice(int n) {
        this.slice = n;
        return this;
    }

    /** {@code $position}: insert at this index instead of appending; a negative index counts from the end */
    public PushOptions position(int index) {
        this.position = index;
        return this;
    }

    /** {@code $sort: 1}: sort the whole array ascending (an array of scalars, or of documents compared as a whole) */
    public PushOptions sortAscending() {
        return sortWhole(1);
    }

    /** {@code $sort: -1}: sort the whole array descending */
    public PushOptions sortDescending() {
        return sortWhole(-1);
    }

    private PushOptions sortWhole(int direction) {
        if (fieldSort != null) {
            throw new IllegalArgumentException("A sort of the whole elements and a sort by a field exclude each other");
        }

        this.wholeSort = direction;
        return this;
    }

    /** {@code $sort: {field: 1|-1}}: sort an array of documents by a field of the elements; call more than once to sort by several fields, in this order */
    public PushOptions sortBy(String field, boolean ascending) {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("The field to sort by must not be null or empty");
        }

        if (wholeSort != null) {
            throw new IllegalArgumentException("A sort of the whole elements and a sort by a field exclude each other");
        }

        if (fieldSort == null) {
            fieldSort = new LinkedHashMap<>();
        }

        fieldSort.put(field, ascending ? 1 : -1);
        return this;
    }

    /** adds the modifiers that are set to the {@code $each} document */
    void addTo(Map<String, Object> eachDocument) {
        if (position != null) {
            eachDocument.put("$position", position);
        }

        if (wholeSort != null) {
            eachDocument.put("$sort", wholeSort);
        } else if (fieldSort != null) {
            eachDocument.put("$sort", new LinkedHashMap<>(fieldSort));
        }

        if (slice != null) {
            eachDocument.put("$slice", slice);
        }
    }
}
