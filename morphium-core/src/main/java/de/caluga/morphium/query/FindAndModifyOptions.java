package de.caluga.morphium.query;

import de.caluga.morphium.Collation;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The options of an atomic find-and-modify ({@link Query#findOneAndUpdate(Map, FindAndModifyOptions)},
 * {@link UpdateBuilder#findOneAndUpdate(FindAndModifyOptions)}).
 * <ul>
 * <li>{@link #upsert(boolean)}: insert a document if none matches. For the update builder this is combined with {@code UpdateBuilder.upsert()} (either one switches the upsert on);</li>
 * <li>{@link #returnDocument(ReturnDocument)}: <b>required</b>, there is no default. {@code BEFORE} returns the document as it was, {@code AFTER} as it is now. An upsert that
 * <b>inserts</b> has no pre-image: with {@code BEFORE} the result is {@code null}, exactly as for "nothing matched" - use
 * {@link UpdateBuilder#tryFindOneAndUpdate(FindAndModifyOptions)} to tell the two apart;</li>
 * <li>{@link #sort(Map)}: which document is taken if several match; replaces the sort of the query;</li>
 * <li>{@link #projection(Map)}: the fields of the returned document ({@code 1} to include, {@code 0} to exclude; Java names are translated to the stored names, {@code _id} is always
 * returned unless excluded); the returned entity is partial. The stored document is complete;</li>
 * <li>{@link #hint(String)} / {@link #hint(Map)}: the index to use (an index name or a key document); a real MongoDB rejects an unknown index. The InMemoryDriver has no
 * query planner and ignores the hint;</li>
 * <li>{@link #collation(Collation)}: replaces the collation of the query (not supported by the InMemoryDriver);</li>
 * <li>{@link #arrayFilters(List)}: replace the array filters of the query;</li>
 * <li>{@link #maxTime(Duration)}: the server aborts the command after this time (ignored by the InMemoryDriver).</li>
 * </ul>
 * An option that is not set leaves what the query carries (sort, collation, array filters) as it is. The query's own projection and hint are not used (the update
 * builder rejects them): give them here. The setters return this object; the getters return copies.
 *
 * @apiNote incubating until 6.3
 */
public final class FindAndModifyOptions {
    private boolean upsert;
    private ReturnDocument returnDocument;
    private Map<String, Object> sort;
    private Map<String, Object> projection;
    private Object hint;
    private Collation collation;
    private List<Map<String, Object>> arrayFilters;
    private Integer maxTimeMS;

    private FindAndModifyOptions() {
    }

    public static FindAndModifyOptions options() {
        return new FindAndModifyOptions();
    }

    public FindAndModifyOptions upsert(boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    public FindAndModifyOptions returnDocument(ReturnDocument returnDocument) {
        if (returnDocument == null) {
            throw new IllegalArgumentException("ReturnDocument must not be null");
        }

        this.returnDocument = returnDocument;
        return this;
    }

    public FindAndModifyOptions sort(Map<String, Object> sort) {
        this.sort = sort == null || sort.isEmpty() ? null : new LinkedHashMap<>(sort);
        return this;
    }

    public FindAndModifyOptions projection(Map<String, Object> projection) {
        if (projection == null || projection.isEmpty()) {
            throw new IllegalArgumentException("a projection needs at least one field");
        }

        this.projection = new LinkedHashMap<>(projection);
        return this;
    }

    public FindAndModifyOptions hint(String indexName) {
        if (indexName == null || indexName.isEmpty()) {
            throw new IllegalArgumentException("the name of an index is required");
        }

        this.hint = indexName;
        return this;
    }

    public FindAndModifyOptions hint(Map<String, Object> indexKeys) {
        if (indexKeys == null || indexKeys.isEmpty()) {
            throw new IllegalArgumentException("the keys of an index are required");
        }

        this.hint = new LinkedHashMap<>(indexKeys);
        return this;
    }

    public FindAndModifyOptions collation(Collation collation) {
        this.collation = collation;
        return this;
    }

    public FindAndModifyOptions arrayFilters(List<Map<String, Object>> arrayFilters) {
        this.arrayFilters = arrayFilters == null || arrayFilters.isEmpty() ? null : new ArrayList<>(arrayFilters);
        return this;
    }

    @SafeVarargs
    public final FindAndModifyOptions arrayFilters(Map<String, Object>... arrayFilters) {
        return arrayFilters(arrayFilters == null ? null : List.of(arrayFilters));
    }

    public FindAndModifyOptions maxTime(Duration maxTime) {
        if (maxTime == null || maxTime.isZero() || maxTime.isNegative() || maxTime.toMillis() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("maxTime must be positive and fit into an int of milliseconds: " + maxTime);
        }

        this.maxTimeMS = (int) Math.max(1, maxTime.toMillis());
        return this;
    }

    public boolean isUpsert() {
        return upsert;
    }

    /** null if not set; the terminals reject that */
    public ReturnDocument getReturnDocument() {
        return returnDocument;
    }

    public Map<String, Object> getSort() {
        return sort == null ? null : new LinkedHashMap<>(sort);
    }

    public Map<String, Object> getProjection() {
        return projection == null ? null : new LinkedHashMap<>(projection);
    }

    /** an index name ({@code String}) or a key document ({@code Map}), or null */
    @SuppressWarnings("unchecked")
    public Object getHint() {
        return hint instanceof Map ? new LinkedHashMap<>((Map<String, Object>) hint) : hint;
    }

    public Collation getCollation() {
        return collation;
    }

    public List<Map<String, Object>> getArrayFilters() {
        return arrayFilters == null ? null : new ArrayList<>(arrayFilters);
    }

    public Integer getMaxTimeMS() {
        return maxTimeMS;
    }

    @Override
    public String toString() {
        return "FindAndModifyOptions{upsert=" + upsert + ", returnDocument=" + returnDocument + (sort != null ? ", sort=" + sort : "") + (projection != null ? ", projection=" + projection : "")
               + (hint != null ? ", hint=" + hint : "") + (collation != null ? ", collation" : "") + (arrayFilters != null ? ", arrayFilters=" + arrayFilters : "")
               + (maxTimeMS != null ? ", maxTimeMS=" + maxTimeMS : "") + "}";
    }
}
