package de.caluga.morphium.driver.inmem;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pure candidate-selection planner for {@link InMemoryDriver} reads: given a query map and the
 * index definitions available on a collection, decides which (if any) index can narrow down the
 * set of candidate documents before the driver's full {@code QueryHelper.matchesQuery} predicate
 * is applied.
 *
 * <p><b>CANDIDATE PREFILTER invariant.</b> A plan produced here is always a conservative
 * prefilter, never the final answer: the set of documents a plan selects is guaranteed to be a
 * superset of the documents that actually match the query (or exactly the match set - never a
 * strict subset). The driver MUST still run every candidate through the full
 * {@code matchesQuery} predicate. This is what keeps the planner safe to be simple/conservative -
 * picking {@link FullScan} (or a broader-than-necessary index plan) is always correct, merely
 * slower. A direct corollary: if a non-{@link FullScan} plan yields zero candidates, the true
 * result is also empty and the driver must NOT fall back to a full scan (a full scan could only
 * ever find a subset of what the prefilter already covers) - see {@code InMemoryDriver}'s single
 * call site that turns a plan's result into candidate data for exactly where that reasoning is
 * encoded.
 *
 * <p>This class touches no documents and no {@link CollectionIndexStore} - it only looks at the
 * query shape and the available {@link IndexDefinition}s, which makes it trivial to unit test in
 * isolation.
 *
 * <h2>Supported shapes (this phase)</h2>
 * <ul>
 *   <li>Top-level equality on an index prefix (all fields of a prefix bound by plain equality,
 *       not necessarily the whole index) - maps to {@link EqualityLookup} if the equality prefix
 *       covers every field of the index, otherwise to a prefix {@link RangeScan}.</li>
 *   <li>An equality prefix (possibly empty) followed by a single trailing range operator
 *       ({@code $gt}/{@code $gte}/{@code $lt}/{@code $lte}) on the very next index field - maps to
 *       a bounded {@link RangeScan}.</li>
 *   <li>An equality prefix followed by {@code $in} on the next field, when that field is the
 *       index's last field (the whole index is bound) - maps to {@link InUnion}, one point lookup
 *       key per {@code $in} value (values are not deduplicated here; a document reachable via two
 *       distinct keys - e.g. a duplicate {@code $in} value - would be visited twice, so callers
 *       executing the union must dedup the resulting documents by identity).</li>
 * </ul>
 * Everything else - {@code $or}/{@code $and} (or any other {@code $}-prefixed top-level key,
 * e.g. {@code $expr}, {@code $text}, {@code $where}), {@code $regex}, negations ({@code $ne},
 * {@code $not}, ...), combined/multi-key operator maps on one field, an empty query, or a
 * document-shaped equality value (a nested {@code Map} that is not itself a supported operator) -
 * maps to {@link FullScan}. A field beyond the point where a query stops being plannable simply
 * ends the prefix walk for that index; fields before it may still yield a (shorter) equality
 * prefix plan.
 *
 * <p>{@code _id} equality is handled by the same general algorithm - no special case is needed as
 * long as the {@code _id} index's {@link IndexDefinition} is among {@code defs}.
 */
public final class IndexPlanner {
    private static final Set<String> RANGE_OPS = Set.of("$gt", "$gte", "$lt", "$lte");

    private IndexPlanner() {
    }

    /** One candidate-selection strategy chosen for a query. See the class Javadoc for the invariant. */
    public sealed interface IndexPlan permits FullScan, EqualityLookup, RangeScan, InUnion {
    }

    /** No usable index: the driver must scan the whole collection. */
    public record FullScan() implements IndexPlan {
        public static final FullScan INSTANCE = new FullScan();
    }

    /** Every field of {@code def} is bound by top-level equality; {@code key} is the full lookup key. */
    public record EqualityLookup(IndexDefinition def, IndexKey key) implements IndexPlan {
    }

    /**
     * A bounded (or prefix-only) scan over {@code def}, in the store's {@code rangeScan} sense:
     * {@code from}/{@code to} are already direction-aware synthetic or exact {@link IndexKey}
     * bounds, ready to pass straight through to {@code CollectionIndexStore.rangeScan}.
     */
    public record RangeScan(IndexDefinition def, IndexKey from, boolean fromInclusive, IndexKey to,
                             boolean toInclusive, boolean descending) implements IndexPlan {
    }

    /** Union of point lookups on {@code def}, one key per {@code $in} value (not deduplicated). */
    public record InUnion(IndexDefinition def, List<IndexKey> keys) implements IndexPlan {
    }

    /**
     * Picks the best available plan for {@code query} across {@code defs}. "Best" means the
     * candidate that binds the most query fields (a longer equality/range/in prefix is assumed
     * more selective); ties keep the first def encountered. Never touches documents - see the
     * class Javadoc.
     */
    public static IndexPlan plan(Map<String, Object> query, Collection<IndexDefinition> defs) {
        if (query == null || query.isEmpty() || defs == null || defs.isEmpty()) {
            return FullScan.INSTANCE;
        }

        for (String key : query.keySet()) {
            if (key.startsWith("$")) {
                // Top-level operator ($and/$or/$nor/$expr/$text/$where/...) - out of scope here,
                // the driver handles $and/$or by planning sub-queries individually before this
                // method ever sees them.
                return FullScan.INSTANCE;
            }
        }

        Candidate best = null;
        for (IndexDefinition def : defs) {
            if (def.fields().isEmpty()) {
                continue;
            }
            Candidate candidate = evaluate(def, query);
            if (candidate != null && (best == null || candidate.boundFieldCount() > best.boundFieldCount())) {
                best = candidate;
            }
        }
        return best != null ? best.plan() : FullScan.INSTANCE;
    }

    private record Candidate(int boundFieldCount, IndexPlan plan) {
    }

    private static Candidate evaluate(IndexDefinition def, Map<String, Object> query) {
        List<String> fields = def.fields();
        List<Object> equalityValues = new ArrayList<>();
        int i = 0;

        for (; i < fields.size(); i++) {
            String field = fields.get(i);
            if (!query.containsKey(field)) {
                break;
            }
            Object v = query.get(field);
            if (v instanceof Map) {
                // Operator expression (or a nested-document equality value - not distinguished,
                // both are conservatively treated as "can't extend the equality prefix here").
                break;
            }
            equalityValues.add(toKeyValue(v));
        }
        int equalityCount = i;

        if (i < fields.size()) {
            Object v = query.get(fields.get(i));
            if (v instanceof Map<?, ?> opMap) {
                RangeOp rangeOp = singleRangeOp(opMap);
                if (rangeOp != null) {
                    IndexPlan rangePlan = buildRangeScan(def, equalityValues, rangeOp);
                    return new Candidate(equalityCount + 1, rangePlan);
                }

                List<?> inList = singleInList(opMap);
                if (inList != null && equalityCount + 1 == fields.size()) {
                    return new Candidate(fields.size(), buildInUnion(def, equalityValues, inList));
                }
                // Any other operator (or a combined multi-key map like {$gte:.., $lt:..}) can't
                // be planned - fall through to whatever equality prefix was already bound.
            }
        }

        if (equalityCount == fields.size() && equalityCount > 0) {
            return new Candidate(equalityCount, new EqualityLookup(def, IndexKey.of(equalityValues)));
        }
        if (equalityCount > 0) {
            IndexKey from = IndexKey.prefixLow(def, equalityValues);
            IndexKey to = IndexKey.prefixHigh(def, equalityValues);
            return new Candidate(equalityCount, new RangeScan(def, from, true, to, true, false));
        }
        return null;
    }

    private static Object toKeyValue(Object v) {
        return v == null ? IndexKey.MISSING : v;
    }

    private record RangeOp(String op, Object value) {
    }

    private static RangeOp singleRangeOp(Map<?, ?> raw) {
        if (raw.size() != 1) {
            return null;
        }
        Map.Entry<?, ?> e = raw.entrySet().iterator().next();
        if (!(e.getKey() instanceof String key) || !RANGE_OPS.contains(key)) {
            return null;
        }
        return new RangeOp(key, e.getValue());
    }

    private static List<?> singleInList(Map<?, ?> raw) {
        if (raw.size() != 1) {
            return null;
        }
        Map.Entry<?, ?> e = raw.entrySet().iterator().next();
        if (!"$in".equals(e.getKey()) || !(e.getValue() instanceof List<?> list)) {
            return null;
        }
        return list;
    }

    /**
     * Builds the {@link RangeScan} bounds for an equality prefix ({@code equalityValues}) plus a
     * single trailing range operator on the very next index field.
     *
     * <p>Direction matters: {@code IndexKey.prefixLow}/{@code prefixHigh} are defined in terms of
     * the index's <em>comparator</em> (TreeMap) order, which already flips for a descending
     * ({@code -1}) field. So "domain greater than v" sits on the comparator-HIGH side only when
     * the range field's direction is ascending; for a descending field it sits on the
     * comparator-LOW side instead. {@code structurallyAbove} below picks the right side; the
     * {@code lowerBound*}/{@code upperBound*} helpers then build the v-boundary itself (exact key
     * when the range field is the index's last field - no room to pad a synthetic sentinel - or a
     * padded synthetic sentinel otherwise).
     */
    private static IndexPlan buildRangeScan(IndexDefinition def, List<Object> equalityValues, RangeOp rangeOp) {
        List<String> fields = def.fields();
        Object v = toKeyValue(rangeOp.value());
        List<Object> prefixWithValue = new ArrayList<>(equalityValues);
        prefixWithValue.add(v);

        String rangeField = fields.get(equalityValues.size());
        int direction = def.direction(rangeField);
        boolean isGreaterOp = "$gt".equals(rangeOp.op()) || "$gte".equals(rangeOp.op());
        boolean includeBoundary = "$gte".equals(rangeOp.op()) || "$lte".equals(rangeOp.op());
        boolean structurallyAbove = (direction == 1) == isGreaterOp;

        IndexKey from;
        boolean fromInclusive;
        IndexKey to;
        boolean toInclusive;

        if (structurallyAbove) {
            Bound lower = includeBoundary ? lowerBoundIncludingV(def, prefixWithValue) : lowerBoundExcludingV(def, prefixWithValue);
            from = lower.key();
            fromInclusive = lower.inclusive();
            to = IndexKey.prefixHigh(def, equalityValues);
            toInclusive = true;
        } else {
            from = IndexKey.prefixLow(def, equalityValues);
            fromInclusive = true;
            Bound upper = includeBoundary ? upperBoundIncludingV(def, prefixWithValue) : upperBoundExcludingV(def, prefixWithValue);
            to = upper.key();
            toInclusive = upper.inclusive();
        }

        return new RangeScan(def, from, fromInclusive, to, toInclusive, false);
    }

    private record Bound(IndexKey key, boolean inclusive) {
    }

    private static boolean isFullLength(IndexDefinition def, List<Object> prefixWithValue) {
        return prefixWithValue.size() == def.fields().size();
    }

    private static Bound lowerBoundIncludingV(IndexDefinition def, List<Object> prefixWithValue) {
        if (isFullLength(def, prefixWithValue)) {
            return new Bound(IndexKey.of(prefixWithValue), true);
        }
        return new Bound(IndexKey.prefixLow(def, prefixWithValue), true);
    }

    private static Bound lowerBoundExcludingV(IndexDefinition def, List<Object> prefixWithValue) {
        if (isFullLength(def, prefixWithValue)) {
            return new Bound(IndexKey.of(prefixWithValue), false);
        }
        return new Bound(IndexKey.prefixHigh(def, prefixWithValue), true);
    }

    private static Bound upperBoundIncludingV(IndexDefinition def, List<Object> prefixWithValue) {
        if (isFullLength(def, prefixWithValue)) {
            return new Bound(IndexKey.of(prefixWithValue), true);
        }
        return new Bound(IndexKey.prefixHigh(def, prefixWithValue), true);
    }

    private static Bound upperBoundExcludingV(IndexDefinition def, List<Object> prefixWithValue) {
        if (isFullLength(def, prefixWithValue)) {
            return new Bound(IndexKey.of(prefixWithValue), false);
        }
        return new Bound(IndexKey.prefixLow(def, prefixWithValue), true);
    }

    private static IndexPlan buildInUnion(IndexDefinition def, List<Object> equalityValues, List<?> inList) {
        List<IndexKey> keys = new ArrayList<>(inList.size());
        for (Object raw : inList) {
            List<Object> full = new ArrayList<>(equalityValues);
            full.add(toKeyValue(raw));
            keys.add(IndexKey.of(full));
        }
        return new InUnion(def, keys);
    }
}
