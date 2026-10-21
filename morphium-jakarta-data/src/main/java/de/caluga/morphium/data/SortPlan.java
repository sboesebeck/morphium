package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;
import jakarta.data.Order;
import jakarta.data.Sort;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The sort order of a repository method (MOR-081): the static order first ({@code OrderBy} in the method name, {@code @OrderBy}), then every dynamic sort in the order of the
 * parameters: a {@code Sort}, several {@code Sort} parameters, a {@code Sort...} varargs parameter and an {@code Order}. A property that is sorted by already keeps its first
 * position and direction. Before, each of them replaced what came before, so a dynamic {@code Sort} or {@code Order} silently dropped the static order and only the last of several sorts counted.
 */
final class SortPlan {

    /** one sort criterion with the Java name of the property */
    record Entry(String property, boolean ascending) {
    }

    private SortPlan() {
    }

    /** the sorts that the arguments carry, in the order of the parameters */
    static List<Entry> dynamic(Object[] args) {
        List<Entry> entries = new ArrayList<>();

        if (args == null) {
            return entries;
        }

        for (Object arg : args) {
            if (arg instanceof Sort<?> sort) {
                entries.add(new Entry(sort.property(), sort.isAscending()));
            } else if (arg instanceof Sort<?>[] sorts) {
                for (Sort<?> sort : sorts) {
                    if (sort != null) {
                        entries.add(new Entry(sort.property(), sort.isAscending()));
                    }
                }
            } else if (arg instanceof Order<?> order) {
                for (Sort<?> sort : order.sorts()) {
                    entries.add(new Entry(sort.property(), sort.isAscending()));
                }
            }
        }

        return entries;
    }

    /** the criteria of an encoded static order, {@code "field:ASC,other:DESC"}; empty for null or "" */
    static List<Entry> fromSpec(String spec) {
        List<Entry> entries = new ArrayList<>();

        if (spec == null || spec.isEmpty()) {
            return entries;
        }

        for (String part : spec.split(",")) {
            String[] fieldAndDirection = part.split(":");
            entries.add(new Entry(fieldAndDirection[0], fieldAndDirection.length < 2 || !"DESC".equals(fieldAndDirection[1])));
        }

        return entries;
    }

    /** the criteria of the order that a method name carries ({@code OrderByAmountDesc}) */
    static List<Entry> fromOrderSpecs(List<QueryDescriptor.OrderSpec> specs) {
        List<Entry> entries = new ArrayList<>();

        if (specs != null) {
            for (QueryDescriptor.OrderSpec spec : specs) {
                entries.add(new Entry(spec.field(), spec.direction() == QueryDescriptor.Direction.ASC));
            }
        }

        return entries;
    }

    /** the static criteria followed by the dynamic ones, a property only once (the first wins) */
    static List<Entry> merge(List<Entry> staticEntries, List<Entry> dynamicEntries) {
        Map<String, Entry> merged = new LinkedHashMap<>();

        for (Entry e : staticEntries) {
            merged.putIfAbsent(e.property(), e);
        }

        for (Entry e : dynamicEntries) {
            merged.putIfAbsent(e.property(), e);
        }

        return new ArrayList<>(merged.values());
    }

    /** sets the order on the query; nothing is set for an empty list */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static void apply(Query query, List<Entry> entries, Morphium morphium, Class entityClass) {
        if (entries.isEmpty()) {
            return;
        }

        Map<String, Integer> sortMap = new LinkedHashMap<>();

        for (Entry e : entries) {
            sortMap.put(QueryExecutor.resolveMongoField(morphium, entityClass, e.property()), e.ascending() ? 1 : -1);
        }

        query.sort(sortMap);
    }
}
