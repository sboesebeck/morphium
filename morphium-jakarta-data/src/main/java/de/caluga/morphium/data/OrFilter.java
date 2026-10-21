package de.caluga.morphium.data;

import de.caluga.morphium.FilterExpression;
import de.caluga.morphium.query.Query;

import java.util.ArrayList;
import java.util.List;

/**
 * An OR next to the other conditions of a query (MOR-091). {@code Query.or(...)} adds its branches to the ONE list of or-queries of the query, so two ORs in an AND chain
 * ({@code (a OR b) AND (c OR d)}), an OR plus the OR of a cursor condition, or an OR plus a negated BETWEEN become one OR of all the branches and widen the result instead of
 * narrowing it. Here the OR is a filter of its own, which is ANDed with the rest.
 */
final class OrFilter {

    private OrFilter() {
    }

    /** adds {@code $or: [branches]} as a condition of the query; every branch is a query with the conditions of that branch */
    @SuppressWarnings({"rawtypes", "unchecked"})
    static void add(Query query, List<Query> branches) {
        List<Object> raw = new ArrayList<>(branches.size());

        for (Query branch : branches) {
            raw.add(branch.toQueryObject());
        }

        FilterExpression or = new FilterExpression();
        or.setField("$or");
        or.setValue(raw);
        query.addChild(or);
    }
}
