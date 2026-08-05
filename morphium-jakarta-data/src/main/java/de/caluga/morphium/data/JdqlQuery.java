package de.caluga.morphium.data;

import java.util.List;

/**
 * Parsed representation of a JDQL (Jakarta Data Query Language) query.
 * Created by {@link JdqlParser} from a {@code @Query} annotation value.
 *
 * @param selectFields      projected field names from SELECT clause, null or empty = all fields
 * @param aggregateFunctions aggregate functions (COUNT/SUM/AVG/MIN/MAX) from the SELECT clause,
 *                           null or empty when the query has no aggregation
 * @param conditions        the WHERE conditions (simple or grouped), empty if there is no WHERE clause
 * @param combinator        how the top-level {@code conditions} are combined ({@code AND} or {@code OR})
 * @param orderBy           the ORDER BY fields and directions, empty if there is no ORDER BY clause
 * @param groupByFields     fields from GROUP BY clause, null when no GROUP BY
 * @param havingConditions  the HAVING conditions filtering aggregated results, null or empty when
 *                          there is no HAVING clause
 * @param havingCombinator  how the top-level {@code havingConditions} are combined ({@code AND} or {@code OR})
 */
public record JdqlQuery(
        List<String> selectFields,
        List<AggregateFunction> aggregateFunctions,
        List<JdqlCondition> conditions,
        Combinator combinator,
        List<OrderSpec> orderBy,
        List<String> groupByFields,
        List<HavingCondition> havingConditions,
        Combinator havingCombinator
) {

    public enum Combinator { AND, OR }

    public enum AggregateType { COUNT, SUM, AVG, MIN, MAX }

    public record AggregateFunction(AggregateType type, String field) {}

    public enum Operator {
        EQ, NE, GT, GTE, LT, LTE,
        BETWEEN, IN, NOT_IN,
        LIKE, IS_NULL, IS_NOT_NULL
    }

    /**
     * A single JDQL condition or a parenthesized group of conditions.
     * <p>
     * Simple condition: {@code fieldName} and {@code operator} are set, {@code groupConditions} is null.
     * Group condition: {@code groupConditions} and {@code groupCombinator} are set, {@code fieldName} is null.
     *
     * @param fieldName        the entity field name (null for group conditions)
     * @param operator         the comparison operator (null for group conditions)
     * @param valueRef         parameter reference (":name") or literal value, null for IS NULL/IS NOT NULL
     * @param valueRef2        second param/literal for BETWEEN, null otherwise
     * @param literal          literal value (Boolean, etc.) when not using a parameter reference
     * @param negated          true if the condition is prefixed with NOT
     * @param groupConditions  nested conditions for parenthesized groups, null for simple conditions
     * @param groupCombinator  combinator (AND/OR) for the group, null for simple conditions
     */
    public record JdqlCondition(
            String fieldName,
            Operator operator,
            String valueRef,
            String valueRef2,
            Object literal,
            boolean negated,
            List<JdqlCondition> groupConditions,
            Combinator groupCombinator
    ) {
        /** Convenience constructor without negation (backwards-compatible). */
        public JdqlCondition(String fieldName, Operator operator, String valueRef,
                             String valueRef2, Object literal) {
            this(fieldName, operator, valueRef, valueRef2, literal, false, null, null);
        }

        /** Convenience constructor with negation but no group. */
        public JdqlCondition(String fieldName, Operator operator, String valueRef,
                             String valueRef2, Object literal, boolean negated) {
            this(fieldName, operator, valueRef, valueRef2, literal, negated, null, null);
        }

        /** Creates a group condition from nested conditions. */
        public static JdqlCondition group(List<JdqlCondition> conditions, Combinator combinator) {
            return new JdqlCondition(null, null, null, null, null, false, conditions, combinator);
        }

        public boolean isGroup() {
            return groupConditions != null;
        }
    }

    public record OrderSpec(String field, boolean ascending) {}

    /**
     * A single HAVING condition referencing an aggregate result.
     *
     * @param aggregateFunction canonical form, e.g. "COUNT(this)" or "SUM(amount)"
     * @param operator          comparison operator
     * @param valueRef          parameter reference (":name") or numeric literal string
     */
    public record HavingCondition(
            String aggregateFunction,
            Operator operator,
            String valueRef
    ) {}
}
