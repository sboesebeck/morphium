package de.caluga.morphium.data;

import java.util.List;

/**
 * Describes a parsed query derived from a repository method name.
 * <p>
 * Built at deploy time by {@link MethodNameParser#parse}, executed at runtime by
 * {@link QueryExecutor#execute}. This is the derived-query counterpart of {@link JdqlQuery}
 * (parsed by {@link JdqlParser} for {@code @Query} methods): both descriptors are plain data that
 * an executor turns into a Morphium {@link de.caluga.morphium.query.Query}.
 *
 * @param prefix     the repository method prefix ({@code findBy}, {@code countBy},
 *                   {@code existsBy}, {@code deleteBy})
 * @param conditions the field conditions parsed from the method name, in order
 * @param combinator how the conditions are combined ({@code AND} or {@code OR})
 * @param orderBy    the sort order parsed from an {@code OrderBy...} suffix, empty if none
 * @param returnType the shape of the result the caller expects
 */
public record QueryDescriptor(
        Prefix prefix,
        List<Condition> conditions,
        Combinator combinator,
        List<OrderSpec> orderBy,
        ReturnType returnType
) {

    public enum Prefix { FIND, COUNT, EXISTS, DELETE }

    public enum Combinator { AND, OR }

    public enum ReturnType { SINGLE, OPTIONAL, LIST, STREAM, COUNT, BOOLEAN }

    /**
     * A single field condition parsed from a method name.
     *
     * @param field      the Java field name
     * @param operator   the comparison operator
     * @param paramIndex the index of the method argument supplying the value, or -1 if the
     *                   operator takes no parameter (e.g. {@code IS_NULL})
     * @param paramIndex2 the index of the second method argument, used only by {@code BETWEEN}
     */
    public record Condition(
            String field,
            Operator operator,
            int paramIndex,
            int paramIndex2   // only used by BETWEEN (second param)
    ) {
        /**
         * Convenience constructor for conditions with at most one parameter.
         *
         * @param field      the Java field name
         * @param operator   the comparison operator
         * @param paramIndex the index of the method argument supplying the value, or -1 if none
         */
        public Condition(String field, Operator operator, int paramIndex) {
            this(field, operator, paramIndex, -1);
        }
    }

    public enum Operator {
        EQ, NE, GT, GTE, LT, LTE, BETWEEN,
        IN, NIN,
        LIKE, STARTS_WITH, ENDS_WITH, CONTAINS, NOT_CONTAINS,
        IS_NULL, IS_NOT_NULL, IS_TRUE, IS_FALSE,
        IS_EMPTY, IS_NOT_EMPTY, SIZE, MATCHES, IGNORE_CASE
    }

    /**
     * A single sort field parsed from an {@code OrderBy...} method-name suffix.
     *
     * @param field     the Java field name
     * @param direction the sort direction
     */
    public record OrderSpec(String field, Direction direction) {}

    public enum Direction { ASC, DESC }
}
