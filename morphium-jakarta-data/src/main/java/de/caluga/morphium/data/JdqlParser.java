package de.caluga.morphium.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a JDQL (Jakarta Data Query Language) string into a {@link JdqlQuery}.
 * <p>
 * This is the entry point of the {@code @Query} processing chain: {@link JdqlMethodBridge} calls
 * {@link #parse} once per distinct JDQL string (results are cached there) and gets back a
 * {@link JdqlQuery} descriptor, analogous to how {@link MethodNameParser} turns a method name into
 * a {@link QueryDescriptor} for derived queries. The descriptor is then translated into a Morphium
 * {@link de.caluga.morphium.query.Query} (or an aggregation pipeline for GROUP BY) by
 * {@code JdqlMethodBridge}, with cursor pagination handled by {@link CursorHelper} and single-result
 * semantics by {@link QueryResultHelper}.
 * <p>
 * Supported JDQL subset (MongoDB-compatible):
 * <pre>{@code
 * [SELECT field1, field2 [FROM EntityName]]
 * [WHERE condition [AND|OR condition ...]]
 * [GROUP BY field1, field2 [HAVING aggCondition [AND|OR ...]]]
 * [ORDER BY field [ASC|DESC] [, ...]]
 * }</pre>
 * Conditions:
 * <ul>
 *   <li>{@code field = :param} / {@code field <> :param} / {@code field != :param}</li>
 *   <li>{@code field > :param} / {@code field >= :param} / {@code field < :param} / {@code field <= :param}</li>
 *   <li>{@code field BETWEEN :min AND :max}</li>
 *   <li>{@code field IN :param}</li>
 *   <li>{@code field NOT IN :param}</li>
 *   <li>{@code field LIKE :param}</li>
 *   <li>{@code field IS NULL} / {@code field IS NOT NULL}</li>
 *   <li>Boolean literals: {@code field = true} / {@code field = false}</li>
 *   <li>Numeric literals: {@code field > 100}</li>
 *   <li>String literals: {@code field = 'value'}</li>
 *   <li>NOT prefix: {@code NOT field = :param} / {@code NOT field LIKE :pattern}</li>
 * </ul>
 * Parenthesized groups: {@code field1 = :a AND (field2 IS NULL OR field2 = '')}
 * NOT prefix: {@code NOT field BETWEEN :min AND :max}, {@code NOT (cond1 OR cond2)}
 * <p>
 * Aggregate functions ({@code COUNT}, {@code SUM}, {@code AVG}, {@code MIN}, {@code MAX}) are
 * recognized in the SELECT clause and turn the query into an aggregation pipeline, for example:
 * <pre>{@code
 * SELECT category, COUNT(this), SUM(price)
 * FROM Product
 * WHERE active = true
 * GROUP BY category
 * HAVING COUNT(this) > 1
 * ORDER BY category
 * }</pre>
 * Not supported: JOINs, subqueries.
 */
public final class JdqlParser {

    private JdqlParser() {}

    // Split ORDER BY from WHERE (case-insensitive)
    private static final Pattern ORDER_BY_SPLIT = Pattern.compile(
            "^(.+?)\\s+ORDER\\s+BY\\s+(.+)$", Pattern.CASE_INSENSITIVE);

    // Match aggregate function: COUNT(this), SUM(amount), AVG(field), MIN(field), MAX(field)
    private static final Pattern AGGREGATE_PATTERN = Pattern.compile(
            "(?i)(COUNT|SUM|AVG|MIN|MAX)\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_.]*|this)\\s*\\)");

    // Match a named parameter :paramName
    private static final Pattern PARAM_REF = Pattern.compile(":([a-zA-Z_][a-zA-Z0-9_]*)");

    // Match numeric literal (int or double)
    private static final Pattern NUMERIC_LITERAL = Pattern.compile("-?\\d+(\\.\\d+)?");

    // BETWEEN :min AND :max
    private static final Pattern BETWEEN_PATTERN = Pattern.compile(
            "(.+?)\\s+BETWEEN\\s+(.+?)\\s+AND\\s+(.+)", Pattern.CASE_INSENSITIVE);

    // NOT IN :param
    private static final Pattern NOT_IN_PATTERN = Pattern.compile(
            "(.+?)\\s+NOT\\s+IN\\s+(.+)", Pattern.CASE_INSENSITIVE);

    // IN :param
    private static final Pattern IN_PATTERN = Pattern.compile(
            "(.+?)\\s+IN\\s+(.+)", Pattern.CASE_INSENSITIVE);

    // LIKE :param
    private static final Pattern LIKE_PATTERN = Pattern.compile(
            "(.+?)\\s+LIKE\\s+(.+)", Pattern.CASE_INSENSITIVE);

    // Comparison operators: >=, <=, <>, !=, >, <, =
    private static final Pattern COMP_PATTERN = Pattern.compile("(.+?)\\s*(>=|<=|<>|!=|>|<|=)\\s*(.+)");

    // HAVING condition: AGGREGATE(field) operator value
    private static final Pattern HAVING_PATTERN = Pattern.compile(
            "(?i)(COUNT|SUM|AVG|MIN|MAX)\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_.]*|this)\\s*\\)" +
            "\\s*(>=|<=|<>|!=|>|<|=)\\s*(.+)");

    /**
     * Parses a JDQL query string.
     *
     * @param jdql the JDQL string (may or may not start with "SELECT" or "WHERE")
     * @return the parsed query descriptor
     * @throws IllegalArgumentException if the JDQL cannot be parsed
     */
    public static JdqlQuery parse(String jdql) {
        if (jdql == null || jdql.isBlank()) {
            return new JdqlQuery(null, null, List.of(), JdqlQuery.Combinator.AND, List.of(), null, null, JdqlQuery.Combinator.AND);
        }

        String trimmed = jdql.trim();
        String upper = trimmed.toUpperCase(Locale.ROOT);

        // --- Parse SELECT clause ---
        List<String> selectFields = null;
        List<JdqlQuery.AggregateFunction> aggregateFunctions = null;
        List<String> selectPlainFields = null; // non-null when SELECT mixes aggs + plain fields
        if (upper.startsWith("SELECT ")) {
            int selectEnd = findSelectEnd(upper);
            String selectPart = trimmed.substring("SELECT ".length(), selectEnd).trim();
            List<String> rawFields = Arrays.stream(selectPart.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();

            // Classify each field: aggregate function or plain field
            List<String> plainFields = new ArrayList<>();
            List<JdqlQuery.AggregateFunction> aggFuncs = new ArrayList<>();
            for (String field : rawFields) {
                Matcher aggMatcher = AGGREGATE_PATTERN.matcher(field);
                if (aggMatcher.matches()) {
                    String funcName = aggMatcher.group(1).toUpperCase(Locale.ROOT);
                    String argName = aggMatcher.group(2);
                    JdqlQuery.AggregateType type = JdqlQuery.AggregateType.valueOf(funcName);
                    aggFuncs.add(new JdqlQuery.AggregateFunction(type, argName));
                } else {
                    plainFields.add(field);
                }
            }

            // Defer validation of mixed agg+fields — GROUP BY may legitimize it
            if (!aggFuncs.isEmpty() && !plainFields.isEmpty()) {
                aggregateFunctions = aggFuncs;
                selectPlainFields = plainFields;
            } else if (!aggFuncs.isEmpty()) {
                aggregateFunctions = aggFuncs;
            } else {
                selectFields = plainFields;
            }

            // Advance past SELECT fields
            trimmed = trimmed.substring(selectEnd).trim();
            upper = trimmed.toUpperCase(Locale.ROOT);

            // Skip optional FROM clause
            if (upper.startsWith("FROM ")) {
                int fromEnd = findFromEnd(upper);
                trimmed = trimmed.substring(fromEnd).trim();
                upper = trimmed.toUpperCase(Locale.ROOT);
            }
        }

        // --- From here, the remainder is [WHERE ...] [GROUP BY ...] [ORDER BY ...] ---
        if (trimmed.isEmpty()) {
            if (selectPlainFields != null) {
                throw new IllegalArgumentException(
                        "Mixing aggregate functions and field projections requires GROUP BY: " + jdql);
            }
            return new JdqlQuery(selectFields, aggregateFunctions, List.of(), JdqlQuery.Combinator.AND, List.of(), null, null, JdqlQuery.Combinator.AND);
        }

        // Split off ORDER BY
        List<JdqlQuery.OrderSpec> orderBy = new ArrayList<>();
        String wherePart = trimmed;

        Matcher orderMatcher = ORDER_BY_SPLIT.matcher(trimmed);
        if (orderMatcher.matches()) {
            wherePart = orderMatcher.group(1).trim();
            String orderPart = orderMatcher.group(2).trim();
            orderBy = parseOrderBy(orderPart);
        }

        // Split off GROUP BY from the remainder (ORDER BY already removed)
        List<String> groupByFields = null;
        String whereUpper = wherePart.toUpperCase(Locale.ROOT);
        int groupByIdx = whereUpper.indexOf(" GROUP BY ");
        if (groupByIdx < 0 && whereUpper.startsWith("GROUP BY ")) {
            groupByIdx = 0;
        }
        List<JdqlQuery.HavingCondition> havingConditions = null;
        JdqlQuery.Combinator havingCombinator = JdqlQuery.Combinator.AND;
        if (groupByIdx >= 0) {
            String groupByPart;
            if (groupByIdx == 0) {
                groupByPart = wherePart.substring("GROUP BY ".length()).trim();
                wherePart = "";
            } else {
                groupByPart = wherePart.substring(groupByIdx + " GROUP BY ".length()).trim();
                wherePart = wherePart.substring(0, groupByIdx).trim();
            }

            // Split HAVING from GROUP BY part (ORDER BY already removed)
            String groupByUpper2 = groupByPart.toUpperCase(Locale.ROOT);
            int havingIdx = groupByUpper2.indexOf(" HAVING ");
            if (havingIdx >= 0) {
                String havingPart = groupByPart.substring(havingIdx + " HAVING ".length()).trim();
                groupByPart = groupByPart.substring(0, havingIdx).trim();
                try {
                    HavingParseResult havingResult = parseHavingClause(havingPart);
                    havingConditions = havingResult.conditions();
                    havingCombinator = havingResult.combinator();
                } catch (IllegalArgumentException e) {
                    int havingStart = jdql.toUpperCase(Locale.ROOT).indexOf("HAVING ");
                    throw new IllegalArgumentException(
                            formatParseError(jdql, havingPart, Math.max(0, havingStart), e.getMessage()), e);
                }
            }

            groupByFields = Arrays.stream(groupByPart.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();
        }

        // Validate GROUP BY constraints
        if (groupByFields != null) {
            if (aggregateFunctions == null || aggregateFunctions.isEmpty()) {
                throw new IllegalArgumentException("GROUP BY without aggregate functions: " + jdql);
            }
            if (selectPlainFields != null) {
                for (String f : selectPlainFields) {
                    if (!groupByFields.contains(f)) {
                        throw new IllegalArgumentException(
                                "SELECT field '" + f + "' must appear in GROUP BY clause: " + jdql);
                    }
                }
            }
            if (havingConditions != null && !havingConditions.isEmpty()
                    && (groupByFields == null || groupByFields.isEmpty())) {
                throw new IllegalArgumentException("HAVING without GROUP BY: " + jdql);
            }
        } else if (selectPlainFields != null) {
            throw new IllegalArgumentException(
                    "Mixing aggregate functions and field projections requires GROUP BY: " + jdql);
        }

        // Strip leading WHERE keyword
        if (wherePart.toUpperCase(Locale.ROOT).startsWith("WHERE ")) {
            wherePart = wherePart.substring(6).trim();
        }

        // If empty after stripping WHERE, no conditions
        if (wherePart.isEmpty()) {
            return new JdqlQuery(selectFields, aggregateFunctions, List.of(), JdqlQuery.Combinator.AND, orderBy, groupByFields, havingConditions, havingCombinator);
        }

        // Determine combinator and split conditions
        JdqlQuery.Combinator combinator = JdqlQuery.Combinator.AND;
        List<String> conditionStrings;

        // Check for OR (case-insensitive, not inside BETWEEN...AND or ORDER BY)
        if (containsTopLevelOr(wherePart)) {
            combinator = JdqlQuery.Combinator.OR;
            conditionStrings = splitTopLevel(wherePart, "OR");
        } else {
            conditionStrings = splitTopLevel(wherePart, "AND");
        }

        List<JdqlQuery.JdqlCondition> conditions = new ArrayList<>();
        int searchFrom = 0;
        for (String condStr : conditionStrings) {
            String trimmedCond = condStr.trim();
            try {
                conditions.add(parseConditionOrGroup(trimmedCond));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(formatParseError(jdql, trimmedCond, searchFrom, e.getMessage()), e);
            }
            int found = jdql.indexOf(trimmedCond, searchFrom);
            if (found >= 0) {
                searchFrom = found + trimmedCond.length();
            }
        }

        return new JdqlQuery(selectFields, aggregateFunctions, conditions, combinator, orderBy, groupByFields, havingConditions, havingCombinator);
    }

    // -- SELECT clause helpers --

    /**
     * Finds the end index of the SELECT field list.
     * The SELECT clause ends at the first FROM, WHERE, or ORDER BY keyword.
     */
    private static int findSelectEnd(String upper) {
        int fromIdx = indexOfKeyword(upper, " FROM ");
        int whereIdx = indexOfKeyword(upper, " WHERE ");
        int orderByIdx = indexOfKeyword(upper, " ORDER BY ");
        int groupByIdx = indexOfKeyword(upper, " GROUP BY ");
        int havingIdx = indexOfKeyword(upper, " HAVING ");
        int end = smallestPositive(fromIdx, whereIdx, orderByIdx, groupByIdx, havingIdx);
        return end > 0 ? end : upper.length();
    }

    /**
     * Finds the end of the FROM clause (the entity name after FROM).
     * FROM clause ends at WHERE, ORDER BY, GROUP BY, HAVING, or end of string.
     */
    private static int findFromEnd(String upper) {
        int whereIdx = indexOfKeyword(upper, " WHERE ");
        int orderByIdx = indexOfKeyword(upper, " ORDER BY ");
        int groupByIdx = indexOfKeyword(upper, " GROUP BY ");
        int havingIdx = indexOfKeyword(upper, " HAVING ");
        int end = smallestPositive(whereIdx, orderByIdx, groupByIdx, havingIdx);
        return end > 0 ? end : upper.length();
    }

    private static int indexOfKeyword(String upper, String keyword) {
        return upper.indexOf(keyword);
    }

    private static int smallestPositive(int... values) {
        int min = Integer.MAX_VALUE;
        for (int v : values) {
            if (v > 0) min = Math.min(min, v);
        }
        return min == Integer.MAX_VALUE ? -1 : min;
    }

    // -- Condition parsing --

    /**
     * Parses a condition string that may be a parenthesized group or a simple condition.
     * E.g. {@code (otaUpdateError IS NULL OR otaUpdateError = '')} is parsed as a group condition.
     */
    private static JdqlQuery.JdqlCondition parseConditionOrGroup(String cond) {
        String trimmed = cond.trim();

        // NOT (...) group negation
        if (trimmed.toUpperCase(Locale.ROOT).startsWith("NOT ")) {
            String afterNot = trimmed.substring(4).trim();
            if (afterNot.startsWith("(") && afterNot.endsWith(")") && isBalancedGroup(afterNot)) {
                JdqlQuery.JdqlCondition innerGroup = parseConditionOrGroup(afterNot);
                if (innerGroup.isGroup()) {
                    // XOR negation: NOT on an already-negated group cancels out
                    return new JdqlQuery.JdqlCondition(null, null, null, null, null, !innerGroup.negated(),
                            innerGroup.groupConditions(), innerGroup.groupCombinator());
                }
                // Single condition wrapped in parens after NOT → just negate it
                return new JdqlQuery.JdqlCondition(innerGroup.fieldName(), innerGroup.operator(),
                        innerGroup.valueRef(), innerGroup.valueRef2(), innerGroup.literal(),
                        !innerGroup.negated(), null, null);
            }
        }

        if (trimmed.startsWith("(") && trimmed.endsWith(")") && isBalancedGroup(trimmed)) {
            String inner = trimmed.substring(1, trimmed.length() - 1).trim();
            JdqlQuery.Combinator groupCombinator = JdqlQuery.Combinator.AND;
            List<String> subConditions;
            if (containsTopLevelOr(inner)) {
                groupCombinator = JdqlQuery.Combinator.OR;
                subConditions = splitTopLevel(inner, "OR");
            } else {
                subConditions = splitTopLevel(inner, "AND");
            }
            // Single condition inside parens — no group needed, just parse directly
            if (subConditions.size() == 1) {
                return parseConditionOrGroup(subConditions.get(0).trim());
            }
            List<JdqlQuery.JdqlCondition> groupConds = new ArrayList<>();
            for (String sub : subConditions) {
                groupConds.add(parseConditionOrGroup(sub.trim()));
            }
            return JdqlQuery.JdqlCondition.group(groupConds, groupCombinator);
        }
        return parseCondition(trimmed);
    }

    /**
     * Checks if a string starting with '(' has the closing ')' at the very end,
     * meaning the outer parentheses wrap the entire expression.
     */
    private static boolean isBalancedGroup(String s) {
        int depth = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == '(') depth++;
            else if (s.charAt(i) == ')') depth--;
            if (depth == 0 && i < s.length() - 1) return false;
        }
        return depth == 0;
    }

    private static JdqlQuery.JdqlCondition parseCondition(String cond) {
        String trimmed = cond.trim();
        String upper = trimmed.toUpperCase(Locale.ROOT);

        // Detect and strip NOT prefix
        boolean negated = false;
        if (upper.startsWith("NOT ")) {
            negated = true;
            trimmed = trimmed.substring(4).trim();
            upper = trimmed.toUpperCase(Locale.ROOT);
        }

        // IS NOT NULL
        if (upper.endsWith(" IS NOT NULL")) {
            String field = trimmed.substring(0, trimmed.length() - " IS NOT NULL".length()).trim();
            JdqlQuery.Operator op = negated ? JdqlQuery.Operator.IS_NULL : JdqlQuery.Operator.IS_NOT_NULL;
            return new JdqlQuery.JdqlCondition(field, op, null, null, null, false);
        }

        // IS NULL
        if (upper.endsWith(" IS NULL")) {
            String field = trimmed.substring(0, trimmed.length() - " IS NULL".length()).trim();
            JdqlQuery.Operator op = negated ? JdqlQuery.Operator.IS_NOT_NULL : JdqlQuery.Operator.IS_NULL;
            return new JdqlQuery.JdqlCondition(field, op, null, null, null, false);
        }

        // BETWEEN :min AND :max
        Matcher betweenMatcher = BETWEEN_PATTERN.matcher(trimmed);
        if (betweenMatcher.matches()) {
            String field = betweenMatcher.group(1).trim();
            String minRef = betweenMatcher.group(2).trim();
            String maxRef = betweenMatcher.group(3).trim();
            return new JdqlQuery.JdqlCondition(field, JdqlQuery.Operator.BETWEEN,
                    extractParamOrLiteral(minRef), extractParamOrLiteral(maxRef), null, negated);
        }

        // NOT IN :param (within condition, NOT as infix operator on field)
        Matcher notInMatcher = NOT_IN_PATTERN.matcher(trimmed);
        if (notInMatcher.matches()) {
            String field = notInMatcher.group(1).trim();
            String paramRef = notInMatcher.group(2).trim();
            // "NOT field NOT IN x" (double negation) → field IN x
            JdqlQuery.Operator op = negated ? JdqlQuery.Operator.IN : JdqlQuery.Operator.NOT_IN;
            return new JdqlQuery.JdqlCondition(field, op,
                    extractParamOrLiteral(paramRef), null, null, false);
        }

        // IN :param
        Matcher inMatcher = IN_PATTERN.matcher(trimmed);
        if (inMatcher.matches()) {
            String field = inMatcher.group(1).trim();
            String paramRef = inMatcher.group(2).trim();
            return new JdqlQuery.JdqlCondition(field, JdqlQuery.Operator.IN,
                    extractParamOrLiteral(paramRef), null, null, negated);
        }

        // LIKE :param
        Matcher likeMatcher = LIKE_PATTERN.matcher(trimmed);
        if (likeMatcher.matches()) {
            String field = likeMatcher.group(1).trim();
            String paramRef = likeMatcher.group(2).trim();
            return new JdqlQuery.JdqlCondition(field, JdqlQuery.Operator.LIKE,
                    extractParamOrLiteral(paramRef), null, null, negated);
        }

        // Comparison operators: >=, <=, <>, !=, >, <, =
        Matcher compMatcher = COMP_PATTERN.matcher(trimmed);
        if (compMatcher.matches()) {
            String field = compMatcher.group(1).trim();
            String op = compMatcher.group(2);
            String valueRef = compMatcher.group(3).trim();

            JdqlQuery.Operator operator = switch (op) {
                case "=" -> JdqlQuery.Operator.EQ;
                case "<>", "!=" -> JdqlQuery.Operator.NE;
                case ">" -> JdqlQuery.Operator.GT;
                case ">=" -> JdqlQuery.Operator.GTE;
                case "<" -> JdqlQuery.Operator.LT;
                case "<=" -> JdqlQuery.Operator.LTE;
                default -> throw new IllegalArgumentException("Unknown operator: " + op);
            };

            // Check for boolean/null literals
            String upperVal = valueRef.toUpperCase(Locale.ROOT);
            if ("TRUE".equals(upperVal)) {
                return new JdqlQuery.JdqlCondition(field, operator, null, null, Boolean.TRUE, negated);
            }
            if ("FALSE".equals(upperVal)) {
                return new JdqlQuery.JdqlCondition(field, operator, null, null, Boolean.FALSE, negated);
            }
            if ("NULL".equals(upperVal)) {
                JdqlQuery.Operator nullOp = operator == JdqlQuery.Operator.EQ
                        ? JdqlQuery.Operator.IS_NULL : JdqlQuery.Operator.IS_NOT_NULL;
                if (negated) {
                    nullOp = nullOp == JdqlQuery.Operator.IS_NULL
                            ? JdqlQuery.Operator.IS_NOT_NULL : JdqlQuery.Operator.IS_NULL;
                }
                return new JdqlQuery.JdqlCondition(field, nullOp, null, null, null, false);
            }

            return new JdqlQuery.JdqlCondition(field, operator,
                    extractParamOrLiteral(valueRef), null, null, negated);
        }

        throw new IllegalArgumentException("Cannot parse JDQL condition: " + cond);
    }

    /**
     * Extracts a parameter reference or literal value from a token.
     * Parameters start with ':', literals are numbers or quoted strings.
     */
    private static String extractParamOrLiteral(String token) {
        token = token.trim();
        if (token.startsWith(":")) {
            return token; // keep the colon prefix to identify as param ref
        }
        // Numeric literal or other literal — return as-is
        return token;
    }

    // -- ORDER BY parsing --

    private static List<JdqlQuery.OrderSpec> parseOrderBy(String orderPart) {
        List<JdqlQuery.OrderSpec> specs = new ArrayList<>();
        String[] parts = orderPart.split(",");
        for (String part : parts) {
            String p = part.trim();
            if (p.isEmpty()) continue;
            String[] tokens = p.split("\\s+");
            String field = tokens[0];
            boolean ascending = true;
            if (tokens.length > 1) {
                ascending = !"DESC".equalsIgnoreCase(tokens[1]);
            }
            specs.add(new JdqlQuery.OrderSpec(field, ascending));
        }
        return specs;
    }

    // -- Top-level AND/OR splitting (avoids splitting inside BETWEEN...AND) --

    private static boolean containsTopLevelOr(String wherePart) {
        String upper = wherePart.toUpperCase(Locale.ROOT);
        int depth = 0;
        for (int i = 0; i < upper.length(); i++) {
            char c = upper.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && i + 4 <= upper.length()
                    && upper.startsWith(" OR ", i)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Splits a WHERE clause on a top-level combinator (AND or OR).
     * Handles BETWEEN...AND by not splitting inside it.
     * Respects parenthesis depth — never splits inside parenthesized groups.
     */
    private static List<String> splitTopLevel(String wherePart, String combinator) {
        List<String> result = new ArrayList<>();
        String upper = wherePart.toUpperCase(Locale.ROOT);
        String sep = " " + combinator + " ";
        int sepLen = sep.length();

        int start = 0;
        int depth = 0;

        for (int i = 0; i < upper.length(); i++) {
            char c = upper.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (depth == 0 && i + sepLen <= upper.length()
                    && upper.startsWith(sep, i)) {
                // Check if this AND is part of BETWEEN...AND
                if ("AND".equals(combinator) && isBetweenAnd(upper, i)) {
                    continue;
                }
                result.add(wherePart.substring(start, i));
                start = i + sepLen;
                i += sepLen - 1; // skip past separator (loop will increment)
            }
        }
        result.add(wherePart.substring(start));

        return result;
    }

    /**
     * Checks if the AND at the given position is part of a BETWEEN...AND construct.
     */
    private static boolean isBetweenAnd(String upper, int andIdx) {
        // Look backwards from AND position for "BETWEEN"
        String before = upper.substring(0, andIdx);
        int betweenIdx = before.lastIndexOf("BETWEEN");
        if (betweenIdx < 0) return false;

        // Check that there's no other AND between BETWEEN and this AND
        String betweenToAnd = before.substring(betweenIdx + "BETWEEN".length());
        return !betweenToAnd.contains(" AND ");
    }

    // -- HAVING parsing --

    private record HavingParseResult(List<JdqlQuery.HavingCondition> conditions,
                                      JdqlQuery.Combinator combinator) {}

    private static HavingParseResult parseHavingClause(String havingPart) {
        List<JdqlQuery.HavingCondition> conditions = new ArrayList<>();
        JdqlQuery.Combinator combinator = JdqlQuery.Combinator.AND;
        List<String> parts;

        if (containsTopLevelOr(havingPart)) {
            combinator = JdqlQuery.Combinator.OR;
            parts = splitTopLevel(havingPart, "OR");
        } else {
            parts = splitTopLevel(havingPart, "AND");
        }

        for (String part : parts) {
            conditions.add(parseHavingCondition(part.trim()));
        }
        return new HavingParseResult(conditions, combinator);
    }

    private static JdqlQuery.HavingCondition parseHavingCondition(String cond) {
        Matcher m = HAVING_PATTERN.matcher(cond.trim());
        if (!m.matches()) {
            throw new IllegalArgumentException("Cannot parse HAVING condition: " + cond
                    + ". Expected: AGGREGATE(field) operator value");
        }

        String funcName = m.group(1).toUpperCase(Locale.ROOT);
        String argName = m.group(2);
        String opStr = m.group(3);
        String valueRef = m.group(4).trim();

        String aggFuncStr = funcName + "(" + argName + ")";

        JdqlQuery.Operator operator = switch (opStr) {
            case "=" -> JdqlQuery.Operator.EQ;
            case "<>", "!=" -> JdqlQuery.Operator.NE;
            case ">" -> JdqlQuery.Operator.GT;
            case ">=" -> JdqlQuery.Operator.GTE;
            case "<" -> JdqlQuery.Operator.LT;
            case "<=" -> JdqlQuery.Operator.LTE;
            default -> throw new IllegalArgumentException("Unknown HAVING operator: " + opStr);
        };

        return new JdqlQuery.HavingCondition(aggFuncStr, operator, extractParamOrLiteral(valueRef));
    }

    /**
     * Formats a parse error with position information and a caret pointer.
     *
     * @param searchFrom index in originalJdql to start searching from, avoids
     *                   pointing at a duplicate earlier occurrence
     */
    private static String formatParseError(String originalJdql, String failedFragment,
                                           int searchFrom, String detail) {
        int pos = originalJdql.indexOf(failedFragment, searchFrom);
        if (pos < 0) {
            pos = originalJdql.indexOf(failedFragment);
        }
        if (pos < 0) {
            return detail + "\n  JDQL: " + originalJdql;
        }
        return "JDQL parse error at position " + pos + ": " + detail
                + "\n  " + originalJdql
                + "\n  " + " ".repeat(pos) + "^";
    }
}
