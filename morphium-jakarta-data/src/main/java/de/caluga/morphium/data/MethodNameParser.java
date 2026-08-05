package de.caluga.morphium.data;

import de.caluga.morphium.data.QueryDescriptor.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a Jakarta Data repository method name into a {@link QueryDescriptor}.
 * <p>
 * This is the entry point of the query-derivation processing chain: {@link QueryMethodBridge}
 * calls {@link #parse} once per distinct method name (results are cached there) and gets back a
 * {@link QueryDescriptor}, which {@link QueryExecutor} then translates into a Morphium
 * {@link de.caluga.morphium.query.Query} and executes, with single-result semantics enforced by
 * {@link QueryResultHelper}. This mirrors what {@link JdqlParser} does for {@code @Query}-annotated
 * methods, but derives the query purely from the method name instead of an explicit query string.
 * <p>
 * Supports prefixes: {@code findBy}, {@code countBy}, {@code existsBy}, {@code deleteBy}.
 * Supports operators: Equals/Is, Not, GreaterThan, GreaterThanEqual, LessThan, LessThanEqual,
 * Between, In, NotIn, Like, StartsWith, EndsWith, Null/IsNull, NotNull/IsNotNull, True, False.
 * Supports combinators: And, Or.
 * Supports OrderBy suffix: {@code OrderByFieldAsc}, {@code OrderByFieldDesc}.
 * <p>
 * Example:
 * <pre>{@code
 * // Method name:
 * List<Product> findByCategoryAndPriceGreaterThanOrderByPriceDesc(String category, double minPrice);
 *
 * // Parses to a QueryDescriptor equivalent to:
 * //   prefix     = FIND
 * //   conditions = [category EQ arg0, price GT arg1]
 * //   combinator = AND
 * //   orderBy    = [price DESC]
 * }</pre>
 */
public final class MethodNameParser {

    private MethodNameParser() {}

    private static final Pattern PREFIX_PATTERN = Pattern.compile(
            "^(find|count|exists|delete)By(.*)$");

    private static final Pattern ORDER_BY_SPLIT = Pattern.compile(
            "^(.+?)OrderBy(.+)$");

    /**
     * Parses the given method name into a {@link QueryDescriptor}.
     *
     * @param methodName the repository method name
     * @param entityFields set of known Java field names on the entity (for validation)
     * @return the parsed descriptor
     * @throws IllegalArgumentException if the method name cannot be parsed
     */
    public static QueryDescriptor parse(String methodName, java.util.Set<String> entityFields) {
        Matcher m = PREFIX_PATTERN.matcher(methodName);
        if (!m.matches()) {
            throw new IllegalArgumentException(
                    "Cannot parse repository method name: " + methodName
                    + ". Expected pattern: findBy.../countBy.../existsBy.../deleteBy...");
        }

        String prefixStr = m.group(1);
        String rest = m.group(2);

        Prefix prefix = switch (prefixStr) {
            case "find" -> Prefix.FIND;
            case "count" -> Prefix.COUNT;
            case "exists" -> Prefix.EXISTS;
            case "delete" -> Prefix.DELETE;
            default -> throw new IllegalArgumentException("Unknown prefix: " + prefixStr);
        };

        ReturnType returnType = switch (prefix) {
            case FIND -> ReturnType.LIST;  // may be overridden to SINGLE by caller
            case COUNT -> ReturnType.COUNT;
            case EXISTS -> ReturnType.BOOLEAN;
            case DELETE -> ReturnType.COUNT;
        };

        // No conditions after "By" → match all entities (e.g. countBy(), findBy(), deleteBy())
        if (rest.isEmpty()) {
            return new QueryDescriptor(prefix, List.of(), Combinator.AND, List.of(), returnType);
        }

        // Split off OrderBy clause
        List<OrderSpec> orderSpecs = new ArrayList<>();
        Matcher orderMatcher = ORDER_BY_SPLIT.matcher(rest);
        if (orderMatcher.matches()) {
            rest = orderMatcher.group(1);
            String orderPart = orderMatcher.group(2);
            orderSpecs = parseOrderBy(orderPart);
        }

        // Determine combinator: check for "Or" or "And"
        // We need to split on And/Or but only at word boundaries between conditions
        //
        // Note: this module deliberately supports only a single combinator per query
        // (see QueryDescriptor's class Javadoc) rather than a full boolean-expression tree.
        // A method name that mixes both "And" and "Or" (e.g. "findByStatusAndCategoryOrPriority")
        // cannot be represented faithfully by that single-combinator model — silently picking one
        // combinator and ignoring the other would produce a query that looks plausible but is
        // wrong. We fail fast instead of guessing.
        boolean hasAnd = containsCombinator(rest, "And");
        boolean hasOr = containsCombinator(rest, "Or");
        if (hasAnd && hasOr) {
            throw new IllegalArgumentException(
                    "Mixed And/Or combinators in a single derived query method are not supported: "
                    + methodName + ". Use @Query with JDQL for complex boolean expressions.");
        }

        Combinator combinator = Combinator.AND;
        String[] parts;
        if (hasOr) {
            combinator = Combinator.OR;
            parts = splitOnCombinator(rest, "Or");
        } else {
            parts = splitOnCombinator(rest, "And");
        }

        // Parse each condition part
        List<Condition> conditions = new ArrayList<>();
        int paramIndex = 0;
        for (String part : parts) {
            ParsedCondition pc = parseCondition(part, paramIndex, entityFields);
            conditions.add(pc.condition);
            paramIndex = pc.nextParamIndex;
        }

        return new QueryDescriptor(prefix, conditions, combinator, orderSpecs, returnType);
    }

    // -- OrderBy parsing --

    private static List<OrderSpec> parseOrderBy(String orderPart) {
        List<OrderSpec> specs = new ArrayList<>();
        // Split on Asc/Desc boundaries while keeping the direction
        // e.g. "PriceDescNameAsc" -> [Price,Desc], [Name,Asc]
        // Pattern: field name followed by optional Asc/Desc
        Pattern p = Pattern.compile("([A-Z][a-z0-9]*(?:[A-Z][a-z0-9]*)*?)(Asc|Desc)?(?=(?:[A-Z])|$)");

        // Simpler approach: split tokens
        List<String> tokens = splitCamelCase(orderPart);
        int i = 0;
        while (i < tokens.size()) {
            StringBuilder fieldBuilder = new StringBuilder();
            fieldBuilder.append(decapitalize(tokens.get(i)));
            i++;
            // Consume tokens until we hit Asc/Desc or end
            while (i < tokens.size() && !tokens.get(i).equals("Asc") && !tokens.get(i).equals("Desc")) {
                fieldBuilder.append(capitalize(tokens.get(i)));
                i++;
            }
            Direction dir = Direction.ASC;
            if (i < tokens.size()) {
                if (tokens.get(i).equals("Desc")) {
                    dir = Direction.DESC;
                }
                i++;
            }
            specs.add(new OrderSpec(fieldBuilder.toString(), dir));
        }
        return specs;
    }

    // -- Condition parsing --

    private record ParsedCondition(Condition condition, int nextParamIndex) {}

    private static ParsedCondition parseCondition(String part, int paramIndex,
                                                   java.util.Set<String> entityFields) {
        // Try to match operators from longest to shortest
        for (OperatorMatch om : OPERATOR_MATCHES) {
            if (part.endsWith(om.suffix)) {
                String fieldPart = part.substring(0, part.length() - om.suffix.length());
                String field = resolveFieldName(fieldPart, entityFields);
                if (om.operator == Operator.BETWEEN) {
                    return new ParsedCondition(
                            new Condition(field, om.operator, paramIndex, paramIndex + 1),
                            paramIndex + 2);
                }
                if (om.paramCount == 0) {
                    return new ParsedCondition(
                            new Condition(field, om.operator, -1),
                            paramIndex);
                }
                return new ParsedCondition(
                        new Condition(field, om.operator, paramIndex),
                        paramIndex + om.paramCount);
            }
        }

        // No operator suffix found → implicit Equals
        String field = resolveFieldName(part, entityFields);
        return new ParsedCondition(
                new Condition(field, Operator.EQ, paramIndex),
                paramIndex + 1);
    }

    private record OperatorMatch(String suffix, Operator operator, int paramCount) {}

    // Ordered longest-first to avoid prefix ambiguity
    private static final List<OperatorMatch> OPERATOR_MATCHES = List.of(
            new OperatorMatch("GreaterThanEqual", Operator.GTE, 1),
            new OperatorMatch("LessThanEqual", Operator.LTE, 1),
            new OperatorMatch("GreaterThan", Operator.GT, 1),
            new OperatorMatch("LessThan", Operator.LT, 1),
            new OperatorMatch("NotContains", Operator.NOT_CONTAINS, 1),
            new OperatorMatch("IsNotEmpty", Operator.IS_NOT_EMPTY, 0),
            new OperatorMatch("IsNotNull", Operator.IS_NOT_NULL, 0),
            new OperatorMatch("IgnoreCase", Operator.IGNORE_CASE, 1),
            new OperatorMatch("NotNull", Operator.IS_NOT_NULL, 0),
            new OperatorMatch("NotEmpty", Operator.IS_NOT_EMPTY, 0),
            new OperatorMatch("IsEmpty", Operator.IS_EMPTY, 0),
            new OperatorMatch("IsNull", Operator.IS_NULL, 0),
            new OperatorMatch("IsTrue", Operator.IS_TRUE, 0),
            new OperatorMatch("IsFalse", Operator.IS_FALSE, 0),
            new OperatorMatch("StartsWith", Operator.STARTS_WITH, 1),
            new OperatorMatch("EndsWith", Operator.ENDS_WITH, 1),
            new OperatorMatch("Contains", Operator.CONTAINS, 1),
            new OperatorMatch("Matches", Operator.MATCHES, 1),
            new OperatorMatch("Between", Operator.BETWEEN, 2),
            new OperatorMatch("NotIn", Operator.NIN, 1),
            new OperatorMatch("Equals", Operator.EQ, 1),
            new OperatorMatch("Regex", Operator.MATCHES, 1),
            new OperatorMatch("Empty", Operator.IS_EMPTY, 0),
            new OperatorMatch("Not", Operator.NE, 1),
            new OperatorMatch("Null", Operator.IS_NULL, 0),
            new OperatorMatch("True", Operator.IS_TRUE, 0),
            new OperatorMatch("False", Operator.IS_FALSE, 0),
            new OperatorMatch("Size", Operator.SIZE, 1),
            new OperatorMatch("Like", Operator.LIKE, 1),
            new OperatorMatch("In", Operator.IN, 1),
            new OperatorMatch("Is", Operator.EQ, 1)
    );

    // -- Field name resolution --

    /**
     * Converts a PascalCase field segment from a method name to a Java field name.
     * E.g. "Status" → "status", "CustomerName" → "customerName".
     */
    private static String resolveFieldName(String part, java.util.Set<String> entityFields) {
        String camelCase = decapitalize(part);
        if (entityFields != null && !entityFields.isEmpty()) {
            // Try exact match first
            if (entityFields.contains(camelCase)) {
                return camelCase;
            }
            // Try case-insensitive match
            for (String f : entityFields) {
                if (f.equalsIgnoreCase(camelCase)) {
                    return f;
                }
            }
            // Not found in either form: this is not a valid field on the entity.
            // Fail fast here instead of silently producing a query that will never
            // match anything once executed against the database.
            throw new IllegalArgumentException(
                    "Unknown field '" + camelCase + "' referenced in derived query method"
                    + " (not found in entity fields: " + entityFields + ")");
        }
        return camelCase;
    }

    // -- Combinator detection and splitting --

    private static boolean containsCombinator(String text, String combinator) {
        // Must appear between two uppercase-starting segments
        int idx = text.indexOf(combinator);
        while (idx > 0 && idx + combinator.length() < text.length()) {
            char before = text.charAt(idx - 1);
            char after = text.charAt(idx + combinator.length());
            if (Character.isLetterOrDigit(before) && Character.isUpperCase(after)) {
                return true;
            }
            idx = text.indexOf(combinator, idx + 1);
        }
        return false;
    }

    private static String[] splitOnCombinator(String text, String combinator) {
        List<String> result = new ArrayList<>();
        int start = 0;
        int idx = text.indexOf(combinator, start);
        while (idx > 0 && idx + combinator.length() < text.length()) {
            char before = text.charAt(idx - 1);
            char after = text.charAt(idx + combinator.length());
            if (Character.isLetterOrDigit(before) && Character.isUpperCase(after)) {
                result.add(text.substring(start, idx));
                start = idx + combinator.length();
            }
            idx = text.indexOf(combinator, idx + 1);
        }
        result.add(text.substring(start));
        return result.toArray(new String[0]);
    }

    // -- Utility --

    private static List<String> splitCamelCase(String s) {
        List<String> tokens = new ArrayList<>();
        int start = 0;
        for (int i = 1; i < s.length(); i++) {
            if (Character.isUpperCase(s.charAt(i))) {
                tokens.add(s.substring(start, i));
                start = i;
            }
        }
        tokens.add(s.substring(start));
        return tokens;
    }

    private static String decapitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        if (s.length() > 1 && Character.isUpperCase(s.charAt(1))) {
            return s; // e.g. "URL" stays "URL"
        }
        return Character.toLowerCase(s.charAt(0)) + s.substring(1);
    }

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }
}
