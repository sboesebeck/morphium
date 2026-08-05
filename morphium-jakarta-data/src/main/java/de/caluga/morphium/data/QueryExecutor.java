package de.caluga.morphium.data;

import de.caluga.morphium.FilterExpression;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Aliases;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.data.QueryDescriptor.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Executes a {@link QueryDescriptor} against a Morphium instance at runtime.
 * <p>
 * This is the runtime counterpart of {@link MethodNameParser}: {@link QueryMethodBridge} parses
 * the method name into a {@link QueryDescriptor} once (cached there) and calls {@link #execute}
 * for every invocation. {@link #execute} builds a Morphium {@link Query} from the descriptor's
 * conditions and sort order, resolving Java field names to MongoDB field names via
 * {@code morphium.getARHelper().getMongoFieldName()}, and — for single-result return types —
 * delegates result-cardinality checks to {@link QueryResultHelper}. This mirrors what
 * {@link JdqlMethodBridge} does for {@code @Query} methods and what {@link FindMethodBridge}
 * does for {@code @Find} methods.
 */
public final class QueryExecutor {

    private static final Logger log = LoggerFactory.getLogger(QueryExecutor.class);

    private QueryExecutor() {}

    /**
     * Executes the given query descriptor and returns the result.
     *
     * @param descriptor the parsed query
     * @param args       the method arguments
     * @param repo       the repository instance (provides Morphium + metadata)
     * @return the query result (List, single entity, long, boolean, or Stream)
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Object execute(QueryDescriptor descriptor,
                                 Object[] args,
                                 AbstractMorphiumRepository<?, ?> repo) {
        Morphium morphium = repo.getMorphium();
        Class entityClass = repo.getMetadata().entityClass();
        Query query = morphium.createQueryFor(entityClass);

        // Apply conditions
        applyConditions(query, descriptor, args, morphium, entityClass);

        // Apply sorting
        if (descriptor.orderBy() != null && !descriptor.orderBy().isEmpty()) {
            applySorting(query, descriptor.orderBy(), morphium, entityClass);
        }

        // Execute based on prefix
        return switch (descriptor.prefix()) {
            case FIND -> switch (descriptor.returnType()) {
                case SINGLE -> QueryResultHelper.requireSingle(query);
                case OPTIONAL -> QueryResultHelper.optionalSingle(query);
                case STREAM -> query.stream();
                default -> query.asList();
            };
            case COUNT -> query.countAll();
            case EXISTS -> query.countAll() > 0;
            // Uses bulk deleteMany — does NOT fire @PreRemove/@PostRemove lifecycle
            // callbacks. This is intentional for performance (avoids loading all entities
            // into memory). Entities requiring lifecycle hooks should use Morphium.delete()
            // directly instead of derived deleteBy* methods.
            //
            // The returned count is read from the "n" key of the MongoDB delete-command
            // result map (matches wire-protocol convention; see InMemoryDriver.delete() and
            // AliasesTest, which read the analogous store-result the same way). If for any
            // reason the driver's result map does not contain a numeric "n" entry, we fall
            // back to a pre-delete countAll(); in that fallback path there is a narrow
            // concurrency window between the count and the actual delete where concurrent
            // inserts/deletes on the same query could make the returned number slightly
            // inaccurate.
            case DELETE -> {
                long preCount = query.countAll();
                Map<String, Object> deleteResult = query.delete();
                Object n = deleteResult == null ? null : deleteResult.get("n");
                yield (n instanceof Number) ? ((Number) n).longValue() : preCount;
            }
        };
    }

    // Visible for testing — called directly by QueryExecutorAliasTest
    @SuppressWarnings({"unchecked", "rawtypes"})
    static void applyConditions(Query query,
                                QueryDescriptor descriptor,
                                Object[] args,
                                Morphium morphium,
                                Class entityClass) {
        boolean isOr = descriptor.combinator() == Combinator.OR;

        if (isOr && descriptor.conditions().size() > 1) {
            // Build OR query using Morphium's or() mechanism
            List<Query> orQueries = new ArrayList<>();
            for (Condition cond : descriptor.conditions()) {
                Query sub = morphium.createQueryFor(entityClass);
                applyCondition(sub, cond, args, morphium, entityClass);
                orQueries.add(sub);
            }
            query.or(orQueries);
        } else {
            for (Condition cond : descriptor.conditions()) {
                applyCondition(query, cond, args, morphium, entityClass);
            }
        }
    }

    // Visible for testing — called indirectly via applyConditions
    @SuppressWarnings({"unchecked", "rawtypes"})
    static void applyCondition(Query query,
                               Condition cond,
                               Object[] args,
                               Morphium morphium,
                               Class entityClass) {
        String mongoField = resolveMongoField(morphium, entityClass, cond.field());
        List<String> aliases = resolveAliases(morphium, entityClass, cond.field());

        if (!aliases.isEmpty()) {
            // Field has @Aliases — build a boolean combination over the current mongo
            // name + all aliases. LinkedHashSet deduplicates in case an alias equals the
            // current mongo name.
            //
            // For POSITIVE operators (EQ, LIKE, CONTAINS, ...) we need "matches under
            // ANY of the names", i.e. $or.
            //
            // For NEGATING operators (NE, NIN, NOT_CONTAINS, IS_NOT_NULL, IS_NOT_EMPTY —
            // anything expressing "not X") we need "does NOT match under ANY of the
            // names", i.e. the condition must hold for EVERY alias branch ($and). Using
            // $or here would be wrong: "field != X OR alias != X" is true for almost any
            // document (e.g. one that has X under the current name but no value at all
            // under the alias name), defeating the negation entirely.
            List<Map<String, Object>> aliasBranches = new ArrayList<>();
            Set<String> uniqueFields = new LinkedHashSet<>();
            uniqueFields.add(mongoField);
            uniqueFields.addAll(aliases);
            for (String f : uniqueFields) {
                aliasBranches.add(buildRawCondition(f, cond, args));
            }
            FilterExpression fe = new FilterExpression();
            fe.setField(isNegatingOperator(cond.operator()) ? "$and" : "$or");
            fe.setValue(aliasBranches);
            query.addChild(fe);
        } else {
            // No aliases — build raw condition and add as FilterExpression.
            // Uses the same buildRawCondition() as the alias path, keeping
            // operator handling in a single place.
            addRawConditionToQuery(query, buildRawCondition(mongoField, cond, args));
        }
    }

    /**
     * Returns {@code true} if the given operator expresses a negation (i.e. its raw
     * condition relies on a {@code $}-prefixed MongoDB negation operator such as
     * {@code $ne}, {@code $nin}, or an equivalent {@code $nor}/exclusion semantic).
     * Used by {@link #applyCondition} to decide whether alias branches must be
     * combined with {@code $and} (all names must satisfy the negation) instead of
     * {@code $or} (which would be trivially true for negated conditions).
     */
    private static boolean isNegatingOperator(Operator operator) {
        return switch (operator) {
            case NE, NIN, NOT_CONTAINS, IS_NOT_NULL, IS_NOT_EMPTY -> true;
            default -> false;
        };
    }

    /**
     * Converts a raw condition map (from {@link #buildRawCondition}) to
     * {@link FilterExpression}s and adds them to the query.
     */
    @SuppressWarnings("rawtypes")
    private static void addRawConditionToQuery(Query query, Map<String, Object> rawCondition) {
        for (Map.Entry<String, Object> entry : rawCondition.entrySet()) {
            FilterExpression fe = new FilterExpression();
            fe.setField(entry.getKey());
            fe.setValue(entry.getValue());
            query.addChild(fe);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static void applySorting(Query query,
                             List<OrderSpec> orderSpecs,
                             Morphium morphium,
                             Class entityClass) {
        Map<String, Integer> sortMap = new LinkedHashMap<>();
        for (OrderSpec spec : orderSpecs) {
            String mongoField = resolveMongoField(morphium, entityClass, spec.field());
            sortMap.put(mongoField, spec.direction() == Direction.ASC ? 1 : -1);
        }
        query.sort(sortMap);
    }

    @SuppressWarnings("unchecked")
    private static String resolveMongoField(Morphium morphium,
                                             Class entityClass,
                                             String javaFieldName) {
        try {
            return morphium.getARHelper().getMongoFieldName(entityClass, javaFieldName);
        } catch (Exception e) {
            return javaFieldName;
        }
    }

    /**
     * Builds a raw MongoDB query condition map for the given field name and operator.
     * Intended for internal use by alias handling, where this map is wrapped in a
     * {@link FilterExpression} and attached to the main query via {@code query.addChild()}.
     * <p>
     * Comparison operators (EQ, NE, GT, GTE, LT, LTE, IN, NIN, IS_NULL, IS_NOT_NULL,
     * IS_TRUE, IS_FALSE) are null-safe. String-based operators (LIKE, STARTS_WITH,
     * ENDS_WITH, MATCHES, IGNORE_CASE) call {@code toString()} on the argument and
     * will throw {@link NullPointerException} if the argument is null.
     */
    private static Map<String, Object> buildRawCondition(String fieldName,
                                                          Condition cond,
                                                          Object[] args) {
        Map<String, Object> result = new LinkedHashMap<>();
        switch (cond.operator()) {
            case EQ -> result.put(fieldName, args[cond.paramIndex()]);
            case NE -> result.put(fieldName, nullSafeOp("$ne", args[cond.paramIndex()]));
            case GT -> result.put(fieldName, nullSafeOp("$gt", args[cond.paramIndex()]));
            case GTE -> result.put(fieldName, nullSafeOp("$gte", args[cond.paramIndex()]));
            case LT -> result.put(fieldName, nullSafeOp("$lt", args[cond.paramIndex()]));
            case LTE -> result.put(fieldName, nullSafeOp("$lte", args[cond.paramIndex()]));
            case BETWEEN -> {
                Map<String, Object> range = new LinkedHashMap<>();
                range.put("$gte", args[cond.paramIndex()]);
                range.put("$lte", args[cond.paramIndex2()]);
                result.put(fieldName, range);
            }
            case IN -> result.put(fieldName, nullSafeOp("$in", args[cond.paramIndex()]));
            case NIN -> result.put(fieldName, nullSafeOp("$nin", args[cond.paramIndex()]));
            case LIKE -> {
                String regex = likeToRegex(args[cond.paramIndex()].toString());
                result.put(fieldName, Map.of("$regex", regex));
            }
            case STARTS_WITH -> result.put(fieldName, Map.of("$regex", "^" + Pattern.quote(args[cond.paramIndex()].toString())));
            case ENDS_WITH -> result.put(fieldName, Map.of("$regex", Pattern.quote(args[cond.paramIndex()].toString()) + "$"));
            case CONTAINS -> result.put(fieldName, Map.of("$regex", Pattern.quote(args[cond.paramIndex()].toString())));
            case NOT_CONTAINS -> result.put(fieldName, Map.of("$not", Map.of("$regex", Pattern.quote(args[cond.paramIndex()].toString()))));
            case MATCHES -> result.put(fieldName, Map.of("$regex", args[cond.paramIndex()].toString()));
            case IGNORE_CASE -> {
                Map<String, Object> regex = new LinkedHashMap<>();
                regex.put("$regex", "^" + Pattern.quote(args[cond.paramIndex()].toString()) + "$");
                regex.put("$options", "i");
                result.put(fieldName, regex);
            }
            case IS_NULL -> result.put(fieldName, null);
            case IS_NOT_NULL -> result.put(fieldName, nullSafeOp("$ne", null));
            case IS_TRUE -> result.put(fieldName, true);
            case IS_FALSE -> result.put(fieldName, false);
            case IS_EMPTY -> result.put(fieldName, Map.of("$size", 0));
            case IS_NOT_EMPTY -> result.put("$nor", List.of(Map.of(fieldName, Map.of("$size", 0))));
            case SIZE -> result.put(fieldName, Map.of("$size", ((Number) args[cond.paramIndex()]).intValue()));
        }
        return result;
    }

    /**
     * Creates a single-entry operator map that tolerates null values.
     * {@code Map.of()} throws NPE for null values; this does not.
     */
    private static Map<String, Object> nullSafeOp(String op, Object value) {
        Map<String, Object> map = new LinkedHashMap<>(1);
        map.put(op, value);
        return map;
    }

    /**
     * Returns the @Aliases values for the given Java field, or an empty list if none.
     */
    private static List<String> resolveAliases(Morphium morphium,
                                                Class entityClass,
                                                String javaFieldName) {
        try {
            Field javaField = morphium.getARHelper().getField(entityClass, javaFieldName);
            if (javaField != null && javaField.isAnnotationPresent(Aliases.class)) {
                return List.of(javaField.getAnnotation(Aliases.class).value());
            }
        } catch (Exception e) {
            log.trace("Could not resolve aliases for field '{}' on {}",
                    javaFieldName, entityClass.getSimpleName(), e);
        }
        return List.of();
    }

    /**
     * Converts a SQL LIKE pattern to a regex, escaping regex metacharacters
     * while converting {@code %} to {@code .*} and {@code _} to {@code .}.
     */
    static String likeToRegex(String likePattern) {
        StringBuilder regex = new StringBuilder();
        StringBuilder literal = new StringBuilder();
        for (int i = 0; i < likePattern.length(); i++) {
            char c = likePattern.charAt(i);
            if (c == '%' || c == '_') {
                if (literal.length() > 0) {
                    regex.append(Pattern.quote(literal.toString()));
                    literal.setLength(0);
                }
                regex.append(c == '%' ? ".*" : ".");
            } else {
                literal.append(c);
            }
        }
        if (literal.length() > 0) {
            regex.append(Pattern.quote(literal.toString()));
        }
        return "^" + regex + "$";
    }
}
