package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Group;
import de.caluga.morphium.query.Query;
import jakarta.data.Limit;
import jakarta.data.Order;
import jakarta.data.Sort;
import jakarta.data.page.PageRequest;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Runtime bridge for {@code @Query} annotated repository methods.
 * <p>
 * This is the runtime counterpart of {@link JdqlParser}: the build-time processor leaves the JDQL
 * string from {@code @Query} untouched and emits a call into this class together with a
 * {@code @Param} name-to-index mapping encoded as a string. At runtime, {@link #executeJdql} parses
 * the JDQL (cached in {@link #CACHE}, keyed by the raw string) into a {@link JdqlQuery} via
 * {@link JdqlParser#parse}, resolves named parameters against the actual method arguments, and then
 * either builds a Morphium {@link Query} (conditions, sorting, projection, paging — with cursor
 * paging delegated to {@link CursorHelper} and single-result semantics to {@link QueryResultHelper})
 * or, if the JDQL contains aggregate functions, builds an {@link de.caluga.morphium.aggregation.Aggregator}
 * pipeline and maps grouped results onto a caller-supplied Java record. This mirrors what
 * {@link QueryExecutor} does for method-name-derived queries and what {@link FindMethodBridge} does
 * for {@code @Find} methods, but for explicit JDQL query strings.
 */
public final class JdqlMethodBridge {

    private static final ConcurrentHashMap<String, JdqlQuery> CACHE = new ConcurrentHashMap<>();

    private JdqlMethodBridge() {}

    /**
     * Executes a {@code @Query} annotated method.
     *
     * @param repo               the repository instance
     * @param jdql               the JDQL query string
     * @param paramMapSpec       encoded param name-to-index mapping: "cat:0,minPrice:1"
     * @param sortParamIndex     index of Sort parameter, -1 if absent
     * @param orderParamIndex    index of Order parameter, -1 if absent
     * @param pageRequestParamIndex index of PageRequest parameter, -1 if absent
     * @param limitParamIndex    index of Limit parameter, -1 if absent
     * @param args               the method arguments
     * @param returnsSingle      true if method returns a single entity T
     * @param returnsCount       true if method returns long (count)
     * @param returnsBoolean     true if method returns boolean (exists)
     * @param returnsOptional    true if method returns Optional&lt;T&gt;
     * @param returnsCursoredPage true if method returns CursoredPage&lt;T&gt;
     * @param orderBySpec        encoded ordering from {@code @OrderBy}: "field1:ASC,field2:DESC"
     * @param returnsStream      true if method returns Stream&lt;T&gt;
     * @param resultRecordClass  FQCN of a Java Record for GROUP BY result mapping, null otherwise
     * @return the query result
     */
    /**
     * Executes a {@code DELETE FROM Entity [WHERE ...]} statement (MOR-017) as one server-side delete and returns the number of removed documents. The
     * conditions are the ones of a query, named or ordinal parameters alike. The entity name has to be the simple name of the entity of the repository.
     * Like the derived {@code deleteBy}, the delete runs no lifecycle callbacks of the entities ({@code @PreRemove}, {@code @PostRemove}: there is no instance);
     * the storage listeners get {@code preRemove(Query)} and {@code postRemove(Query)}.
     *
     * @param paramMapSpec encoded param name-to-index mapping, see {@link #executeJdql}
     * @return the number of removed documents
     * @throws IllegalArgumentException if the statement names another entity, has an ORDER BY, GROUP BY or HAVING, or is not valid
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static long executeJdqlDelete(AbstractMorphiumRepository<?, ?> repo, String jdql, String paramMapSpec, Object[] args) {
        Morphium morphium = repo.getMorphium();
        Class entityClass = repo.getMetadata().entityClass();
        String entityName = JdqlDelete.entityName(jdql);

        if (entityName != null && !entityName.equals(entityClass.getSimpleName())) {
            throw new IllegalArgumentException("The statement \"" + jdql + "\" deletes from " + entityName + ", but the repository is for " + entityClass.getSimpleName());
        }

        String where = JdqlDelete.whereClause(jdql);

        if (JdqlDelete.hasForbiddenClause(where)) {
            throw new IllegalArgumentException("A DELETE statement has no ORDER BY, GROUP BY or HAVING: \"" + jdql + "\"");
        }

        if (JdqlOrdinalParameters.has(where)) {
            if (JdqlOrdinalParameters.mixesStyles(where)) {
                throw new IllegalArgumentException("The statement \"" + jdql + "\" mixes named and ordinal parameters; use one style");
            }

            paramMapSpec = JdqlOrdinalParameters.paramMapSpec(where, args.length, -1, -1, -1, -1);
            where = JdqlOrdinalParameters.rewrite(where);
        }

        JdqlQuery query = JdqlParser.parse(where);
        Map<String, Object> paramValues = buildParamMap(paramMapSpec, args);
        Query mQuery = morphium.createQueryFor(entityClass);
        applyConditions(mQuery, query, paramValues, morphium, entityClass);
        Map<String, Object> result = mQuery.delete();
        Object n = result == null ? null : result.get("n");
        return n instanceof Number num ? num.longValue() : 0L;
    }

    /**
     * Executes an {@code UPDATE Entity SET ... [WHERE ...]} statement (MOR-018) as ONE {@code update} command, built with the update builder
     * ({@link Query#modify()}), so field names are translated and values are marshalled like in the rest of the update path, and a {@code @Version} field
     * is incremented. See {@link JdqlUpdate} for the SET items that map to a single command.
     *
     * @param paramMapSpec encoded param name-to-index mapping, see {@link #executeJdql}
     * @return the number of MATCHED documents (not the number of modified ones)
     * @throws IllegalArgumentException if the statement names another entity, has an ORDER BY, GROUP BY or HAVING, an item that is not one update command, or a
     *                                  parameter without an argument or of the wrong type
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static long executeJdqlUpdate(AbstractMorphiumRepository<?, ?> repo, String jdql, String paramMapSpec, Object[] args) {
        Morphium morphium = repo.getMorphium();
        Class entityClass = repo.getMetadata().entityClass();

        if (JdqlOrdinalParameters.has(jdql)) {
            if (JdqlOrdinalParameters.mixesStyles(jdql)) {
                throw new IllegalArgumentException("The statement \"" + jdql + "\" mixes named and ordinal parameters; use one style");
            }

            paramMapSpec = JdqlOrdinalParameters.paramMapSpec(jdql, args.length, -1, -1, -1, -1);
            jdql = JdqlOrdinalParameters.rewrite(jdql);
        }

        JdqlUpdate.Statement statement = JdqlUpdate.parse(jdql);

        if (statement.entityName() != null && !statement.entityName().equals(entityClass.getSimpleName())) {
            throw new IllegalArgumentException("The statement \"" + jdql + "\" updates " + statement.entityName() + ", but the repository is for " + entityClass.getSimpleName());
        }

        if (JdqlDelete.hasForbiddenClause(statement.where())) {
            throw new IllegalArgumentException("An UPDATE statement has no ORDER BY, GROUP BY or HAVING: \"" + jdql + "\"");
        }

        Map<String, Object> paramValues = buildParamMap(paramMapSpec, args);
        Query mQuery = morphium.createQueryFor(entityClass);
        applyConditions(mQuery, JdqlParser.parse(statement.where()), paramValues, morphium, entityClass);
        de.caluga.morphium.query.UpdateBuilder builder = mQuery.modify();

        for (JdqlUpdate.Item item : statement.items()) {
            switch (item.op()) {
                case UNSET -> builder.unset(item.field());
                case SET -> builder.set(item.field(), updateValue(item, paramValues));
                case INC -> builder.inc(item.field(), updateNumber(item, paramValues, false));
                case DEC -> builder.inc(item.field(), updateNumber(item, paramValues, true));
                case MUL -> builder.mul(item.field(), updateNumber(item, paramValues, false));
            }
        }

        return builder.updateMany().getMatchedCount();
    }

    private static Object updateValue(JdqlUpdate.Item item, Map<String, Object> paramValues) {
        if (item.value().startsWith(":")) {
            String name = item.value().substring(1);

            if (!paramValues.containsKey(name)) {
                throw new IllegalArgumentException("No argument for the parameter :" + name + " of the SET item for " + item.field());
            }

            return paramValues.get(name);
        }

        return JdqlUpdate.literal(item.value());
    }

    private static Number updateNumber(JdqlUpdate.Item item, Map<String, Object> paramValues, boolean negate) {
        Object value = updateValue(item, paramValues);

        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException("The SET item for " + item.field() + " needs a number, but " + item.value() + " is " + (value == null ? "null" : value.getClass().getName()));
        }

        if (!negate) {
            return number;
        }

        if (number instanceof Integer i) {
            return -i;
        } else if (number instanceof Long l) {
            return -l;
        } else if (number instanceof Short sh) {
            return -sh;
        } else if (number instanceof Byte b) {
            return -b;
        } else if (number instanceof Float f) {
            return -f;
        } else if (number instanceof Double d) {
            return -d;
        }

        return new java.math.BigDecimal(number.toString()).negate().doubleValue();
    }

    /**
     * The count form of an {@link AtomicUpdate} (MOR-021): ONE {@code update} command, built with the update builder. The result is the number of documents that
     * were matched or inserted by the upsert; a lost conditional upsert (duplicate key) is 0.
     *
     * @param encodedSpec the {@link AtomicUpdateSpec#encode() encoded} spec
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static long executeAtomicCount(AbstractMorphiumRepository<?, ?> repo, String encodedSpec, Object[] args) {
        AtomicUpdateSpec spec = AtomicUpdateSpec.decode(encodedSpec);
        de.caluga.morphium.query.UpdateBuilder builder = atomicBuilder(repo, spec, args);

        try {
            de.caluga.morphium.query.UpdateResult result = spec.many() ? builder.updateMany() : builder.updateOne();
            return result.getMatchedCount() + (result.getUpsertedId() != null ? 1 : 0);
        } catch (de.caluga.morphium.driver.MorphiumDriverException e) {
            if (spec.upsert() && e.isDuplicateKey()) {
                return 0;
            }

            throw e;
        }
    }

    /**
     * The find-and-modify form of an {@link AtomicUpdate} (MOR-021): ONE {@code findAndModify} command. The result is the document as the spec says, or null if
     * nothing matched, the conditional upsert was lost, or the upsert inserted and the state BEFORE was asked for.
     *
     * @param encodedSpec the {@link AtomicUpdateSpec#encode() encoded} spec
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Object executeAtomicDocument(AbstractMorphiumRepository<?, ?> repo, String encodedSpec, Object[] args) {
        AtomicUpdateSpec spec = AtomicUpdateSpec.decode(encodedSpec);
        de.caluga.morphium.query.UpdateBuilder builder = atomicBuilder(repo, spec, args);
        de.caluga.morphium.query.UpdateOutcome outcome = builder.tryFindOneAndUpdate(spec.returns());

        return switch (outcome.getKind()) {
            case INSERTED, UPDATED -> outcome.getDocument();
            default -> null;
        };
    }

    /** {@link #executeAtomicCount} for a method that returns {@code boolean}: at least one document was matched or inserted */
    public static boolean executeAtomicBoolean(AbstractMorphiumRepository<?, ?> repo, String encodedSpec, Object[] args) {
        return executeAtomicCount(repo, encodedSpec, args) > 0;
    }

    /** {@link #executeAtomicCount} for a method that returns {@code int}; a count above {@code Integer.MAX_VALUE} fails */
    public static int executeAtomicInt(AbstractMorphiumRepository<?, ?> repo, String encodedSpec, Object[] args) {
        return Math.toIntExact(executeAtomicCount(repo, encodedSpec, args));
    }

    /** {@link #executeAtomicDocument} for a method that returns an {@code Optional} */
    public static java.util.Optional<Object> executeAtomicOptional(AbstractMorphiumRepository<?, ?> repo, String encodedSpec, Object[] args) {
        return java.util.Optional.ofNullable(executeAtomicDocument(repo, encodedSpec, args));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static de.caluga.morphium.query.UpdateBuilder atomicBuilder(AbstractMorphiumRepository<?, ?> repo, AtomicUpdateSpec spec, Object[] args) {
        Morphium morphium = repo.getMorphium();
        Class entityClass = repo.getMetadata().entityClass();
        String where = spec.where().isBlank() ? "" : spec.where().strip();

        if (!where.isEmpty() && !where.toUpperCase(java.util.Locale.ROOT).startsWith("WHERE ")) {
            where = "WHERE " + where;
        }

        Map<String, Object> paramValues = buildParamMap(spec.paramMapSpec(), args);
        Query mQuery = morphium.createQueryFor(entityClass);
        applyConditions(mQuery, JdqlParser.parse(where), paramValues, morphium, entityClass);

        if (spec.orderBy() != null && !spec.orderBy().isEmpty()) {
            Map<String, Integer> sortMap = new LinkedHashMap<>();

            for (String part : spec.orderBy().split(",")) {
                String[] fieldAndDirection = part.split(":");
                sortMap.put(resolveMongoField(morphium, entityClass, fieldAndDirection[0]), fieldAndDirection.length > 1 && fieldAndDirection[1].equals("DESC") ? -1 : 1);
            }

            mQuery.sort(sortMap);
        }

        de.caluga.morphium.query.UpdateBuilder builder = mQuery.modify();

        for (String item : spec.set()) {
            int eq = item.indexOf('=');
            builder.set(item.substring(0, eq), updateValue(new JdqlUpdate.Item(item.substring(0, eq), JdqlUpdate.Op.SET, item.substring(eq + 1)), paramValues));
        }

        for (String item : spec.setOnInsert()) {
            int eq = item.indexOf('=');
            builder.setOnInsert(item.substring(0, eq), updateValue(new JdqlUpdate.Item(item.substring(0, eq), JdqlUpdate.Op.SET, item.substring(eq + 1)), paramValues));
        }

        for (String item : spec.inc()) {
            int eq = item.indexOf('=');
            builder.inc(item.substring(0, eq), updateNumber(new JdqlUpdate.Item(item.substring(0, eq), JdqlUpdate.Op.INC, item.substring(eq + 1)), paramValues, false));
        }

        return spec.upsert() ? builder.upsert() : builder;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Object executeJdql(AbstractMorphiumRepository<?, ?> repo,
                                     String jdql,
                                     String paramMapSpec,
                                     int sortParamIndex,
                                     int orderParamIndex,
                                     int pageRequestParamIndex,
                                     int limitParamIndex,
                                     Object[] args,
                                     boolean returnsSingle,
                                     boolean returnsCount,
                                     boolean returnsBoolean,
                                     boolean returnsOptional,
                                     boolean returnsCursoredPage,
                                     String orderBySpec,
                                     boolean returnsStream,
                                     String resultRecordClass) {
        Morphium morphium = repo.getMorphium();
        Class entityClass = repo.getMetadata().entityClass();

        // ordinal parameters (?1): rewrite to names and bind them to the arguments (MOR-024)
        if (JdqlOrdinalParameters.has(jdql)) {
            if (JdqlOrdinalParameters.mixesStyles(jdql)) {
                throw new IllegalArgumentException("The query \"" + jdql + "\" mixes named and ordinal parameters; use one style");
            }

            paramMapSpec = JdqlOrdinalParameters.paramMapSpec(jdql, args.length, sortParamIndex, orderParamIndex, pageRequestParamIndex, limitParamIndex);
            jdql = JdqlOrdinalParameters.rewrite(jdql);
        }

        // Parse JDQL (cached)
        JdqlQuery query = CACHE.computeIfAbsent(jdql, JdqlParser::parse);

        // Build param name → value map
        Map<String, Object> paramValues = buildParamMap(paramMapSpec, args);

        // Aggregate functions → Aggregation Pipeline (early return)
        if (query.aggregateFunctions() != null && !query.aggregateFunctions().isEmpty()) {
            PageRequest aggPageRequest = (pageRequestParamIndex >= 0 && args[pageRequestParamIndex] != null)
                    ? (PageRequest) args[pageRequestParamIndex] : null;
            return executeAggregate(repo, query, paramValues, morphium, entityClass,
                    resultRecordClass, aggPageRequest);
        }

        // Build Morphium query
        Query mQuery = morphium.createQueryFor(entityClass);

        // Apply conditions
        applyConditions(mQuery, query, paramValues, morphium, entityClass);

        // The ORDER BY of the query first, then every dynamic Sort / Sort... / Order in the order of the parameters (MOR-081)
        List<SortPlan.Entry> jdqlOrder = new ArrayList<>();
        for (JdqlQuery.OrderSpec spec : query.orderBy()) {
            jdqlOrder.add(new SortPlan.Entry(spec.field(), spec.ascending()));
        }
        List<SortPlan.Entry> dynamicOrder = SortPlan.dynamic(args);
        SortPlan.apply(mQuery, SortPlan.merge(jdqlOrder, dynamicOrder), morphium, entityClass);

        // Apply Limit
        if (limitParamIndex >= 0 && args[limitParamIndex] != null) {
            Limit limit = (Limit) args[limitParamIndex];
            mQuery.skip((int) (limit.startAt() - 1));
            mQuery.limit(limit.maxResults());
        }

        // Apply SELECT projection (skip for COUNT/EXISTS — they don't need it)
        if (query.selectFields() != null && !query.selectFields().isEmpty()
                && !returnsCount && !returnsBoolean) {
            for (String field : query.selectFields()) {
                String mongoField = resolveMongoField(morphium, entityClass, field);
                mQuery.addProjection(mongoField);
            }
        }

        // Apply PageRequest → return Page<T> or CursoredPage<T>
        if (pageRequestParamIndex >= 0 && args[pageRequestParamIndex] != null) {
            PageRequest pageRequest = (PageRequest) args[pageRequestParamIndex];

            if (returnsCursoredPage) {
                if (valueClass(query, resultRecordClass, entityClass) != null) {
                    throw new IllegalArgumentException("A CursoredPage of values is not supported: the cursor is made of the sort fields of the entity. Return CursoredPage<"
                        + entityClass.getSimpleName() + "> or a Page of values instead: \"" + jdql + "\"");
                }

                return executeCursoredJdql(mQuery, pageRequest, orderBySpec, dynamicOrder, query, paramValues,
                        morphium, entityClass);
            }

            int size = pageRequest.size();
            long page = pageRequest.page();
            int skip = (int) ((page - 1) * size);
            mQuery.skip(skip).limit(size);

            List content = mQuery.asList();
            long totalElements = -1;
            if (pageRequest.requestTotal()) {
                Query countQuery = morphium.createQueryFor(entityClass);
                applyConditions(countQuery, query, paramValues, morphium, entityClass);
                totalElements = countQuery.countAll();
            }
            return projected(new MorphiumPage<>(content, totalElements, pageRequest), query, resultRecordClass, morphium, entityClass);
        }

        // Execute
        if (returnsCount) {
            return mQuery.countAll();
        }
        if (returnsBoolean) {
            return mQuery.countAll() > 0;
        }
        if (returnsOptional) {
            return projected(QueryResultHelper.optionalSingle(mQuery), query, resultRecordClass, morphium, entityClass);
        }
        if (returnsSingle) {
            return projected(QueryResultHelper.requireSingle(mQuery), query, resultRecordClass, morphium, entityClass);
        }
        if (returnsStream) {
            return projected(mQuery.stream(), query, resultRecordClass, morphium, entityClass);
        }
        return projected(mQuery.asList(), query, resultRecordClass, morphium, entityClass);
    }

    /**
     * {@code SELECT field}: when the method returns the values of the field (the caller passes the class of the element, which is neither the entity nor a record), the entities
     * that were read with only that field are replaced by the values (MOR-083). The element may be a list entry, the content of a page, the value of an Optional, a stream entry or
     * the result itself.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object projected(Object result, JdqlQuery query, String elementClassName, Morphium morphium, Class entityClass) {
        Class<?> element = valueClass(query, elementClassName, entityClass);

        if (element == null) {
            return result;
        }

        String field = query.selectFields().get(0);
        java.util.function.Function<Object, Object> value = entity -> convertValue(morphium.getARHelper().getValue(entity, field), element);

        if (result instanceof MorphiumPage<?> page) {
            List<Object> values = new ArrayList<>();
            for (Object entity : page.content()) {
                values.add(value.apply(entity));
            }
            return new MorphiumPage<>(values, page.hasTotals() ? page.totalElements() : -1, page.pageRequest());
        }
        if (result instanceof List<?> list) {
            List<Object> values = new ArrayList<>(list.size());
            for (Object entity : list) {
                values.add(value.apply(entity));
            }
            return values;
        }
        if (result instanceof java.util.Optional<?> optional) {
            return optional.map(value);
        }
        if (result instanceof java.util.stream.Stream<?> stream) {
            return ((java.util.stream.Stream<Object>) stream).map(value);
        }

        return result == null ? null : value.apply(result);
    }

    /** the class of the values if the query selects ONE field and the method returns them, null if the method returns the entity, a record or the type is unknown */
    private static Class<?> valueClass(JdqlQuery query, String elementClassName, Class<?> entityClass) {
        if (elementClassName == null || elementClassName.isEmpty() || query.selectFields() == null || query.selectFields().size() != 1
                || (query.aggregateFunctions() != null && !query.aggregateFunctions().isEmpty())) {
            return null;
        }

        Class<?> element = loadClass(elementClassName);
        return element.isAssignableFrom(entityClass) || element.isRecord() ? null : element;
    }

    private static Class<?> loadClass(String name) {
        try {
            return Thread.currentThread().getContextClassLoader().loadClass(name);
        } catch (ClassNotFoundException | NullPointerException e) {
            // the context classloader may not see the class (or may be null): fall back to the classloader of the bridge
            try {
                return JdqlMethodBridge.class.getClassLoader().loadClass(name);
            } catch (ClassNotFoundException e2) {
                throw new IllegalArgumentException("Result class not found: " + name, e2);
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object executeCursoredJdql(Query mQuery, PageRequest pageRequest,
                                               String orderBySpec, List<SortPlan.Entry> dynamicOrder, JdqlQuery jdqlQuery,
                                               Map<String, Object> paramValues,
                                               Morphium morphium, Class entityClass) {
        // The keyset: the ORDER BY of the query, or @OrderBy if the query has none (not merged: that combination is not defined), then every dynamic Sort, Sort... and
        // Order in the order of the parameters (MOR-087).
        List<SortPlan.Entry> staticOrder = new ArrayList<>();
        if (!jdqlQuery.orderBy().isEmpty()) {
            for (JdqlQuery.OrderSpec spec : jdqlQuery.orderBy()) {
                staticOrder.add(new SortPlan.Entry(spec.field(), spec.ascending()));
            }
        } else {
            for (CursorHelper.SortSpec spec : CursorHelper.parseSortSpecs(orderBySpec)) {
                staticOrder.add(new SortPlan.Entry(spec.javaField(), spec.ascending()));
            }
        }
        List<CursorHelper.SortSpec> sortSpecs = SortPlan.merge(staticOrder, dynamicOrder).stream()
            .map(e -> new CursorHelper.SortSpec(e.property(), e.ascending())).toList();
        return CursorHelper.page(mQuery, pageRequest, sortSpecs, () -> {
            Query countQuery = morphium.createQueryFor(entityClass);
            applyConditions(countQuery, jdqlQuery, paramValues, morphium, entityClass);
            return countQuery;
        }, morphium, entityClass);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void applyConditions(Query mQuery,
                                         JdqlQuery query,
                                         Map<String, Object> paramValues,
                                         Morphium morphium,
                                         Class entityClass) {
        boolean isOr = query.combinator() == JdqlQuery.Combinator.OR;

        if (isOr && query.conditions().size() > 1) {
            List<Query> orQueries = new ArrayList<>();
            for (JdqlQuery.JdqlCondition cond : query.conditions()) {
                Query sub = morphium.createQueryFor(entityClass);
                applyCondition(sub, cond, paramValues, morphium, entityClass);
                orQueries.add(sub);
            }
            mQuery.or(orQueries);
        } else {
            for (JdqlQuery.JdqlCondition cond : query.conditions()) {
                applyCondition(mQuery, cond, paramValues, morphium, entityClass);
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void applyCondition(Query mQuery,
                                        JdqlQuery.JdqlCondition cond,
                                        Map<String, Object> paramValues,
                                        Morphium morphium,
                                        Class entityClass) {
        // Handle parenthesized group conditions (e.g. "(a IS NULL OR a = '')")
        if (cond.isGroup()) {
            if (cond.negated()) {
                // NOT (...) → De Morgan: NOT (A OR B) = NOT A AND NOT B
                //                        NOT (A AND B) = NOT A OR NOT B
                JdqlQuery.Combinator flipped = cond.groupCombinator() == JdqlQuery.Combinator.OR
                        ? JdqlQuery.Combinator.AND : JdqlQuery.Combinator.OR;
                List<JdqlQuery.JdqlCondition> negatedSubs = new ArrayList<>();
                for (JdqlQuery.JdqlCondition sub : cond.groupConditions()) {
                    negatedSubs.add(negateCondition(sub));
                }
                applyCondition(mQuery, JdqlQuery.JdqlCondition.group(negatedSubs, flipped),
                        paramValues, morphium, entityClass);
                return;
            }
            boolean isGroupOr = cond.groupCombinator() == JdqlQuery.Combinator.OR;
            if (isGroupOr && cond.groupConditions().size() > 1) {
                List<Query> orQueries = new ArrayList<>();
                for (JdqlQuery.JdqlCondition sub : cond.groupConditions()) {
                    Query subQuery = morphium.createQueryFor(entityClass);
                    applyCondition(subQuery, sub, paramValues, morphium, entityClass);
                    orQueries.add(subQuery);
                }
                mQuery.or(orQueries);
            } else {
                for (JdqlQuery.JdqlCondition sub : cond.groupConditions()) {
                    applyCondition(mQuery, sub, paramValues, morphium, entityClass);
                }
            }
            return;
        }

        String mongoField = resolveMongoField(morphium, entityClass, cond.fieldName());
        var field = mQuery.f(mongoField);

        boolean enumField = isEnumField(morphium, entityClass, cond.fieldName());
        Object value = resolveValue(cond.valueRef(), paramValues, enumField);

        // Resolve the effective operator (invert if negated)
        JdqlQuery.Operator op = cond.negated() ? invertOperator(cond.operator()) : cond.operator();

        switch (op) {
            case EQ -> {
                if (cond.literal() != null) {
                    field.eq(cond.literal());
                } else {
                    field.eq(value);
                }
            }
            case NE -> {
                if (cond.literal() != null) {
                    field.ne(cond.literal());
                } else {
                    field.ne(value);
                }
            }
            case GT -> field.gt(value);
            case GTE -> field.gte(value);
            case LT -> field.lt(value);
            case LTE -> field.lte(value);
            case BETWEEN -> {
                Object value2 = resolveValue(cond.valueRef2(), paramValues, enumField);
                if (cond.negated()) {
                    // NOT BETWEEN a AND b → field < a OR field > b
                    List<Query> orQueries = new ArrayList<>();
                    Query ltQuery = morphium.createQueryFor(entityClass);
                    ltQuery.f(mongoField).lt(value);
                    orQueries.add(ltQuery);
                    Query gtQuery = morphium.createQueryFor(entityClass);
                    gtQuery.f(mongoField).gt(value2);
                    orQueries.add(gtQuery);
                    mQuery.or(orQueries);
                } else {
                    field.gte(value);
                    mQuery.f(mongoField).lte(value2);
                }
            }
            case IN -> field.in((Collection) value);
            case NOT_IN -> field.nin((Collection) value);
            case LIKE -> {
                String pattern = QueryExecutor.likeToRegex(value.toString());
                if (cond.negated()) {
                    // NOT LIKE → $not with $regex
                    field.not();
                    field.matches(Pattern.compile(pattern));
                } else {
                    field.matches(Pattern.compile(pattern));
                }
            }
            case IS_NULL -> field.eq(null);
            case IS_NOT_NULL -> field.ne(null);
        }
    }

    /**
     * Negates a condition for De Morgan transformation of NOT (...) groups.
     * Simple conditions get their negated flag flipped; groups are recursively De Morgan'd.
     */
    private static JdqlQuery.JdqlCondition negateCondition(JdqlQuery.JdqlCondition cond) {
        if (cond.isGroup()) {
            if (cond.negated()) {
                // Double negation: NOT applied to already-negated group → cancel out
                return JdqlQuery.JdqlCondition.group(cond.groupConditions(), cond.groupCombinator());
            }
            // De Morgan: NOT (A OR B) = NOT A AND NOT B
            JdqlQuery.Combinator flipped = cond.groupCombinator() == JdqlQuery.Combinator.OR
                    ? JdqlQuery.Combinator.AND : JdqlQuery.Combinator.OR;
            List<JdqlQuery.JdqlCondition> negatedChildren = new ArrayList<>();
            for (JdqlQuery.JdqlCondition child : cond.groupConditions()) {
                negatedChildren.add(negateCondition(child));
            }
            return JdqlQuery.JdqlCondition.group(negatedChildren, flipped);
        }
        return new JdqlQuery.JdqlCondition(cond.fieldName(), cond.operator(), cond.valueRef(),
                cond.valueRef2(), cond.literal(), !cond.negated(), null, null);
    }

    /**
     * Inverts an operator for NOT negation.
     */
    private static JdqlQuery.Operator invertOperator(JdqlQuery.Operator op) {
        return switch (op) {
            case EQ -> JdqlQuery.Operator.NE;
            case NE -> JdqlQuery.Operator.EQ;
            case GT -> JdqlQuery.Operator.LTE;
            case GTE -> JdqlQuery.Operator.LT;
            case LT -> JdqlQuery.Operator.GTE;
            case LTE -> JdqlQuery.Operator.GT;
            case IN -> JdqlQuery.Operator.NOT_IN;
            case NOT_IN -> JdqlQuery.Operator.IN;
            case IS_NULL -> JdqlQuery.Operator.IS_NOT_NULL;
            case IS_NOT_NULL -> JdqlQuery.Operator.IS_NULL;
            // LIKE and BETWEEN: keep as-is, handle negation in applyCondition directly
            case LIKE, BETWEEN -> op;
        };
    }

    private static Object resolveValue(String valueRef, Map<String, Object> paramValues) {
        return resolveValue(valueRef, paramValues, false);
    }

    /**
     * @param enumField whether the field of the condition is an enum: only then is a qualified name ({@code com.example.Kind.PRIME}) the constant of that enum,
     *                  otherwise it stays the text that it is
     */
    private static Object resolveValue(String valueRef, Map<String, Object> paramValues, boolean enumField) {
        if (valueRef == null) return null;
        if (valueRef.startsWith(":")) {
            String paramName = valueRef.substring(1);
            if (!paramValues.containsKey(paramName)) {
                throw new IllegalArgumentException(
                        "JDQL parameter :" + paramName + " not found. Available: " + paramValues.keySet());
            }
            return paramValues.get(paramName);
        }
        // a list of operands, as after IN: ('a', 'b', :p)
        if (JdqlLiterals.isList(valueRef)) {
            List<Object> items = new ArrayList<>();
            for (String item : JdqlLiterals.splitList(valueRef)) {
                Object value = resolveValue(item, paramValues, enumField);
                // a parameter inside the list that holds a collection contributes its elements
                if (item.startsWith(":") && value instanceof Collection<?> c) {
                    items.addAll(c);
                } else {
                    items.add(value);
                }
            }
            return items;
        }
        if (JdqlLiterals.isStringLiteral(valueRef)) {
            return JdqlLiterals.unquote(valueRef);
        }
        // Try numeric literal
        try {
            if (valueRef.contains(".")) {
                return Double.parseDouble(valueRef);
            }
            return Long.parseLong(valueRef);
        } catch (NumberFormatException e) {
            if (valueRef.startsWith("\"") && valueRef.endsWith("\"") && valueRef.length() >= 2) {
                return valueRef.substring(1, valueRef.length() - 1);
            }
            // the constant of an enum is stored by its name
            if (enumField && JdqlLiterals.isQualifiedName(valueRef)) {
                return JdqlLiterals.constantName(valueRef);
            }
            return valueRef;
        }
    }

    private static Map<String, Object> buildParamMap(String paramMapSpec, Object[] args) {
        Map<String, Object> map = new HashMap<>();
        if (paramMapSpec == null || paramMapSpec.isEmpty()) return map;
        for (String entry : paramMapSpec.split(",")) {
            String[] parts = entry.split(":");
            String name = parts[0];
            int idx = Integer.parseInt(parts[1]);
            map.put(name, args[idx]);
        }
        return map;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object executeAggregate(AbstractMorphiumRepository<?, ?> repo,
                                            JdqlQuery query,
                                            Map<String, Object> paramValues,
                                            Morphium morphium, Class entityClass,
                                            String resultRecordClass,
                                            PageRequest pageRequest) {
        Aggregator agg = morphium.createAggregator(entityClass, Map.class);

        // $match stage: WHERE conditions
        if (!query.conditions().isEmpty()) {
            Query matchQuery = morphium.createQueryFor(entityClass);
            applyConditions(matchQuery, query, paramValues, morphium, entityClass);
            agg.match(matchQuery);
        }

        boolean isGrouped = query.groupByFields() != null && !query.groupByFields().isEmpty();
        boolean isCompoundGroup = isGrouped && query.groupByFields().size() > 1;

        // $addFields for COUNT(field) NULL filtering — must come before $group
        Map<String, Object> addFieldsMap = new LinkedHashMap<>();
        for (int i = 0; i < query.aggregateFunctions().size(); i++) {
            JdqlQuery.AggregateFunction func = query.aggregateFunctions().get(i);
            if (func.type() == JdqlQuery.AggregateType.COUNT && !"this".equals(func.field())) {
                String mongoField = resolveMongoField(morphium, entityClass, func.field());
                String helperField = "_cnt_notnull_" + i;
                addFieldsMap.put(helperField, Map.of(
                        "$cond", Arrays.asList(
                                Map.of("$ne", Arrays.asList("$" + mongoField, null)),
                                1, 0
                        )
                ));
            }
        }
        if (!addFieldsMap.isEmpty()) {
            agg.addFields(addFieldsMap);
        }

        // $group stage
        Group group;
        if (isCompoundGroup) {
            Map<String, Object> compoundId = new LinkedHashMap<>();
            for (String field : query.groupByFields()) {
                compoundId.put(field, "$" + resolveMongoField(morphium, entityClass, field));
            }
            group = agg.group(compoundId);
        } else if (isGrouped) {
            String groupField = query.groupByFields().get(0);
            String mongoGroupField = "$" + resolveMongoField(morphium, entityClass, groupField);
            group = agg.group(mongoGroupField);
        } else {
            group = agg.group((String) null);
        }

        for (int i = 0; i < query.aggregateFunctions().size(); i++) {
            JdqlQuery.AggregateFunction func = query.aggregateFunctions().get(i);
            String resultField = "agg_" + i;
            String mongoField = "this".equals(func.field()) ? null
                    : "$" + resolveMongoField(morphium, entityClass, func.field());

            switch (func.type()) {
                case COUNT -> {
                    if ("this".equals(func.field())) {
                        group.sum(resultField, 1);
                    } else {
                        group.sum(resultField, "$_cnt_notnull_" + i);
                    }
                }
                case SUM -> group.sum(resultField, mongoField);
                case AVG -> group.avg(resultField, mongoField);
                case MIN -> group.min(resultField, mongoField);
                case MAX -> group.max(resultField, mongoField);
            }
        }
        group.end();

        // $match stages for HAVING (post-group filter)
        if (query.havingConditions() != null && !query.havingConditions().isEmpty()) {
            if (query.havingCombinator() == JdqlQuery.Combinator.OR) {
                // OR: single $match with $or array
                List<Map<String, Object>> orConditions = new ArrayList<>();
                for (JdqlQuery.HavingCondition hc : query.havingConditions()) {
                    String aggField = resolveAggFieldForHaving(hc.aggregateFunction(), query);
                    Object value = resolveValue(hc.valueRef(), paramValues);
                    orConditions.add(Map.of(aggField, Map.of(toMongoOperator(hc.operator()), value)));
                }
                agg.addOperator(Map.of("$match", Map.of("$or", orConditions)));
            } else {
                // AND: separate $match stages (InMemory multi-field workaround)
                for (JdqlQuery.HavingCondition hc : query.havingConditions()) {
                    String aggField = resolveAggFieldForHaving(hc.aggregateFunction(), query);
                    Object value = resolveValue(hc.valueRef(), paramValues);
                    agg.addOperator(Map.of("$match",
                            Map.of(aggField, Map.of(toMongoOperator(hc.operator()), value))));
                }
            }
        }

        // For compound GROUP BY: $project to promote _id sub-fields to top level
        // (InMemAggregator's $sort doesn't handle dotted paths like _id.fieldName)
        if (isCompoundGroup) {
            Map<String, Object> projectFields = new LinkedHashMap<>();
            for (String field : query.groupByFields()) {
                projectFields.put(field, "$_id." + field);
            }
            for (int i = 0; i < query.aggregateFunctions().size(); i++) {
                projectFields.put("agg_" + i, 1);
            }
            projectFields.put("_id", 0);
            agg.addOperator(Map.of("$project", projectFields));
        }

        // $sort stage (only for GROUP BY with ORDER BY)
        if (isGrouped && !query.orderBy().isEmpty()) {
            Map<String, Integer> sortMap = new LinkedHashMap<>();
            for (JdqlQuery.OrderSpec spec : query.orderBy()) {
                String sortKey = resolveAggSortKey(spec.field(), query, morphium, entityClass);
                sortMap.put(sortKey, spec.ascending() ? 1 : -1);
            }
            agg.sort(sortMap);
        }

        List<Map<String, Object>> results = agg.aggregateMap();

        // Grouped → List<Record> or Page<Record>
        if (isGrouped) {
            List<Object> allMapped = mapGroupedResults(results, query, resultRecordClass);

            if (pageRequest != null) {
                int size = pageRequest.size();
                long page = pageRequest.page();
                int skip = (int) ((page - 1) * size);
                long totalElements = allMapped.size();

                List<Object> pageContent;
                if (skip >= allMapped.size()) {
                    pageContent = List.of();
                } else {
                    int end = Math.min(skip + size, allMapped.size());
                    pageContent = allMapped.subList(skip, end);
                }

                long effectiveTotal = pageRequest.requestTotal() ? totalElements : -1;
                return new MorphiumPage<>(pageContent, effectiveTotal, pageRequest);
            }

            return allMapped;
        }

        // --- Global aggregation (existing v1 logic) ---
        if (results.isEmpty()) {
            if (query.aggregateFunctions().size() == 1) {
                return defaultAggregateValue(query.aggregateFunctions().get(0).type());
            }
            Object[] arr = new Object[query.aggregateFunctions().size()];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = defaultAggregateValue(query.aggregateFunctions().get(i).type());
            }
            return arr;
        }

        Map<String, Object> result = results.get(0);

        if (query.aggregateFunctions().size() == 1) {
            return toNumber(result.get("agg_0"), query.aggregateFunctions().get(0).type());
        }

        // Multiple aggregates → Object[]
        Object[] arr = new Object[query.aggregateFunctions().size()];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = toNumber(result.get("agg_" + i), query.aggregateFunctions().get(i).type());
        }
        return arr;
    }

    private static String toMongoOperator(JdqlQuery.Operator op) {
        return switch (op) {
            case EQ -> "$eq";
            case NE -> "$ne";
            case GT -> "$gt";
            case GTE -> "$gte";
            case LT -> "$lt";
            case LTE -> "$lte";
            default -> throw new IllegalArgumentException("Unsupported HAVING operator: " + op);
        };
    }

    private static String resolveAggFieldForHaving(String aggFuncStr, JdqlQuery query) {
        Matcher aggMatcher = Pattern.compile(
                "(?i)(COUNT|SUM|AVG|MIN|MAX)\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_.]*|this)\\s*\\)")
                .matcher(aggFuncStr);
        if (!aggMatcher.matches()) {
            throw new IllegalArgumentException("HAVING references invalid aggregate: " + aggFuncStr);
        }
        String funcName = aggMatcher.group(1).toUpperCase(Locale.ROOT);
        String argName = aggMatcher.group(2);
        JdqlQuery.AggregateType type = JdqlQuery.AggregateType.valueOf(funcName);
        for (int i = 0; i < query.aggregateFunctions().size(); i++) {
            JdqlQuery.AggregateFunction f = query.aggregateFunctions().get(i);
            if (f.type() == type && f.field().equals(argName)) {
                return "agg_" + i;
            }
        }
        throw new IllegalArgumentException("HAVING references unknown aggregate: " + aggFuncStr);
    }

    private static String resolveAggSortKey(String orderField, JdqlQuery query,
                                             Morphium morphium, Class entityClass) {
        // Check if it's an aggregate function reference like "COUNT(this)"
        Matcher aggMatcher = Pattern.compile(
                "(?i)(COUNT|SUM|AVG|MIN|MAX)\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_.]*|this)\\s*\\)")
                .matcher(orderField);
        if (aggMatcher.matches()) {
            String funcName = aggMatcher.group(1).toUpperCase(Locale.ROOT);
            String argName = aggMatcher.group(2);
            JdqlQuery.AggregateType type = JdqlQuery.AggregateType.valueOf(funcName);
            for (int i = 0; i < query.aggregateFunctions().size(); i++) {
                JdqlQuery.AggregateFunction f = query.aggregateFunctions().get(i);
                if (f.type() == type && f.field().equals(argName)) {
                    return "agg_" + i;
                }
            }
            throw new IllegalArgumentException("ORDER BY references unknown aggregate: " + orderField);
        }
        // Check if it's a group field → _id (scalar) or plain field name (compound, after $project)
        if (query.groupByFields() != null && query.groupByFields().contains(orderField)) {
            return query.groupByFields().size() == 1 ? "_id" : orderField;
        }
        return resolveMongoField(morphium, entityClass, orderField);
    }

    @SuppressWarnings("unchecked")
    private static List<Object> mapGroupedResults(List<Map<String, Object>> results,
                                                   JdqlQuery query,
                                                   String resultRecordClass) {
        if (resultRecordClass == null || resultRecordClass.isEmpty()) {
            throw new IllegalArgumentException(
                    "GROUP BY queries must return List<RecordType>. " +
                    "Declare a Java record matching the SELECT clause.");
        }

        Class<?> recordClass = loadClass(resultRecordClass);

        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException(
                    "GROUP BY queries must return List<RecordType>, not " + recordClass.getName() + ". Declare a Java record matching the SELECT clause.");
        }

        RecordComponent[] components = recordClass.getRecordComponents();
        int groupFieldCount = query.groupByFields() != null ? query.groupByFields().size() : 0;
        int expectedCount = groupFieldCount + query.aggregateFunctions().size();
        if (components.length != expectedCount) {
            throw new IllegalArgumentException(
                    "Record " + recordClass.getSimpleName() + " has " + components.length
                    + " components but SELECT has " + expectedCount + " fields");
        }

        Class<?>[] paramTypes = new Class<?>[components.length];
        for (int i = 0; i < components.length; i++) {
            paramTypes[i] = components[i].getType();
        }
        Constructor<?> ctor;
        try {
            ctor = recordClass.getDeclaredConstructor(paramTypes);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("No canonical constructor for " + recordClass.getName(), e);
        }

        List<Object> mapped = new ArrayList<>();
        for (Map<String, Object> row : results) {
            Object[] ctorArgs = new Object[components.length];
            // Group fields from _id (scalar) or top-level (compound, after $project)
            if (groupFieldCount == 1) {
                ctorArgs[0] = convertValue(row.get("_id"), paramTypes[0]);
            } else {
                for (int i = 0; i < groupFieldCount; i++) {
                    String fieldName = query.groupByFields().get(i);
                    ctorArgs[i] = convertValue(row.get(fieldName), paramTypes[i]);
                }
            }
            // Aggregates from agg_0, agg_1, ...
            for (int i = 0; i < query.aggregateFunctions().size(); i++) {
                Object val = row.get("agg_" + i);
                ctorArgs[groupFieldCount + i] = convertAggValue(val, paramTypes[groupFieldCount + i]);
            }
            try {
                mapped.add(ctor.newInstance(ctorArgs));
            } catch (Exception e) {
                throw new RuntimeException("Failed to instantiate " + recordClass.getSimpleName(), e);
            }
        }
        return mapped;
    }

    private static Object convertValue(Object value, Class<?> targetType) {
        if (value == null) return null;
        if (targetType.isInstance(value)) return value;
        if (targetType == String.class) return value.toString();
        if (targetType == long.class || targetType == Long.class) return ((Number) value).longValue();
        if (targetType == int.class || targetType == Integer.class) return ((Number) value).intValue();
        if (targetType == double.class || targetType == Double.class) return ((Number) value).doubleValue();
        if (targetType == boolean.class || targetType == Boolean.class) return Boolean.valueOf(value.toString());
        return value;
    }

    private static Object convertAggValue(Object value, Class<?> targetType) {
        if (value == null) {
            if (targetType == long.class || targetType == Long.class) return 0L;
            if (targetType == double.class || targetType == Double.class) return 0.0;
            if (targetType == int.class || targetType == Integer.class) return 0;
            return null;
        }
        return convertValue(value, targetType);
    }

    private static Object defaultAggregateValue(JdqlQuery.AggregateType type) {
        return switch (type) {
            case COUNT -> 0L;
            case SUM, AVG, MIN, MAX -> 0.0;
        };
    }

    private static Object toNumber(Object value, JdqlQuery.AggregateType type) {
        if (value == null) return defaultAggregateValue(type);
        if (type == JdqlQuery.AggregateType.COUNT) {
            return ((Number) value).longValue();
        }
        if (value instanceof Number n) {
            // AVG must always be returned as a double, regardless of the underlying MongoDB number
            // subtype: if every value being averaged happens to be an integer, some drivers/the
            // in-memory aggregator return an Integer/Long for the average instead of a Double, but
            // Jakarta Data callers of an AVG aggregate expect a double/Double result unconditionally
            // (e.g. AVG of exactly 5 must come back as 5.0, not 5L). SUM/MIN/MAX are left on the
            // existing int/long-preserving-unless-fractional heuristic since a SUM or MIN/MAX of an
            // integer field is plausibly a whole number and no caller convention requires double there.
            if (type == JdqlQuery.AggregateType.AVG) {
                return n.doubleValue();
            }
            return (n instanceof Integer || n instanceof Long) ? n.longValue() : n.doubleValue();
        }
        return value;
    }

    /**
     * Asynchronous variant of {@link #executeJdql}, running the query on the repository's
     * async executor.
     *
     * @param repo               the repository instance
     * @param jdql               the JDQL query string
     * @param paramMapSpec       encoded param name-to-index mapping, see {@link #executeJdql}
     * @param sortParamIndex     index of Sort parameter, -1 if absent
     * @param orderParamIndex    index of Order parameter, -1 if absent
     * @param pageRequestParamIndex index of PageRequest parameter, -1 if absent
     * @param limitParamIndex    index of Limit parameter, -1 if absent
     * @param args               the method arguments
     * @param returnsSingle      true if method returns a single entity T
     * @param returnsCount       true if method returns long (count)
     * @param returnsBoolean     true if method returns boolean (exists)
     * @param returnsOptional    true if method returns Optional&lt;T&gt;
     * @param returnsCursoredPage true if method returns CursoredPage&lt;T&gt;
     * @param orderBySpec        encoded ordering from {@code @OrderBy}
     * @param returnsStream      true if method returns Stream&lt;T&gt;
     * @param resultRecordClass  FQCN of a Java Record for GROUP BY result mapping, null otherwise
     * @return a completion stage yielding the result of {@link #executeJdql}
     */
    public static CompletionStage<Object> executeJdqlAsync(AbstractMorphiumRepository<?, ?> repo,
                                                              String jdql,
                                                              String paramMapSpec,
                                                              int sortParamIndex,
                                                              int orderParamIndex,
                                                              int pageRequestParamIndex,
                                                              int limitParamIndex,
                                                              Object[] args,
                                                              boolean returnsSingle,
                                                              boolean returnsCount,
                                                              boolean returnsBoolean,
                                                              boolean returnsOptional,
                                                              boolean returnsCursoredPage,
                                                              String orderBySpec,
                                                              boolean returnsStream,
                                                              String resultRecordClass) {
        return CompletableFuture.supplyAsync(
                () -> executeJdql(repo, jdql, paramMapSpec, sortParamIndex, orderParamIndex,
                        pageRequestParamIndex, limitParamIndex, args, returnsSingle, returnsCount,
                        returnsBoolean, returnsOptional, returnsCursoredPage, orderBySpec,
                        returnsStream, resultRecordClass),
                repo.getAsyncExecutor());
    }

    @SuppressWarnings("unchecked")
    private static boolean isEnumField(Morphium morphium, Class entityClass, String javaFieldName) {
        try {
            var field = morphium.getARHelper().getField(entityClass, javaFieldName);
            return field != null && field.getType().isEnum();
        } catch (Exception e) {
            return false;
        }
    }

    private static String resolveMongoField(Morphium morphium, Class entityClass, String javaFieldName) {
        try {
            return morphium.getARHelper().getMongoFieldName(entityClass, javaFieldName);
        } catch (Exception e) {
            return javaFieldName;
        }
    }
}
