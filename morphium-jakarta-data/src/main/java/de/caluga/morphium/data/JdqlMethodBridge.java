package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Group;
import de.caluga.morphium.query.Query;
import jakarta.data.Limit;
import jakarta.data.Order;
import jakarta.data.Sort;
import jakarta.data.page.PageRequest;
import jakarta.data.page.impl.CursoredPageRecord;

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

        // Apply JDQL ORDER BY
        if (!query.orderBy().isEmpty()) {
            Map<String, Integer> sortMap = new LinkedHashMap<>();
            for (JdqlQuery.OrderSpec spec : query.orderBy()) {
                String mongoField = resolveMongoField(morphium, entityClass, spec.field());
                sortMap.put(mongoField, spec.ascending() ? 1 : -1);
            }
            mQuery.sort(sortMap);
        }

        // Apply dynamic Sort<T> parameter
        if (sortParamIndex >= 0 && args[sortParamIndex] != null) {
            Sort sort = (Sort) args[sortParamIndex];
            Map<String, Integer> sortMap = new LinkedHashMap<>();
            String mongoField = resolveMongoField(morphium, entityClass, sort.property());
            sortMap.put(mongoField, sort.isAscending() ? 1 : -1);
            mQuery.sort(sortMap);
        }

        // Apply dynamic Order<T> parameter
        if (orderParamIndex >= 0 && args[orderParamIndex] != null) {
            Order order = (Order) args[orderParamIndex];
            if (!order.sorts().isEmpty()) {
                Map<String, Integer> sortMap = new LinkedHashMap<>();
                for (Object s : order.sorts()) {
                    Sort sort = (Sort) s;
                    String mongoField = resolveMongoField(morphium, entityClass, sort.property());
                    sortMap.put(mongoField, sort.isAscending() ? 1 : -1);
                }
                mQuery.sort(sortMap);
            }
        }

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
                return executeCursoredJdql(mQuery, pageRequest, orderBySpec, query, paramValues,
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
            return new MorphiumPage<>(content, totalElements, pageRequest);
        }

        // Execute
        if (returnsCount) {
            return mQuery.countAll();
        }
        if (returnsBoolean) {
            return mQuery.countAll() > 0;
        }
        if (returnsOptional) {
            return QueryResultHelper.optionalSingle(mQuery);
        }
        if (returnsSingle) {
            return QueryResultHelper.requireSingle(mQuery);
        }
        if (returnsStream) {
            return mQuery.stream();
        }
        return mQuery.asList();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object executeCursoredJdql(Query mQuery, PageRequest pageRequest,
                                               String orderBySpec, JdqlQuery jdqlQuery,
                                               Map<String, Object> paramValues,
                                               Morphium morphium, Class entityClass) {
        // The keyset for cursor pagination can come from two independent sources: the JDQL string's
        // own ORDER BY clause (jdqlQuery.orderBy(), already applied to mQuery further up in
        // executeJdql() for the non-cursor path) and the separate @OrderBy annotation
        // (orderBySpec). Using only orderBySpec here (as before) silently dropped the JDQL ORDER BY
        // whenever it was the only one present, leaving the cursor without a sort key at all.
        // Decision: if the JDQL query itself specifies an ORDER BY, it wins — it is part of the
        // explicit query text and takes precedence over the declarative @OrderBy annotation, which
        // is only a fallback for methods that don't spell out their own ordering. We do not attempt
        // to merge the two field lists (e.g. JDQL primary keys + @OrderBy tiebreakers) because that
        // ordering combination is ambiguous and not defined by JDQL semantics; callers wanting both
        // should express both fields in their ORDER BY clause directly.
        List<CursorHelper.SortSpec> sortSpecs;
        if (!jdqlQuery.orderBy().isEmpty()) {
            sortSpecs = new ArrayList<>(jdqlQuery.orderBy().size());
            for (JdqlQuery.OrderSpec spec : jdqlQuery.orderBy()) {
                sortSpecs.add(new CursorHelper.SortSpec(spec.field(), spec.ascending()));
            }
        } else {
            sortSpecs = CursorHelper.parseSortSpecs(orderBySpec);
        }
        boolean isForward = pageRequest.mode() != PageRequest.Mode.CURSOR_PREVIOUS;
        int requestedSize = pageRequest.size();

        if (pageRequest.mode() != PageRequest.Mode.OFFSET) {
            PageRequest.Cursor cursor = pageRequest.cursor()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "PageRequest mode is " + pageRequest.mode() + " but no cursor provided"));
            CursorHelper.applyCursorCondition(mQuery, cursor, sortSpecs, morphium, entityClass, isForward);
        } else {
            // CursoredPage requested in classic offset mode (PageRequest.Mode.OFFSET): no cursor
            // condition applies, but we still need to skip to the requested page, exactly like the
            // Page<T> branch above (mQuery.skip(skip).limit(size)) does for a normal offset page.
            int skip = (int) ((pageRequest.page() - 1) * requestedSize);
            mQuery.skip(skip);
        }

        CursorHelper.applySort(mQuery, sortSpecs, morphium, entityClass, isForward);
        mQuery.limit(requestedSize + 1);

        List content = mQuery.asList();
        boolean hasMore = content.size() > requestedSize;
        if (hasMore) {
            content = new ArrayList(content.subList(0, requestedSize));
        }
        if (!isForward) {
            Collections.reverse(content);
        }

        List<String> sortFields = sortSpecs.stream().map(CursorHelper.SortSpec::javaField).toList();
        List<PageRequest.Cursor> cursors = CursorHelper.extractCursors(content, sortFields, morphium, entityClass);

        long totalElements = -1;
        if (pageRequest.requestTotal()) {
            Query countQuery = morphium.createQueryFor(entityClass);
            applyConditions(countQuery, jdqlQuery, paramValues, morphium, entityClass);
            totalElements = countQuery.countAll();
        }

        boolean isFirstPage = pageRequest.mode() == PageRequest.Mode.OFFSET;
        boolean isLastPage = !hasMore;

        if (content.isEmpty()) {
            return new CursoredPageRecord<>(content, cursors, totalElements, pageRequest,
                    (PageRequest) null, (PageRequest) null);
        }

        return new CursoredPageRecord<>(content, cursors, totalElements, pageRequest,
                isFirstPage, isLastPage);
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

        Object value = resolveValue(cond.valueRef(), paramValues);

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
                Object value2 = resolveValue(cond.valueRef2(), paramValues);
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
                String pattern = value.toString()
                        .replace("%", ".*")
                        .replace("_", ".");
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
        if (valueRef == null) return null;
        if (valueRef.startsWith(":")) {
            String paramName = valueRef.substring(1);
            if (!paramValues.containsKey(paramName)) {
                throw new IllegalArgumentException(
                        "JDQL parameter :" + paramName + " not found. Available: " + paramValues.keySet());
            }
            return paramValues.get(paramName);
        }
        // Try numeric literal
        try {
            if (valueRef.contains(".")) {
                return Double.parseDouble(valueRef);
            }
            return Long.parseLong(valueRef);
        } catch (NumberFormatException e) {
            // Return as string literal (strip quotes if present)
            if ((valueRef.startsWith("'") && valueRef.endsWith("'"))
                    || (valueRef.startsWith("\"") && valueRef.endsWith("\""))) {
                return valueRef.substring(1, valueRef.length() - 1);
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

        Class<?> recordClass;
        try {
            recordClass = Thread.currentThread().getContextClassLoader().loadClass(resultRecordClass);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Record class not found: " + resultRecordClass, e);
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
    private static String resolveMongoField(Morphium morphium, Class entityClass, String javaFieldName) {
        try {
            return morphium.getARHelper().getMongoFieldName(entityClass, javaFieldName);
        } catch (Exception e) {
            return javaFieldName;
        }
    }
}
