package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;
import jakarta.data.Limit;
import jakarta.data.Order;
import jakarta.data.Sort;
import jakarta.data.exceptions.EmptyResultException;
import jakarta.data.exceptions.NonUniqueResultException;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;
import jakarta.data.page.impl.CursoredPageRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

/**
 * Runtime bridge called by Gizmo-generated repository methods for
 * {@code @Find}, {@code @Delete} annotated methods with {@code @By} parameters.
 * <p>
 * The build-time annotation processor does not generate query-building bytecode itself; instead
 * it encodes the query specification as simple strings (field:paramIndex pairs for {@code @By}
 * conditions, {@code field:ASC/DESC} pairs for {@code @OrderBy}) and emits a call into this class.
 * At runtime, {@link #executeFind} decodes those strings, builds a Morphium {@link Query}, applies
 * any dynamic {@code Sort}/{@code Order}/{@code Limit}/{@code PageRequest} parameters, delegates
 * offset paging to {@link MorphiumPage} and cursor paging to {@link CursorHelper}, and finally
 * turns the query into the shape the repository method declares (single entity via
 * {@link QueryResultHelper}, {@code Optional}, {@code Stream}, {@code List}, {@code Page}, or
 * {@code CursoredPage}). This mirrors what {@link QueryExecutor} does for {@code findBy*}-style
 * derived methods, but for methods explicitly annotated with {@code @Find}.
 */
public final class FindMethodBridge {

    private FindMethodBridge() {}

    /**
     * Executes a {@code @Find} annotated method.
     *
     * @param repo               the repository instance
     * @param conditionsSpec     encoded conditions: "field1:0,field2:1" (fieldName:paramIndex, all EQ)
     * @param orderBySpec        encoded ordering: "field1:ASC,field2:DESC" or "" for none
     * @param sortParamIndex     index of Sort parameter, -1 if absent
     * @param orderParamIndex    index of Order parameter, -1 if absent
     * @param pageRequestParamIndex index of PageRequest parameter, -1 if absent
     * @param limitParamIndex    index of Limit parameter, -1 if absent
     * @param args               the method arguments
     * @param returnsSingle      true if method returns a single entity T (not List/Stream/Page/Optional)
     * @param returnsOptional    true if method returns Optional&lt;T&gt;
     * @param returnsCursoredPage true if method returns CursoredPage&lt;T&gt;
     * @param returnsStream      true if method returns Stream&lt;T&gt;
     * @return the query result
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Object executeFind(AbstractMorphiumRepository<?, ?> repo,
                                     String conditionsSpec,
                                     String orderBySpec,
                                     int sortParamIndex,
                                     int orderParamIndex,
                                     int pageRequestParamIndex,
                                     int limitParamIndex,
                                     Object[] args,
                                     boolean returnsSingle,
                                     boolean returnsOptional,
                                     boolean returnsCursoredPage,
                                     boolean returnsStream) {
        Morphium morphium = repo.getMorphium();
        Class entityClass = repo.getMetadata().entityClass();
        Query query = morphium.createQueryFor(entityClass);

        // Apply equality conditions from @By parameters
        if (!conditionsSpec.isEmpty()) {
            for (String part : conditionsSpec.split(",")) {
                String[] fieldAndIdx = part.split(":");
                String javaField = fieldAndIdx[0];
                int paramIdx = Integer.parseInt(fieldAndIdx[1]);
                String mongoField = resolveMongoField(morphium, entityClass, javaField);
                query.f(mongoField).eq(args[paramIdx]);
            }
        }

        // Apply static @OrderBy sorting
        if (!orderBySpec.isEmpty()) {
            Map<String, Integer> sortMap = new LinkedHashMap<>();
            for (String part : orderBySpec.split(",")) {
                String[] fieldAndDir = part.split(":");
                String javaField = fieldAndDir[0];
                String dir = fieldAndDir[1];
                String mongoField = resolveMongoField(morphium, entityClass, javaField);
                sortMap.put(mongoField, "DESC".equals(dir) ? -1 : 1);
            }
            query.sort(sortMap);
        }

        // Apply dynamic Sort<T> parameter
        if (sortParamIndex >= 0 && args[sortParamIndex] != null) {
            Sort sort = (Sort) args[sortParamIndex];
            Map<String, Integer> sortMap = new LinkedHashMap<>();
            String mongoField = resolveMongoField(morphium, entityClass, sort.property());
            sortMap.put(mongoField, sort.isAscending() ? 1 : -1);
            query.sort(sortMap);
        }

        // Apply dynamic Order<T> parameter (contains multiple Sort entries)
        if (orderParamIndex >= 0 && args[orderParamIndex] != null) {
            Order order = (Order) args[orderParamIndex];
            if (!order.sorts().isEmpty()) {
                Map<String, Integer> sortMap = new LinkedHashMap<>();
                for (Object s : order.sorts()) {
                    Sort sort = (Sort) s;
                    String mongoField = resolveMongoField(morphium, entityClass, sort.property());
                    sortMap.put(mongoField, sort.isAscending() ? 1 : -1);
                }
                query.sort(sortMap);
            }
        }

        // Apply Limit parameter
        if (limitParamIndex >= 0 && args[limitParamIndex] != null) {
            Limit limit = (Limit) args[limitParamIndex];
            query.skip((int) (limit.startAt() - 1));
            query.limit(limit.maxResults());
        }

        // Apply PageRequest parameter → return Page<T> or CursoredPage<T>
        if (pageRequestParamIndex >= 0 && args[pageRequestParamIndex] != null) {
            PageRequest pageRequest = (PageRequest) args[pageRequestParamIndex];

            if (returnsCursoredPage) {
                return executeCursoredFind(query, pageRequest, conditionsSpec, orderBySpec,
                        morphium, entityClass, args);
            }

            int size = pageRequest.size();
            long page = pageRequest.page();
            int skip = (int) ((page - 1) * size);
            query.skip(skip).limit(size);

            List content = query.asList();
            long totalElements = -1;
            if (pageRequest.requestTotal()) {
                // Re-create query for total count (without skip/limit)
                Query countQuery = morphium.createQueryFor(entityClass);
                if (!conditionsSpec.isEmpty()) {
                    for (String p : conditionsSpec.split(",")) {
                        String[] fieldAndIdx = p.split(":");
                        String mongoField = resolveMongoField(morphium, entityClass, fieldAndIdx[0]);
                        countQuery.f(mongoField).eq(args[Integer.parseInt(fieldAndIdx[1])]);
                    }
                }
                totalElements = countQuery.countAll();
            }
            return new MorphiumPage<>(content, totalElements, pageRequest);
        }

        // Execute
        if (returnsOptional) {
            return QueryResultHelper.optionalSingle(query);
        }
        if (returnsSingle) {
            return QueryResultHelper.requireSingle(query);
        }
        if (returnsStream) {
            return query.stream();
        }
        return query.asList();
    }

    /**
     * Executes the cursor-paged branch of {@link #executeFind} for {@code @Find} methods
     * returning {@code CursoredPage<T>}.
     *
     * @param query           the base query with conditions and static ordering already applied
     * @param pageRequest     the requested page (cursor/offset mode, size, whether to compute totals)
     * @param conditionsSpec  encoded conditions, used to rebuild an unrestricted count query
     * @param orderBySpec     encoded ordering, used to derive the keyset fields for cursor support
     * @param morphium        the Morphium instance
     * @param entityClass     the entity class
     * @param args            the method arguments (for re-applying conditions to the count query)
     * @return the cursored page of results
     * @throws IllegalArgumentException if {@code pageRequest} requires a cursor but none is present
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object executeCursoredFind(Query query, PageRequest pageRequest,
                                               String conditionsSpec, String orderBySpec,
                                               Morphium morphium, Class entityClass,
                                               Object[] args) {
        List<CursorHelper.SortSpec> sortSpecs = CursorHelper.parseSortSpecs(orderBySpec);
        boolean isForward = pageRequest.mode() != PageRequest.Mode.CURSOR_PREVIOUS;

        if (pageRequest.mode() != PageRequest.Mode.OFFSET) {
            // Cursor-based: apply cursor condition and adjusted sort
            PageRequest.Cursor cursor = pageRequest.cursor()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "PageRequest mode is " + pageRequest.mode() + " but no cursor provided"));
            CursorHelper.applyCursorCondition(query, cursor, sortSpecs, morphium, entityClass, isForward);
        }

        // Apply sort (inverted for CURSOR_PREVIOUS)
        CursorHelper.applySort(query, sortSpecs, morphium, entityClass, isForward);
        // Fetch one extra to determine hasNext precisely
        int requestedSize = pageRequest.size();
        query.limit(requestedSize + 1);

        List content = query.asList();
        boolean hasMore = content.size() > requestedSize;
        if (hasMore) {
            content = new ArrayList(content.subList(0, requestedSize));
        }
        if (!isForward) {
            Collections.reverse(content);
        }

        // Extract cursors for each row
        List<String> sortFields = sortSpecs.stream().map(CursorHelper.SortSpec::javaField).toList();
        List<PageRequest.Cursor> cursors = CursorHelper.extractCursors(content, sortFields, morphium, entityClass);

        long totalElements = -1;
        if (pageRequest.requestTotal()) {
            Query countQuery = morphium.createQueryFor(entityClass);
            if (!conditionsSpec.isEmpty()) {
                for (String p : conditionsSpec.split(",")) {
                    String[] fieldAndIdx = p.split(":");
                    String mongoField = resolveMongoField(morphium, entityClass, fieldAndIdx[0]);
                    countQuery.f(mongoField).eq(args[Integer.parseInt(fieldAndIdx[1])]);
                }
            }
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

    /**
     * Executes a {@code @Delete} annotated method with {@code @By} parameters.
     *
     * @param repo           the repository instance
     * @param conditionsSpec encoded conditions (same format as executeFind)
     * @param args           the method arguments
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void executeAnnotatedDelete(AbstractMorphiumRepository<?, ?> repo,
                                              String conditionsSpec,
                                              Object[] args) {
        Morphium morphium = repo.getMorphium();
        Class entityClass = repo.getMetadata().entityClass();
        Query query = morphium.createQueryFor(entityClass);

        if (!conditionsSpec.isEmpty()) {
            for (String part : conditionsSpec.split(",")) {
                String[] fieldAndIdx = part.split(":");
                String javaField = fieldAndIdx[0];
                int paramIdx = Integer.parseInt(fieldAndIdx[1]);
                String mongoField = resolveMongoField(morphium, entityClass, javaField);
                query.f(mongoField).eq(args[paramIdx]);
            }
        }

        List toDelete = query.asList();
        for (Object entity : toDelete) {
            morphium.delete(entity);
        }
    }

    /**
     * Asynchronous variant of {@link #executeFind}, running the query on the repository's
     * async executor.
     *
     * @param repo               the repository instance
     * @param conditionsSpec     encoded conditions, see {@link #executeFind}
     * @param orderBySpec        encoded ordering, see {@link #executeFind}
     * @param sortParamIndex     index of Sort parameter, -1 if absent
     * @param orderParamIndex    index of Order parameter, -1 if absent
     * @param pageRequestParamIndex index of PageRequest parameter, -1 if absent
     * @param limitParamIndex    index of Limit parameter, -1 if absent
     * @param args               the method arguments
     * @param returnsSingle      true if method returns a single entity T
     * @param returnsOptional    true if method returns Optional&lt;T&gt;
     * @param returnsCursoredPage true if method returns CursoredPage&lt;T&gt;
     * @param returnsStream      true if method returns Stream&lt;T&gt;
     * @return a completion stage yielding the result of {@link #executeFind}
     */
    public static CompletionStage<Object> executeFindAsync(AbstractMorphiumRepository<?, ?> repo,
                                                              String conditionsSpec,
                                                              String orderBySpec,
                                                              int sortParamIndex,
                                                              int orderParamIndex,
                                                              int pageRequestParamIndex,
                                                              int limitParamIndex,
                                                              Object[] args,
                                                              boolean returnsSingle,
                                                              boolean returnsOptional,
                                                              boolean returnsCursoredPage,
                                                              boolean returnsStream) {
        return CompletableFuture.supplyAsync(
                () -> executeFind(repo, conditionsSpec, orderBySpec, sortParamIndex, orderParamIndex,
                        pageRequestParamIndex, limitParamIndex, args, returnsSingle, returnsOptional,
                        returnsCursoredPage, returnsStream),
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
