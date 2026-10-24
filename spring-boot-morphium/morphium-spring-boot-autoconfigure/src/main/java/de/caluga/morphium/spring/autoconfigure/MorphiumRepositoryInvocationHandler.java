package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.data.*;
import jakarta.data.Order;
import jakarta.data.Sort;
import jakarta.data.Limit;
import jakarta.data.page.CursoredPage;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;
import jakarta.data.repository.By;
import jakarta.data.repository.Delete;
import jakarta.data.repository.Find;
import jakarta.data.repository.OrderBy;
import jakarta.data.repository.Param;
import jakarta.data.repository.Query;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * JDK dynamic proxy handler for Morphium repository interfaces.
 * Dispatches method calls to CRUD operations on {@link AbstractMorphiumRepository}
 * or to the query bridges ({@link QueryMethodBridge}, {@link JdqlMethodBridge},
 * {@link FindMethodBridge}) from the shared morphium-jakarta-data module.
 */
class MorphiumRepositoryInvocationHandler implements InvocationHandler {

    private final SpringMorphiumRepository delegate;
    private final Class<?> repositoryInterface;
    private final ConcurrentHashMap<Method, MethodHandler> handlers = new ConcurrentHashMap<>();

    MorphiumRepositoryInvocationHandler(Morphium morphium, RepositoryMetadata metadata,
                                         Class<?> repositoryInterface) {
        this.repositoryInterface = repositoryInterface;
        this.delegate = new SpringMorphiumRepository(metadata);
        this.delegate.setMorphium(morphium);

        // A Jakarta Data 1.1 feature that the query methods do not implement (MOR-019) fails when the repository is created, not at the first call
        for (Method method : repositoryInterface.getMethods()) {
            if (!method.isDefault() && (method.isAnnotationPresent(Find.class) || method.isAnnotationPresent(Delete.class) || method.isAnnotationPresent(Query.class))) {
                rejectUnsupportedParameterStyles(method);
            }

            // so does a wrong @AtomicUpdate (MOR-021)
            if (!method.isDefault() && method.isAnnotationPresent(AtomicUpdate.class)) {
                handlers.put(method, buildAtomicUpdateHandler(method, method.getAnnotation(AtomicUpdate.class)));
            }
        }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // Object methods
        if (method.getDeclaringClass() == Object.class) {
            return switch (method.getName()) {
                case "toString" -> repositoryInterface.getSimpleName() + "@MorphiumProxy";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                default -> method.invoke(this, args);
            };
        }

        // A default method carries its own implementation, so it must run as written instead
        // of being analysed as a query. Handled here rather than in analyzeMethod() because
        // InvocationHandler.invokeDefault needs the proxy instance, which the MethodHandler
        // functional interface (args only) cannot carry - and handled BEFORE the derived-query
        // check further down, since a default method is free to be named findBy*/countBy*.
        // Without this, any default method ended in the "Unsupported repository method"
        // UnsupportedOperationException at the bottom of analyzeMethod().
        if (method.isDefault()) {
            return InvocationHandler.invokeDefault(proxy, method, args);
        }

        Object result = handlers.computeIfAbsent(method, this::analyzeMethod).handle(args);

        // a method that returns an array of the entity (MOR-080)
        return method.getReturnType().isArray() ? ResultShapes.toArray(result, method.getReturnType().getComponentType()) : result;
    }

    private MethodHandler analyzeMethod(Method method) {
        String name = method.getName();
        Class<?> returnType = method.getReturnType();

        // Morphium extension (MOR-021): one atomic update with structured assignments
        if (method.isAnnotationPresent(AtomicUpdate.class)) {
            return buildAtomicUpdateHandler(method, method.getAnnotation(AtomicUpdate.class));
        }

        // --- CrudRepository standard methods ---
        if (name.equals("findById") && method.getParameterCount() == 1) {
            return args -> delegate.doFindById(args[0]);
        }
        if (name.equals("findAll") && method.getParameterCount() == 0) {
            return args -> delegate.doFindAll();
        }
        if (name.equals("findAll") && method.getParameterCount() == 2) {
            return args -> delegate.doFindAllPaged((PageRequest) args[0], (Order) args[1]);
        }
        if (name.equals("save") && method.getParameterCount() == 1) {
            return args -> delegate.doSave(args[0]);
        }
        if (name.equals("saveAll") && method.getParameterCount() == 1) {
            return args -> delegate.doSaveAll((List<?>) args[0]);
        }
        if (name.equals("insert") && method.getParameterCount() == 1) {
            return args -> delegate.doInsert(args[0]);
        }
        if (name.equals("insertAll") && method.getParameterCount() == 1) {
            return args -> delegate.doInsertAll((List<?>) args[0]);
        }
        if (name.equals("update") && method.getParameterCount() == 1) {
            return args -> delegate.doUpdate(args[0]);
        }
        if (name.equals("updateAll") && method.getParameterCount() == 1) {
            return args -> delegate.doUpdateAll((List<?>) args[0]);
        }
        if (name.equals("delete") && method.getParameterCount() == 1) {
            return args -> { delegate.doDelete(args[0]); return null; };
        }
        if (name.equals("deleteById") && method.getParameterCount() == 1) {
            return args -> { delegate.doDeleteById(args[0]); return null; };
        }
        if (name.equals("deleteAll") && method.getParameterCount() == 1) {
            return args -> { delegate.doDeleteAll((List<?>) args[0]); return null; };
        }
        if (name.equals("deleteAll") && method.getParameterCount() == 0) {
            return args -> { delegate.doDeleteAllNoArg(); return null; };
        }

        // --- MorphiumRepository extensions ---
        if (name.equals("distinct") && method.getParameterCount() == 1) {
            return args -> delegate.doDistinct((String) args[0]);
        }
        if (name.equals("morphium") && method.getParameterCount() == 0) {
            return args -> delegate.doMorphium();
        }
        if (name.equals("query") && method.getParameterCount() == 0) {
            return args -> delegate.doQuery();
        }

        // --- @Query (JDQL) ---
        Query queryAnno = method.getAnnotation(Query.class);
        if (queryAnno != null) {
            return buildJdqlHandler(method, queryAnno);
        }

        // --- @Find ---
        Find findAnno = method.getAnnotation(Find.class);
        if (findAnno != null) {
            return buildFindHandler(method);
        }

        // --- @Delete ---
        Delete deleteAnno = method.getAnnotation(Delete.class);
        if (deleteAnno != null) {
            return buildDeleteHandler(method);
        }

        // --- Derived query (findBy*, countBy*, existsBy*, deleteBy*) ---
        if (MethodNameParser.isDerivedQueryName(name)) {
            return buildDerivedQueryHandler(method);
        }

        throw new UnsupportedOperationException(
                "Unsupported repository method: " + repositoryInterface.getSimpleName() + "." + name);
    }

    private MethodHandler buildDerivedQueryHandler(Method method) {
        boolean returnsAsync = CompletionStage.class.isAssignableFrom(method.getReturnType());
        boolean returnsSingle = !List.class.isAssignableFrom(method.getReturnType())
                && !method.getReturnType().isArray()
                && !Stream.class.isAssignableFrom(method.getReturnType())
                && !Page.class.isAssignableFrom(method.getReturnType())
                && !Iterable.class.isAssignableFrom(method.getReturnType())
                && !method.getReturnType().equals(long.class)
                && !method.getReturnType().equals(Long.class)
                && !method.getReturnType().equals(boolean.class)
                && !method.getReturnType().equals(Boolean.class)
                && !Optional.class.isAssignableFrom(method.getReturnType())
                && !returnsAsync;
        boolean returnsOptional = Optional.class.isAssignableFrom(method.getReturnType());
        boolean returnsBoolean = method.getReturnType() == boolean.class
                || method.getReturnType() == Boolean.class;
        boolean returnsStream = Stream.class.isAssignableFrom(method.getReturnType());

        String orderBySpec = getOrderBySpec(method);

        // Regression fix: this handler never looked up dynamic Sort/Order/PageRequest/Limit
        // parameters and always dispatched to the simple executeQuery overload -- a Page<T>
        // method got a plain List back (ClassCastException at the proxy boundary) and a
        // Sort<T> argument was silently dropped (wrong order, no error). Determine the four
        // indices the same way buildJdqlHandler/buildFindHandler already do and always call
        // the overload that takes them; it short-circuits back to the simple overload itself
        // when all four indices are -1, so this is safe for the common case too.
        int sortIdx = findParamIndex(method, Sort.class);
        int orderIdx = findParamIndex(method, Order.class);
        int pageRequestIdx = findParamIndex(method, PageRequest.class);
        int limitIdx = findParamIndex(method, Limit.class);

        // Strip the "Async" suffix for parsing (e.g. "findByStatusAsync" -> "findByStatus"),
        // matching quarkus-morphium's MorphiumDataProcessor convention for derived-query
        // methods with a CompletionStage return type.
        String methodName = method.getName();
        String parseableName = returnsAsync && methodName.endsWith("Async")
                ? methodName.substring(0, methodName.length() - 5) : methodName;

        if (returnsAsync) {
            return args -> QueryMethodBridge.executeQueryAsync(
                    delegate, parseableName, args != null ? args : new Object[0],
                    returnsSingle, returnsOptional, returnsBoolean, returnsStream, orderBySpec,
                    sortIdx, orderIdx, pageRequestIdx, limitIdx);
        }
        return args -> QueryMethodBridge.executeQuery(
                delegate, parseableName, args != null ? args : new Object[0],
                returnsSingle, returnsOptional, returnsBoolean, returnsStream, orderBySpec,
                sortIdx, orderIdx, pageRequestIdx, limitIdx);
    }

    /**
     * Throws for a Jakarta Data 1.1 feature that the {@code @Find}, {@code @Delete} and {@code @Query} methods do not implement (MOR-019): {@code @Is}, a
     * constraint or {@code Restriction} parameter and {@code @First}. Without this check the parameter would be bound as an equality. Works on names, so
     * it does not need the 1.1 API.
     */
    private static void rejectUnsupportedParameterStyles(Method method) {
        List<String> methodAnnotations = new ArrayList<>();

        for (java.lang.annotation.Annotation a : method.getAnnotations()) {
            methodAnnotations.add(a.annotationType().getName());
        }

        List<UnsupportedParameterStyles.Param> params = new ArrayList<>();
        java.lang.reflect.Parameter[] parameters = method.getParameters();

        for (java.lang.reflect.Parameter p : parameters) {
            List<String> names = new ArrayList<>();

            for (java.lang.annotation.Annotation a : p.getAnnotations()) {
                names.add(a.annotationType().getName());
            }

            params.add(new UnsupportedParameterStyles.Param(p.getType().getName(), names));
        }

        String problem = UnsupportedParameterStyles.check(method.getDeclaringClass().getName() + "." + method.getName(), methodAnnotations, params);

        if (problem != null) {
            throw new IllegalStateException(problem);
        }
    }

    /**
     * The handler of an {@link AtomicUpdate} method (MOR-021). Everything that can be checked without a call is checked here, so a wrong annotation fails when the
     * repository is created.
     */
    private MethodHandler buildAtomicUpdateHandler(Method method, AtomicUpdate au) {
        String where = method.getDeclaringClass().getName() + "." + method.getName();
        Class<?> returnType = method.getReturnType();
        List<String> parameterNames = new ArrayList<>();

        for (java.lang.reflect.Parameter p : method.getParameters()) {
            Class<?> t = p.getType();

            if (t == Sort.class || t == Order.class || t == PageRequest.class || t == Limit.class) {
                throw new IllegalStateException("@AtomicUpdate on " + where + ": takes no Sort, Order, PageRequest or Limit parameter (use @OrderBy for the order)");
            }
        }

        boolean countForm = returnType == void.class || returnType == boolean.class || returnType == int.class || returnType == long.class;
        Class<?> entityClass = delegate.getMetadata().entityClass();
        boolean optional = returnType == Optional.class;

        if (!countForm && !optional && returnType != entityClass) {
            throw new IllegalStateException("@AtomicUpdate on " + where + ": the return type " + returnType.getName() + " is not supported. void, boolean, int or long give the count form "
                    + "(an update command), " + entityClass.getName() + " or Optional<" + entityClass.getName() + "> the find-and-modify form; collections, streams and everything else are refused");
        }

        String paramMapSpec = buildParamMapSpec(method);
        java.util.Set<String> paramNames = new java.util.HashSet<>();

        if (!paramMapSpec.isEmpty()) {
            for (String entry : paramMapSpec.split(",")) {
                paramNames.add(entry.split(":")[0]);
            }
        }

        AtomicUpdateSpec spec = new AtomicUpdateSpec(au.where(), assignments(au.set(), where), assignments(au.setOnInsert(), where), assignments(au.inc(), where), au.upsert(), au.many(),
                au.returns(), getOrderBySpec(method), paramMapSpec);
        java.util.Set<String> entityFields = new java.util.HashSet<>();

        for (java.lang.reflect.Field f : delegate.getMorphium().getARHelper().getAllFields(entityClass)) {
            entityFields.add(f.getName());
        }

        spec.validate(where, countForm ? AtomicUpdateSpec.Form.COUNT : AtomicUpdateSpec.Form.DOCUMENT, paramNames, entityFields);
        String encoded = spec.encode();

        if (returnType == void.class) {
            return args -> {
                JdqlMethodBridge.executeAtomicCount(delegate, encoded, args != null ? args : new Object[0]);
                return null;
            };
        } else if (returnType == boolean.class) {
            return args -> JdqlMethodBridge.executeAtomicBoolean(delegate, encoded, args != null ? args : new Object[0]);
        } else if (returnType == int.class) {
            return args -> JdqlMethodBridge.executeAtomicInt(delegate, encoded, args != null ? args : new Object[0]);
        } else if (returnType == long.class) {
            return args -> JdqlMethodBridge.executeAtomicCount(delegate, encoded, args != null ? args : new Object[0]);
        } else if (optional) {
            return args -> JdqlMethodBridge.executeAtomicOptional(delegate, encoded, args != null ? args : new Object[0]);
        }

        return args -> JdqlMethodBridge.executeAtomicDocument(delegate, encoded, args != null ? args : new Object[0]);
    }

    private static List<String> assignments(Assign[] assigns, String where) {
        List<String> result = new ArrayList<>();

        for (Assign a : assigns) {
            try {
                result.add(AtomicUpdateSpec.assignment(a.field(), a.param(), a.value()));
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("@AtomicUpdate on " + where + ": " + e.getMessage(), e);
            }
        }

        return result;
    }

    private MethodHandler buildJdqlHandler(Method method, Query queryAnno) {
        rejectUnsupportedParameterStyles(method);
        String jdql = queryAnno.value();
        String paramMapSpec = buildParamMapSpec(method);

        // DELETE FROM Entity [WHERE ...] (MOR-017) and UPDATE Entity SET ... [WHERE ...] (MOR-018): one server-side command, returns the number of
        // deleted or matched documents
        boolean isUpdate = JdqlUpdate.is(jdql);

        if (isUpdate || JdqlDelete.is(jdql)) {
            Class<?> returnType = method.getReturnType();

            if (returnType != void.class && returnType != int.class && returnType != long.class) {
                throw new IllegalStateException("@Query on " + method + ": a " + (isUpdate ? "UPDATE" : "DELETE") + " statement returns void, int or long, not "
                        + returnType.getName() + ": \"" + jdql + "\"");
            }

            try {
                String whereClause = isUpdate ? JdqlUpdate.parse(jdql).where() : JdqlDelete.whereClause(jdql);
                String operandProblem = JdqlOperands.check(whereClause);

                if (operandProblem != null) {
                    throw new IllegalStateException("@Query on " + method + ": the statement is not valid JDQL: " + operandProblem + ": \"" + jdql + "\"");
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("@Query on " + method + ": " + e.getMessage(), e);
            }

            return args -> {
                Object[] a = args != null ? args : new Object[0];
                long count = isUpdate ? JdqlMethodBridge.executeJdqlUpdate(delegate, jdql, paramMapSpec, a) : JdqlMethodBridge.executeJdqlDelete(delegate, jdql, paramMapSpec, a);
                return returnType == void.class ? null : returnType == int.class ? (Object) Math.toIntExact(count) : (Object) count;
            };
        }
        int sortIdx = findParamIndex(method, Sort.class);
        int orderIdx = findParamIndex(method, Order.class);
        int pageRequestIdx = findParamIndex(method, PageRequest.class);
        int limitIdx = findParamIndex(method, Limit.class);

        boolean returnsAsync = CompletionStage.class.isAssignableFrom(method.getReturnType());
        boolean returnsSingle = isSingleReturn(method);
        boolean returnsCount = method.getReturnType() == long.class || method.getReturnType() == Long.class;
        boolean returnsBoolean = method.getReturnType() == boolean.class || method.getReturnType() == Boolean.class;
        boolean returnsOptional = Optional.class.isAssignableFrom(method.getReturnType());
        boolean returnsCursoredPage = CursoredPage.class.isAssignableFrom(method.getReturnType());
        boolean returnsStream = Stream.class.isAssignableFrom(method.getReturnType());
        String orderBySpec = getOrderBySpec(method);

        // Regression fix: neither this method nor isSingleReturn(Method) excluded
        // CompletionStage, so a `CompletionStage<List<T>>` method was analyzed as a
        // single-entity query, ran synchronously on the caller's thread, and handed the
        // entity itself to the proxy where a CompletionStage was expected --
        // ClassCastException. Dispatch to the async bridge with the same parameters as
        // the sync call, matching quarkus-morphium's convention (used already by
        // buildDerivedQueryHandler) that a CompletionStage-returning query method
        // resolves to a plain (non-single, non-Optional) result.
        if (returnsAsync) {
            return args -> JdqlMethodBridge.executeJdqlAsync(
                    delegate, jdql, paramMapSpec,
                    sortIdx, orderIdx, pageRequestIdx, limitIdx,
                    args != null ? args : new Object[0],
                    returnsSingle, returnsCount, returnsBoolean, returnsOptional,
                    returnsCursoredPage, orderBySpec, returnsStream, null);
        }

        return args -> JdqlMethodBridge.executeJdql(
                delegate, jdql, paramMapSpec,
                sortIdx, orderIdx, pageRequestIdx, limitIdx,
                args != null ? args : new Object[0],
                returnsSingle, returnsCount, returnsBoolean, returnsOptional,
                returnsCursoredPage, orderBySpec, returnsStream, null);
    }

    private MethodHandler buildFindHandler(Method method) {
        rejectUnsupportedParameterStyles(method);
        String conditionsSpec = buildConditionsSpec(method);
        String orderBySpec = getOrderBySpec(method);
        int sortIdx = findParamIndex(method, Sort.class);
        int orderIdx = findParamIndex(method, Order.class);
        int pageRequestIdx = findParamIndex(method, PageRequest.class);
        int limitIdx = findParamIndex(method, Limit.class);

        boolean returnsAsync = CompletionStage.class.isAssignableFrom(method.getReturnType());
        boolean returnsSingle = isSingleReturn(method);
        boolean returnsOptional = Optional.class.isAssignableFrom(method.getReturnType());
        boolean returnsCursoredPage = CursoredPage.class.isAssignableFrom(method.getReturnType());
        boolean returnsStream = Stream.class.isAssignableFrom(method.getReturnType());

        // Regression fix: same CompletionStage gap as buildJdqlHandler above -- a
        // `CompletionStage<List<T>>` @Find method ran synchronously and returned the
        // entity/list directly instead of a CompletionStage, causing a
        // ClassCastException at the proxy boundary.
        if (returnsAsync) {
            return args -> FindMethodBridge.executeFindAsync(
                    delegate, conditionsSpec, orderBySpec,
                    sortIdx, orderIdx, pageRequestIdx, limitIdx,
                    args != null ? args : new Object[0],
                    returnsSingle, returnsOptional, returnsCursoredPage, returnsStream);
        }

        return args -> FindMethodBridge.executeFind(
                delegate, conditionsSpec, orderBySpec,
                sortIdx, orderIdx, pageRequestIdx, limitIdx,
                args != null ? args : new Object[0],
                returnsSingle, returnsOptional, returnsCursoredPage, returnsStream);
    }

    private MethodHandler buildDeleteHandler(Method method) {
        rejectUnsupportedParameterStyles(method);
        String conditionsSpec = buildConditionsSpec(method);
        Class<?> returnType = method.getReturnType();

        // Regression fix: Jakarta Data 1.0 permits void, int, and long return types for
        // @Delete methods -- the numeric variants must return the number of deleted
        // entities. This used to always call the void bridge and return null, which blew
        // up as a NullPointerException when the proxy tried to unbox null into a
        // primitive long/int return value.
        if (returnType == long.class || returnType == Long.class) {
            return args -> FindMethodBridge.executeAnnotatedDeleteCounted(
                    delegate, conditionsSpec, args != null ? args : new Object[0]);
        }
        if (returnType == int.class || returnType == Integer.class) {
            return args -> (int) FindMethodBridge.executeAnnotatedDeleteCounted(
                    delegate, conditionsSpec, args != null ? args : new Object[0]);
        }
        return args -> {
            FindMethodBridge.executeAnnotatedDelete(
                    delegate, conditionsSpec, args != null ? args : new Object[0]);
            return null;
        };
    }

    // --- Helpers ---

    private String buildParamMapSpec(Method method) {
        StringBuilder sb = new StringBuilder();
        Parameter[] params = method.getParameters();
        for (int i = 0; i < params.length; i++) {
            Param paramAnno = params[i].getAnnotation(Param.class);
            if (paramAnno != null) {
                if (sb.length() > 0) sb.append(",");
                sb.append(paramAnno.value()).append(":").append(i);
            } else if (!isSpecialParam(params[i].getType())) {
                // Use parameter name (requires -parameters compiler flag)
                if (sb.length() > 0) sb.append(",");
                sb.append(params[i].getName()).append(":").append(i);
            }
        }
        return sb.toString();
    }

    private String buildConditionsSpec(Method method) {
        StringBuilder sb = new StringBuilder();
        Parameter[] params = method.getParameters();
        for (int i = 0; i < params.length; i++) {
            if (isSpecialParam(params[i].getType())) continue;
            By byAnno = params[i].getAnnotation(By.class);
            String fieldName = byAnno != null ? byAnno.value() : params[i].getName();
            if (sb.length() > 0) sb.append(",");
            sb.append(fieldName).append(":").append(i);
        }
        return sb.toString();
    }

    private static boolean isSpecialParam(Class<?> type) {
        return Sort.class.isAssignableFrom(type)
                || Order.class.isAssignableFrom(type)
                || PageRequest.class.isAssignableFrom(type)
                || Limit.class.isAssignableFrom(type);
    }

    private static int findParamIndex(Method method, Class<?> paramType) {
        Parameter[] params = method.getParameters();
        for (int i = 0; i < params.length; i++) {
            if (paramType.isAssignableFrom(params[i].getType())) {
                return i;
            }
        }
        return -1;
    }

    private boolean isSingleReturn(Method method) {
        Class<?> rt = method.getReturnType();
        return !List.class.isAssignableFrom(rt)
                && !rt.isArray()
                && !Stream.class.isAssignableFrom(rt)
                && !Page.class.isAssignableFrom(rt)
                && !CursoredPage.class.isAssignableFrom(rt)
                && !Iterable.class.isAssignableFrom(rt)
                && !Optional.class.isAssignableFrom(rt)
                && !CompletionStage.class.isAssignableFrom(rt)
                && rt != long.class && rt != Long.class
                && rt != boolean.class && rt != Boolean.class
                && rt != void.class && rt != Void.class;
    }

    private static String getOrderBySpec(Method method) {
        OrderBy orderBy = method.getAnnotation(OrderBy.class);
        if (orderBy == null) return "";
        return orderBy.value();
    }

    @FunctionalInterface
    private interface MethodHandler {
        Object handle(Object[] args) throws Exception;
    }

    /**
     * Concrete (non-abstract) subclass of AbstractMorphiumRepository for Spring proxy use.
     * The setMorphium() method is package-visible through the parent.
     */
    private static class SpringMorphiumRepository extends AbstractMorphiumRepository<Object, Object> {
        SpringMorphiumRepository(RepositoryMetadata metadata) {
            super(metadata);
        }

        @Override
        public void setMorphium(Morphium morphium) {
            super.setMorphium(morphium);
        }
    }
}
