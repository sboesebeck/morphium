package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.data.*;
import jakarta.data.Order;
import jakarta.data.Sort;
import jakarta.data.Limit;
import jakarta.data.page.CursoredPage;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;
import jakarta.data.repository.Delete;
import jakarta.data.repository.Find;
import jakarta.data.repository.OrderBy;
import jakarta.data.repository.Param;
import jakarta.data.repository.Query;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
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

        return handlers.computeIfAbsent(method, this::analyzeMethod).handle(args);
    }

    private MethodHandler analyzeMethod(Method method) {
        String name = method.getName();
        Class<?> returnType = method.getReturnType();

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
        if (name.startsWith("findBy") || name.startsWith("countBy")
                || name.startsWith("existsBy") || name.startsWith("deleteBy")) {
            return buildDerivedQueryHandler(method);
        }

        throw new UnsupportedOperationException(
                "Unsupported repository method: " + repositoryInterface.getSimpleName() + "." + name);
    }

    private MethodHandler buildDerivedQueryHandler(Method method) {
        boolean returnsSingle = !List.class.isAssignableFrom(method.getReturnType())
                && !Stream.class.isAssignableFrom(method.getReturnType())
                && !Page.class.isAssignableFrom(method.getReturnType())
                && !Iterable.class.isAssignableFrom(method.getReturnType())
                && !method.getReturnType().equals(long.class)
                && !method.getReturnType().equals(Long.class)
                && !method.getReturnType().equals(boolean.class)
                && !method.getReturnType().equals(Boolean.class)
                && !Optional.class.isAssignableFrom(method.getReturnType())
                && !CompletionStage.class.isAssignableFrom(method.getReturnType());
        boolean returnsOptional = Optional.class.isAssignableFrom(method.getReturnType());
        boolean returnsBoolean = method.getReturnType() == boolean.class
                || method.getReturnType() == Boolean.class;
        boolean returnsStream = Stream.class.isAssignableFrom(method.getReturnType());

        String orderBySpec = getOrderBySpec(method);

        return args -> QueryMethodBridge.executeQuery(
                delegate, method.getName(), args != null ? args : new Object[0],
                returnsSingle, returnsOptional, returnsBoolean, returnsStream, orderBySpec);
    }

    private MethodHandler buildJdqlHandler(Method method, Query queryAnno) {
        String jdql = queryAnno.value();
        String paramMapSpec = buildParamMapSpec(method);
        int sortIdx = findParamIndex(method, Sort.class);
        int orderIdx = findParamIndex(method, Order.class);
        int pageRequestIdx = findParamIndex(method, PageRequest.class);
        int limitIdx = findParamIndex(method, Limit.class);

        boolean returnsSingle = isSingleReturn(method);
        boolean returnsCount = method.getReturnType() == long.class || method.getReturnType() == Long.class;
        boolean returnsBoolean = method.getReturnType() == boolean.class || method.getReturnType() == Boolean.class;
        boolean returnsOptional = Optional.class.isAssignableFrom(method.getReturnType());
        boolean returnsCursoredPage = CursoredPage.class.isAssignableFrom(method.getReturnType());
        boolean returnsStream = Stream.class.isAssignableFrom(method.getReturnType());
        String orderBySpec = getOrderBySpec(method);

        return args -> JdqlMethodBridge.executeJdql(
                delegate, jdql, paramMapSpec,
                sortIdx, orderIdx, pageRequestIdx, limitIdx,
                args != null ? args : new Object[0],
                returnsSingle, returnsCount, returnsBoolean, returnsOptional,
                returnsCursoredPage, orderBySpec, returnsStream, null);
    }

    private MethodHandler buildFindHandler(Method method) {
        String conditionsSpec = buildConditionsSpec(method);
        String orderBySpec = getOrderBySpec(method);
        int sortIdx = findParamIndex(method, Sort.class);
        int orderIdx = findParamIndex(method, Order.class);
        int pageRequestIdx = findParamIndex(method, PageRequest.class);
        int limitIdx = findParamIndex(method, Limit.class);

        boolean returnsSingle = isSingleReturn(method);
        boolean returnsOptional = Optional.class.isAssignableFrom(method.getReturnType());
        boolean returnsCursoredPage = CursoredPage.class.isAssignableFrom(method.getReturnType());
        boolean returnsStream = Stream.class.isAssignableFrom(method.getReturnType());

        return args -> FindMethodBridge.executeFind(
                delegate, conditionsSpec, orderBySpec,
                sortIdx, orderIdx, pageRequestIdx, limitIdx,
                args != null ? args : new Object[0],
                returnsSingle, returnsOptional, returnsCursoredPage, returnsStream);
    }

    private MethodHandler buildDeleteHandler(Method method) {
        String conditionsSpec = buildConditionsSpec(method);
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
            Param paramAnno = params[i].getAnnotation(Param.class);
            String fieldName = paramAnno != null ? paramAnno.value() : params[i].getName();
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
                && !Stream.class.isAssignableFrom(rt)
                && !Page.class.isAssignableFrom(rt)
                && !CursoredPage.class.isAssignableFrom(rt)
                && !Iterable.class.isAssignableFrom(rt)
                && !Optional.class.isAssignableFrom(rt)
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
