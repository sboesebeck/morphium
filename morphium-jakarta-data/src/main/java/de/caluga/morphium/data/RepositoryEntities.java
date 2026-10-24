package de.caluga.morphium.data;

import de.caluga.morphium.annotations.Entity;
import jakarta.data.repository.Delete;
import jakarta.data.repository.Insert;
import jakarta.data.repository.Query;
import jakarta.data.repository.Save;
import jakarta.data.repository.Update;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Which entity a method of a repository works on (MOR-077). A repository that extends {@code BasicRepository<T, K>} has its primary entity in the type arguments. One that extends nothing
 * (a repository that only declares {@code @Insert}, {@code @Query} and the like) has the entity of its lifecycle methods ({@code @Insert}, {@code @Save}, {@code @Update} and a
 * {@code @Delete} that takes entities), if they all use the same one; otherwise it has no primary entity. A single method can work on another entity than the primary one: a JDQL statement names
 * its entity ({@code DELETE FROM Box}), a lifecycle method gets it from its parameter, and a query or a find method from the type it returns.
 * <p>
 * The rules work on reflection; the Quarkus build applies the same rules to the Jandex types ({@code MorphiumDataProcessor}).
 */
public final class RepositoryEntities {

    private RepositoryEntities() {
    }

    /** whether the class is an entity of Morphium */
    public static boolean isEntity(Class<?> type) {
        return type != null && type.isAnnotationPresent(Entity.class);
    }

    /** the type that a type stands for: {@code T}, {@code T[]}, {@code T...}, or the type argument of {@code List<T>}, {@code Optional<T>}, {@code Stream<T>}, {@code Page<T>} and the like */
    public static Class<?> element(Type type) {
        if (type instanceof Class<?> c) {
            return c.isArray() ? element(c.getComponentType()) : c;
        }

        if (type instanceof GenericArrayType g) {
            return element(g.getGenericComponentType());
        }

        if (type instanceof ParameterizedType p && p.getActualTypeArguments().length == 1) {
            return element(p.getActualTypeArguments()[0]);
        }

        return null;
    }

    /** whether the method is a lifecycle method: {@code @Insert}, {@code @Save}, {@code @Update}, or a {@code @Delete} whose first parameter is an entity or entities */
    public static boolean isLifecycle(Method method) {
        return lifecycleEntity(method) != null;
    }

    /** the entity of a lifecycle method, taken from its first parameter; null if the method is no lifecycle method */
    public static Class<?> lifecycleEntity(Method method) {
        boolean annotated = method.isAnnotationPresent(Insert.class) || method.isAnnotationPresent(Save.class) || method.isAnnotationPresent(Update.class)
            || method.isAnnotationPresent(Delete.class);

        if (!annotated || method.getParameterCount() != 1) {
            return null;
        }

        Class<?> parameter = element(method.getGenericParameterTypes()[0]);
        return isEntity(parameter) ? parameter : null;
    }

    /**
     * The primary entity: the type argument of {@code DataRepository<T, K>} if there is one, otherwise the entity of the lifecycle methods if they all agree, otherwise null.
     */
    public static Class<?> primary(Class<?> repository) {
        Class<?> declared = declared(repository);

        if (declared != null) {
            return declared;
        }

        Set<Class<?>> lifecycle = new LinkedHashSet<>();

        for (Method m : repository.getMethods()) {
            Class<?> entity = lifecycleEntity(m);

            if (entity != null) {
                lifecycle.add(entity);
            }
        }

        return lifecycle.size() == 1 ? lifecycle.iterator().next() : null;
    }

    private static Class<?> declared(Class<?> repository) {
        for (Type iface : repository.getGenericInterfaces()) {
            if (iface instanceof ParameterizedType pt && pt.getRawType() instanceof Class<?> raw && jakarta.data.repository.DataRepository.class.isAssignableFrom(raw)
                && pt.getActualTypeArguments().length >= 1 && pt.getActualTypeArguments()[0] instanceof Class<?> entity) {
                return entity;
            }
        }

        for (Class<?> superInterface : repository.getInterfaces()) {
            Class<?> found = declared(superInterface);

            if (found != null) {
                return found;
            }
        }

        return null;
    }

    /** every entity that the repository works on, by lower-cased simple name: the primary one, the ones of the lifecycle methods, and the ones that methods return or take */
    public static Map<String, Class<?>> known(Class<?> repository) {
        Map<String, Class<?>> known = new LinkedHashMap<>();
        Class<?> primary = primary(repository);

        if (primary != null) {
            known.put(primary.getSimpleName().toLowerCase(Locale.ROOT), primary);
        }

        for (Method m : repository.getMethods()) {
            Class<?> lifecycle = lifecycleEntity(m);

            if (lifecycle != null) {
                known.putIfAbsent(lifecycle.getSimpleName().toLowerCase(Locale.ROOT), lifecycle);
            }

            Class<?> returned = element(m.getGenericReturnType());

            if (isEntity(returned)) {
                known.putIfAbsent(returned.getSimpleName().toLowerCase(Locale.ROOT), returned);
            }
        }

        return known;
    }

    /**
     * The entity a method works on. A lifecycle method: the entity of its parameter. A JDQL statement that names its entity: that entity, which must be one of the repository. Otherwise the
     * entity that the method returns, and if it returns none, the primary entity.
     *
     * @return the entity, null if the repository has no primary entity and the method names none
     * @throws IllegalStateException if the statement names an entity that is none of the repository
     */
    public static Class<?> forMethod(Method method, Class<?> primary, Map<String, Class<?>> known) {
        Class<?> lifecycle = lifecycleEntity(method);

        if (lifecycle != null) {
            return lifecycle;
        }

        Query query = method.getAnnotation(Query.class);
        String named = null;

        if (query != null) {
            try {
                named = JdqlEntityName.of(query.value());
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("@Query on " + method + ": " + e.getMessage(), e);
            }
        }

        if (named != null) {
            Class<?> entity = known.get(named.toLowerCase(Locale.ROOT));

            if (entity != null) {
                return entity;
            }

            // DELETE and UPDATE have always been checked against the entity of the repository; the name after FROM of a query was never looked at and still is not
            if (JdqlDelete.is(query.value()) || JdqlUpdate.is(query.value())) {
                throw new IllegalStateException("@Query on " + method + " names the entity " + named + ", which is no entity of the repository (known: "
                    + known.values().stream().map(Class::getSimpleName).toList() + ")");
            }
        }

        Class<?> returned = element(method.getGenericReturnType());
        return isEntity(returned) ? returned : primary;
    }
}
