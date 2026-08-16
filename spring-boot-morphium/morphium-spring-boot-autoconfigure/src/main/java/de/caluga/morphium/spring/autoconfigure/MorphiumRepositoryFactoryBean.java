package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.data.RepositoryMetadata;
import jakarta.data.repository.CrudRepository;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;

/**
 * Spring {@link FactoryBean} that creates a {@link Proxy JDK dynamic proxy}
 * implementing a Morphium Jakarta Data repository interface. {@link
 * MorphiumRepositoryRegistrar} registers exactly one bean definition of this type per
 * {@code @Repository} interface discovered under {@link EnableMorphiumRepositories} —
 * application code never instantiates this class directly.
 *
 * <p>At bean-creation time (see {@link #getObject()}), it resolves the repository
 * interface's entity and ID type arguments, finds the entity's {@code @Id} field,
 * builds a {@code RepositoryMetadata}, and creates a proxy backed by a
 * {@link MorphiumRepositoryInvocationHandler}. This is the mechanism-level detail
 * behind {@link EnableMorphiumRepositories}'s "JDK dynamic proxy at runtime" — no
 * class is generated or compiled; {@code java.lang.reflect.Proxy} synthesizes the
 * implementing class in the running JVM, once per repository interface, the first
 * time the bean is requested (the bean is a singleton, so this happens at most once
 * per application context).
 *
 * @param <T> the repository interface type this factory bean produces
 */
public class MorphiumRepositoryFactoryBean<T> implements FactoryBean<T> {

    private final Class<T> repositoryInterface;

    @Autowired
    private Morphium morphium;

    /**
     * Creates a factory bean for the given repository interface. Called only by
     * {@link MorphiumRepositoryRegistrar} while building the bean definition; the
     * {@link Morphium} dependency is injected afterwards by Spring
     * ({@code @Autowired}), not passed here.
     *
     * @param repositoryInterface the {@code @Repository} interface this factory bean
     *                            will produce a proxy implementation for
     */
    public MorphiumRepositoryFactoryBean(Class<T> repositoryInterface) {
        this.repositoryInterface = repositoryInterface;
    }

    /**
     * Creates the JDK dynamic proxy implementing {@code repositoryInterface}. Resolves
     * the entity and ID type arguments from the interface's {@code CrudRepository<T,
     * K>} supertype (see {@link #resolveTypeArguments(Class)}), locates the entity's
     * {@code @Id} field name (see {@link #findIdFieldName(Class)}), and wires both
     * into a new {@link MorphiumRepositoryInvocationHandler} that dispatches every
     * proxied method call to the shared {@code morphium-jakarta-data} runtime.
     *
     * @return a new proxy instance implementing the repository interface; a fresh
     *         instance is returned on every call, but {@link #isSingleton()} tells
     *         Spring to only call this once and cache the result
     * @throws IllegalArgumentException if the entity/ID type arguments cannot be
     *         resolved from the repository interface hierarchy, or if the entity class
     *         has no {@code @Id}-annotated field and no fallback {@code id}/{@code
     *         morphiumId} field either
     */
    @Override
    @SuppressWarnings("unchecked")
    public T getObject() {
        var typeArgs = resolveTypeArguments(repositoryInterface);
        Class<?> entityClass = typeArgs[0];
        Class<?> idClass = typeArgs[1];
        String idFieldName = findIdFieldName(entityClass);

        var metadata = new RepositoryMetadata(entityClass, idClass, idFieldName);
        var handler = new MorphiumRepositoryInvocationHandler(morphium, metadata, repositoryInterface);

        return (T) Proxy.newProxyInstance(
                repositoryInterface.getClassLoader(),
                new Class<?>[]{ repositoryInterface },
                handler);
    }

    /**
     * Reports the repository interface itself as this factory bean's product type,
     * so Spring's type-based autowiring (e.g. {@code @Autowired ProductRepository})
     * resolves to the proxy this factory bean produces.
     *
     * @return the {@code @Repository} interface class passed to the constructor
     */
    @Override
    public Class<?> getObjectType() {
        return repositoryInterface;
    }

    /**
     * Declares that {@link #getObject()} is called at most once and its result cached
     * by Spring — the same proxy instance is returned to every injection point.
     *
     * @return always {@code true}
     */
    @Override
    public boolean isSingleton() {
        return true;
    }

    /**
     * Walks the interface hierarchy to find CrudRepository&lt;T, K&gt; type arguments.
     *
     * @param repoInterface the repository interface (or a super-interface reached
     *                     through recursion) to inspect
     * @return a two-element array {@code { entityClass, idClass }} resolved from the
     *         first {@code CrudRepository<T, K>}-parameterized supertype found
     * @throws IllegalArgumentException if no generic {@code CrudRepository<T, K>}
     *         supertype with resolvable type arguments exists anywhere in the
     *         interface hierarchy
     */
    static Class<?>[] resolveTypeArguments(Class<?> repoInterface) {
        for (Type iface : repoInterface.getGenericInterfaces()) {
            if (iface instanceof ParameterizedType pt) {
                Type raw = pt.getRawType();
                if (raw instanceof Class<?> rawClass && CrudRepository.class.isAssignableFrom(rawClass)) {
                    Type[] args = pt.getActualTypeArguments();
                    if (args.length >= 2 && args[0] instanceof Class<?> entity && args[1] instanceof Class<?> id) {
                        return new Class<?>[]{ entity, id };
                    }
                }
            }
        }
        // Recurse into super-interfaces
        for (Class<?> superIface : repoInterface.getInterfaces()) {
            try {
                return resolveTypeArguments(superIface);
            } catch (IllegalArgumentException ignored) {
            }
        }
        throw new IllegalArgumentException(
                "Cannot resolve entity/id types from " + repoInterface.getName());
    }

    /**
     * Finds the field annotated with {@code @Id} in the entity class hierarchy.
     *
     * @param entityClass the entity class to search, including its superclasses
     * @return the name of the {@code @Id}-annotated field, or — if none is found — the
     *         name of a field literally called {@code id} or {@code morphiumId} as a
     *         fallback
     * @throws IllegalArgumentException if neither an {@code @Id}-annotated field nor a
     *         fallback {@code id}/{@code morphiumId} field exists on {@code entityClass}
     */
    private static String findIdFieldName(Class<?> entityClass) {
        Class<?> cls = entityClass;
        while (cls != null && cls != Object.class) {
            for (Field f : cls.getDeclaredFields()) {
                if (f.isAnnotationPresent(Id.class)) {
                    return f.getName();
                }
            }
            cls = cls.getSuperclass();
        }
        // Fallback: look for a field named "id" or "morphiumId"
        try {
            entityClass.getDeclaredField("id");
            return "id";
        } catch (NoSuchFieldException ignored) {
        }
        try {
            entityClass.getDeclaredField("morphiumId");
            return "morphiumId";
        } catch (NoSuchFieldException ignored) {
        }
        throw new IllegalArgumentException(
                "No @Id field found in " + entityClass.getName());
    }
}
