package de.caluga.morphium.spring.autoconfigure;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Enables scanning for Jakarta Data {@code @Repository} interfaces (extending
 * {@code jakarta.data.repository.CrudRepository} or
 * {@code de.caluga.morphium.data.MorphiumRepository}) and registers a Spring bean for
 * each one found, backed by a JDK dynamic proxy.
 *
 * <p>By default the scan covers the package of the class annotated with
 * {@code @EnableMorphiumRepositories} and its sub-packages; pass explicit packages via
 * {@link #value()} or {@link #basePackages()} to scan elsewhere.
 *
 * <pre>{@code
 * @SpringBootApplication
 * @EnableMorphiumRepositories
 * public class MyApplication {
 *     public static void main(String[] args) {
 *         SpringApplication.run(MyApplication.class, args);
 *     }
 * }
 * }</pre>
 *
 * <pre>{@code
 * @Repository
 * public interface ProductRepository extends MorphiumRepository<Product, MorphiumId> {
 *     List<Product> findByCategory(String category);
 * }
 *
 * @Service
 * public class ProductService {
 *     @Autowired ProductRepository products; // JDK proxy, injected like any bean
 * }
 * }</pre>
 *
 * <h2>How the proxy mechanism works</h2>
 * This annotation triggers {@code @Import(MorphiumRepositoryRegistrar.class)}. At
 * context-startup time, {@link MorphiumRepositoryRegistrar} scans the configured
 * base packages for {@code @Repository} interfaces and registers one
 * {@link MorphiumRepositoryFactoryBean} bean definition per interface found. Each
 * {@code FactoryBean} creates a {@link java.lang.reflect.Proxy JDK dynamic proxy}
 * implementing the repository interface, backed by a
 * {@link MorphiumRepositoryInvocationHandler} that dispatches every method call —
 * derived queries ({@code findBy*}), {@code @Query} (JDQL), {@code @Find}/{@code
 * @Delete}, and plain CRUD — to the shared, framework-agnostic runtime in
 * {@code morphium-jakarta-data}. No implementation class is ever generated or
 * compiled; the interface's bytecode is used unmodified, and Java's built-in
 * {@code java.lang.reflect.Proxy} mechanism creates the implementing class
 * <strong>at application startup, in the running JVM</strong>.
 *
 * <p>This is deliberately different from the {@code quarkus-morphium} extension's
 * approach to the same problem: Quarkus generates a concrete implementation class for
 * each repository interface via Gizmo bytecode generation <strong>at build
 * time</strong>, so no proxy or reflection exists at runtime at all — the generated
 * class is compiled into the application the same as any other class. The trade-off
 * is the classic one between the two approaches: this module's JDK proxies need zero
 * build-time tooling and work unmodified with plain {@code javac}, at the cost of a
 * small amount of reflective dispatch overhead per repository call and no build-time
 * validation of query derivation; Quarkus's build-time generation shifts that
 * validation earlier and avoids the runtime dispatch cost, at the cost of requiring
 * its build-time augmentation phase. Both approaches share the same query engine
 * (query derivation, JDQL parsing, pagination, CRUD) via {@code morphium-jakarta-data}
 * — only the mechanism that wires a repository interface to that engine differs.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(MorphiumRepositoryRegistrar.class)
public @interface EnableMorphiumRepositories {

    /**
     * Base packages to scan for {@code @Repository} interfaces. Equivalent to
     * {@link #basePackages()} — both arrays are merged if both are given. Defaults to
     * an empty array, in which case the package of the annotated class is scanned.
     */
    String[] value() default {};

    /**
     * Alias for {@link #value()}, provided for readability when only base packages
     * (and no other attribute) are specified.
     */
    String[] basePackages() default {};
}
