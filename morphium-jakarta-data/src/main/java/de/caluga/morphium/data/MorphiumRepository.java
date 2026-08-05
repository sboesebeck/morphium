package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;
import jakarta.data.repository.CrudRepository;

import java.util.List;

/**
 * Morphium-specific extension of Jakarta Data's {@link CrudRepository}.
 *
 * <p>Provides access to Morphium features that have no equivalent in the Jakarta Data 1.0
 * specification, such as {@code distinct()} queries and direct access to the {@link Morphium}
 * instance for aggregation pipelines, atomic field operations, and other advanced features.</p>
 *
 * <p>All standard Jakarta Data features (query derivation, {@code @Find}, {@code @Query}/JDQL,
 * pagination, sorting) work exactly as with {@code CrudRepository}. Additionally, all Morphium
 * ORM annotations ({@code @Version}, {@code @CreationTime}, {@code @PreStore}, {@code @Cache},
 * {@code @Reference}) work transparently because the generated implementation delegates to the
 * Morphium API.</p>
 *
 * <p>An interface extending {@code MorphiumRepository} is implemented at build time by a
 * generated subclass of {@link AbstractMorphiumRepository}. Each interface method is routed to
 * one of the runtime bridges depending on how it is declared: method-name-derived queries go
 * through {@link MethodNameParser} / {@link QueryExecutor} (via {@link QueryMethodBridge}),
 * {@code @Find} methods through {@link FindMethodBridge}, and {@code @Query}/JDQL methods through
 * {@link JdqlParser} / {@link JdqlMethodBridge}. {@link #distinct(String)}, {@link #morphium()},
 * and {@link #query()} below bypass all of that and call directly into
 * {@link AbstractMorphiumRepository}.</p>
 *
 * <h2>Usage</h2>
 * <pre>
 * {@code @Repository}
 * public interface ProductRepository extends MorphiumRepository&lt;Product, MorphiumId&gt; {
 *
 *     List&lt;Product&gt; findByCategory(String category);
 * }
 *
 * // In your service:
 * List&lt;Object&gt; categories = productRepository.distinct("category");
 *
 * // Escape hatch for aggregations, inc/push/pull etc.:
 * Morphium m = productRepository.morphium();
 * m.createAggregator(Product.class, Map.class)
 *     .group("$category").sum("total", "$price").end()
 *     .aggregate();
 * </pre>
 *
 * @param <T> the entity type
 * @param <K> the primary-key type
 */
public interface MorphiumRepository<T, K> extends CrudRepository<T, K> {

    /**
     * Returns distinct values for the given field across all documents of this entity type.
     *
     * <p>This has no equivalent in Jakarta Data 1.0. It maps to
     * {@code morphium.createQueryFor(entityClass).distinct(fieldName)}.</p>
     *
     * @param fieldName the Java field name (resolved to MongoDB field name via {@code @Property})
     * @return distinct values for the field
     */
    List<Object> distinct(String fieldName);

    /**
     * Returns the underlying {@link Morphium} instance for operations that have no
     * Jakarta Data equivalent: aggregation pipelines, atomic updates ({@code inc},
     * {@code push}, {@code pull}, {@code set}), change streams, messaging, etc.
     *
     * @return the Morphium instance
     */
    Morphium morphium();

    /**
     * Creates a Morphium {@link Query} for the entity type of this repository.
     *
     * <p>Convenience shortcut for {@code morphium().createQueryFor(entityClass)}.</p>
     *
     * @return a new query instance
     */
    Query<T> query();
}
