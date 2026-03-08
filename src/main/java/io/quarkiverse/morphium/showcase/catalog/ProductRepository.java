package io.quarkiverse.morphium.showcase.catalog;

import de.caluga.morphium.driver.MorphiumId;
import io.quarkiverse.morphium.showcase.catalog.entity.Product;
import jakarta.data.Limit;
import jakarta.data.Order;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;
import de.caluga.morphium.quarkus.data.MorphiumRepository;
import jakarta.data.repository.Find;
import jakarta.data.repository.OrderBy;
import jakarta.data.repository.Param;
import jakarta.data.repository.Query;
import jakarta.data.repository.Repository;
import jakarta.data.repository.By;
import jakarta.data.repository.Delete;
import jakarta.data.repository.Insert;
import jakarta.data.repository.Save;

import java.util.List;

/**
 * Jakarta Data repository for {@link Product}.
 *
 * <p>Extends {@link MorphiumRepository} which adds {@code distinct()}, {@code morphium()},
 * and {@code query()} on top of standard {@code CrudRepository} CRUD operations.
 * The quarkus-morphium extension generates an implementation at build time.</p>
 *
 * <h3>Comparison: Morphium API vs Jakarta Data</h3>
 * <pre>
 * // Morphium (imperative)
 * morphium.createQueryFor(Product.class)
 *     .f("name").eq(name)
 *     .asList();
 *
 * // Jakarta Data (declarative)
 * productRepository.findByName(name);
 *
 * // MorphiumRepository escape hatch
 * List&lt;Object&gt; categories = productRepository.distinct("category.name");
 * productRepository.morphium().inc(product, "stock", 5);
 * </pre>
 */
@Repository
public interface ProductRepository extends MorphiumRepository<Product, MorphiumId> {

    // ---- Query Derivation ----

    /** {@code WHERE name = ?} — equivalent to {@code morphium.createQueryFor(Product.class).f("name").eq(name).asList()} */
    List<Product> findByName(String name);

    /** {@code WHERE price > ?} — equivalent to {@code .f("price").gt(minPrice).asList()} */
    List<Product> findByPriceGreaterThan(double minPrice);

    /** {@code WHERE price >= ? AND price <= ?} — equivalent to {@code .f("price").gte(min).f("price").lte(max).asList()} */
    List<Product> findByPriceBetween(double min, double max);

    /** {@code WHERE stock < ?} — useful for low-stock alerts */
    List<Product> findByStockLessThan(int threshold);

    /** {@code COUNT WHERE stock < ?} — returns count instead of entities */
    long countByStockLessThan(int threshold);

    /** {@code EXISTS WHERE name = ?} — returns true/false without loading the entity */
    boolean existsByName(String name);

    /** {@code COUNT *} — Jakarta Data has no built-in count() on BasicRepository/CrudRepository */
    long countByPriceGreaterThanEqual(double minPrice);

    // ---- Pagination & Sorting ----

    /** Paginated findAll with dynamic sort order via {@link Order} and {@link PageRequest}. */
    Page<Product> findAll(PageRequest pageRequest, Order<Product> order);

    /** Paginated query with filter condition. */
    Page<Product> findByPriceGreaterThan(double minPrice, PageRequest pageRequest);

    // ---- @Find / @By / @OrderBy ----

    /** Find by embedded field using dot notation — equivalent to {@code .f("category.name").eq(categoryName).asList()} */
    @Find
    List<Product> findByCategory(@By("category.name") String categoryName);

    /** Top N products in a category, sorted by price descending. */
    @Find
    @OrderBy(value = "price", descending = true)
    List<Product> topByCategory(@By("category.name") String categoryName, Limit limit);

    /** Delete by name — equivalent to {@code query.f("name").eq(name)} then delete each match. */
    @Delete
    void removeByName(@By("name") String name);

    /** Insert a new product — equivalent to {@code morphium.insert(product)} */
    @Insert
    Product addProduct(Product product);

    /** Save (upsert) — equivalent to {@code morphium.store(product)} */
    @Save
    Product upsertProduct(Product product);

    // ---- @Query / JDQL ----

    /** JDQL: search by name pattern (LIKE) — equivalent to Morphium's {@code .f("name").matches(pattern)} */
    @Query("WHERE name LIKE :pattern ORDER BY price ASC")
    List<Product> searchByNameLike(@Param("pattern") String pattern);

    /** JDQL: price range with sort — equivalent to {@code .f("price").gte(min).f("price").lte(max).sort(...)} */
    @Query("WHERE price >= :min AND price <= :max ORDER BY price ASC")
    List<Product> queryByPriceRange(@Param("min") double min, @Param("max") double max);

    /** JDQL: count with condition */
    @Query("WHERE price >= :min")
    long countByMinPrice(@Param("min") double minPrice);
}
