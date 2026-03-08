package io.quarkiverse.morphium.showcase.catalog;

import de.caluga.morphium.Morphium;
import io.quarkiverse.morphium.showcase.catalog.entity.Product;
import io.quarkiverse.morphium.showcase.catalog.entity.Category;
import de.caluga.morphium.driver.MorphiumId;
import jakarta.data.Limit;
import jakarta.data.Order;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Optional;

/**
 * Service layer using Jakarta Data's {@link ProductRepository} instead of the Morphium API directly.
 *
 * <p>This is the Jakarta Data counterpart to {@link ProductService}. Both services provide the
 * same functionality, but this one uses declarative repository methods instead of imperative
 * Morphium calls. Compare the two to see the difference in coding style.</p>
 *
 * <h3>Side-by-side comparison</h3>
 * <table>
 *   <tr><th>Operation</th><th>ProductService (Morphium)</th><th>ProductDataService (Jakarta Data)</th></tr>
 *   <tr><td>Find all</td><td>{@code morphium.createQueryFor(Product.class).asList()}</td><td>{@code repository.findAll().toList()}</td></tr>
 *   <tr><td>Find by ID</td><td>{@code morphium.createQueryFor(...).f("id").eq(id).get()}</td><td>{@code repository.findById(id)}</td></tr>
 *   <tr><td>Save</td><td>{@code morphium.store(product)}</td><td>{@code repository.save(product)}</td></tr>
 *   <tr><td>Delete</td><td>{@code morphium.delete(product)}</td><td>{@code repository.delete(product)}</td></tr>
 *   <tr><td>Count</td><td>{@code morphium.createQueryFor(...).countAll()}</td><td>{@code repository.count()}</td></tr>
 *   <tr><td>Price range</td><td>{@code .f("price").gte(min).f("price").lte(max).sort(...).asList()}</td><td>{@code repository.findByPriceBetween(min, max)}</td></tr>
 * </table>
 */
@ApplicationScoped
public class ProductDataService {

    @Inject
    ProductRepository repository;

    @Inject
    Morphium morphium;

    // ---- CRUD ----

    public List<Product> findAll() {
        // Morphium: morphium.createQueryFor(Product.class).asList()
        // Jakarta Data: one method call
        return repository.findAll().toList();
    }

    public Optional<Product> findById(String id) {
        // Morphium: morphium.createQueryFor(Product.class).f("id").eq(new MorphiumId(id)).get()
        // Jakarta Data: type-safe, null-safe
        return repository.findById(new MorphiumId(id));
    }

    public Product save(Product product) {
        // Morphium: morphium.store(product); return product;
        // Jakarta Data: same effect, declarative
        return repository.save(product);
    }

    public Product create(String name, String description, double price, int stock,
                          String categoryName, String categoryDesc, List<String> tags) {
        Product product = Product.builder()
                .name(name)
                .description(description)
                .price(price)
                .stock(stock)
                .category(new Category(categoryName, categoryDesc))
                .tags(tags)
                .build();
        // Morphium: morphium.store(product)
        // Jakarta Data: repository.insert() for explicit insert (fails if exists)
        return repository.insert(product);
    }

    public void delete(String id) {
        repository.findById(new MorphiumId(id)).ifPresent(repository::delete);
    }

    public void deleteAll() {
        // Note: Jakarta Data's CrudRepository does not provide dropCollection().
        // For that, the direct Morphium API is still needed.
        // Here we delete all entities one by one (less efficient than dropCollection).
        repository.findAll().forEach(repository::delete);
    }

    public long count() {
        // Note: Jakarta Data's BasicRepository/CrudRepository does not include a built-in count().
        // For a simple total count, the Morphium API is the most direct approach.
        return morphium.createQueryFor(Product.class).countAll();
    }

    // ---- Query Derivation ----

    public List<Product> findByName(String name) {
        // Morphium: morphium.createQueryFor(Product.class).f("name").eq(name).asList()
        return repository.findByName(name);
    }

    public List<Product> findByPriceRange(double min, double max) {
        // Morphium: .f("price").gte(min).f("price").lte(max).sort(Map.of("price", 1)).asList()
        return repository.findByPriceBetween(min, max);
    }

    public List<Product> findByPriceGreaterThan(double minPrice) {
        return repository.findByPriceGreaterThan(minPrice);
    }

    public List<Product> findLowStock(int threshold) {
        return repository.findByStockLessThan(threshold);
    }

    public boolean existsByName(String name) {
        // Morphium: morphium.createQueryFor(Product.class).f("name").eq(name).countAll() > 0
        return repository.existsByName(name);
    }

    // ---- Pagination ----

    public Page<Product> findPage(int page, int size, Order<Product> order) {
        // Morphium: query.sort(...).skip((page-1)*size).limit(size).asList() + separate countAll()
        // Jakarta Data: one call, returns Page with totalElements, totalPages, hasNext etc.
        return repository.findAll(PageRequest.ofPage(page, size, true), order);
    }

    // ---- @Find / @By ----

    public List<Product> findByCategory(String categoryName) {
        // Morphium: morphium.createQueryFor(Product.class).f("category.name").eq(categoryName).asList()
        return repository.findByCategory(categoryName);
    }

    public List<Product> topByCategory(String categoryName, int limit) {
        return repository.topByCategory(categoryName, Limit.of(limit));
    }

    // ---- JDQL ----

    public List<Product> searchByName(String pattern) {
        // Morphium: .f("name").matches("(?i)" + pattern).asList()
        // JDQL: WHERE name LIKE :pattern (uses % wildcards instead of regex)
        return repository.searchByNameLike("%" + pattern + "%");
    }

    public List<Product> queryByPriceRange(double min, double max) {
        return repository.queryByPriceRange(min, max);
    }

    // ---- Seed Data ----

    public void seedData() {
        if (count() > 0) return;

        List<Product> products = List.of(
                Product.builder().name("Laptop Pro 15").description("High-performance laptop").price(1299.99).stock(50)
                        .category(new Category("Electronics", "Computers & Accessories")).tags(List.of("laptop", "pro", "portable")).build(),
                Product.builder().name("Wireless Mouse").description("Ergonomic wireless mouse").price(29.99).stock(200)
                        .category(new Category("Electronics", "Peripherals")).tags(List.of("mouse", "wireless", "ergonomic")).build(),
                Product.builder().name("Standing Desk").description("Adjustable height standing desk").price(449.00).stock(30)
                        .category(new Category("Furniture", "Office")).tags(List.of("desk", "standing", "ergonomic")).build(),
                Product.builder().name("Mechanical Keyboard").description("Cherry MX Blue switches").price(89.99).stock(120)
                        .category(new Category("Electronics", "Peripherals")).tags(List.of("keyboard", "mechanical", "gaming")).build(),
                Product.builder().name("Monitor 27\"").description("4K IPS display").price(399.99).stock(75)
                        .category(new Category("Electronics", "Displays")).tags(List.of("monitor", "4k", "ips")).build(),
                Product.builder().name("USB-C Hub").description("7-in-1 USB-C adapter").price(49.99).stock(300)
                        .category(new Category("Electronics", "Accessories")).tags(List.of("usb-c", "hub", "adapter")).build(),
                Product.builder().name("Office Chair").description("Mesh back ergonomic chair").price(299.00).stock(45)
                        .category(new Category("Furniture", "Office")).tags(List.of("chair", "ergonomic", "office")).build(),
                Product.builder().name("Webcam HD").description("1080p webcam with mic").price(59.99).stock(150)
                        .category(new Category("Electronics", "Accessories")).tags(List.of("webcam", "hd", "streaming")).build()
        );
        // Morphium: morphium.storeList(products)
        // Jakarta Data: repository.insertAll() for bulk insert
        repository.insertAll(products);
    }
}
