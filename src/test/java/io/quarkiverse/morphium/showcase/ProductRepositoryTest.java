package io.quarkiverse.morphium.showcase;

import de.caluga.morphium.Morphium;
import io.quarkiverse.morphium.showcase.catalog.ProductDataService;
import io.quarkiverse.morphium.showcase.catalog.ProductRepository;
import io.quarkiverse.morphium.showcase.catalog.entity.Category;
import io.quarkiverse.morphium.showcase.catalog.entity.Product;
import jakarta.data.Limit;
import jakarta.data.Sort;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ProductRepository} and {@link ProductDataService} — the Jakarta Data approach.
 * Compare with {@link ProductServiceTest} which tests the same operations via Morphium API.
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ProductRepositoryTest {

    @Inject
    ProductRepository repository;

    @Inject
    ProductDataService dataService;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.dropCollection(Product.class);
    }

    private long countProducts() {
        return morphium.createQueryFor(Product.class).countAll();
    }

    // ---- CRUD ----

    @Test
    @Order(1)
    @DisplayName("CRUD: insert and findById via Jakarta Data")
    void shouldInsertAndFindById() {
        Product product = Product.builder()
                .name("Test Laptop").description("A test laptop").price(999.99).stock(10)
                .category(new Category("Electronics", "Computers"))
                .tags(List.of("laptop"))
                .build();

        Product saved = repository.insert(product);
        assertThat(saved.getId()).isNotNull();

        var found = repository.findById(saved.getId());
        assertThat(found).isPresent();
        assertThat(found.get().getName()).isEqualTo("Test Laptop");
        assertThat(found.get().getCategory().getName()).isEqualTo("Electronics");
    }

    @Test
    @Order(2)
    @DisplayName("CRUD: save (upsert) via Jakarta Data")
    void shouldSaveAndUpdate() {
        Product product = repository.insert(Product.builder()
                .name("Widget").price(19.99).stock(100)
                .category(new Category("General", "Misc"))
                .build());

        product.setPrice(24.99);
        repository.save(product);

        var updated = repository.findById(product.getId());
        assertThat(updated).isPresent();
        assertThat(updated.get().getPrice()).isEqualTo(24.99);
    }

    @Test
    @Order(3)
    @DisplayName("CRUD: delete via Jakarta Data")
    void shouldDelete() {
        Product product = repository.insert(Product.builder()
                .name("ToDelete").price(5.0).stock(1)
                .category(new Category("Cat", "Desc"))
                .build());

        assertThat(countProducts()).isEqualTo(1);
        repository.delete(product);
        assertThat(countProducts()).isZero();
    }

    @Test
    @Order(4)
    @DisplayName("CRUD: count via Morphium (no count() on CrudRepository)")
    void shouldCount() {
        assertThat(countProducts()).isZero();
        repository.insert(Product.builder().name("P1").price(10).stock(1).category(new Category("C", "D")).build());
        repository.insert(Product.builder().name("P2").price(20).stock(2).category(new Category("C", "D")).build());
        assertThat(countProducts()).isEqualTo(2);
    }

    // ---- Query Derivation ----

    @Test
    @Order(10)
    @DisplayName("Query Derivation: findByName")
    void shouldFindByName() {
        repository.insert(Product.builder().name("Laptop").price(999).stock(10).category(new Category("E", "C")).build());
        repository.insert(Product.builder().name("Mouse").price(29).stock(200).category(new Category("E", "P")).build());

        List<Product> result = repository.findByName("Laptop");
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getName()).isEqualTo("Laptop");
    }

    @Test
    @Order(11)
    @DisplayName("Query Derivation: findByPriceGreaterThan")
    void shouldFindByPriceGreaterThan() {
        repository.insert(Product.builder().name("Cheap").price(10).stock(1).category(new Category("C", "D")).build());
        repository.insert(Product.builder().name("Mid").price(50).stock(1).category(new Category("C", "D")).build());
        repository.insert(Product.builder().name("Expensive").price(200).stock(1).category(new Category("C", "D")).build());

        List<Product> result = repository.findByPriceGreaterThan(40);
        assertThat(result).hasSize(2);
        assertThat(result).extracting(Product::getName).containsExactlyInAnyOrder("Mid", "Expensive");
    }

    @Test
    @Order(12)
    @DisplayName("Query Derivation: findByPriceBetween")
    void shouldFindByPriceBetween() {
        repository.insert(Product.builder().name("A").price(10).stock(1).category(new Category("C", "D")).build());
        repository.insert(Product.builder().name("B").price(50).stock(1).category(new Category("C", "D")).build());
        repository.insert(Product.builder().name("C").price(200).stock(1).category(new Category("C", "D")).build());

        List<Product> result = repository.findByPriceBetween(20, 100);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getName()).isEqualTo("B");
    }

    @Test
    @Order(13)
    @DisplayName("Query Derivation: existsByName")
    void shouldCheckExists() {
        repository.insert(Product.builder().name("Laptop").price(999).stock(10).category(new Category("E", "C")).build());

        assertThat(repository.existsByName("Laptop")).isTrue();
        assertThat(repository.existsByName("Nonexistent")).isFalse();
    }

    @Test
    @Order(14)
    @DisplayName("Query Derivation: countByStockLessThan")
    void shouldCountLowStock() {
        repository.insert(Product.builder().name("LowStock").price(10).stock(3).category(new Category("C", "D")).build());
        repository.insert(Product.builder().name("HighStock").price(20).stock(200).category(new Category("C", "D")).build());

        assertThat(repository.countByStockLessThan(10)).isEqualTo(1);
    }

    // ---- Pagination ----

    @Test
    @Order(20)
    @DisplayName("Pagination: findAll with PageRequest and Order")
    void shouldPaginate() {
        for (int i = 1; i <= 10; i++) {
            repository.insert(Product.builder().name("P" + i).price(i * 10.0).stock(i)
                    .category(new Category("C", "D")).build());
        }

        Page<Product> page1 = repository.findAll(
                PageRequest.ofPage(1, 3, true),
                jakarta.data.Order.by(Sort.desc("price")));

        assertThat(page1.content()).hasSize(3);
        // Sorted by price DESC: 100, 90, 80
        assertThat(page1.content().get(0).getPrice()).isEqualTo(100.0);
        assertThat(page1.content().get(1).getPrice()).isEqualTo(90.0);
        assertThat(page1.content().get(2).getPrice()).isEqualTo(80.0);
    }

    // ---- @Find / @By ----

    @Test
    @Order(30)
    @DisplayName("@Find/@By: findByCategory (embedded field)")
    void shouldFindByCategory() {
        repository.insert(Product.builder().name("Laptop").price(999).stock(10)
                .category(new Category("Electronics", "Computers")).build());
        repository.insert(Product.builder().name("Desk").price(449).stock(30)
                .category(new Category("Furniture", "Office")).build());

        List<Product> electronics = repository.findByCategory("Electronics");
        assertThat(electronics).hasSize(1);
        assertThat(electronics.get(0).getName()).isEqualTo("Laptop");
    }

    @Test
    @Order(31)
    @DisplayName("@Find/@OrderBy: topByCategory with Limit")
    void shouldFindTopByCategory() {
        repository.insert(Product.builder().name("Mouse").price(29.99).stock(200)
                .category(new Category("Electronics", "Peripherals")).build());
        repository.insert(Product.builder().name("Keyboard").price(89.99).stock(120)
                .category(new Category("Electronics", "Peripherals")).build());
        repository.insert(Product.builder().name("Monitor").price(399.99).stock(75)
                .category(new Category("Electronics", "Displays")).build());

        List<Product> top2 = repository.topByCategory("Electronics", Limit.of(2));
        assertThat(top2).hasSize(2);
        // Sorted by price DESC
        assertThat(top2.get(0).getPrice()).isEqualTo(399.99);
        assertThat(top2.get(1).getPrice()).isEqualTo(89.99);
    }

    @Test
    @Order(32)
    @DisplayName("@Delete: removeByName")
    void shouldRemoveByName() {
        repository.insert(Product.builder().name("ToRemove").price(10).stock(1)
                .category(new Category("C", "D")).build());
        repository.insert(Product.builder().name("ToKeep").price(20).stock(1)
                .category(new Category("C", "D")).build());

        repository.removeByName("ToRemove");

        assertThat(countProducts()).isEqualTo(1);
        assertThat(repository.findByName("ToKeep")).hasSize(1);
    }

    // ---- JDQL ----

    @Test
    @Order(40)
    @DisplayName("JDQL: searchByNameLike (WHERE name LIKE pattern)")
    void shouldSearchByNameLike() {
        repository.insert(Product.builder().name("Laptop Pro").price(999).stock(10)
                .category(new Category("E", "C")).build());
        repository.insert(Product.builder().name("Mouse Pad").price(9).stock(50)
                .category(new Category("E", "P")).build());

        List<Product> result = repository.searchByNameLike("%Laptop%");
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getName()).isEqualTo("Laptop Pro");
    }

    @Test
    @Order(41)
    @DisplayName("JDQL: queryByPriceRange (WHERE price BETWEEN)")
    void shouldQueryByPriceRange() {
        repository.insert(Product.builder().name("A").price(10).stock(1).category(new Category("C", "D")).build());
        repository.insert(Product.builder().name("B").price(50).stock(1).category(new Category("C", "D")).build());
        repository.insert(Product.builder().name("C").price(200).stock(1).category(new Category("C", "D")).build());

        List<Product> result = repository.queryByPriceRange(20, 100);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getName()).isEqualTo("B");
    }

    @Test
    @Order(42)
    @DisplayName("JDQL: countByMinPrice")
    void shouldCountByMinPrice() {
        repository.insert(Product.builder().name("A").price(10).stock(1).category(new Category("C", "D")).build());
        repository.insert(Product.builder().name("B").price(50).stock(1).category(new Category("C", "D")).build());
        repository.insert(Product.builder().name("C").price(200).stock(1).category(new Category("C", "D")).build());

        assertThat(repository.countByMinPrice(40)).isEqualTo(2); // B + C
    }

    // ---- Metamodel ----

    @Test
    @Order(50)
    @DisplayName("Metamodel: Product_ class exists and has correct attributes")
    void shouldHaveMetamodel() throws Exception {
        Class<?> metamodel = Class.forName("io.quarkiverse.morphium.showcase.catalog.entity.Product_");
        assertThat(metamodel).isNotNull();

        var annotation = metamodel.getAnnotation(jakarta.data.metamodel.StaticMetamodel.class);
        assertThat(annotation).isNotNull();
        assertThat(annotation.value()).isEqualTo(Product.class);
    }

    // ---- ProductDataService comparison ----

    @Test
    @Order(60)
    @DisplayName("DataService: seedData and findAll via Jakarta Data")
    void shouldSeedAndFindAll() {
        dataService.seedData();
        List<Product> all = dataService.findAll();
        assertThat(all).hasSize(8);
    }

    @Test
    @Order(61)
    @DisplayName("DataService: findByPriceRange via Jakarta Data")
    void shouldFindByPriceRangeViaDataService() {
        dataService.seedData();
        List<Product> result = dataService.findByPriceRange(50, 100);
        assertThat(result).isNotEmpty();
        assertThat(result).allSatisfy(p -> {
            assertThat(p.getPrice()).isBetween(50.0, 100.0);
        });
    }

    @Test
    @Order(62)
    @DisplayName("DataService: searchByName (LIKE) via Jakarta Data")
    void shouldSearchByNameViaDataService() {
        dataService.seedData();
        List<Product> result = dataService.searchByName("Laptop");
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getName()).contains("Laptop");
    }
}
