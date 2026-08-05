package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that {@link AbstractMorphiumRepository#doUpdate(Object)} and
 * {@link AbstractMorphiumRepository#doUpdateAll(java.util.List)} implement genuine
 * {@code CrudRepository.update()} semantics — updating an entity whose id does not yet exist
 * must fail with {@link IllegalStateException} rather than silently upserting it (which is what a
 * bare {@code Morphium.store()} call, used by {@link AbstractMorphiumRepository#doSave(Object)},
 * would do). Uses a real {@link Morphium} instance backed by {@link InMemoryDriver}, following the
 * same setup pattern as {@link QueryExecutorAliasTest}, since a fake/mocked Morphium would not
 * exercise the actual existence-check roundtrip via {@code findById()}.
 */
class AbstractMorphiumRepositoryUpdateTest {

    private static Morphium morphium;

    @Entity
    static class Product {
        @Id
        private String id;
        private String name;

        Product() {}

        Product(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }

    static class ProductRepositoryImpl extends AbstractMorphiumRepository<Product, String> {
        ProductRepositoryImpl(Morphium morphium) {
            super(new RepositoryMetadata(Product.class, String.class, "id"));
            setMorphium(morphium);
        }
    }

    private ProductRepositoryImpl repo;

    @BeforeAll
    static void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("test");
        cfg.addHostToSeed("localhost");
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
    }

    @AfterAll
    static void tearDown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    @BeforeEach
    void initRepo() {
        morphium.clearCollection(Product.class);
        repo = new ProductRepositoryImpl(morphium);
    }

    @Test
    @DisplayName("doUpdate() on an existing entity succeeds and persists the change")
    void doUpdateOnExistingEntitySucceeds() {
        Product product = new Product("p1", "Widget");
        morphium.store(product);

        product.setName("Widget v2");
        Object result = repo.doUpdate(product);

        assertThat(result).isSameAs(product);
        Product reloaded = morphium.findById(Product.class, "p1", null);
        assertThat(reloaded).isNotNull();
        assertThat(reloaded.getName()).isEqualTo("Widget v2");
    }

    @Test
    @DisplayName("doUpdate() on a non-existent id throws IllegalStateException instead of upserting")
    void doUpdateOnNonExistentEntityThrows() {
        Product ghost = new Product("does-not-exist", "Ghost");

        assertThatThrownBy(() -> repo.doUpdate(ghost))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does-not-exist");

        // Crucially: no document must have been created as a side effect of the failed update.
        Product reloaded = morphium.findById(Product.class, "does-not-exist", null);
        assertThat(reloaded).isNull();
    }

    @Test
    @DisplayName("doUpdateAll() with all-existing entities succeeds")
    void doUpdateAllOnExistingEntitiesSucceeds() {
        Product p1 = new Product("p1", "One");
        Product p2 = new Product("p2", "Two");
        morphium.store(p1);
        morphium.store(p2);

        p1.setName("One-updated");
        p2.setName("Two-updated");
        repo.doUpdateAll(List.of(p1, p2));

        assertThat(morphium.findById(Product.class, "p1", null).getName()).isEqualTo("One-updated");
        assertThat(morphium.findById(Product.class, "p2", null).getName()).isEqualTo("Two-updated");
    }

    @Test
    @DisplayName("doUpdateAll() rejects the whole batch if any entity does not exist (no partial update)")
    void doUpdateAllRejectsPartialBatchOnMissingEntity() {
        Product existing = new Product("p1", "One");
        morphium.store(existing);

        existing.setName("One-should-not-be-applied");
        Product ghost = new Product("missing", "Ghost");

        assertThatThrownBy(() -> repo.doUpdateAll(List.of(existing, ghost)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing");

        // The existing entity must remain unchanged since the batch was rejected before storing.
        Product reloaded = morphium.findById(Product.class, "p1", null);
        assertThat(reloaded.getName()).isEqualTo("One");
        assertThat(morphium.findById(Product.class, "missing", null)).isNull();
    }
}
