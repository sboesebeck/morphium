package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Jakarta Data CRUD operations via {@link ItemRepository}.
 */
@QuarkusTest
@DisplayName("Jakarta Data CRUD operations")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataCrudTest {

    @Inject
    ItemRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void cleanCollection() {
        morphium.clearCollection(ItemEntity.class);
    }

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    @Test
    @Order(1)
    @DisplayName("repository is injectable")
    void repository_isInjectable() {
        assertThat(repository).isNotNull();
    }

    @Test
    @Order(2)
    @DisplayName("save() persists entity and assigns id")
    void save_persistsEntity() {
        var item = new ItemEntity();
        item.setName("Widget");
        item.setPrice(9.99);

        ItemEntity saved = repository.save(item);

        assertThat(saved).isNotNull();
        assertThat(saved.getId()).isNotNull().isNotBlank();
    }

    @Test
    @Order(3)
    @DisplayName("findById() returns Optional with saved entity")
    void findById_returnsEntity() {
        var item = new ItemEntity();
        item.setName("Gadget");
        item.setPrice(19.99);
        repository.save(item);

        Optional<ItemEntity> found = repository.findById(item.getId());

        assertThat(found).isPresent();
        assertThat(found.get().getName()).isEqualTo("Gadget");
        assertThat(found.get().getPrice()).isEqualTo(19.99);
    }

    @Test
    @Order(4)
    @DisplayName("findById() returns empty Optional for non-existing id")
    void findById_returnsEmpty() {
        Optional<ItemEntity> found = repository.findById("non-existing-id");
        assertThat(found).isEmpty();
    }

    @Test
    @Order(5)
    @DisplayName("delete() removes entity")
    void delete_removesEntity() {
        var item = new ItemEntity();
        item.setName("ToDelete");
        repository.save(item);
        String id = item.getId();

        repository.delete(item);

        assertThat(repository.findById(id)).isEmpty();
    }

    @Test
    @Order(6)
    @DisplayName("deleteById() removes entity by id")
    void deleteById_removesEntity() {
        var item = new ItemEntity();
        item.setName("ToDeleteById");
        repository.save(item);
        String id = item.getId();

        repository.deleteById(id);

        assertThat(repository.findById(id)).isEmpty();
    }

    @Test
    @Order(7)
    @DisplayName("insert() creates new entity")
    void insert_createsEntity() {
        var item = new ItemEntity();
        item.setName("Inserted");
        item.setPrice(5.0);

        ItemEntity inserted = repository.insert(item);

        assertThat(inserted.getId()).isNotNull();
        assertThat(repository.findById(inserted.getId())).isPresent();
    }

    @Test
    @Order(8)
    @DisplayName("insertAll() creates multiple entities")
    void insertAll_createsEntities() {
        var a = new ItemEntity();
        a.setName("Batch-A");
        var b = new ItemEntity();
        b.setName("Batch-B");

        List<ItemEntity> inserted = repository.insertAll(List.of(a, b));

        assertThat(inserted).hasSize(2);
    }

    @Test
    @Order(9)
    @DisplayName("findAll() returns all entities as Stream")
    void findAll_returnsAll() {
        var item1 = new ItemEntity();
        item1.setName("One");
        var item2 = new ItemEntity();
        item2.setName("Two");
        repository.save(item1);
        repository.save(item2);

        List<ItemEntity> all = repository.findAll().collect(Collectors.toList());

        assertThat(all).hasSize(2);
    }

    @Test
    @Order(10)
    @DisplayName("update() stores changes")
    void update_storesChanges() {
        var item = new ItemEntity();
        item.setName("Original");
        item.setPrice(10.0);
        repository.save(item);

        item.setName("Updated");
        repository.update(item);

        Optional<ItemEntity> found = repository.findById(item.getId());
        assertThat(found).isPresent();
        assertThat(found.get().getName()).isEqualTo("Updated");
    }

    @Test
    @Order(11)
    @DisplayName("saveAll() persists multiple entities")
    void saveAll_persistsAll() {
        var a = new ItemEntity();
        a.setName("SaveAll-A");
        var b = new ItemEntity();
        b.setName("SaveAll-B");

        List<ItemEntity> saved = repository.saveAll(List.of(a, b));

        assertThat(saved).hasSize(2);
        assertThat(saved).allSatisfy(item ->
            assertThat(item.getId()).isNotNull());
    }

    @Test
    @Order(12)
    @DisplayName("deleteAll() removes multiple entities")
    void deleteAll_removesAll() {
        var a = new ItemEntity();
        a.setName("DelAll-A");
        var b = new ItemEntity();
        b.setName("DelAll-B");
        repository.saveAll(List.of(a, b));

        repository.deleteAll(List.of(a, b));

        assertThat(repository.findAll().count()).isZero();
    }
}
