package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.data.Limit;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for Jakarta Data Phase 4: @Find, @By, @OrderBy,
 * @Delete, @Insert, @Save, @Update annotations.
 */
@QuarkusTest
@DisplayName("Jakarta Data Annotated Queries (@Find/@By/@OrderBy)")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataAnnotatedQueryTest {

    @Inject
    ItemRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.clearCollection(ItemEntity.class);

        createItem("Apple", 1.50, "fruit");
        createItem("Banana", 0.80, "fruit");
        createItem("Carrot", 2.00, "vegetable");
        createItem("Daikon", 3.50, "vegetable");
        createItem("Eggplant", 2.50, "vegetable");
    }

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    // -- @Find / @By tests --

    @Test
    @Order(1)
    @DisplayName("@Find @By tag returns matching entities")
    void findByTag() {
        List<ItemEntity> fruits = repository.searchByTag("fruit");

        assertThat(fruits).hasSize(2);
        assertThat(fruits).allSatisfy(i -> assertThat(i.getTag()).isEqualTo("fruit"));
    }

    @Test
    @Order(2)
    @DisplayName("@Find @By name returns single entity")
    void findOneByName() {
        ItemEntity item = repository.findOneByName("Apple");

        assertThat(item).isNotNull();
        assertThat(item.getName()).isEqualTo("Apple");
        assertThat(item.getPrice()).isEqualTo(1.50);
    }

    @Test
    @Order(3)
    @DisplayName("@Find @By name throws EmptyResultException for non-existing")
    void findOneByName_notFound() {
        assertThatThrownBy(() -> repository.findOneByName("NonExistent"))
                .isInstanceOf(jakarta.data.exceptions.EmptyResultException.class);
    }

    // -- @Find / @OrderBy tests --

    @Test
    @Order(4)
    @DisplayName("@Find @OrderBy(price) sorts ascending")
    void findByTagSortedByPriceAsc() {
        List<ItemEntity> vegs = repository.findByTagSortedByPrice("vegetable");

        assertThat(vegs).hasSize(3);
        assertThat(vegs.get(0).getName()).isEqualTo("Carrot");    // 2.00
        assertThat(vegs.get(1).getName()).isEqualTo("Eggplant");  // 2.50
        assertThat(vegs.get(2).getName()).isEqualTo("Daikon");    // 3.50
    }

    @Test
    @Order(5)
    @DisplayName("@Find @OrderBy(price, descending=true) sorts descending")
    void findByTagSortedByPriceDesc() {
        List<ItemEntity> vegs = repository.findByTagSortedByPriceDesc("vegetable");

        assertThat(vegs).hasSize(3);
        assertThat(vegs.get(0).getName()).isEqualTo("Daikon");    // 3.50
        assertThat(vegs.get(1).getName()).isEqualTo("Eggplant");  // 2.50
        assertThat(vegs.get(2).getName()).isEqualTo("Carrot");    // 2.00
    }

    // -- @Find with Limit --

    @Test
    @Order(6)
    @DisplayName("@Find with Limit restricts results")
    void findWithLimit() {
        List<ItemEntity> result = repository.findWithLimit("vegetable", Limit.of(2));

        assertThat(result).hasSize(2);
    }

    // -- @Delete tests --

    @Test
    @Order(7)
    @DisplayName("@Delete @By removes matching entities")
    void deleteByTag() {
        assertThat(repository.searchByTag("fruit")).hasSize(2);

        repository.removeByTag("fruit");

        assertThat(repository.searchByTag("fruit")).isEmpty();
        // Vegetables should be untouched
        assertThat(repository.searchByTag("vegetable")).hasSize(3);
    }

    // -- @Insert tests --

    @Test
    @Order(8)
    @DisplayName("@Insert single entity")
    void insertSingle() {
        morphium.clearCollection(ItemEntity.class);

        var item = new ItemEntity();
        item.setName("Fig");
        item.setPrice(4.00);
        item.setTag("fruit");

        ItemEntity result = repository.addItem(item);

        assertThat(result).isNotNull();
        assertThat(result.getId()).isNotNull();
        assertThat(repository.findOneByName("Fig")).isNotNull();
    }

    @Test
    @Order(9)
    @DisplayName("@Insert list of entities")
    void insertList() {
        morphium.clearCollection(ItemEntity.class);

        var a = new ItemEntity();
        a.setName("A");
        a.setPrice(1.0);

        var b = new ItemEntity();
        b.setName("B");
        b.setPrice(2.0);

        List<ItemEntity> result = repository.addItems(List.of(a, b));

        assertThat(result).hasSize(2);
        assertThat(repository.findByName("A")).hasSize(1);
        assertThat(repository.findByName("B")).hasSize(1);
    }

    // -- @Save tests --

    @Test
    @Order(10)
    @DisplayName("@Save stores entity (upsert)")
    void saveItem() {
        morphium.clearCollection(ItemEntity.class);

        var item = new ItemEntity();
        item.setName("Grape");
        item.setPrice(5.00);

        ItemEntity saved = repository.storeItem(item);

        assertThat(saved).isNotNull();
        assertThat(saved.getId()).isNotNull();
    }

    // -- @Update tests --

    @Test
    @Order(11)
    @DisplayName("@Update modifies existing entity")
    void updateItem() {
        ItemEntity existing = repository.findOneByName("Apple");
        assertThat(existing).isNotNull();

        existing.setPrice(9.99);
        repository.updateItem(existing);

        ItemEntity updated = repository.findOneByName("Apple");
        assertThat(updated.getPrice()).isEqualTo(9.99);
    }

    private void createItem(String name, double price, String tag) {
        var item = new ItemEntity();
        item.setName(name);
        item.setPrice(price);
        item.setTag(tag);
        morphium.store(item);
    }
}
