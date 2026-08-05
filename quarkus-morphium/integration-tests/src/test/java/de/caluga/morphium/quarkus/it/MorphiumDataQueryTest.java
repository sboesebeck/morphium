package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Jakarta Data query derivation via {@link OrderRepository}.
 */
@QuarkusTest
@DisplayName("Jakarta Data Query Derivation")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataQueryTest {

    @Inject
    OrderRepository repository;

    @Inject
    ItemRepository itemRepository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.clearCollection(OrderEntity.class);

        var o1 = new OrderEntity();
        o1.setCustomerId("C1");
        o1.setAmount(100.0);
        o1.setStatus("OPEN");

        var o2 = new OrderEntity();
        o2.setCustomerId("C2");
        o2.setAmount(250.0);
        o2.setStatus("OPEN");

        var o3 = new OrderEntity();
        o3.setCustomerId("C3");
        o3.setAmount(50.0);
        o3.setStatus("CLOSED");

        morphium.store(o1);
        morphium.store(o2);
        morphium.store(o3);
    }

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    @Test
    @Order(1)
    @DisplayName("findByStatus returns matching entities")
    void findByStatus() {
        List<OrderEntity> open = repository.findByStatus("OPEN");

        assertThat(open).hasSize(2);
        assertThat(open).allSatisfy(o -> assertThat(o.getStatus()).isEqualTo("OPEN"));
    }

    @Test
    @Order(2)
    @DisplayName("findByAmountGreaterThan filters correctly")
    void findByAmountGreaterThan() {
        List<OrderEntity> result = repository.findByAmountGreaterThan(100.0);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getAmount()).isEqualTo(250.0);
    }

    @Test
    @Order(3)
    @DisplayName("findByAmountGreaterThanEqual includes boundary")
    void findByAmountGreaterThanEqual() {
        List<OrderEntity> result = repository.findByAmountGreaterThanEqual(100.0);

        assertThat(result).hasSize(2);
    }

    @Test
    @Order(4)
    @DisplayName("findByAmountLessThan filters correctly")
    void findByAmountLessThan() {
        List<OrderEntity> result = repository.findByAmountLessThan(100.0);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getAmount()).isEqualTo(50.0);
    }

    @Test
    @Order(5)
    @DisplayName("findByStatusAndAmountGreaterThan combines conditions")
    void findByStatusAndAmount() {
        List<OrderEntity> result = repository.findByStatusAndAmountGreaterThan("OPEN", 150.0);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getAmount()).isEqualTo(250.0);
    }

    @Test
    @Order(6)
    @DisplayName("countByStatus returns correct count")
    void countByStatus() {
        long count = repository.countByStatus("OPEN");

        assertThat(count).isEqualTo(2);
    }

    @Test
    @Order(7)
    @DisplayName("existsByStatus returns true for existing")
    void existsByStatus_true() {
        assertThat(repository.existsByStatus("OPEN")).isTrue();
    }

    @Test
    @Order(8)
    @DisplayName("existsByStatus returns false for non-existing")
    void existsByStatus_false() {
        assertThat(repository.existsByStatus("CANCELLED")).isFalse();
    }

    @Test
    @Order(9)
    @DisplayName("findByName on ItemRepository with custom queries")
    void findByName_onItemRepository() {
        morphium.clearCollection(ItemEntity.class);

        var item = new ItemEntity();
        item.setName("TestItem");
        item.setPrice(42.0);
        itemRepository.save(item);

        List<ItemEntity> found = itemRepository.findByName("TestItem");

        assertThat(found).hasSize(1);
        assertThat(found.get(0).getPrice()).isEqualTo(42.0);
    }

    @Test
    @Order(10)
    @DisplayName("findByPriceGreaterThan on ItemRepository")
    void findByPriceGreaterThan() {
        morphium.clearCollection(ItemEntity.class);

        var cheap = new ItemEntity();
        cheap.setName("Cheap");
        cheap.setPrice(5.0);

        var expensive = new ItemEntity();
        expensive.setName("Expensive");
        expensive.setPrice(100.0);

        itemRepository.save(cheap);
        itemRepository.save(expensive);

        List<ItemEntity> result = itemRepository.findByPriceGreaterThan(50.0);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getName()).isEqualTo("Expensive");
    }
}
