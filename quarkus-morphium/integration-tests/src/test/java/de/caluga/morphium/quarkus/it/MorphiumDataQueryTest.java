package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.data.Limit;
import jakarta.data.Sort;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;
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

    // -- Regression: dynamic Sort/Limit/PageRequest parameters on a derived findBy* method --

    @Test
    @Order(11)
    @DisplayName("findByStatus(Sort): dynamic Sort parameter is applied, not silently ignored")
    void findByStatusSorted() {
        List<OrderEntity> ascending = repository.findByStatus("OPEN", Sort.asc("amount"));
        assertThat(ascending).extracting(OrderEntity::getAmount).containsExactly(100.0, 250.0);

        List<OrderEntity> descending = repository.findByStatus("OPEN", Sort.desc("amount"));
        assertThat(descending).extracting(OrderEntity::getAmount).containsExactly(250.0, 100.0);
    }

    @Test
    @Order(12)
    @DisplayName("findByStatus(Limit): dynamic Limit parameter is applied, not silently ignored")
    void findByStatusLimited() {
        List<OrderEntity> limited = repository.findByStatus("OPEN", Limit.of(1));
        assertThat(limited).hasSize(1);
    }

    @Test
    @Order(13)
    @DisplayName("findByStatus(PageRequest): dynamic PageRequest parameter returns a Page, not a ClassCastException")
    void findByStatusPaged() {
        Page<OrderEntity> page = repository.findByStatus("OPEN", PageRequest.ofSize(1));
        assertThat(page.content()).hasSize(1);
        assertThat(page.totalElements()).isEqualTo(2);
    }

    // -- Regression: dynamic Sort parameter on deleteBy*/countBy*/existsBy* --
    //
    // Before the fix, QueryMethodBridge#executeQuery(..., sortParamIndex, ...) always fell
    // through to the FIND branch once any dynamic parameter was present, regardless of the
    // method's actual prefix. For deleteByStatus(Sort) that meant the query was built and
    // sorted/asList()'d but never deleted -- the collection was left untouched while a
    // "successful" long was still returned. For countByStatus(Sort)/existsByStatus(Sort) the
    // FIND branch returned a List where the generated bytecode expected a Long/boolean,
    // throwing a ClassCastException. These tests assert the real, observable effect (actual
    // document count in the database, not just the return value) so a regression back to the
    // old behaviour would be caught.

    @Test
    @Order(14)
    @DisplayName("deleteByStatus(Sort): dynamic Sort parameter is accepted and the matching documents are actually deleted")
    void deleteByStatusSorted() {
        long before = morphium.createQueryFor(OrderEntity.class).countAll();
        assertThat(before).isEqualTo(3);

        long deleted = repository.deleteByStatus("OPEN", Sort.asc("amount"));

        assertThat(deleted).isEqualTo(2);

        // The real effect: the OPEN documents must actually be gone from the database, not just
        // a plausible-looking return value while the collection stayed untouched.
        long after = morphium.createQueryFor(OrderEntity.class).countAll();
        assertThat(after).isEqualTo(1);
        assertThat(repository.findByStatus("OPEN")).isEmpty();
        assertThat(repository.findByStatus("CLOSED")).hasSize(1);
    }

    @Test
    @Order(15)
    @DisplayName("countByStatus(Sort): dynamic Sort parameter is accepted and the correct count (a long, not a List) is returned")
    void countByStatusSorted() {
        long count = repository.countByStatus("OPEN", Sort.asc("amount"));

        assertThat(count).isEqualTo(2);
    }

    @Test
    @Order(16)
    @DisplayName("existsByStatus(Sort): dynamic Sort parameter is accepted and the correct boolean (not a List) is returned")
    void existsByStatusSorted() {
        assertThat(repository.existsByStatus("OPEN", Sort.asc("amount"))).isTrue();
        assertThat(repository.existsByStatus("CANCELLED", Sort.asc("amount"))).isFalse();
    }
}
