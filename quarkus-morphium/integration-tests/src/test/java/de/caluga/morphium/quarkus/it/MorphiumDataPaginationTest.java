package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Jakarta Data pagination and sorting.
 */
@QuarkusTest
@DisplayName("Jakarta Data Pagination & Sorting")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataPaginationTest {

    @Inject
    OrderRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.clearCollection(OrderEntity.class);
        for (int i = 1; i <= 25; i++) {
            var order = new OrderEntity();
            order.setCustomerId("C" + i);
            order.setAmount(i * 10.0);
            order.setStatus(i % 2 == 0 ? "OPEN" : "CLOSED");
            morphium.store(order);
        }
    }

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    @Test
    @Order(1)
    @DisplayName("findByStatus returns correct filtered results")
    void findByStatus_paginationPrep() {
        var openOrders = repository.findByStatus("OPEN");
        assertThat(openOrders).hasSize(12); // even numbers 2,4,...,24

        var closedOrders = repository.findByStatus("CLOSED");
        assertThat(closedOrders).hasSize(13); // odd numbers 1,3,...,25
    }

    @Test
    @Order(2)
    @DisplayName("findAll returns all entities as Stream")
    void findAll_total() {
        long total = repository.findAll().count();
        assertThat(total).isEqualTo(25);
    }

    @Test
    @Order(3)
    @DisplayName("countByStatus returns correct filtered count")
    void countByStatus() {
        assertThat(repository.countByStatus("OPEN")).isEqualTo(12);
        assertThat(repository.countByStatus("CLOSED")).isEqualTo(13);
    }

    @Test
    @Order(4)
    @DisplayName("findByAmountGreaterThan with boundary value")
    void findByAmountGreaterThan_boundary() {
        var result = repository.findByAmountGreaterThan(200.0);
        assertThat(result).hasSize(5); // 210, 220, 230, 240, 250
    }

    @Test
    @Order(5)
    @DisplayName("existsByStatus works for query derivation")
    void existsByStatus() {
        assertThat(repository.existsByStatus("OPEN")).isTrue();
        assertThat(repository.existsByStatus("INVALID")).isFalse();
    }
}
