package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Jakarta Data #8: JDQL Aggregate Functions.
 * Tests COUNT, SUM, AVG, MIN, MAX with global aggregation (_id: null).
 */
@QuarkusTest
@DisplayName("Jakarta Data JDQL Aggregate Functions")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataAggregateTest {

    @Inject
    OrderRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.clearCollection(OrderEntity.class);

        // 5 OPEN orders: 100, 200, 300, 400, 500
        for (int i = 1; i <= 5; i++) {
            createOrder("C" + i, i * 100.0, "OPEN");
        }
        // 5 CLOSED orders: 600, 700, 800, 900, 1000
        for (int i = 6; i <= 10; i++) {
            createOrder("C" + i, i * 100.0, "CLOSED");
        }
    }

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    @Test
    @Order(1)
    @DisplayName("COUNT(this) WHERE status = 'OPEN'")
    void count_byStatus() {
        long count = repository.countByStatusJdql("OPEN");
        assertThat(count).isEqualTo(5L);
    }

    @Test
    @Order(2)
    @DisplayName("SUM(amount) WHERE status = 'OPEN'")
    void sum_byStatus() {
        double sum = repository.sumAmountByStatus("OPEN");
        assertThat(sum).isEqualTo(1500.0);
    }

    @Test
    @Order(3)
    @DisplayName("AVG(amount) WHERE status = 'OPEN'")
    void avg_byStatus() {
        double avg = repository.avgAmountByStatus("OPEN");
        assertThat(avg).isEqualTo(300.0);
    }

    @Test
    @Order(4)
    @DisplayName("MIN(amount) WHERE status = 'OPEN'")
    void min_byStatus() {
        double min = repository.minAmountByStatus("OPEN");
        assertThat(min).isEqualTo(100.0);
    }

    @Test
    @Order(5)
    @DisplayName("MAX(amount) WHERE status = 'OPEN'")
    void max_byStatus() {
        double max = repository.maxAmountByStatus("OPEN");
        assertThat(max).isEqualTo(500.0);
    }

    @Test
    @Order(6)
    @DisplayName("COUNT(this) WHERE amount > 500")
    void count_withFilter() {
        long count = repository.countByAmountGreaterThan(500.0);
        assertThat(count).isEqualTo(5L); // 600, 700, 800, 900, 1000
    }

    @Test
    @Order(7)
    @DisplayName("COUNT(this) WHERE status = 'NONEXISTENT' → 0")
    void count_noResults() {
        long count = repository.countByStatusJdql("NONEXISTENT");
        assertThat(count).isEqualTo(0L);
    }

    @Test
    @Order(8)
    @DisplayName("SUM(amount) WHERE status = 'NONEXISTENT' → 0.0")
    void sum_noResults() {
        double sum = repository.sumAmountByStatus("NONEXISTENT");
        assertThat(sum).isEqualTo(0.0);
    }

    private void createOrder(String customerId, double amount, String status) {
        var order = new OrderEntity();
        order.setCustomerId(customerId);
        order.setAmount(amount);
        order.setStatus(status);
        morphium.store(order);
    }
}
