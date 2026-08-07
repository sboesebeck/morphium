package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Jakarta Data #8v3: multi-field GROUP BY + GAP-A2 HAVING.
 *
 * Test data: 8 orders across 2 statuses x 3 customers:
 * - OPEN, C1: 100, 200 (count=2, sum=300)
 * - OPEN, C2: 300 (count=1, sum=300)
 * - OPEN, C3: 400, 500 (count=2, sum=900)
 * - CLOSED, C1: 600 (count=1, sum=600)
 * - CLOSED, C2: 700, 800 (count=2, sum=1500)
 * Totals: OPEN=5/1500.0, CLOSED=3/2100.0
 */
@QuarkusTest
@DisplayName("Jakarta Data JDQL GROUP BY v3 + HAVING")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataGroupByV3Test {

    @Inject
    OrderRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.clearCollection(OrderEntity.class);

        // OPEN, C1: 100, 200
        createOrder("C1", 100.0, "OPEN");
        createOrder("C1", 200.0, "OPEN");
        // OPEN, C2: 300
        createOrder("C2", 300.0, "OPEN");
        // OPEN, C3: 400, 500
        createOrder("C3", 400.0, "OPEN");
        createOrder("C3", 500.0, "OPEN");
        // CLOSED, C1: 600
        createOrder("C1", 600.0, "CLOSED");
        // CLOSED, C2: 700, 800
        createOrder("C2", 700.0, "CLOSED");
        createOrder("C2", 800.0, "CLOSED");
    }

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    // --- Multi-field GROUP BY ---

    @Test
    @Order(1)
    @DisplayName("Multi-field GROUP BY → 5 groups")
    void multiGroupBy_count() {
        List<StatusCustomerCount> results = repository.countByStatusAndCustomer();
        assertThat(results).hasSize(5);

        Map<String, Long> map = results.stream()
                .collect(Collectors.toMap(
                        r -> r.status() + ":" + r.customerId(),
                        StatusCustomerCount::count));
        assertThat(map).containsEntry("OPEN:C1", 2L);
        assertThat(map).containsEntry("OPEN:C2", 1L);
        assertThat(map).containsEntry("OPEN:C3", 2L);
        assertThat(map).containsEntry("CLOSED:C1", 1L);
        assertThat(map).containsEntry("CLOSED:C2", 2L);
    }

    @Test
    @Order(2)
    @DisplayName("Multi-field GROUP BY sorted by status ASC, customerId ASC")
    void multiGroupBy_sorted() {
        List<StatusCustomerCount> results = repository.countByStatusAndCustomerSorted();
        assertThat(results).hasSize(5);
        // CLOSED:C1, CLOSED:C2, OPEN:C1, OPEN:C2, OPEN:C3
        assertThat(results.get(0).status()).isEqualTo("CLOSED");
        assertThat(results.get(0).customerId()).isEqualTo("C1");
        assertThat(results.get(results.size() - 1).status()).isEqualTo("OPEN");
        assertThat(results.get(results.size() - 1).customerId()).isEqualTo("C3");
    }

    @Test
    @Order(3)
    @DisplayName("Multi-field GROUP BY with WHERE filter")
    void multiGroupBy_filtered() {
        // amount > 500: only CLOSED orders (600, 700, 800)
        List<StatusCustomerCount> results = repository.countByStatusAndCustomerFiltered(500.0);
        assertThat(results).hasSize(2);

        Map<String, Long> map = results.stream()
                .collect(Collectors.toMap(
                        r -> r.status() + ":" + r.customerId(),
                        StatusCustomerCount::count));
        assertThat(map).containsEntry("CLOSED:C1", 1L);
        assertThat(map).containsEntry("CLOSED:C2", 2L);
    }

    @Test
    @Order(4)
    @DisplayName("Single-field GROUP BY still works (regression)")
    void singleGroupBy_stillWorks() {
        List<StatusCount> results = repository.countGroupByStatus();
        assertThat(results).hasSize(2);

        Map<String, Long> map = results.stream()
                .collect(Collectors.toMap(StatusCount::status, StatusCount::count));
        assertThat(map).containsEntry("OPEN", 5L);
        assertThat(map).containsEntry("CLOSED", 3L);
    }

    // --- HAVING ---

    @Test
    @Order(5)
    @DisplayName("HAVING COUNT(this) > :minCount → filters groups")
    void having_countGreaterThan() {
        // minCount=3: only OPEN (5) passes, CLOSED (3) fails
        List<StatusCount> results = repository.statusesWithMinCount(3L);
        assertThat(results).hasSize(1);
        assertThat(results.get(0).status()).isEqualTo("OPEN");
        assertThat(results.get(0).count()).isEqualTo(5L);
    }

    @Test
    @Order(6)
    @DisplayName("HAVING COUNT(this) > 0 → all groups pass")
    void having_countAll() {
        List<StatusCount> results = repository.statusesWithMinCount(0L);
        assertThat(results).hasSize(2);
    }

    @Test
    @Order(7)
    @DisplayName("HAVING COUNT(this) > 10 → no groups pass → empty list")
    void having_countNone() {
        List<StatusCount> results = repository.statusesWithMinCount(10L);
        assertThat(results).isEmpty();
    }

    @Test
    @Order(8)
    @DisplayName("HAVING SUM(amount) >= :minTotal ORDER BY SUM(amount) DESC")
    void having_sumWithOrderBy() {
        // minTotal=2000: only CLOSED (2100) passes, OPEN (1500) fails
        List<StatusStats> results = repository.statusesWithMinTotal(2000.0);
        assertThat(results).hasSize(1);
        assertThat(results.get(0).status()).isEqualTo("CLOSED");
        assertThat(results.get(0).totalAmount()).isEqualTo(2100.0);
    }

    @Test
    @Order(9)
    @DisplayName("HAVING with numeric literal: COUNT(this) >= 5")
    void having_numericLiteral() {
        List<StatusCount> results = repository.statusesWithAtLeast5();
        assertThat(results).hasSize(1);
        assertThat(results.get(0).status()).isEqualTo("OPEN");
        assertThat(results.get(0).count()).isEqualTo(5L);
    }

    @Test
    @Order(10)
    @DisplayName("HAVING with multiple AND conditions")
    void having_multipleConditions() {
        // COUNT > 2 AND SUM >= 2000: only CLOSED (count=3, sum=2100) passes
        List<StatusStats> results = repository.statusesWithMultipleHaving(2L, 2000.0);
        assertThat(results).hasSize(1);
        assertThat(results.get(0).status()).isEqualTo("CLOSED");
        assertThat(results.get(0).count()).isEqualTo(3L);
        assertThat(results.get(0).totalAmount()).isEqualTo(2100.0);
    }

    private void createOrder(String customerId, double amount, String status) {
        var order = new OrderEntity();
        order.setCustomerId(customerId);
        order.setAmount(amount);
        order.setStatus(status);
        morphium.store(order);
    }
}
