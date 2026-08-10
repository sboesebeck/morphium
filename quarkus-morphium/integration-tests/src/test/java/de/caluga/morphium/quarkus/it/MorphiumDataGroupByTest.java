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
 * Integration tests for Jakarta Data #8v2: JDQL GROUP BY with Record mapping.
 * Tests single-field GROUP BY with COUNT, SUM, WHERE, and ORDER BY.
 */
@QuarkusTest
@DisplayName("Jakarta Data JDQL GROUP BY")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataGroupByTest {

    @Inject
    OrderRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.clearCollection(OrderEntity.class);

        // 5 OPEN orders: amounts 100, 200, 300, 400, 500 (total=1500)
        for (int i = 1; i <= 5; i++) {
            createOrder("C" + i, i * 100.0, "OPEN");
        }
        // 3 CLOSED orders: amounts 600, 700, 800 (total=2100)
        for (int i = 6; i <= 8; i++) {
            createOrder("C" + i, i * 100.0, "CLOSED");
        }
    }

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    @Test
    @Order(1)
    @DisplayName("GROUP BY status → COUNT(this)")
    void groupBy_count() {
        List<StatusCount> results = repository.countGroupByStatus();
        assertThat(results).hasSize(2);

        Map<String, Long> map = results.stream()
                .collect(Collectors.toMap(StatusCount::status, StatusCount::count));
        assertThat(map).containsEntry("OPEN", 5L);
        assertThat(map).containsEntry("CLOSED", 3L);
    }

    @Test
    @Order(2)
    @DisplayName("GROUP BY status → COUNT(this), SUM(amount)")
    void groupBy_countAndSum() {
        List<StatusStats> results = repository.statsByStatus();
        assertThat(results).hasSize(2);

        Map<String, StatusStats> map = results.stream()
                .collect(Collectors.toMap(StatusStats::status, s -> s));
        assertThat(map.get("OPEN").count()).isEqualTo(5L);
        assertThat(map.get("OPEN").totalAmount()).isEqualTo(1500.0);
        assertThat(map.get("CLOSED").count()).isEqualTo(3L);
        assertThat(map.get("CLOSED").totalAmount()).isEqualTo(2100.0);
    }

    @Test
    @Order(3)
    @DisplayName("GROUP BY with WHERE filter")
    void groupBy_withWhere() {
        // Only CLOSED orders have amount > 500 (600, 700, 800)
        List<StatusStats> results = repository.statsByStatusFiltered(500.0);
        assertThat(results).hasSize(1);
        assertThat(results.get(0).status()).isEqualTo("CLOSED");
        assertThat(results.get(0).count()).isEqualTo(3L);
        assertThat(results.get(0).totalAmount()).isEqualTo(2100.0);
    }

    @Test
    @Order(4)
    @DisplayName("GROUP BY with ORDER BY COUNT(this) DESC")
    void groupBy_orderByCount() {
        List<StatusCount> results = repository.countGroupByStatusOrderByCount();
        assertThat(results).hasSize(2);
        // OPEN (5) should come first (DESC), CLOSED (3) second
        assertThat(results.get(0).status()).isEqualTo("OPEN");
        assertThat(results.get(0).count()).isEqualTo(5L);
        assertThat(results.get(1).status()).isEqualTo("CLOSED");
        assertThat(results.get(1).count()).isEqualTo(3L);
    }

    @Test
    @Order(5)
    @DisplayName("GROUP BY with ORDER BY field ASC")
    void groupBy_orderByField() {
        List<StatusStats> results = repository.statsByStatusFiltered(0.0);
        assertThat(results).hasSize(2);
        // ORDER BY status ASC: CLOSED first, OPEN second
        assertThat(results.get(0).status()).isEqualTo("CLOSED");
        assertThat(results.get(1).status()).isEqualTo("OPEN");
    }

    @Test
    @Order(6)
    @DisplayName("GROUP BY with no matching results → empty list")
    void groupBy_noResults() {
        morphium.clearCollection(OrderEntity.class);
        List<StatusCount> results = repository.countGroupByStatus();
        assertThat(results).isEmpty();
    }

    private void createOrder(String customerId, double amount, String status) {
        var order = new OrderEntity();
        order.setCustomerId(customerId);
        order.setAmount(amount);
        order.setStatus(status);
        morphium.store(order);
    }
}
