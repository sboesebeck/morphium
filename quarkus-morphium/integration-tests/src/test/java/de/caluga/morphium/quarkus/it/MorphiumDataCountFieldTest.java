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
 * Integration tests for GAP-A3: COUNT(field) should exclude NULL values.
 *
 * Test data: orders with some null customerIds:
 * - OPEN, C1, 100
 * - OPEN, null, 200
 * - OPEN, C2, 300
 * - CLOSED, C3, 400
 * - CLOSED, null, 500
 */
@QuarkusTest
@DisplayName("Jakarta Data JDQL COUNT(field) NULL filtering")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataCountFieldTest {

    @Inject
    OrderRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.clearCollection(OrderEntity.class);

        createOrder("C1", 100.0, "OPEN");
        createOrder(null, 200.0, "OPEN");
        createOrder("C2", 300.0, "OPEN");
        createOrder("C3", 400.0, "CLOSED");
        createOrder(null, 500.0, "CLOSED");
    }

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    @Test
    @Order(1)
    @DisplayName("COUNT(customerId) excludes NULLs")
    void countField_excludesNulls() {
        List<StatusCount> results = repository.countNonNullCustomerByStatus();
        assertThat(results).hasSize(2);

        Map<String, Long> map = results.stream()
                .collect(Collectors.toMap(StatusCount::status, StatusCount::count));
        // OPEN: C1 + C2 = 2 (not 3)
        assertThat(map.get("OPEN")).isEqualTo(2L);
        // CLOSED: C3 = 1 (not 2)
        assertThat(map.get("CLOSED")).isEqualTo(1L);
    }

    @Test
    @Order(2)
    @DisplayName("COUNT(this) still includes all rows (regression)")
    void countThis_includesAll() {
        List<StatusCount> results = repository.countGroupByStatus();
        assertThat(results).hasSize(2);

        Map<String, Long> map = results.stream()
                .collect(Collectors.toMap(StatusCount::status, StatusCount::count));
        assertThat(map.get("OPEN")).isEqualTo(3L);
        assertThat(map.get("CLOSED")).isEqualTo(2L);
    }

    private void createOrder(String customerId, double amount, String status) {
        var order = new OrderEntity();
        order.setCustomerId(customerId);
        order.setAmount(amount);
        order.setStatus(status);
        morphium.store(order);
    }
}
