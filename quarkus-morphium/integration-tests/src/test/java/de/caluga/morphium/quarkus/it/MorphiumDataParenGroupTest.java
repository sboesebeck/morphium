package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for JDQL parenthesized group conditions.
 * Verifies that queries like {@code WHERE a = :a AND (b IS NULL OR b = '')}
 * correctly scope the OR to the parenthesized group and don't leak data
 * across unrelated filter values.
 */
@QuarkusTest
@DisplayName("JDQL Parenthesized Group Queries")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataParenGroupTest {

    @Inject
    OrderRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.clearCollection(OrderEntity.class);
    }

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    @Test
    @Order(1)
    @DisplayName("AND with OR group: only matching status + (NULL or empty) customerId")
    void queryByStatusWithNullOrEmptyCustomerId() {
        // OPEN with null customerId — should match
        createOrder(null, 100.0, "OPEN", false);
        // OPEN with empty customerId — should match
        createOrder("", 200.0, "OPEN", false);
        // OPEN with non-empty customerId — should NOT match
        createOrder("C1", 300.0, "OPEN", false);
        // CLOSED with null customerId — should NOT match (wrong status)
        createOrder(null, 400.0, "CLOSED", false);
        // CLOSED with empty customerId — should NOT match (wrong status)
        createOrder("", 500.0, "CLOSED", false);

        List<OrderEntity> result = repository.queryByStatusWithNullOrEmptyCustomerId("OPEN");

        // Only the 2 OPEN orders with null/empty customerId
        assertThat(result).hasSize(2);
        assertThat(result).allSatisfy(o -> {
            assertThat(o.getStatus()).isEqualTo("OPEN");
            assertThat(o.getCustomerId() == null || o.getCustomerId().isEmpty()).isTrue();
        });
    }

    @Test
    @Order(2)
    @DisplayName("No cross-status leakage: CLOSED orders not returned when querying OPEN")
    void noCrossStatusLeakage() {
        // This is the exact bug scenario from OTA Authority:
        // Without parenthesis-aware parsing, the OR would be top-level,
        // returning ALL orders where customerId IS NULL regardless of status.
        createOrder(null, 100.0, "OPEN", false);
        createOrder(null, 200.0, "CLOSED", false);
        createOrder(null, 300.0, "PENDING", false);

        List<OrderEntity> result = repository.queryByStatusWithNullOrEmptyCustomerId("OPEN");

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getStatus()).isEqualTo("OPEN");
    }

    @Test
    @Order(3)
    @DisplayName("AND with OR group using params and boolean: status + (amount > min OR urgent)")
    void queryByStatusWithAmountOrUrgent() {
        // OPEN, amount 50, not urgent — should NOT match (50 <= 100 and not urgent)
        createOrder("C1", 50.0, "OPEN", false);
        // OPEN, amount 200, not urgent — should match (200 > 100)
        createOrder("C2", 200.0, "OPEN", false);
        // OPEN, amount 30, urgent — should match (urgent = true)
        createOrder("C3", 30.0, "OPEN", true);
        // CLOSED, amount 200, urgent — should NOT match (wrong status)
        createOrder("C4", 200.0, "CLOSED", true);

        List<OrderEntity> result = repository.queryByStatusWithAmountOrUrgent("OPEN", 100.0);

        assertThat(result).hasSize(2);
        assertThat(result).allSatisfy(o -> assertThat(o.getStatus()).isEqualTo("OPEN"));
        // Sorted by amount ASC: 30 (urgent), then 200
        assertThat(result.get(0).getAmount()).isEqualTo(30.0);
        assertThat(result.get(1).getAmount()).isEqualTo(200.0);
    }

    @Test
    @Order(4)
    @DisplayName("Empty result when no matches for parenthesized group")
    void emptyResultWhenNoMatch() {
        createOrder("C1", 100.0, "OPEN", false);
        createOrder("C2", 200.0, "OPEN", false);

        // All OPEN orders have non-empty customerId → no match
        List<OrderEntity> result = repository.queryByStatusWithNullOrEmptyCustomerId("OPEN");

        assertThat(result).isEmpty();
    }

    private void createOrder(String customerId, double amount, String status, boolean urgent) {
        var order = new OrderEntity();
        order.setCustomerId(customerId);
        order.setAmount(amount);
        order.setStatus(status);
        order.setUrgent(urgent);
        morphium.store(order);
    }
}
