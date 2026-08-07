package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for GAP-A8: Pagination with GROUP BY queries.
 *
 * Test data: 8 orders across 2 statuses (CLOSED, OPEN — sorted ASC).
 * - OPEN: count=5
 * - CLOSED: count=3
 */
@QuarkusTest
@DisplayName("Jakarta Data JDQL GROUP BY Pagination")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataGroupByPageTest {

    @Inject
    OrderRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.clearCollection(OrderEntity.class);

        createOrder("C1", 100.0, "OPEN");
        createOrder("C1", 200.0, "OPEN");
        createOrder("C2", 300.0, "OPEN");
        createOrder("C3", 400.0, "OPEN");
        createOrder("C3", 500.0, "OPEN");
        createOrder("C1", 600.0, "CLOSED");
        createOrder("C2", 700.0, "CLOSED");
        createOrder("C2", 800.0, "CLOSED");
    }

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    @Test
    @Order(1)
    @DisplayName("First page of grouped results")
    void firstPage() {
        Page<StatusCount> page = repository.countGroupByStatusPaged(
                PageRequest.ofPage(1, 1, true));

        assertThat(page.content()).hasSize(1);
        // ORDER BY status ASC → CLOSED first
        assertThat(page.content().get(0).status()).isEqualTo("CLOSED");
        assertThat(page.totalElements()).isEqualTo(2);
        assertThat(page.hasNext()).isTrue();
    }

    @Test
    @Order(2)
    @DisplayName("Second page of grouped results")
    void secondPage() {
        Page<StatusCount> page = repository.countGroupByStatusPaged(
                PageRequest.ofPage(2, 1, true));

        assertThat(page.content()).hasSize(1);
        assertThat(page.content().get(0).status()).isEqualTo("OPEN");
        assertThat(page.totalElements()).isEqualTo(2);
        assertThat(page.hasNext()).isFalse();
    }

    @Test
    @Order(3)
    @DisplayName("Beyond last page → empty")
    void beyondLast() {
        Page<StatusCount> page = repository.countGroupByStatusPaged(
                PageRequest.ofPage(3, 1, true));

        assertThat(page.content()).isEmpty();
        assertThat(page.totalElements()).isEqualTo(2);
    }

    @Test
    @Order(4)
    @DisplayName("All results in one page")
    void allInOnePage() {
        Page<StatusCount> page = repository.countGroupByStatusPaged(
                PageRequest.ofPage(1, 10, true));

        assertThat(page.content()).hasSize(2);
        assertThat(page.totalElements()).isEqualTo(2);
    }

    @Test
    @Order(5)
    @DisplayName("No total requested → hasTotals false")
    void noTotalRequested() {
        Page<StatusCount> page = repository.countGroupByStatusPaged(
                PageRequest.ofPage(1, 1, false));

        assertThat(page.content()).hasSize(1);
        assertThat(page.hasTotals()).isFalse();
    }

    private void createOrder(String customerId, double amount, String status) {
        var order = new OrderEntity();
        order.setCustomerId(customerId);
        order.setAmount(amount);
        order.setStatus(status);
        morphium.store(order);
    }
}
