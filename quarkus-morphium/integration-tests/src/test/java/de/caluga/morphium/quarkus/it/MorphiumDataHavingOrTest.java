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
 * Integration tests for HAVING OR support.
 *
 * Test data: 8 orders across 2 statuses:
 * - OPEN: count=5, sum=1500.0
 * - CLOSED: count=3, sum=2100.0
 */
@QuarkusTest
@DisplayName("Jakarta Data JDQL HAVING OR")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataHavingOrTest {

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
    @DisplayName("HAVING OR: both conditions match different groups")
    void havingOr_bothMatch() {
        // COUNT > 4 matches OPEN (5), SUM >= 2000 matches CLOSED (2100)
        List<StatusStats> results = repository.statusesWithCountOrTotal(4L, 2000.0);
        assertThat(results).hasSize(2);

        Map<String, Long> countMap = results.stream()
                .collect(Collectors.toMap(StatusStats::status, StatusStats::count));
        assertThat(countMap).containsKeys("OPEN", "CLOSED");
    }

    @Test
    @Order(2)
    @DisplayName("HAVING OR: only one condition matches")
    void havingOr_oneMatches() {
        // COUNT > 10 matches neither, SUM >= 2000 matches CLOSED only
        List<StatusStats> results = repository.statusesWithCountOrTotal(10L, 2000.0);
        assertThat(results).hasSize(1);
        assertThat(results.get(0).status()).isEqualTo("CLOSED");
    }

    @Test
    @Order(3)
    @DisplayName("HAVING OR: no condition matches")
    void havingOr_noneMatch() {
        List<StatusStats> results = repository.statusesWithCountOrTotal(10L, 5000.0);
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
