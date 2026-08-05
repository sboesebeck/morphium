package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Jakarta Data Phase 5: @Query with JDQL.
 */
@QuarkusTest
@DisplayName("Jakarta Data @Query / JDQL")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataJdqlTest {

    @Inject
    OrderRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.clearCollection(OrderEntity.class);

        createOrder("C1", 100.0, "OPEN");
        createOrder("C2", 250.0, "OPEN");
        createOrder("C3", 50.0, "CLOSED");
        createOrder("C4", 300.0, "CLOSED");
        createOrder("C5", 150.0, "PENDING");
    }

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    @Test
    @Order(1)
    @DisplayName("@Query WHERE status = :status ORDER BY amount")
    void queryByStatus() {
        List<OrderEntity> open = repository.queryByStatus("OPEN");

        assertThat(open).hasSize(2);
        assertThat(open).allSatisfy(o -> assertThat(o.getStatus()).isEqualTo("OPEN"));
        // Should be sorted by amount ASC
        assertThat(open.get(0).getAmount()).isEqualTo(100.0);
        assertThat(open.get(1).getAmount()).isEqualTo(250.0);
    }

    @Test
    @Order(2)
    @DisplayName("@Query WHERE status AND amount > :min (multiple params)")
    void queryByStatusAndMinAmount() {
        List<OrderEntity> result = repository.queryByStatusAndMinAmount("OPEN", 150.0);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getAmount()).isEqualTo(250.0);
    }

    @Test
    @Order(3)
    @DisplayName("@Query WHERE amount BETWEEN :min AND :max ORDER BY amount DESC")
    void queryByAmountRange() {
        List<OrderEntity> result = repository.queryByAmountRange(100.0, 250.0);

        assertThat(result).hasSize(3); // 100, 150, 250
        // Should be sorted DESC
        assertThat(result.get(0).getAmount()).isEqualTo(250.0);
        assertThat(result.get(1).getAmount()).isEqualTo(150.0);
        assertThat(result.get(2).getAmount()).isEqualTo(100.0);
    }

    @Test
    @Order(4)
    @DisplayName("@Query with count return type")
    void countByMinAmount() {
        long count = repository.countByMinAmount(200.0);

        assertThat(count).isEqualTo(2); // 250, 300
    }

    @Test
    @Order(5)
    @DisplayName("@Query with boolean return type (exists)")
    void existsWithStatus() {
        assertThat(repository.existsWithStatus("OPEN")).isTrue();
        assertThat(repository.existsWithStatus("CANCELLED")).isFalse();
    }

    @Test
    @Order(6)
    @DisplayName("@Query WHERE field IS NOT NULL")
    void queryAllWithCustomerId() {
        List<OrderEntity> result = repository.queryAllWithCustomerId();

        assertThat(result).hasSize(5); // All have customerId set
        // Should be sorted by customerId ASC
        assertThat(result.get(0).getCustomerId()).isEqualTo("C1");
        assertThat(result.get(4).getCustomerId()).isEqualTo("C5");
    }

    @Test
    @Order(7)
    @DisplayName("@Query with OR combinator")
    void queryByEitherStatus() {
        List<OrderEntity> result = repository.queryByEitherStatus("OPEN", "PENDING");

        assertThat(result).hasSize(3); // 2 OPEN + 1 PENDING
        assertThat(result).allSatisfy(o ->
                assertThat(o.getStatus()).isIn("OPEN", "PENDING"));
    }

    @Test
    @Order(8)
    @DisplayName("@Query with implicit @Param via -parameters compiler option (single param)")
    void queryByStatusImplicitParam() {
        List<OrderEntity> open = repository.queryByStatusImplicitParam("OPEN");

        assertThat(open).hasSize(2);
        assertThat(open).allSatisfy(o -> assertThat(o.getStatus()).isEqualTo("OPEN"));
        assertThat(open.get(0).getAmount()).isEqualTo(100.0);
        assertThat(open.get(1).getAmount()).isEqualTo(250.0);
    }

    @Test
    @Order(9)
    @DisplayName("@Query with implicit @Param via -parameters compiler option (multiple params)")
    void queryByStatusAndMinAmountImplicit() {
        List<OrderEntity> result = repository.queryByStatusAndMinAmountImplicit("OPEN", 150.0);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getAmount()).isEqualTo(250.0);
    }

    private void createOrder(String customerId, double amount, String status) {
        var order = new OrderEntity();
        order.setCustomerId(customerId);
        order.setAmount(amount);
        order.setStatus(status);
        morphium.store(order);
    }
}
