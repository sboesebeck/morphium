package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for JDQL string literals and NOT operator (#10).
 */
@QuarkusTest
@DisplayName("Jakarta Data JDQL String Literals + NOT Operator")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataJdqlEnhancedTest {

    @Inject
    OrderRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.clearCollection(OrderEntity.class);

        // 3 OPEN orders
        for (int i = 1; i <= 3; i++) {
            var order = new OrderEntity();
            order.setCustomerId("CUST-" + i);
            order.setAmount(i * 100.0);
            order.setStatus("OPEN");
            order.setUrgent(i == 3); // only #3 is urgent
            morphium.store(order);
        }

        // 3 CLOSED orders
        for (int i = 4; i <= 6; i++) {
            var order = new OrderEntity();
            order.setCustomerId("CUST-" + i);
            order.setAmount(i * 100.0);
            order.setStatus("CLOSED");
            order.setUrgent(false);
            morphium.store(order);
        }

        // 2 CANCELLED orders
        for (int i = 7; i <= 8; i++) {
            var order = new OrderEntity();
            order.setCustomerId("CUST-" + i);
            order.setAmount(i * 100.0);
            order.setStatus("CANCELLED");
            order.setUrgent(false);
            morphium.store(order);
        }
    }

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    // --- String Literals ---

    @Test
    @Order(1)
    @DisplayName("#1 String literal: WHERE status = 'OPEN'")
    void stringLiteral_basic() {
        List<OrderEntity> result = repository.queryByStringLiteral();
        assertThat(result).hasSize(3);
        assertThat(result).allMatch(o -> "OPEN".equals(o.getStatus()));
        // Verify sorted by amount ASC
        for (int i = 1; i < result.size(); i++) {
            assertThat(result.get(i).getAmount()).isGreaterThanOrEqualTo(result.get(i - 1).getAmount());
        }
    }

    @Test
    @Order(2)
    @DisplayName("#2 String literal mixed with named param")
    void stringLiteral_mixedWithParam() {
        List<OrderEntity> result = repository.queryByStringLiteralAndParam(150.0);
        assertThat(result).hasSize(2);
        assertThat(result).allMatch(o -> "OPEN".equals(o.getStatus()) && o.getAmount() > 150.0);
    }

    @Test
    @Order(3)
    @DisplayName("#3 String literal in aggregate: COUNT(this) WHERE status = 'OPEN'")
    void stringLiteral_aggregate() {
        long count = repository.countOpenLiteral();
        assertThat(count).isEqualTo(3L);
    }

    // --- NOT Operator ---

    @Test
    @Order(4)
    @DisplayName("#4 NOT with param: WHERE NOT status = :status")
    void not_withParam() {
        List<OrderEntity> result = repository.queryNotByStatus("OPEN");
        assertThat(result).hasSize(5);
        assertThat(result).noneMatch(o -> "OPEN".equals(o.getStatus()));
    }

    @Test
    @Order(5)
    @DisplayName("#5 NOT with string literal: WHERE NOT status = 'CANCELLED'")
    void not_withStringLiteral() {
        List<OrderEntity> result = repository.queryNotCancelled();
        assertThat(result).hasSize(6);
        assertThat(result).noneMatch(o -> "CANCELLED".equals(o.getStatus()));
    }

    @Test
    @Order(6)
    @DisplayName("#6 NOT combined with AND: WHERE status = :s AND NOT urgent = true")
    void not_combinedWithAnd() {
        List<OrderEntity> result = repository.queryByStatusNotUrgent("OPEN");
        assertThat(result).hasSize(2);
        assertThat(result).allMatch(o -> "OPEN".equals(o.getStatus()) && !o.isUrgent());
    }

    @Test
    @Order(7)
    @DisplayName("#7 NOT with comparison: WHERE NOT amount > :max")
    void not_comparison() {
        List<OrderEntity> result = repository.queryNotAmountGreaterThan(400.0);
        // amount NOT > 400 → amount <= 400 → 100, 200, 300, 400
        assertThat(result).hasSize(4);
        assertThat(result).allMatch(o -> o.getAmount() <= 400.0);
    }

    @Test
    @Order(8)
    @DisplayName("#8 NOT IN: WHERE NOT status IN :statuses")
    void not_in() {
        List<OrderEntity> result = repository.queryNotInStatuses(List.of("OPEN", "CLOSED"));
        assertThat(result).hasSize(2);
        assertThat(result).allMatch(o -> "CANCELLED".equals(o.getStatus()));
    }

    @Test
    @Order(9)
    @DisplayName("#9 NOT LIKE: WHERE NOT status LIKE :pattern")
    void not_like() {
        List<OrderEntity> result = repository.queryNotLike("OPEN%");
        assertThat(result).hasSize(5);
        assertThat(result).noneMatch(o -> o.getStatus().startsWith("OPEN"));
    }
}
