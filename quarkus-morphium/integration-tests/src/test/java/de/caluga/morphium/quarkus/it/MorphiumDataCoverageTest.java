package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Jakarta Data #4: Test Coverage Extension.
 * Tests untested operators: LessThanEqual, Not, Between, In, NotIn,
 * StartsWith, EndsWith, Like, IsNull, IsNotNull, IsTrue, IsFalse,
 * OR combinator, multiple OrderBy, Stream return type.
 */
@QuarkusTest
@DisplayName("Jakarta Data Query Derivation — Coverage Extension")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataCoverageTest {

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
    @DisplayName("findByAmountLessThanEqual returns orders with amount <= threshold")
    void findByAmountLessThanEqual() {
        morphium.store(order("C1", 50, "OPEN"));
        morphium.store(order("C2", 100, "OPEN"));
        morphium.store(order("C3", 200, "OPEN"));

        List<OrderEntity> result = repository.findByAmountLessThanEqual(100);

        assertThat(result).hasSize(2);
        assertThat(result).extracting(OrderEntity::getAmount)
                .allMatch(a -> a <= 100);
    }

    @Test
    @Order(2)
    @DisplayName("findByStatusNot excludes orders with given status and sorts by @OrderBy(amount DESC)")
    void findByStatusNot() {
        morphium.store(order("C1", 100, "OPEN"));
        morphium.store(order("C2", 50, "CLOSED"));
        morphium.store(order("C3", 200, "CLOSED"));
        morphium.store(order("C4", 150, "PENDING"));

        List<OrderEntity> result = repository.findByStatusNot("OPEN");

        assertThat(result).hasSize(3);
        assertThat(result).extracting(OrderEntity::getStatus)
                .allMatch(s -> !"OPEN".equals(s));
        // Verify @OrderBy(value = "amount", descending = true) on query derivation method
        assertThat(result).extracting(OrderEntity::getAmount)
                .containsExactly(200.0, 150.0, 50.0);
    }

    @Test
    @Order(3)
    @DisplayName("findByAmountBetween returns orders within range")
    void findByAmountBetween() {
        morphium.store(order("C1", 50, "OPEN"));
        morphium.store(order("C2", 100, "OPEN"));
        morphium.store(order("C3", 200, "OPEN"));

        List<OrderEntity> result = repository.findByAmountBetween(80, 150);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getAmount()).isEqualTo(100);
    }

    @Test
    @Order(4)
    @DisplayName("findByStatusIn returns orders matching any of given statuses")
    void findByStatusIn() {
        morphium.store(order("C1", 100, "OPEN"));
        morphium.store(order("C2", 200, "CLOSED"));
        morphium.store(order("C3", 50, "PENDING"));

        List<OrderEntity> result = repository.findByStatusIn(List.of("OPEN", "PENDING"));

        assertThat(result).hasSize(2);
        assertThat(result).extracting(OrderEntity::getStatus)
                .containsExactlyInAnyOrder("OPEN", "PENDING");
    }

    @Test
    @Order(5)
    @DisplayName("findByStatusNotIn excludes orders matching given statuses")
    void findByStatusNotIn() {
        morphium.store(order("C1", 100, "OPEN"));
        morphium.store(order("C2", 200, "CLOSED"));
        morphium.store(order("C3", 50, "PENDING"));

        List<OrderEntity> result = repository.findByStatusNotIn(List.of("OPEN"));

        assertThat(result).hasSize(2);
        assertThat(result).extracting(OrderEntity::getStatus)
                .containsExactlyInAnyOrder("CLOSED", "PENDING");
    }

    @Test
    @Order(6)
    @DisplayName("findByCustomerIdStartsWith matches prefix")
    void findByCustomerIdStartsWith() {
        morphium.store(order("C-100", 100, "OPEN"));
        morphium.store(order("C-200", 200, "OPEN"));
        morphium.store(order("D-300", 50, "OPEN"));

        List<OrderEntity> result = repository.findByCustomerIdStartsWith("C-");

        assertThat(result).hasSize(2);
        assertThat(result).extracting(OrderEntity::getCustomerId)
                .allMatch(id -> id.startsWith("C-"));
    }

    @Test
    @Order(7)
    @DisplayName("findByCustomerIdEndsWith matches suffix")
    void findByCustomerIdEndsWith() {
        morphium.store(order("abc-1", 100, "OPEN"));
        morphium.store(order("def-1", 200, "OPEN"));
        morphium.store(order("abc-2", 50, "OPEN"));

        List<OrderEntity> result = repository.findByCustomerIdEndsWith("-1");

        assertThat(result).hasSize(2);
        assertThat(result).extracting(OrderEntity::getCustomerId)
                .allMatch(id -> id.endsWith("-1"));
    }

    @Test
    @Order(8)
    @DisplayName("findByCustomerIdLike matches SQL wildcard patterns")
    void findByCustomerIdLike() {
        morphium.store(order("C-100", 100, "OPEN"));
        morphium.store(order("C-200", 200, "OPEN"));
        morphium.store(order("D-300", 50, "OPEN"));

        // % wildcard
        List<OrderEntity> percentResult = repository.findByCustomerIdLike("C-%");
        assertThat(percentResult).hasSize(2);

        // _ wildcard (single char)
        List<OrderEntity> underscoreResult = repository.findByCustomerIdLike("_-100");
        assertThat(underscoreResult).hasSize(1);
        assertThat(underscoreResult.get(0).getCustomerId()).isEqualTo("C-100");
    }

    @Test
    @Order(9)
    @DisplayName("findByCustomerIdIsNull returns orders without customerId")
    void findByCustomerIdIsNull() {
        morphium.store(order("C1", 100, "OPEN"));
        morphium.store(order("C2", 200, "OPEN"));
        morphium.store(order(null, 50, "CLOSED"));

        List<OrderEntity> result = repository.findByCustomerIdIsNull();

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getCustomerId()).isNull();
    }

    @Test
    @Order(10)
    @DisplayName("findByCustomerIdIsNotNull returns orders with customerId set")
    void findByCustomerIdIsNotNull() {
        morphium.store(order("C1", 100, "OPEN"));
        morphium.store(order("C2", 200, "OPEN"));
        morphium.store(order(null, 50, "CLOSED"));

        List<OrderEntity> result = repository.findByCustomerIdIsNotNull();

        assertThat(result).hasSize(2);
        assertThat(result).extracting(OrderEntity::getCustomerId)
                .doesNotContainNull();
    }

    @Test
    @Order(11)
    @DisplayName("findByUrgentIsTrue returns only urgent orders")
    void findByUrgentIsTrue() {
        morphium.store(urgentOrder("C1", 100, "OPEN", true));
        morphium.store(urgentOrder("C2", 200, "OPEN", false));
        morphium.store(urgentOrder("C3", 50, "CLOSED", false));

        List<OrderEntity> result = repository.findByUrgentIsTrue();

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getCustomerId()).isEqualTo("C1");
        assertThat(result.get(0).isUrgent()).isTrue();
    }

    @Test
    @Order(12)
    @DisplayName("findByUrgentIsFalse returns only non-urgent orders")
    void findByUrgentIsFalse() {
        morphium.store(urgentOrder("C1", 100, "OPEN", true));
        morphium.store(urgentOrder("C2", 200, "OPEN", false));
        morphium.store(urgentOrder("C3", 50, "CLOSED", false));

        List<OrderEntity> result = repository.findByUrgentIsFalse();

        assertThat(result).hasSize(2);
        assertThat(result).extracting(OrderEntity::isUrgent)
                .containsOnly(false);
    }

    @Test
    @Order(13)
    @DisplayName("findByStatusOrCustomerId combines conditions with OR")
    void findByStatusOrCustomerId() {
        morphium.store(order("C1", 100, "OPEN"));
        morphium.store(order("C2", 200, "CLOSED"));
        morphium.store(order("C1", 50, "PENDING"));

        // OPEN status OR customerId=C2
        List<OrderEntity> result = repository.findByStatusOrCustomerId("OPEN", "C2");

        assertThat(result).hasSize(2);
        assertThat(result).extracting(OrderEntity::getCustomerId)
                .containsExactlyInAnyOrder("C1", "C2");
    }

    @Test
    @Order(14)
    @DisplayName("findByStatus with multiple OrderBy sorts by amount ASC then customerId DESC")
    void findByStatus_multipleOrderBy() {
        morphium.store(order("B", 100, "OPEN"));
        morphium.store(order("A", 100, "OPEN"));
        morphium.store(order("C", 50, "OPEN"));

        List<OrderEntity> result = repository.findByStatusOrderByAmountAscCustomerIdDesc("OPEN");

        assertThat(result).hasSize(3);
        // amount ASC: 50 first, then 100, 100
        assertThat(result.get(0).getAmount()).isEqualTo(50);
        assertThat(result.get(0).getCustomerId()).isEqualTo("C");
        // among amount=100: customerId DESC → B before A
        assertThat(result.get(1).getCustomerId()).isEqualTo("B");
        assertThat(result.get(2).getCustomerId()).isEqualTo("A");
    }

    @Test
    @Order(15)
    @DisplayName("findBy* with Stream<T> return type returns a stream")
    void findBy_streamReturnType() {
        morphium.store(order("C1", 100, "OPEN"));
        morphium.store(order("C2", 200, "OPEN"));
        morphium.store(order("C3", 300, "OPEN"));

        try (Stream<OrderEntity> stream = repository.findByAmountGreaterThanEqualOrderByAmountAsc(100)) {
            List<OrderEntity> result = stream.toList();

            assertThat(result).hasSize(3);
            // verify ordering
            assertThat(result).extracting(OrderEntity::getAmount)
                    .containsExactly(100.0, 200.0, 300.0);
        }
    }

    private OrderEntity order(String customerId, double amount, String status) {
        var o = new OrderEntity();
        o.setCustomerId(customerId);
        o.setAmount(amount);
        o.setStatus(status);
        return o;
    }

    private OrderEntity urgentOrder(String customerId, double amount, String status, boolean urgent) {
        var o = order(customerId, amount, status);
        o.setUrgent(urgent);
        return o;
    }
}
