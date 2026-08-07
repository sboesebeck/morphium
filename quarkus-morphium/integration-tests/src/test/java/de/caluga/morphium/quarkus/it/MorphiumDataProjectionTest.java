package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for JDQL SELECT with Projection (#7).
 */
@QuarkusTest
@DisplayName("Jakarta Data JDQL SELECT Projection")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataProjectionTest {

    @Inject
    OrderRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.clearCollection(OrderEntity.class);
        for (int i = 1; i <= 10; i++) {
            var order = new OrderEntity();
            order.setCustomerId("C" + i);
            order.setAmount(i * 100.0);
            order.setStatus(i <= 5 ? "OPEN" : "CLOSED");
            morphium.store(order);
        }
    }

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    @Test
    @Order(1)
    @DisplayName("#1 SELECT fields — only projected fields populated, others null/default")
    void selectFields_onlyProjectedFieldsPopulated() {
        List<OrderEntity> results = repository.queryProjectedByStatus("OPEN");

        assertThat(results).hasSize(5);
        for (OrderEntity e : results) {
            // Projected fields are populated
            assertThat(e.getCustomerId()).isNotNull();
            assertThat(e.getAmount()).isGreaterThan(0);
            // _id is always included by MongoDB
            assertThat(e.getId()).isNotNull();
            // Non-projected fields are null/default
            assertThat(e.getStatus()).isNull();
            assertThat(e.getCreatedAt()).isNull();
            assertThat(e.getTags()).isNull();
        }
        // Verify ordering
        assertThat(results).extracting(OrderEntity::getAmount).isSorted();
    }

    @Test
    @Order(2)
    @DisplayName("#2 SELECT with FROM clause — FROM is ignored, same results")
    void selectWithFrom_ignored() {
        List<OrderEntity> results = repository.queryProjectedWithFrom("OPEN");

        assertThat(results).hasSize(5);
        for (OrderEntity e : results) {
            assertThat(e.getCustomerId()).isNotNull();
            assertThat(e.getAmount()).isGreaterThan(0);
            assertThat(e.getStatus()).isNull();
        }
    }

    @Test
    @Order(3)
    @DisplayName("#3 SELECT with Stream return type")
    void selectWithStream_works() {
        try (Stream<OrderEntity> stream = repository.queryProjectedStream(500.0)) {
            List<OrderEntity> results = stream.toList();
            // amounts > 500: 600, 700, 800, 900, 1000 = 5 items
            assertThat(results).hasSize(5);
            for (OrderEntity e : results) {
                assertThat(e.getCustomerId()).isNotNull();
                // amount is not projected — should be 0.0 (primitive default)
                assertThat(e.getAmount()).isEqualTo(0.0);
                assertThat(e.getStatus()).isNull();
            }
        }
    }

    @Test
    @Order(4)
    @DisplayName("#4 SELECT with single Optional result")
    void selectSingle_projection() {
        Optional<OrderEntity> result = repository.queryProjectedSingle("C1");

        assertThat(result).isPresent();
        OrderEntity e = result.get();
        assertThat(e.getCustomerId()).isEqualTo("C1");
        assertThat(e.getAmount()).isEqualTo(100.0);
        assertThat(e.getStatus()).isNull();
        assertThat(e.getCreatedAt()).isNull();
    }

    @Test
    @Order(5)
    @DisplayName("#5 No SELECT — all fields populated (regression check)")
    void noSelect_allFieldsPopulated() {
        List<OrderEntity> results = repository.queryByStatus("OPEN");

        assertThat(results).hasSize(5);
        for (OrderEntity e : results) {
            assertThat(e.getCustomerId()).isNotNull();
            assertThat(e.getAmount()).isGreaterThan(0);
            assertThat(e.getStatus()).isEqualTo("OPEN");
            assertThat(e.getCreatedAt()).isNotNull();
        }
    }

    @Test
    @Order(6)
    @DisplayName("#6 SELECT with ORDER BY — correctly sorted and projected")
    void selectWithOrderBy_works() {
        List<OrderEntity> results = repository.queryProjectedByStatus("CLOSED");

        assertThat(results).hasSize(5);
        assertThat(results).extracting(OrderEntity::getAmount).isSorted();
        for (OrderEntity e : results) {
            assertThat(e.getCustomerId()).isNotNull();
            assertThat(e.getAmount()).isGreaterThan(0);
            assertThat(e.getStatus()).isNull();
        }
    }
}
