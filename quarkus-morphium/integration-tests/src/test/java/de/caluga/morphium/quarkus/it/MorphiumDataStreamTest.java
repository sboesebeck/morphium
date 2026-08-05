package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for lazy Stream support in Jakarta Data repositories.
 */
@QuarkusTest
@DisplayName("Jakarta Data Lazy Stream Support")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataStreamTest {

    @Inject
    OrderRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.clearCollection(OrderEntity.class);
        for (int i = 1; i <= 50; i++) {
            var order = new OrderEntity();
            order.setCustomerId("C" + i);
            order.setAmount(i * 10.0);
            order.setStatus(i <= 30 ? "OPEN" : "CLOSED");
            morphium.store(order);
        }
    }

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    @Test
    @Order(1)
    @DisplayName("#1 findAll() stream with limit returns partial results")
    void findAll_streamWithLimit() {
        try (Stream<OrderEntity> stream = repository.findAll()) {
            List<OrderEntity> limited = stream.limit(5).toList();
            assertThat(limited).hasSize(5);
        }
    }

    @Test
    @Order(2)
    @DisplayName("#2 findAll() stream close is idempotent")
    void findAll_streamCloseIsIdempotent() {
        Stream<OrderEntity> stream = repository.findAll();
        stream.close();
        stream.close(); // second close should not throw
    }

    @Test
    @Order(3)
    @DisplayName("#3 Query derivation stream returns correct sorted results")
    void queryDerivation_streamReturn() {
        try (Stream<OrderEntity> stream = repository.findByAmountGreaterThanEqualOrderByAmountAsc(400)) {
            List<OrderEntity> result = stream.toList();
            assertThat(result).hasSize(11); // amounts 400,410,...,500
            assertThat(result).extracting(OrderEntity::getAmount)
                    .isSorted();
        }
    }

    @Test
    @Order(4)
    @DisplayName("#4 @Find annotated method with Stream return works")
    void findAnnotation_streamReturn() {
        try (Stream<OrderEntity> stream = repository.findStreamByStatus("OPEN")) {
            List<OrderEntity> result = stream.toList();
            assertThat(result).hasSize(30);
            // Verify ordering by amount (from @OrderBy)
            assertThat(result).extracting(OrderEntity::getAmount)
                    .isSorted();
        }
    }

    @Test
    @Order(5)
    @DisplayName("#5 @Query annotated method with Stream return works")
    void queryAnnotation_streamReturn() {
        try (Stream<OrderEntity> stream = repository.queryStreamByStatus("CLOSED")) {
            List<OrderEntity> result = stream.toList();
            assertThat(result).hasSize(20);
            assertThat(result).extracting(OrderEntity::getAmount)
                    .isSorted();
        }
    }

    @Test
    @Order(6)
    @DisplayName("#6 Stream with try-with-resources and intermediate operations")
    void stream_withTryWithResources() {
        try (Stream<OrderEntity> stream = repository.findAll()) {
            long count = stream
                    .filter(o -> o.getAmount() > 200)
                    .count();
            assertThat(count).isEqualTo(30); // amounts 210..500
        }
    }
}
