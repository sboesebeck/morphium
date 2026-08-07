package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for CompletionStage async support in Jakarta Data repositories.
 */
@QuarkusTest
@DisplayName("Jakarta Data Async (CompletionStage) Support")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataAsyncTest {

    @Inject
    OrderRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.clearCollection(OrderEntity.class);
        for (int i = 1; i <= 10; i++) {
            var order = new OrderEntity();
            order.setCustomerId("CUST-" + i);
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
    @DisplayName("#1 Query derivation async: findByStatusAsync returns list")
    void queryDerivation_findByStatusAsync() throws Exception {
        CompletionStage<List<OrderEntity>> stage = repository.findByStatusAsync("OPEN");
        List<OrderEntity> result = stage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(result).hasSize(5);
        assertThat(result).allMatch(o -> "OPEN".equals(o.getStatus()));
    }

    @Test
    @Order(2)
    @DisplayName("#2 Query derivation async: findByCustomerIdAsync returns Optional")
    void queryDerivation_findByCustomerIdAsync() throws Exception {
        CompletionStage<Optional<OrderEntity>> stage = repository.findByCustomerIdAsync("CUST-3");
        Optional<OrderEntity> result = stage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(result).isPresent();
        assertThat(result.get().getCustomerId()).isEqualTo("CUST-3");
    }

    @Test
    @Order(3)
    @DisplayName("#3 Query derivation async: findByCustomerIdAsync returns empty Optional")
    void queryDerivation_findByCustomerIdAsync_notFound() throws Exception {
        CompletionStage<Optional<OrderEntity>> stage = repository.findByCustomerIdAsync("NONEXISTENT");
        Optional<OrderEntity> result = stage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(result).isEmpty();
    }

    @Test
    @Order(4)
    @DisplayName("#4 @Find async: findAsyncByStatus returns sorted list")
    void findAnnotation_async() throws Exception {
        CompletionStage<List<OrderEntity>> stage = repository.findAsyncByStatus("OPEN");
        List<OrderEntity> result = stage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(result).hasSize(5);
        assertThat(result).allMatch(o -> "OPEN".equals(o.getStatus()));
        // Verify sorted by amount ASC (@OrderBy("amount"))
        for (int i = 1; i < result.size(); i++) {
            assertThat(result.get(i).getAmount()).isGreaterThanOrEqualTo(result.get(i - 1).getAmount());
        }
    }

    @Test
    @Order(5)
    @DisplayName("#5 @Query JDQL async: queryByStatusAsync returns sorted list")
    void jdqlQuery_async() throws Exception {
        CompletionStage<List<OrderEntity>> stage = repository.queryByStatusAsync("CLOSED");
        List<OrderEntity> result = stage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(result).hasSize(5);
        assertThat(result).allMatch(o -> "CLOSED".equals(o.getStatus()));
        // Verify sorted by amount ASC (ORDER BY amount ASC in JDQL)
        for (int i = 1; i < result.size(); i++) {
            assertThat(result.get(i).getAmount()).isGreaterThanOrEqualTo(result.get(i - 1).getAmount());
        }
    }

    @Test
    @Order(6)
    @DisplayName("#6 @Query JDQL aggregate async: countByStatusAsync")
    void jdqlAggregate_async() throws Exception {
        CompletionStage<Long> stage = repository.countByStatusAsync("OPEN");
        Long result = stage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(result).isEqualTo(5L);
    }

    @Test
    @Order(7)
    @DisplayName("#7 Async with empty result set")
    void async_emptyResult() throws Exception {
        CompletionStage<List<OrderEntity>> stage = repository.findByStatusAsync("NONEXISTENT");
        List<OrderEntity> result = stage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(result).isEmpty();
    }
}
