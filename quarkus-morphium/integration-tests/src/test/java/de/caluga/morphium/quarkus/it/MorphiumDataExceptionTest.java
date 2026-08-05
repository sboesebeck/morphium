package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.data.exceptions.EmptyResultException;
import jakarta.data.exceptions.NonUniqueResultException;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests verifying that Jakarta Data standard exceptions are thrown
 * when single-result repository methods encounter no result or multiple results.
 */
@QuarkusTest
@DisplayName("Jakarta Data Standard Exceptions")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataExceptionTest {

    @Inject
    OrderRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.clearCollection(OrderEntity.class);
    }

    // --- Query derivation: T return type ---

    @Test
    @Order(1)
    @DisplayName("findByX returning T throws EmptyResultException when no result")
    void findSingle_noResult_throwsEmptyResult() {
        assertThatThrownBy(() -> repository.findByCustomerId("nonexistent"))
                .isInstanceOf(EmptyResultException.class);
    }

    @Test
    @Order(2)
    @DisplayName("findByX returning T throws NonUniqueResultException when multiple results")
    void findSingle_multipleResults_throwsNonUnique() {
        // Store two orders with same customerId
        var o1 = new OrderEntity();
        o1.setCustomerId("DUPE");
        o1.setAmount(100.0);
        o1.setStatus("OPEN");
        morphium.store(o1);

        var o2 = new OrderEntity();
        o2.setCustomerId("DUPE");
        o2.setAmount(200.0);
        o2.setStatus("CLOSED");
        morphium.store(o2);

        assertThatThrownBy(() -> repository.findByCustomerId("DUPE"))
                .isInstanceOf(NonUniqueResultException.class);
    }

    @Test
    @Order(3)
    @DisplayName("findByX returning T returns entity when exactly one result")
    void findSingle_exactlyOne_returnsEntity() {
        var o = new OrderEntity();
        o.setCustomerId("UNIQUE");
        o.setAmount(42.0);
        o.setStatus("OPEN");
        morphium.store(o);

        OrderEntity result = repository.findByCustomerId("UNIQUE");
        assertThat(result).isNotNull();
        assertThat(result.getCustomerId()).isEqualTo("UNIQUE");
    }

    // --- Query derivation: Optional<T> return type ---

    @Test
    @Order(4)
    @DisplayName("findOptionalByX returns Optional.empty() when no result (no exception)")
    void findOptional_noResult_returnsEmpty() {
        Optional<OrderEntity> result = repository.findOptionalByCustomerId("nonexistent");
        assertThat(result).isEmpty();
    }

    @Test
    @Order(5)
    @DisplayName("findOptionalByX throws NonUniqueResultException when multiple results")
    void findOptional_multipleResults_throwsNonUnique() {
        var o1 = new OrderEntity();
        o1.setCustomerId("DUPE2");
        o1.setAmount(10.0);
        o1.setStatus("OPEN");
        morphium.store(o1);

        var o2 = new OrderEntity();
        o2.setCustomerId("DUPE2");
        o2.setAmount(20.0);
        o2.setStatus("OPEN");
        morphium.store(o2);

        assertThatThrownBy(() -> repository.findOptionalByCustomerId("DUPE2"))
                .isInstanceOf(NonUniqueResultException.class);
    }

    // --- JDQL @Query: T return type ---

    @Test
    @Order(6)
    @DisplayName("@Query returning T throws EmptyResultException when no result")
    void jdql_noResult_throwsEmptyResult() {
        assertThatThrownBy(() -> repository.queryByCustomerId("nonexistent"))
                .isInstanceOf(EmptyResultException.class);
    }

    @Test
    @Order(7)
    @DisplayName("@Query returning T throws NonUniqueResultException when multiple results")
    void jdql_multipleResults_throwsNonUnique() {
        var o1 = new OrderEntity();
        o1.setCustomerId("JDUPE");
        o1.setAmount(10.0);
        o1.setStatus("OPEN");
        morphium.store(o1);

        var o2 = new OrderEntity();
        o2.setCustomerId("JDUPE");
        o2.setAmount(20.0);
        o2.setStatus("OPEN");
        morphium.store(o2);

        assertThatThrownBy(() -> repository.queryByCustomerId("JDUPE"))
                .isInstanceOf(NonUniqueResultException.class);
    }

    // --- JDQL @Query: Optional<T> return type ---

    @Test
    @Order(8)
    @DisplayName("@Query returning Optional<T> returns empty when no result")
    void jdqlOptional_noResult_returnsEmpty() {
        Optional<OrderEntity> result = repository.queryOptionalByCustomerId("nonexistent");
        assertThat(result).isEmpty();
    }

    @Test
    @Order(9)
    @DisplayName("@Query returning Optional<T> throws NonUniqueResultException when multiple results")
    void jdqlOptional_multipleResults_throwsNonUnique() {
        var o1 = new OrderEntity();
        o1.setCustomerId("JDUPE2");
        o1.setAmount(10.0);
        o1.setStatus("OPEN");
        morphium.store(o1);

        var o2 = new OrderEntity();
        o2.setCustomerId("JDUPE2");
        o2.setAmount(20.0);
        o2.setStatus("OPEN");
        morphium.store(o2);

        assertThatThrownBy(() -> repository.queryOptionalByCustomerId("JDUPE2"))
                .isInstanceOf(NonUniqueResultException.class);
    }
}
