package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Jakarta Data #3: deleteAll() no-arg + deleteBy* query derivation.
 */
@QuarkusTest
@DisplayName("Jakarta Data Delete — deleteAll() no-arg + deleteBy* derivation")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataDeleteTest {

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
    @DisplayName("deleteByStatus returns count of deleted entities")
    void deleteByStatus_returnsCount() {
        morphium.store(order("C1", 100, "OPEN"));
        morphium.store(order("C2", 200, "OPEN"));
        morphium.store(order("C3", 300, "OPEN"));
        morphium.store(order("C4", 400, "CLOSED"));
        morphium.store(order("C5", 500, "CLOSED"));

        long deleted = repository.deleteByStatus("OPEN");

        assertThat(deleted).isEqualTo(3);
        assertThat(repository.findByStatus("OPEN")).isEmpty();
        assertThat(repository.findByStatus("CLOSED")).hasSize(2);
    }

    @Test
    @Order(2)
    @DisplayName("deleteByStatus with no match returns zero")
    void deleteByStatus_noMatch_returnsZero() {
        morphium.store(order("C1", 100, "OPEN"));
        morphium.store(order("C2", 200, "OPEN"));

        long deleted = repository.deleteByStatus("CLOSED");

        assertThat(deleted).isZero();
        assertThat(repository.countByStatus("OPEN")).isEqualTo(2);
    }

    @Test
    @Order(3)
    @DisplayName("deleteByAmountLessThan (void return) deletes matching entities")
    void deleteByAmountLessThan_void() {
        morphium.store(order("C1", 50, "OPEN"));
        morphium.store(order("C2", 100, "OPEN"));
        morphium.store(order("C3", 200, "OPEN"));

        repository.deleteByAmountLessThan(100.0);

        List<OrderEntity> remaining = repository.findByStatus("OPEN");
        assertThat(remaining).hasSize(2);
        assertThat(remaining).extracting(OrderEntity::getAmount)
                .containsExactlyInAnyOrder(100.0, 200.0);
    }

    @Test
    @Order(4)
    @DisplayName("deleteByCustomerId (boolean return) returns true when entities deleted")
    void deleteByCustomerId_boolean_true() {
        morphium.store(order("C1", 100, "OPEN"));

        boolean deleted = repository.deleteByCustomerId("C1");

        assertThat(deleted).isTrue();
        assertThat(repository.findByStatus("OPEN")).isEmpty();
    }

    @Test
    @Order(5)
    @DisplayName("deleteByCustomerId (boolean return) returns false when nothing to delete")
    void deleteByCustomerId_boolean_false() {
        boolean deleted = repository.deleteByCustomerId("C1");

        assertThat(deleted).isFalse();
    }

    @Test
    @Order(6)
    @DisplayName("deleteAll() no-arg clears entire collection")
    void deleteAll_noArg_clearsCollection() {
        morphium.store(order("C1", 100, "OPEN"));
        morphium.store(order("C2", 200, "OPEN"));
        morphium.store(order("C3", 300, "CLOSED"));
        morphium.store(order("C4", 400, "CLOSED"));
        morphium.store(order("C5", 500, "PENDING"));

        repository.deleteAll();

        assertThat(repository.findAll().toList()).isEmpty();
    }

    @Test
    @Order(7)
    @DisplayName("deleteAll() no-arg on empty collection does not throw")
    void deleteAll_noArg_emptyCollection() {
        repository.deleteAll();

        assertThat(repository.findAll().toList()).isEmpty();
    }

    @Test
    @Order(8)
    @DisplayName("silent data loss fix: remove(entity) with an entity-typed @Delete parameter actually deletes that document")
    void removeByEntityParameter_deletesTheDocument() {
        OrderEntity toDelete = order("C1", 100, "OPEN");
        morphium.store(toDelete);
        morphium.store(order("C2", 200, "OPEN"));
        morphium.store(order("C3", 300, "CLOSED"));

        // This is the check the original bug report needed: the document count BEFORE and AFTER
        // the call. Before the fix, the entity parameter was misclassified as a @By-condition
        // (falling back to the parameter name), building a bogus query such as
        // {order: <OrderEntity instance>} that never matches anything in MongoDB -- so
        // query.delete() silently removed zero documents while the method still returned
        // normally. Asserting only "no exception was thrown" would NOT have caught that; the
        // count comparison is what actually detects the data loss.
        long countBefore = morphium.createQueryFor(OrderEntity.class).countAll();
        assertThat(countBefore).isEqualTo(3);

        repository.remove(toDelete);

        long countAfter = morphium.createQueryFor(OrderEntity.class).countAll();
        assertThat(countAfter).isEqualTo(countBefore - 1);
        assertThat(morphium.createQueryFor(OrderEntity.class)
                .f("customerId").eq("C1").countAll()).isZero();
        assertThat(morphium.createQueryFor(OrderEntity.class)
                .f("customerId").eq("C2").countAll()).isEqualTo(1);
        assertThat(morphium.createQueryFor(OrderEntity.class)
                .f("customerId").eq("C3").countAll()).isEqualTo(1);
    }

    private OrderEntity order(String customerId, double amount, String status) {
        var o = new OrderEntity();
        o.setCustomerId(customerId);
        o.setAmount(amount);
        o.setStatus(status);
        return o;
    }
}
