package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Jakarta Data #2: missing query derivation operators.
 * Tests Contains, NotContains, IsEmpty, IsNotEmpty, Size, Matches, IgnoreCase.
 */
@QuarkusTest
@DisplayName("Jakarta Data Query Derivation — New Operators")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataOperatorTest {

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
    @DisplayName("findByTagsContains finds orders containing the given tag")
    void findByTagsContains_findsMatching() {
        var o1 = order("C1", 100, "OPEN", List.of("VIP", "RUSH"));
        var o2 = order("C2", 200, "OPEN", List.of("STANDARD"));
        morphium.store(o1);
        morphium.store(o2);

        List<OrderEntity> result = repository.findByTagsContains("VIP");

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getCustomerId()).isEqualTo("C1");
    }

    @Test
    @Order(2)
    @DisplayName("findByTagsNotContains excludes orders containing the given tag")
    void findByTagsNotContains_excludesMatching() {
        var o1 = order("C1", 100, "OPEN", List.of("VIP", "RUSH"));
        var o2 = order("C2", 200, "OPEN", List.of("STANDARD"));
        var o3 = order("C3", 50, "CLOSED", null);
        morphium.store(o1);
        morphium.store(o2);
        morphium.store(o3);

        List<OrderEntity> result = repository.findByTagsNotContains("VIP");

        assertThat(result).hasSize(2);
        assertThat(result).extracting(OrderEntity::getCustomerId)
                .containsExactlyInAnyOrder("C2", "C3");
    }

    @Test
    @Order(3)
    @DisplayName("findByTagsIsEmpty finds orders with empty tags array")
    void findByTagsIsEmpty_findsEmpty() {
        var o1 = order("C1", 100, "OPEN", List.of("VIP"));
        var o2 = order("C2", 200, "OPEN", List.of());
        morphium.store(o1);
        morphium.store(o2);

        List<OrderEntity> result = repository.findByTagsIsEmpty();

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getCustomerId()).isEqualTo("C2");
    }

    @Test
    @Order(4)
    @DisplayName("findByTagsIsNotEmpty finds orders with non-empty tags")
    void findByTagsIsNotEmpty_findsNonEmpty() {
        var o1 = order("C1", 100, "OPEN", List.of("VIP"));
        var o2 = order("C2", 200, "OPEN", List.of());
        morphium.store(o1);
        morphium.store(o2);

        List<OrderEntity> result = repository.findByTagsIsNotEmpty();

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getCustomerId()).isEqualTo("C1");
    }

    @Test
    @Order(5)
    @DisplayName("findByTagsSize matches exact array size")
    void findByTagsSize_matchesExact() {
        var o1 = order("C1", 100, "OPEN", List.of());
        var o2 = order("C2", 200, "OPEN", List.of("VIP", "RUSH"));
        var o3 = order("C3", 50, "CLOSED", List.of("A", "B", "C"));
        morphium.store(o1);
        morphium.store(o2);
        morphium.store(o3);

        List<OrderEntity> result = repository.findByTagsSize(2);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getCustomerId()).isEqualTo("C2");
    }

    @Test
    @Order(6)
    @DisplayName("findByCustomerIdMatches filters by regex pattern")
    void findByCustomerIdMatches_regex() {
        var o1 = order("CUST-001", 100, "OPEN", List.of());
        var o2 = order("CUST-002", 200, "OPEN", List.of());
        var o3 = order("OTHER", 50, "CLOSED", List.of());
        morphium.store(o1);
        morphium.store(o2);
        morphium.store(o3);

        List<OrderEntity> result = repository.findByCustomerIdMatches("CUST-.*");

        assertThat(result).hasSize(2);
        assertThat(result).extracting(OrderEntity::getCustomerId)
                .containsExactlyInAnyOrder("CUST-001", "CUST-002");
    }

    @Test
    @Order(7)
    @DisplayName("findByStatusIgnoreCase matches case-insensitively")
    void findByStatusIgnoreCase_caseInsensitive() {
        var o1 = order("C1", 100, "OPEN", List.of());
        var o2 = order("C2", 200, "Open", List.of());
        var o3 = order("C3", 50, "open", List.of());
        var o4 = order("C4", 75, "CLOSED", List.of());
        morphium.store(o1);
        morphium.store(o2);
        morphium.store(o3);
        morphium.store(o4);

        List<OrderEntity> result = repository.findByStatusIgnoreCase("open");

        assertThat(result).hasSize(3);
        assertThat(result).extracting(OrderEntity::getCustomerId)
                .containsExactlyInAnyOrder("C1", "C2", "C3");
    }

    private OrderEntity order(String customerId, double amount, String status, List<String> tags) {
        var o = new OrderEntity();
        o.setCustomerId(customerId);
        o.setAmount(amount);
        o.setStatus(status);
        o.setTags(tags);
        return o;
    }
}
