package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link MorphiumItemRepository} — verifies that
 * MorphiumRepository's distinct(), morphium() and query() methods work.
 */
@QuarkusTest
@DisplayName("MorphiumRepository operations")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MorphiumDataMorphiumRepositoryTest {

    @Inject
    MorphiumItemRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void cleanCollection() {
        morphium.clearCollection(ItemEntity.class);
    }

    @AfterEach
    void resetThreadLocals() {
        morphium.resetThreadLocalOverrides();
    }

    @Test
    @Order(1)
    @DisplayName("MorphiumRepository is injectable")
    void repository_isInjectable() {
        assertThat(repository).isNotNull();
    }

    @Test
    @Order(2)
    @DisplayName("CRUD operations work through MorphiumRepository")
    void crud_worksThroughMorphiumRepository() {
        var item = new ItemEntity();
        item.setName("Widget");
        item.setPrice(9.99);
        item.setTag("tools");
        repository.save(item);

        assertThat(item.getId()).isNotNull();
        assertThat(repository.findById(item.getId())).isPresent();
    }

    @Test
    @Order(3)
    @DisplayName("distinct() returns unique field values")
    void distinct_returnsUniqueValues() {
        createItem("A", "electronics");
        createItem("B", "electronics");
        createItem("C", "tools");
        createItem("D", "books");

        List<Object> distinctTags = repository.distinct("tag");
        assertThat(distinctTags).containsExactlyInAnyOrder("electronics", "tools", "books");
    }

    @Test
    @Order(4)
    @DisplayName("morphium() returns the Morphium instance")
    void morphium_returnsMorphiumInstance() {
        Morphium m = repository.morphium();
        assertThat(m).isNotNull();
        assertThat(m).isSameAs(morphium);
    }

    @Test
    @Order(5)
    @DisplayName("query() creates a typed Query for the entity")
    void query_createsTypedQuery() {
        createItem("Alpha", "tools");
        createItem("Beta", "electronics");

        Query<ItemEntity> q = repository.query();
        assertThat(q).isNotNull();

        q.f("tag").eq("tools");
        List<ItemEntity> results = q.asList();
        assertThat(results).hasSize(1);
        assertThat(results.get(0).getName()).isEqualTo("Alpha");
    }

    @Test
    @Order(6)
    @DisplayName("query derivation works through MorphiumRepository")
    void queryDerivation_worksThroughMorphiumRepository() {
        createItem("X", "widgets");
        createItem("Y", "widgets");
        createItem("Z", "gadgets");

        List<ItemEntity> widgets = repository.findByTag("widgets");
        assertThat(widgets).hasSize(2);
    }

    private void createItem(String name, String tag) {
        var item = new ItemEntity();
        item.setName(name);
        item.setPrice(10.0);
        item.setTag(tag);
        repository.save(item);
    }
}
