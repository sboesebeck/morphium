package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.data.CursorHelper.SortSpec;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import jakarta.data.page.PageRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link CursorHelper}, focusing on the keyset-must-be-non-empty guard in
 * {@link CursorHelper#applyCursorCondition}. Uses a real {@link Morphium} instance backed by
 * {@link InMemoryDriver}, following the same setup pattern as {@link QueryExecutorTest}.
 */
class CursorHelperTest {

    private static Morphium morphium;

    @Entity
    static class Product {
        @Id
        private String id;
        private String name;
        private int amount;

        Product() {}

        Product(String id, String name, int amount) {
            this.id = id;
            this.name = name;
            this.amount = amount;
        }

        public String getId() { return id; }
        public String getName() { return name; }
        public int getAmount() { return amount; }
    }

    @BeforeAll
    static void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("test");
        cfg.addHostToSeed("localhost");
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
    }

    @AfterAll
    static void tearDown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    @BeforeEach
    void initCollection() {
        morphium.clearCollection(Product.class);
    }

    // -- BUG 1: cursor pagination without a sort keyset must fail loudly, not silently -----

    @Test
    @DisplayName("applyCursorCondition throws IllegalArgumentException when sortSpecs is empty")
    void applyCursorConditionThrowsOnEmptySortSpecs() {
        Query<Product> query = morphium.createQueryFor(Product.class);
        PageRequest.Cursor cursor = PageRequest.Cursor.forKey(200);

        assertThatThrownBy(() -> CursorHelper.applyCursorCondition(
                query, cursor, List.of(), morphium, Product.class, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-empty sort order");
    }

    @Test
    @DisplayName("applyCursorCondition throws IllegalArgumentException when sortSpecs is null")
    void applyCursorConditionThrowsOnNullSortSpecs() {
        Query<Product> query = morphium.createQueryFor(Product.class);
        PageRequest.Cursor cursor = PageRequest.Cursor.forKey(200);

        assertThatThrownBy(() -> CursorHelper.applyCursorCondition(
                query, cursor, null, morphium, Product.class, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-empty sort order");
    }

    @Test
    @DisplayName("applyCursorCondition with a non-empty sort keyset still builds the expected $or condition")
    void applyCursorConditionWithSortSpecsBuildsOrCondition() {
        Query<Product> query = morphium.createQueryFor(Product.class);
        PageRequest.Cursor cursor = PageRequest.Cursor.forKey(200);
        List<SortSpec> sortSpecs = List.of(new SortSpec("amount", true));

        CursorHelper.applyCursorCondition(query, cursor, sortSpecs, morphium, Product.class, true);

        assertThat(query.toQueryObject()).containsKey("$or");
    }
}
