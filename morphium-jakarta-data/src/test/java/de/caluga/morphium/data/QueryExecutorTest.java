package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.data.QueryDescriptor.Combinator;
import de.caluga.morphium.data.QueryDescriptor.Condition;
import de.caluga.morphium.data.QueryDescriptor.Operator;
import de.caluga.morphium.data.QueryDescriptor.Prefix;
import de.caluga.morphium.data.QueryDescriptor.ReturnType;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@link QueryExecutor#execute} behavior that is not specific to alias handling:
 * CONTAINS substring matching and the {@code DELETE} prefix's returned count. Uses a real
 * {@link Morphium} instance backed by {@link InMemoryDriver}, following the same setup pattern
 * as {@link QueryExecutorAliasTest} and {@link AbstractMorphiumRepositoryUpdateTest}.
 */
class QueryExecutorTest {

    private static Morphium morphium;

    @Entity
    static class Product {
        @Id
        private String id;
        private String name;
        private String status;

        Product() {}

        Product(String id, String name, String status) {
            this.id = id;
            this.name = name;
            this.status = status;
        }

        public String getId() { return id; }
        public String getName() { return name; }
        public String getStatus() { return status; }
    }

    static class ProductRepositoryImpl extends AbstractMorphiumRepository<Product, String> {
        ProductRepositoryImpl(Morphium morphium) {
            super(new RepositoryMetadata(Product.class, String.class, "id"));
            setMorphium(morphium);
        }
    }

    private ProductRepositoryImpl repo;

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
    void initRepo() {
        morphium.clearCollection(Product.class);
        repo = new ProductRepositoryImpl(morphium);
    }

    // -- BUG 1: CONTAINS must be a substring match, not an exact match -----------------

    @Test
    @DisplayName("CONTAINS generates a non-anchored $regex (substring match), not an exact-match equality")
    void containsGeneratesUnanchoredRegex() {
        QueryDescriptor descriptor = new QueryDescriptor(
                Prefix.FIND,
                List.of(new Condition("name", Operator.CONTAINS, 0)),
                Combinator.AND,
                List.of(),
                ReturnType.LIST
        );

        Query<?> query = morphium.createQueryFor(Product.class);
        QueryExecutor.applyConditions(query, descriptor, new Object[]{"idg"}, morphium, Product.class);
        Map<String, Object> queryObj = query.toQueryObject();

        assertThat(queryObj).containsKey("name");
        Object nameCondition = queryObj.get("name");
        // Must NOT be the plain raw argument (that would be an exact-match equality).
        assertThat(nameCondition).isNotEqualTo("idg");
        assertThat(nameCondition).isInstanceOf(Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> regexMap = (Map<String, Object>) nameCondition;
        assertThat(regexMap).containsKey("$regex");
        String regex = (String) regexMap.get("$regex");
        // Unanchored: no leading ^ / trailing $ around the literal.
        assertThat(regex).doesNotStartWith("^");
        assertThat(regex).doesNotEndWith("$");
        assertThat(regex).contains("\\Qidg\\E");
    }

    @Test
    @DisplayName("CONTAINS matches documents where the argument occurs anywhere in the field value")
    void containsMatchesSubstringAgainstRealData() {
        morphium.store(new Product("p1", "Widget", "ACTIVE"));
        morphium.store(new Product("p2", "Gadget", "ACTIVE"));
        morphium.store(new Product("p3", "Gizmo", "ACTIVE"));

        QueryDescriptor descriptor = new QueryDescriptor(
                Prefix.FIND,
                List.of(new Condition("name", Operator.CONTAINS, 0)),
                Combinator.AND,
                List.of(),
                ReturnType.LIST
        );

        @SuppressWarnings("unchecked")
        Object result = QueryExecutor.execute(descriptor, new Object[]{"dget"}, repo);
        @SuppressWarnings("unchecked")
        List<Product> found = (List<Product>) result;

        // "dget" is a substring of both "Widget" and "Gadget" but not "Gizmo" —
        // an exact-match CONTAINS (the bug) would find nothing at all.
        assertThat(found).extracting(Product::getName).containsExactlyInAnyOrder("Widget", "Gadget");
    }

    // -- BUG 2: DELETE must return the actually-deleted count, not a pre-delete count ---

    @Test
    @DisplayName("DELETE prefix returns the actually deleted document count")
    void deletePrefixReturnsActualDeletedCount() {
        morphium.store(new Product("p1", "Widget", "INACTIVE"));
        morphium.store(new Product("p2", "Gadget", "INACTIVE"));
        morphium.store(new Product("p3", "Gizmo", "ACTIVE"));

        QueryDescriptor descriptor = new QueryDescriptor(
                Prefix.DELETE,
                List.of(new Condition("status", Operator.EQ, 0)),
                Combinator.AND,
                List.of(),
                ReturnType.COUNT
        );

        Object result = QueryExecutor.execute(descriptor, new Object[]{"INACTIVE"}, repo);

        assertThat(result).isInstanceOf(Long.class);
        assertThat((Long) result).isEqualTo(2L);

        // Verify the documents are indeed gone and the untouched one remains.
        assertThat(morphium.createQueryFor(Product.class).countAll()).isEqualTo(1);
        assertThat(morphium.createQueryFor(Product.class).asList())
                .extracting(Product::getName)
                .containsExactly("Gizmo");
    }

    @Test
    @DisplayName("DELETE prefix returns 0 when no documents match")
    void deletePrefixReturnsZeroWhenNoMatch() {
        morphium.store(new Product("p1", "Widget", "ACTIVE"));

        QueryDescriptor descriptor = new QueryDescriptor(
                Prefix.DELETE,
                List.of(new Condition("status", Operator.EQ, 0)),
                Combinator.AND,
                List.of(),
                ReturnType.COUNT
        );

        Object result = QueryExecutor.execute(descriptor, new Object[]{"DOES-NOT-EXIST"}, repo);

        assertThat(result).isEqualTo(0L);
        assertThat(morphium.createQueryFor(Product.class).countAll()).isEqualTo(1);
    }
}
