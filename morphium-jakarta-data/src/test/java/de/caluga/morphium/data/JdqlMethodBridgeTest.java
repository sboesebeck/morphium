package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@link JdqlMethodBridge#executeJdql} LIKE-pattern handling. Uses a real
 * {@link Morphium} instance backed by {@link InMemoryDriver}, following the same setup pattern
 * as {@link QueryExecutorTest}.
 *
 * <p>Regression test for a bug where JDQL {@code LIKE} built its regex directly from the raw
 * literal (only translating {@code %}/{@code _} wildcards) without escaping regex metacharacters
 * or anchoring the pattern with {@code ^...$} — unlike the derived-query {@code LIKE} path, which
 * already used {@link QueryExecutor#likeToRegex} correctly. As a result, {@code WHERE code LIKE
 * 'A.1'} would match {@code "AX1"} (the {@code .} was interpreted as regex "any character"
 * instead of a literal dot), and a wildcard-free pattern like {@code WHERE name LIKE 'Widget'}
 * matched any value merely containing {@code "Widget"} instead of requiring an exact match.
 */
class JdqlMethodBridgeTest {

    private static Morphium morphium;

    @Entity
    static class Product {
        @Id
        private String id;
        private String code;
        private String name;

        Product() {}

        Product(String id, String code, String name) {
            this.id = id;
            this.code = code;
            this.name = name;
        }

        public String getId() { return id; }
        public String getCode() { return code; }
        public String getName() { return name; }
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

    @Test
    @DisplayName("JDQL LIKE without wildcards requires an exact match, not a substring match")
    void likeWithoutWildcardsRequiresExactMatch() {
        morphium.store(new Product("p1", "C1", "Widget"));
        morphium.store(new Product("p2", "C2", "SuperWidgetPro"));

        Object result = JdqlMethodBridge.executeJdql(
                repo, "WHERE name LIKE :name", "name:0",
                -1, -1, -1, -1, new Object[]{"Widget"},
                false, false, false, false, false, "", false, null);

        @SuppressWarnings("unchecked")
        List<Product> found = (List<Product>) result;

        // "SuperWidgetPro" contains "Widget" as a substring; the unanchored-regex bug
        // would incorrectly match it too. Only the exact match must be returned.
        assertThat(found).extracting(Product::getName).containsExactly("Widget");
    }

    @Test
    @DisplayName("JDQL LIKE escapes regex metacharacters in the literal portion of the pattern")
    void likeEscapesRegexMetacharacters() {
        morphium.store(new Product("p1", "A.1", "Dotted"));
        morphium.store(new Product("p2", "AX1", "AnyChar"));

        Object result = JdqlMethodBridge.executeJdql(
                repo, "WHERE code LIKE :code", "code:0",
                -1, -1, -1, -1, new Object[]{"A.1"},
                false, false, false, false, false, "", false, null);

        @SuppressWarnings("unchecked")
        List<Product> found = (List<Product>) result;

        // A literal "." in the LIKE pattern must match only a literal dot, not "any character" --
        // the unescaped-regex bug would treat it as a regex wildcard and also match "AX1".
        assertThat(found).extracting(Product::getCode).containsExactly("A.1");
    }

    @Test
    @DisplayName("JDQL LIKE still supports % and _ SQL wildcards after the fix")
    void likeStillSupportsSqlWildcards() {
        morphium.store(new Product("p1", "C1", "Widget"));
        morphium.store(new Product("p2", "C2", "Gadget"));
        morphium.store(new Product("p3", "C3", "Gizmo"));

        Object result = JdqlMethodBridge.executeJdql(
                repo, "WHERE name LIKE :pattern", "pattern:0",
                -1, -1, -1, -1, new Object[]{"%dget"},
                false, false, false, false, false, "", false, null);

        @SuppressWarnings("unchecked")
        List<Product> found = (List<Product>) result;

        assertThat(found).extracting(Product::getName).containsExactlyInAnyOrder("Widget", "Gadget");
    }
}
