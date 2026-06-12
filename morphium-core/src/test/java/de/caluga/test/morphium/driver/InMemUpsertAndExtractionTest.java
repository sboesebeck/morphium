package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.DeleteMongoCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the InMemoryDriver upsert bug: equality predicates nested inside an
 * {@code $and} filter must seed the upserted document (mirroring MongoDB), so that a fixed
 * {@code _id} is used instead of a freshly generated one.
 *
 * <p>Concrete trigger: the quarkus-morphium migration lock acquires its lock via
 * {@code findAndModify({$and:[{_id:"migration_lock"},{expires_at:{$lte:now}}]}, upsert:true)}
 * and later deletes by {@code _id="migration_lock"}. Before the fix the upserted document got a
 * generated ObjectId, so the delete never matched and the lock leaked.
 */
@Tag("inmemory")
public class InMemUpsertAndExtractionTest {

    private static final String DB = "upsert_and_db";
    private static final String COLL = "locks";

    private InMemoryDriver driver;

    @BeforeEach
    public void setUp() throws Exception {
        driver = new InMemoryDriver();
        driver.connect();
        new ClearCollectionCommand(driver).setDb(DB).setColl(COLL).doClear();
    }

    @AfterEach
    public void tearDown() {
        driver.close();
    }

    @Test
    public void upsertSeedsIdFromAndFilter() throws Exception {
        Date now = new Date();
        // {$and:[{_id:"migration_lock"},{expires_at:{$lte:now}}]} — exactly what the lock acquire builds
        Map<String, Object> filter = and(
                eq("_id", "migration_lock"),
                operator("expires_at", "$lte", now));
        Map<String, Object> setOwner = set("owner", "owner-1");

        Map<String, Object> result = upsert(filter, setOwner);

        assertThat(result.get("upserted")).as("upsert should have inserted a document").isNotNull();

        Map<String, Object> stored = onlyDocument();
        assertThat(stored.get("_id"))
                .as("_id must be seeded from the $and equality predicate, not generated")
                .isEqualTo("migration_lock");
        assertThat(stored.get("owner")).isEqualTo("owner-1");
    }

    @Test
    public void deleteByIdRemovesUpsertedLock() throws Exception {
        Date now = new Date();
        Map<String, Object> filter = and(
                eq("_id", "migration_lock"),
                operator("expires_at", "$lte", now));

        upsert(filter, set("owner", "owner-1"));

        // This is exactly the regression: releaseLock() deletes by _id="migration_lock".
        // Before the fix the upserted doc had a generated id, so this delete matched nothing
        // and the lock leaked.
        deleteById("migration_lock");

        assertThat(find(Doc.of()))
                .as("delete by the fixed _id must remove the upserted lock")
                .isEmpty();
    }

    @Test
    public void upsertSeedsIdFromNestedAndFilter() throws Exception {
        // {$and:[{$and:[{_id:"nested"}]}]} — recursion must still reach the equality field
        Map<String, Object> filter = and(and(eq("_id", "nested")));

        upsert(filter, set("owner", "owner-1"));

        assertThat(onlyDocument().get("_id"))
                .as("_id must be seeded through nested $and recursion")
                .isEqualTo("nested");
    }

    @Test
    public void operatorPredicatesDoNotSeedUpsert() throws Exception {
        // _id is an equality predicate (seeded); counter has only a $gt predicate (not seeded).
        Map<String, Object> filter = and(
                eq("_id", "x"),
                operator("counter", "$gt", 5));

        upsert(filter, set("name", "n"));

        Map<String, Object> stored = onlyDocument();
        assertThat(stored.get("_id")).isEqualTo("x");
        assertThat(stored.get("name")).isEqualTo("n");
        assertThat(stored.get("counter"))
                .as("an operator predicate ($gt) must not seed a value")
                .isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void dottedEqualityPredicateSeedsNestedDocument() throws Exception {
        // {$and:[{_id:"x"},{"a.b": 1}]} — MongoDB seeds the dotted path as a nested doc {a:{b:1}}
        Map<String, Object> filter = and(
                eq("_id", "x"),
                eq("a.b", 1));

        upsert(filter, set("name", "n"));

        Map<String, Object> stored = onlyDocument();
        assertThat(stored.get("_id")).isEqualTo("x");
        assertThat(stored.get("a"))
                .as("dotted path a.b must be seeded as a nested document, not a literal key")
                .isInstanceOf(Map.class);
        assertThat(((Map<String, Object>) stored.get("a")).get("b")).isEqualTo(1);
        assertThat(stored.containsKey("a.b"))
                .as("the literal key \"a.b\" must not exist")
                .isFalse();
    }

    // --- helpers -------------------------------------------------------------

    /** {field: value} — an equality predicate. */
    private Map<String, Object> eq(String field, Object value) {
        return Doc.of(field, value);
    }

    /** {field: {op: value}} — an operator predicate such as {@code {$lte: now}}. */
    private Map<String, Object> operator(String field, String op, Object value) {
        return Doc.of(field, Doc.of(op, value));
    }

    /** {$and: [branch, branch, ...]}. */
    @SafeVarargs
    private Map<String, Object> and(Map<String, Object>... branches) {
        return Doc.of("$and", List.of(branches));
    }

    /** {$set: {field: value}}. */
    private Map<String, Object> set(String field, Object value) {
        return Doc.of("$set", Doc.of(field, value));
    }

    private Map<String, Object> upsert(Map<String, Object> filter, Map<String, Object> update) throws Exception {
        return new UpdateMongoCommand(driver)
                .setDb(DB).setColl(COLL)
                .addUpdate(Doc.of("q", filter, "u", update, "upsert", true))
                .execute();
    }

    private List<Map<String, Object>> find(Map<String, Object> filter) throws Exception {
        return new FindCommand(driver).setDb(DB).setColl(COLL).setFilter(filter).execute();
    }

    private void deleteById(Object id) throws Exception {
        // limit 1: deleting by a unique _id matches at most one document
        new DeleteMongoCommand(driver).setDb(DB).setColl(COLL)
                .addDelete(Doc.of("_id", id), 1, null, null)
                .execute();
    }

    private Map<String, Object> onlyDocument() throws Exception {
        List<Map<String, Object>> all = find(Doc.of());
        assertThat(all).hasSize(1);
        return all.get(0);
    }
}
