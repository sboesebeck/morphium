package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for queries on nested (dotted) fields against the in-memory driver:
 * find with $regex / $eq / $or on dotted paths (optionally with a projection) and distinct on a
 * dotted field. A dotted query key like "meta.source" must be resolved into the nested sub-document
 * instead of being looked up / rewritten as a flat key.
 */
@Tag("inmemory")
public class InMemNestedFieldQueryTest {
    String db = "nesteddb";
    String coll = "nestedcoll";

    @Test
    public void nestedRegexWithProjectionFindsDocument() throws Exception {
        var drv = new InMemoryDriver();
        drv.connect();
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(Doc.of(
                "name", "doc1",
                "category", "books",
                "meta", Doc.of("source", "import-2026-03-28.zip"))))
            .execute();

        var f = new FindCommand(drv).setDb(db).setColl(coll)
            .setFilter(Doc.of("meta.source", Doc.of("$regex", "import-2026-03-28.zip")))
            .setProjection(Doc.of("category", 1))
            .setLimit(1);
        List<Map<String, Object>> res = f.execute();

        assertEquals(1, res.size(), "nested $regex + projection must find the document");
        assertEquals("books", res.get(0).get("category"));
    }

    @Test
    public void nestedRegexWithoutProjectionFindsDocument() throws Exception {
        var drv = new InMemoryDriver();
        drv.connect();
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(Doc.of(
                "category", "books",
                "meta", Doc.of("source", "import-2026-03-28.zip"))))
            .execute();

        var f = new FindCommand(drv).setDb(db).setColl(coll)
            .setFilter(Doc.of("meta.source", Doc.of("$regex", "import-2026-03-28.zip")));
        List<Map<String, Object>> res = f.execute();

        assertEquals(1, res.size(), "nested $regex must find the document");
    }

    @Test
    public void topLevelNestedEqFindsDocument() throws Exception {
        // {category: x, meta.date: y} - dotted key as a top-level query field with $eq
        var drv = new InMemoryDriver();
        drv.connect();
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(Doc.of(
                "category", "books",
                "meta", Doc.of("date", "2026-03-28"))))
            .execute();

        var f = new FindCommand(drv).setDb(db).setColl(coll)
            .setFilter(Doc.of("category", "books", "meta.date", "2026-03-28"));
        List<Map<String, Object>> res = f.execute();

        assertEquals(1, res.size(), "top-level nested .eq must find the document");
    }

    @Test
    public void orWithNestedEqFindsDocument() throws Exception {
        // dotted keys nested inside $or branches
        var drv = new InMemoryDriver();
        drv.connect();
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(Doc.of(
                "category", "books",
                "related", Doc.of("primary", "id-123"))))
            .execute();

        var f = new FindCommand(drv).setDb(db).setColl(coll)
            .setFilter(Doc.of(
                "category", "books",
                "$or", List.of(
                    Doc.of("related.primary", "id-123"),
                    Doc.of("related.secondary", "id-123"))));
        List<Map<String, Object>> res = f.execute();

        assertEquals(1, res.size(), "nested .eq inside $or must find the document");
    }

    @Test
    public void distinctOnNestedFieldReturnsValues() throws Exception {
        // distinct on a nested field "meta.source" with a filter that also uses a dotted key
        var drv = new InMemoryDriver();
        drv.connect();
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(
                Doc.of("category", "books", "meta", Doc.of("date", "2026-03-28", "source", "a.zip")),
                Doc.of("category", "books", "meta", Doc.of("date", "2026-03-28", "source", "b.zip"))))
            .execute();

        List<Object> values = drv.distinct(db, coll, "meta.source",
            Doc.of("category", "books", "meta.date", "2026-03-28"), null);

        assertEquals(2, values.size(), "distinct on nested field must return the nested values");
        assertTrue(values.contains("a.zip"));
        assertTrue(values.contains("b.zip"));
    }
}
