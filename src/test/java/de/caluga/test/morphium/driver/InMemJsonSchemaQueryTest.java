package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@Tag("inmemory")
public class InMemJsonSchemaQueryTest {
    private final String db = "jsonschemadb";
    private final String coll = "jsonschemacoll";

    @Test
    public void matchesDocumentsThatSatisfySchema() throws Exception {
        var drv = new InMemoryDriver();
        drv.connect();

        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(
                Doc.of("name", "Alice", "age", 32, "tags", List.of("admin", "beta")),
                Doc.of("name", "Bob", "age", 16, "tags", List.of("user"))
            ))
            .execute();

        Doc schema = Doc.of(
            "bsonType", "object",
            "required", List.of("name", "age"),
            "properties", Doc.of(
                "name", Doc.of("bsonType", "string", "minLength", 3),
                "age", Doc.of("bsonType", List.of("int", "long"), "minimum", 18),
                "tags", Doc.of(
                    "bsonType", "array",
                    "items", Doc.of("bsonType", "string"),
                    "uniqueItems", true
                )
            )
        );

        List<Map<String, Object>> result = new FindCommand(drv)
            .setDb(db)
            .setColl(coll)
            .setFilter(Doc.of("$jsonSchema", schema))
            .execute();

        assertEquals(1, result.size(), "Only Alice should satisfy the schema");
        assertEquals("Alice", result.get(0).get("name"));
    }

    @Test
    public void rejectsDocumentsMissingRequiredFieldsOrWithExtras() throws Exception {
        var drv = new InMemoryDriver();
        drv.connect();

        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(
                Doc.of("code", "ABC", "meta", Doc.of("trace", "foo")),
                Doc.of("code", "XYZ", "extra", 42),
                Doc.of("meta", Doc.of("trace", "missing"))
            ))
            .execute();

        Doc schema = Doc.of(
            "bsonType", "object",
            "required", List.of("code"),
            "properties", Doc.of(
                "code", Doc.of("bsonType", "string", "pattern", "^[A-Z]{3}$"),
                "meta", Doc.of("bsonType", "object")
            ),
            "additionalProperties", false
        );

        List<Map<String, Object>> result = new FindCommand(drv)
            .setDb(db)
            .setColl(coll)
            .setFilter(Doc.of("$jsonSchema", schema))
            .execute();

        assertEquals(1, result.size(), "Only the document without extra fields should match");
        assertEquals("ABC", result.get(0).get("code"));
    }

    @Test
    public void enforcesArrayItemConstraints() throws Exception {
        var drv = new InMemoryDriver();
        drv.connect();

        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(
                Doc.of("name", "Ivan", "scores", List.of(10, 20)),
                Doc.of("name", "Jill", "scores", List.of(10, 10)),
                Doc.of("name", "NoScores")
            ))
            .execute();

        Doc schema = Doc.of(
            "bsonType", "object",
            "properties", Doc.of(
                "scores", Doc.of(
                    "bsonType", "array",
                    "items", Doc.of("bsonType", "int", "minimum", 0),
                    "minItems", 2,
                    "maxItems", 4,
                    "uniqueItems", true
                )
            )
        );

        List<Map<String, Object>> result = new FindCommand(drv)
            .setDb(db)
            .setColl(coll)
            .setFilter(Doc.of("$jsonSchema", schema))
            .execute();

        List<String> names = result.stream()
            .map(doc -> (String) doc.get("name"))
            .collect(Collectors.toList());

        assertTrue(names.contains("Ivan"), "Ivan should satisfy array constraints");
        assertTrue(names.contains("NoScores"), "Documents without the optional field should pass");
        assertFalse(names.contains("Jill"), "Jill should be rejected due to non-unique scores");
    }
}
