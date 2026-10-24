package de.caluga.morphium.objectmapping;

import de.caluga.morphium.driver.Doc;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-014: the helper that tells a test whether a stored document holds snake_case keys, which is how a mismatch between an owner and an embedded class shows up in the data.
 */
@Tag("core")
public class StoredDocumentKeysTest {

    @Test
    public void idAndClassNameAreNotSnakeCase() {
        assertTrue(StoredDocumentKeys.snakeCaseKeys(Doc.of("_id", 1, "ownerName", "n", "class_name", "x.Y")).isEmpty());
    }

    @Test
    public void nestedDocumentsListsAndMapsAreWalkedAndThePathIsGiven() {
        Map<String, Object> doc = Doc.of("_id", 1, "ownerName", "n",
                                         "child", Doc.of("file_name", "f"),
                                         "children", List.of(Doc.of("fileName", "ok"), Doc.of("item_version_id", "v", "class_name", "x.Y")),
                                         "map", Doc.of("outer", Doc.of("deep_value", 1)));
        assertEquals(List.of("child.file_name", "children.1.item_version_id", "map.outer.deep_value"), StoredDocumentKeys.snakeCaseKeys(doc));
    }

    @Test
    public void aDocumentWithoutUnderscoresIsClean() {
        assertTrue(StoredDocumentKeys.snakeCaseKeys(Doc.of("_id", 1, "a", Doc.of("b", List.of(1, 2)))).isEmpty());
        assertTrue(StoredDocumentKeys.snakeCaseKeys(null).isEmpty());
    }
}
