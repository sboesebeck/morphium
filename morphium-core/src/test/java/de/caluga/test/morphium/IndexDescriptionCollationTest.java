package de.caluga.test.morphium;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MongoDB 9.0 reports {@code collation: {locale: "simple"}} in listIndexes for indexes created
 * without a collation (SERVER-89953). The index check must not treat those as missing.
 */
@Tag("core")
public class IndexDescriptionCollationTest {

    private static IndexDescription fromEntity() {
        return IndexDescription.fromMaps(Doc.of("counter", 1), null);
    }

    private static IndexDescription fromServer(Doc collation) {
        var idx = Doc.of("key", Doc.of("counter", 1), "name", "counter_1", "v", 2);
        if (collation != null) {
            idx.put("collation", collation);
        }
        return IndexDescription.fromMap(idx);
    }

    @Test
    void simpleCollationEqualsNoCollation() {
        var server = fromServer(Doc.of("locale", "simple"));
        var entity = fromEntity();

        assertEquals(entity, server);
        assertEquals(server, entity);
        assertEquals(entity.hashCode(), server.hashCode());
        // getMissingIndicesFor() relies on List.contains
        assertTrue(List.of(server).contains(entity));
    }

    @Test
    void simpleCollationWithVersionFieldEqualsNoCollation() {
        assertEquals(fromEntity(), fromServer(Doc.of("locale", "simple", "version", "57.1")));
    }

    @Test
    void realCollationStillDiffers() {
        var server = fromServer(Doc.of("locale", "de", "strength", 2));

        assertNotEquals(fromEntity(), server);
        assertFalse(List.of(server).contains(fromEntity()));
    }

    @Test
    void serverAddedVersionAndDefaultNormalizationAreIgnored() {
        var fromAnnotation = fromServer(Doc.of("locale", "de", "strength", 2));
        var server = fromServer(Doc.of("locale", "de", "strength", 2, "normalization", false, "version", "57.1"));

        assertEquals(fromAnnotation, server);
        assertEquals(fromAnnotation.hashCode(), server.hashCode());
        assertNotEquals(fromAnnotation, fromServer(Doc.of("locale", "de", "strength", 2, "normalization", true)));
    }

    @Test
    void sameRealCollationStillEqual() {
        var a = fromServer(Doc.of("locale", "de", "strength", 2));
        var b = fromServer(Doc.of("locale", "de", "strength", 2));

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }
}
