package de.caluga.morphium.query;

import de.caluga.morphium.Collation;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("core")
public class FindAndModifyOptionsTest {

    @Test
    public void nothingIsSetByDefault() {
        FindAndModifyOptions o = FindAndModifyOptions.options();
        assertFalse(o.isUpsert());
        assertNull(o.getReturnDocument(), "there is deliberately no default");
        assertNull(o.getSort());
        assertNull(o.getProjection());
        assertNull(o.getHint());
        assertNull(o.getCollation());
        assertNull(o.getArrayFilters());
        assertNull(o.getMaxTimeMS());
    }

    @Test
    public void theSettersAreFluent() {
        Collation c = new Collation().locale("en");
        FindAndModifyOptions o = FindAndModifyOptions.options().upsert(true).returnDocument(ReturnDocument.AFTER).sort(Map.of("n", -1)).projection(Map.of("a", 1))
                                 .hint("idx").collation(c).arrayFilters(Map.of("e.x", 1)).maxTime(Duration.ofSeconds(2));
        assertTrue(o.isUpsert());
        assertEquals(ReturnDocument.AFTER, o.getReturnDocument());
        assertEquals(Map.of("n", -1), o.getSort());
        assertEquals(Map.of("a", 1), o.getProjection());
        assertEquals("idx", o.getHint());
        assertSame(c, o.getCollation());
        assertEquals(List.of(Map.of("e.x", 1)), o.getArrayFilters());
        assertEquals(2000, o.getMaxTimeMS());
    }

    @Test
    public void aHintIsAnIndexNameOrAKeyDocument() {
        assertEquals(Map.of("a", 1), FindAndModifyOptions.options().hint(Map.of("a", 1)).getHint());
        assertThrows(IllegalArgumentException.class, () -> FindAndModifyOptions.options().hint(""));
    }

    @Test
    public void invalidValuesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> FindAndModifyOptions.options().maxTime(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> FindAndModifyOptions.options().maxTime(Duration.ofSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> FindAndModifyOptions.options().maxTime(Duration.ofDays(60)), "does not fit into an int of milliseconds");
        assertThrows(IllegalArgumentException.class, () -> FindAndModifyOptions.options().projection(Map.of()));
        assertThrows(IllegalArgumentException.class, () -> FindAndModifyOptions.options().returnDocument(null));
    }

    @Test
    public void theGettersReturnCopies() {
        Map<String, Object> projection = new java.util.HashMap<>(Map.of("a", 1));
        FindAndModifyOptions o = FindAndModifyOptions.options().projection(projection);
        projection.put("b", 1);
        assertEquals(Map.of("a", 1), o.getProjection(), "the options are not changed by a later change of the map");
    }
}
