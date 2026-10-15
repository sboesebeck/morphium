package de.caluga.morphium.query;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The table is derived from the probe on a real MongoDB, see docs/research/upsert-id-seeding-probe.md:
 * "pinned" means that the server seeds the _id of an upserted document from the filter (or the filter has a top-level
 * _id key, which was always treated as pinned), so {@code Query#findOneAndUpdate} must not add a generated _id.
 */
@Tag("core")
public class UpsertFilterInspectorTest {

    private static Map<String, Object> m(Object... kv) {
        Map<String, Object> r = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            r.put((String) kv[i], kv[i + 1]);
        }
        return r;
    }

    private static List<Object> l(Object... v) {
        return new ArrayList<>(java.util.Arrays.asList(v));
    }

    private static void pinned(Map<String, Object> filter) {
        assertTrue(UpsertFilterInspector.pinsId(filter), "expected pinned: " + filter);
    }

    private static void notPinned(Map<String, Object> filter) {
        assertFalse(UpsertFilterInspector.pinsId(filter), "expected NOT pinned: " + filter);
    }

    @Test
    public void nullAndEmptyFilterDoNotPin() {
        notPinned(null);
        notPinned(m());
    }

    @Test
    public void topLevelIdKeyAlwaysPins() {
        pinned(m("_id", "X"));
        pinned(m("_id", m("$eq", "X")));
        // unchanged historic behaviour: a top-level _id key never gets a generated id, whatever the predicate
        pinned(m("_id", m("$gt", "A")));
        pinned(m("_id", m("$in", l("X", "Y"))));
        pinned(m("_id", Pattern.compile("^X")));
        pinned(m("_id", null));
    }

    @Test
    public void dottedIdKeyPins() {
        pinned(m("_id.a", 1));
        pinned(m("$and", l(m("_id.a", 1))));
    }

    @Test
    public void equalityInsideAndPins() {
        pinned(m("$and", l(m("_id", "X"))));
        pinned(m("$and", l(m("a", 1), m("_id", "X"))));
        pinned(m("$and", l(m("_id", m("$eq", "X")))));
        pinned(m("$and", l(m("_id", m("a", 1)))));
        pinned(m("$and", l(m("_id", null))));
        pinned(m("$and", l(m("a", 1), m("$and", l(m("_id", "X"))))));
        pinned(m("$and", l(m("_id", "X"), m("$or", l(m("a", 1), m("b", 2))))));
    }

    @Test
    public void singleElementInPins() {
        pinned(m("$and", l(m("_id", m("$in", l("X"))))));
        pinned(m("_id", m("$in", l("X"))));
    }

    @Test
    public void singleBranchOrPins() {
        pinned(m("$or", l(m("_id", "X"))));
        pinned(m("$and", l(m("$or", l(m("_id", "X"))))));
    }

    @Test
    public void shapesTheServerDoesNotSeedFromDoNotPinInsideCombinators() {
        notPinned(m("$and", l(m("a", 1), m("b", 2))));
        notPinned(m("$and", l(m("_id", m("$gt", "A")))));
        notPinned(m("$and", l(m("_id", m("$ne", "X")))));
        notPinned(m("$and", l(m("_id", m("$in", l("X", "Y"))))));
        notPinned(m("$and", l(m("_id", m("$in", l())))));
        notPinned(m("$and", l(m("_id", Pattern.compile("^X")))));
        notPinned(m("$and", l(m("_id", m("$regex", "^X")))));
        notPinned(m("$or", l(m("_id", "X"), m("_id", "Y"))));
        notPinned(m("$and", l(m("$or", l(m("_id", "X"), m("_id", "Y"))))));
        notPinned(m("$nor", l(m("_id", "X"))));
        notPinned(m("$and", l(m("$nor", l(m("_id", "X"))))));
    }

    @Test
    public void idOnlyInsideEmbeddedDocumentOrValueDoesNotPin() {
        notPinned(m("a", m("_id", 1)));
        notPinned(m("owner", "X", "other", m("$eq", "_id")));
    }

    @Test
    public void malformedCombinatorsDoNotPinAndDoNotThrow() {
        notPinned(m("$and", "x"));
        notPinned(m("$and", l("x", 1, null)));
        notPinned(m("$or", "x"));
        notPinned(m("$or", l()));
    }

    @Test
    public void filterIsNotModified() {
        Map<String, Object> f = m("$and", l(m("_id", "X"), m("a", 1)));
        String before = f.toString();
        UpsertFilterInspector.pinsId(f);
        assertEquals(before, f.toString());
    }
}
