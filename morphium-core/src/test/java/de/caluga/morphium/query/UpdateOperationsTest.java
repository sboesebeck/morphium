package de.caluga.morphium.query;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-006: merge and conflict rules of {@link UpdateOperations}. The class works on keys that are already translated and values that are already
 * marshalled (the builder maps every call through the UpdateDocumentMapper first), so this test needs no database.
 */
@Tag("core")
public class UpdateOperationsTest {

    private UpdateOperations ops() {
        return new UpdateOperations();
    }

    @Test
    public void aNewInstanceIsEmpty() {
        UpdateOperations o = ops();
        assertTrue(o.isEmpty());
        assertTrue(o.toOperatorMap().isEmpty());
        assertFalse(o.hasModifyingOperator());
        assertFalse(o.hasOnlySetOnInsert());
    }

    @Test
    public void twoSetCallsAreOneSetOperator() {
        UpdateOperations o = ops();
        o.add("$set", "a", 1);
        o.add("$set", "b", 2);
        assertEquals(Map.of("$set", Map.of("a", 1, "b", 2)), o.toOperatorMap());
    }

    @Test
    public void operatorsAndKeysKeepTheirInsertionOrder() {
        UpdateOperations o = ops();
        o.add("$setOnInsert", "z", 1);
        o.add("$set", "y", 1);
        o.add("$set", "a", 1);
        o.add("$inc", "m", 1);
        assertEquals(List.of("$setOnInsert", "$set", "$inc"), List.copyOf(o.toOperatorMap().keySet()));
        assertEquals(List.of("y", "a"), List.copyOf(((Map<?, ?>) o.toOperatorMap().get("$set")).keySet()));
    }

    @Test
    public void sameKeyWithEqualValuesMerges() {
        UpdateOperations o = ops();
        o.add("$set", "a", "x");
        o.add("$set", "a", "x");
        o.add("$set", "l", List.of("a", Map.of("k", 1)));
        o.add("$set", "l", List.of("a", Map.of("k", 1)));
        o.add("$unset", "u", "");
        o.add("$unset", "u", "");
        assertEquals(Map.of("a", "x", "l", List.of("a", Map.of("k", 1))), o.toOperatorMap().get("$set"));
        assertEquals(Map.of("u", ""), o.toOperatorMap().get("$unset"));
    }

    @Test
    public void sameKeyWithDifferentValuesThrowsAndNamesKeyAndOperator() {
        UpdateOperations o = ops();
        o.add("$set", "a", 1);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> o.add("$set", "a", 2));
        assertTrue(e.getMessage().contains("'a'") && e.getMessage().contains("$set"), e.getMessage());
        assertEquals(Map.of("$set", Map.of("a", 1)), o.toOperatorMap(), "a rejected call leaves the operations unchanged");
    }

    @Test
    public void deepEqualsHandlesArraysInsideValues() {
        UpdateOperations o = ops();
        o.add("$set", "bytes", new byte[] {1, 2});
        o.add("$set", "bytes", new byte[] {1, 2});
        assertEquals(1, ((Map<?, ?>) o.toOperatorMap().get("$set")).size());
    }

    @Test
    public void aSecondIncOnTheSameFieldAlwaysThrows() {
        // inc(a, 1).inc(a, 1) is ambiguous (one increment or two?), so it is rejected even for equal amounts
        UpdateOperations o = ops();
        o.add("$inc", "a", 1);
        assertThrows(IllegalArgumentException.class, () -> o.add("$inc", "a", 1));
    }

    @Test
    public void nullIsAValueForSetAndSetOnInsertButNotForInc() {
        UpdateOperations o = ops();
        o.add("$set", "a", null);
        o.add("$setOnInsert", "b", null);
        assertTrue(((Map<?, ?>) o.toOperatorMap().get("$set")).containsKey("a"));
        assertThrows(IllegalArgumentException.class, () -> ops().add("$inc", "a", null));
    }

    @Test
    public void nullOrEmptyKeysAndOperatorsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> ops().add("$set", null, 1));
        assertThrows(IllegalArgumentException.class, () -> ops().add("$set", "", 1));
        assertThrows(IllegalArgumentException.class, () -> ops().add(null, "a", 1));
        assertThrows(IllegalArgumentException.class, () -> ops().add("set", "a", 1));
    }

    @Test
    public void theSamePathInDifferentOperatorsConflicts() {
        for (String[] pair : new String[][] {{"$set", "$inc"}, {"$set", "$unset"}, {"$set", "$setOnInsert"}, {"$inc", "$setOnInsert"}, {"$inc", "$unset"}}) {
            UpdateOperations o = ops();
            o.add(pair[0], "a", pair[0].equals("$unset") ? "" : 1);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> o.add(pair[1], "a", pair[1].equals("$unset") ? "" : 1), pair[0] + "+" + pair[1]);
            assertTrue(e.getMessage().contains("'a'"), e.getMessage());
        }
    }

    @Test
    public void aPathAndItsChildConflictInEitherOrderAndWithinOneOperator() {
        UpdateOperations o = ops();
        o.add("$set", "a", 1);
        assertThrows(IllegalArgumentException.class, () -> o.add("$set", "a.b", 1));
        assertThrows(IllegalArgumentException.class, () -> o.add("$inc", "a.b.c", 1));

        UpdateOperations p = ops();
        p.add("$set", "a.b.c", 1);
        assertThrows(IllegalArgumentException.class, () -> p.add("$set", "a", 1));
        assertThrows(IllegalArgumentException.class, () -> p.add("$unset", "a.b", ""));
    }

    @Test
    public void siblingPathsAndSegmentPrefixesDoNotConflict() {
        UpdateOperations o = ops();
        o.add("$set", "a.b", 1);
        o.add("$set", "a.c", 1);
        o.add("$set", "ab", 1);
        o.add("$inc", "a.d", 1);
        o.add("$set", "list.0", 1);
        o.add("$set", "list.1", 1);
        assertEquals(5, ((Map<?, ?>) o.toOperatorMap().get("$set")).size());
        assertThrows(IllegalArgumentException.class, () -> o.add("$set", "list", 1));
    }

    @Test
    public void pathsWithPositionalSegmentsAreNotJudged() {
        UpdateOperations o = ops();
        o.add("$set", "items.$.name", 1);
        o.add("$inc", "items.$.qty", 1);
        o.add("$set", "items.$[x].qty", 1);
        o.add("$inc", "items.$[x]", 1);
        assertEquals(2, ((Map<?, ?>) o.toOperatorMap().get("$set")).size());
        // the very same positional path in two operators is still the same path
        assertThrows(IllegalArgumentException.class, () -> o.add("$unset", "items.$.name", ""));
    }

    @Test
    public void modifyingAndSetOnInsertOnlyDetection() {
        UpdateOperations onlyInsert = ops();
        onlyInsert.add("$setOnInsert", "a", 1);
        assertTrue(onlyInsert.hasOnlySetOnInsert());
        assertFalse(onlyInsert.hasModifyingOperator());

        for (String op : new String[] {"$set", "$inc", "$unset"}) {
            UpdateOperations o = ops();
            o.add("$setOnInsert", "a", 1);
            o.add(op, "b", op.equals("$unset") ? "" : 1);
            assertTrue(o.hasModifyingOperator(), op);
            assertFalse(o.hasOnlySetOnInsert(), op);
        }
    }

    @Test
    public void touchesFindsAKeyInAnyOperator() {
        UpdateOperations o = ops();
        o.add("$setOnInsert", "version", 1L);
        o.add("$set", "x", 1);
        assertTrue(o.touches("version"));
        assertTrue(o.touches("x"));
        assertFalse(o.touches("y"));
        assertFalse(o.touches("versions"));
    }

    @Test
    public void toOperatorMapReturnsACopy() {
        UpdateOperations o = ops();
        o.add("$set", "a", 1);
        Map<String, Object> copy = o.toOperatorMap();
        ((Map<String, Object>) copy.get("$set")).put("hack", 1);
        copy.put("$inc", Map.of());
        assertEquals(Map.of("$set", Map.of("a", 1)), o.toOperatorMap());
    }
}
