package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.driver.inmem.IndexDefinition;
import de.caluga.morphium.driver.inmem.IndexKey;
import de.caluga.morphium.driver.inmem.IndexPlanner;
import de.caluga.morphium.driver.inmem.IndexPlanner.EqualityLookup;
import de.caluga.morphium.driver.inmem.IndexPlanner.FullScan;
import de.caluga.morphium.driver.inmem.IndexPlanner.InUnion;
import de.caluga.morphium.driver.inmem.IndexPlanner.IndexPlan;
import de.caluga.morphium.driver.inmem.IndexPlanner.RangeScan;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link IndexPlanner} - pure query-shape-to-plan mapping, no store/documents
 * involved (Phase B1 read-path integration, Task 3).
 */
@Tag("inmemory")
public class IndexPlannerTest {

    private static IndexDefinition index(String name, String field, int direction) {
        Map<String, Object> indexMap = new LinkedHashMap<>();
        indexMap.put(field, direction);
        indexMap.put("$options", Map.of("name", name));
        return IndexDefinition.fromIndexMap(indexMap);
    }

    private static IndexDefinition compoundIndex(String name, Object... fieldsAndDirections) {
        Map<String, Object> indexMap = new LinkedHashMap<>();
        for (int i = 0; i < fieldsAndDirections.length; i += 2) {
            indexMap.put((String) fieldsAndDirections[i], fieldsAndDirections[i + 1]);
        }
        indexMap.put("$options", Map.of("name", name));
        return IndexDefinition.fromIndexMap(indexMap);
    }

    private static IndexDefinition idIndex() {
        Map<String, Object> indexMap = new LinkedHashMap<>();
        indexMap.put("_id", 1);
        indexMap.put("$options", Map.of("name", "_id_", "unique", true));
        return IndexDefinition.fromIndexMap(indexMap);
    }

    // ---------------------------------------------------------------- equality

    @Test
    void equalityOnSingleFieldIndexPlansEqualityLookup() {
        IndexDefinition idx = index("counter_1", "counter", 1);
        IndexPlan plan = IndexPlanner.plan(Map.of("counter", 4711), List.of(idx));

        EqualityLookup eq = assertInstanceOf(EqualityLookup.class, plan);
        assertEquals(idx, eq.def());
        assertEquals(IndexKey.of(List.of(4711)), eq.key());
    }

    @Test
    void idEqualityAlwaysPlansEqualityLookup() {
        IndexDefinition idDef = idIndex();
        IndexPlan plan = IndexPlanner.plan(Map.of("_id", 42), List.of(idDef));

        EqualityLookup eq = assertInstanceOf(EqualityLookup.class, plan);
        assertEquals(idDef, eq.def());
        assertEquals(IndexKey.of(List.of(42)), eq.key());
    }

    @Test
    void equalityOnFullyBoundCompoundIndexPlansEqualityLookup() {
        IndexDefinition idx = compoundIndex("a_1_b_1", "a", 1, "b", 1);
        Map<String, Object> query = new LinkedHashMap<>();
        query.put("a", 1);
        query.put("b", 2);
        IndexPlan plan = IndexPlanner.plan(query, List.of(idx));

        EqualityLookup eq = assertInstanceOf(EqualityLookup.class, plan);
        assertEquals(IndexKey.of(List.of(1, 2)), eq.key());
    }

    @Test
    void nullQueryValueMapsToMissingSentinelInEqualityKey() {
        IndexDefinition idx = index("u_1", "u", 1);
        Map<String, Object> query = new LinkedHashMap<>();
        query.put("u", null);
        IndexPlan plan = IndexPlanner.plan(query, List.of(idx));

        EqualityLookup eq = assertInstanceOf(EqualityLookup.class, plan);
        assertEquals(IndexKey.of(List.of(IndexKey.MISSING)), eq.key());
    }

    @Test
    void partialEqualityPrefixWithoutTrailingOperatorPlansRangeScanOverPrefix() {
        IndexDefinition idx = compoundIndex("a_1_b_1_c_1", "a", 1, "b", 1, "c", 1);
        Map<String, Object> query = new LinkedHashMap<>();
        query.put("a", 1);
        query.put("b", 2);
        IndexPlan plan = IndexPlanner.plan(query, List.of(idx));

        RangeScan rs = assertInstanceOf(RangeScan.class, plan);
        assertEquals(IndexKey.prefixLow(idx, List.of(1, 2)), rs.from());
        assertEquals(IndexKey.prefixHigh(idx, List.of(1, 2)), rs.to());
        assertTrue(rs.fromInclusive());
        assertTrue(rs.toInclusive());
        assertEquals(false, rs.descending());
    }

    // ---------------------------------------------------------------- trailing range, ascending

    @Test
    void trailingGtOnLastFieldAscendingIsExclusiveExactBound() {
        IndexDefinition idx = compoundIndex("a_1_b_1", "a", 1, "b", 1);
        Map<String, Object> query = new LinkedHashMap<>();
        query.put("a", 1);
        query.put("b", Map.of("$gt", 5));
        IndexPlan plan = IndexPlanner.plan(query, List.of(idx));

        RangeScan rs = assertInstanceOf(RangeScan.class, plan);
        assertEquals(IndexKey.of(List.of(1, 5)), rs.from());
        assertTrue(!rs.fromInclusive(), "$gt on the index's last field must exclude the exact value");
        assertEquals(IndexKey.prefixHigh(idx, List.of(1)), rs.to());
        assertTrue(rs.toInclusive());
    }

    @Test
    void trailingGteOnLastFieldAscendingIsInclusiveExactBound() {
        IndexDefinition idx = compoundIndex("a_1_b_1", "a", 1, "b", 1);
        Map<String, Object> query = new LinkedHashMap<>();
        query.put("a", 1);
        query.put("b", Map.of("$gte", 5));
        IndexPlan plan = IndexPlanner.plan(query, List.of(idx));

        RangeScan rs = assertInstanceOf(RangeScan.class, plan);
        assertEquals(IndexKey.of(List.of(1, 5)), rs.from());
        assertTrue(rs.fromInclusive());
        assertEquals(IndexKey.prefixHigh(idx, List.of(1)), rs.to());
    }

    @Test
    void trailingLtOnLastFieldAscendingIsExclusiveExactBound() {
        IndexDefinition idx = compoundIndex("a_1_b_1", "a", 1, "b", 1);
        Map<String, Object> query = new LinkedHashMap<>();
        query.put("a", 1);
        query.put("b", Map.of("$lt", 5));
        IndexPlan plan = IndexPlanner.plan(query, List.of(idx));

        RangeScan rs = assertInstanceOf(RangeScan.class, plan);
        assertEquals(IndexKey.prefixLow(idx, List.of(1)), rs.from());
        assertTrue(rs.fromInclusive());
        assertEquals(IndexKey.of(List.of(1, 5)), rs.to());
        assertTrue(!rs.toInclusive(), "$lt on the index's last field must exclude the exact value");
    }

    @Test
    void trailingLteOnLastFieldAscendingIsInclusiveExactBound() {
        IndexDefinition idx = compoundIndex("a_1_b_1", "a", 1, "b", 1);
        Map<String, Object> query = new LinkedHashMap<>();
        query.put("a", 1);
        query.put("b", Map.of("$lte", 5));
        IndexPlan plan = IndexPlanner.plan(query, List.of(idx));

        RangeScan rs = assertInstanceOf(RangeScan.class, plan);
        assertEquals(IndexKey.prefixLow(idx, List.of(1)), rs.from());
        assertEquals(IndexKey.of(List.of(1, 5)), rs.to());
        assertTrue(rs.toInclusive());
    }

    @Test
    void rangeOnFirstFieldNoEqualityPrefixPlansRangeScan() {
        IndexDefinition idx = index("counter_1", "counter", 1);
        IndexPlan plan = IndexPlanner.plan(Map.of("counter", Map.of("$gte", 100)), List.of(idx));

        RangeScan rs = assertInstanceOf(RangeScan.class, plan);
        assertEquals(IndexKey.of(List.of(100)), rs.from());
        assertTrue(rs.fromInclusive());
        assertEquals(IndexKey.prefixHigh(idx, List.of()), rs.to());
    }

    @Test
    void trailingGtWithFieldsRemainingAfterUsesSyntheticPrefixBounds() {
        IndexDefinition idx = compoundIndex("a_1_b_1_c_1", "a", 1, "b", 1, "c", 1);
        Map<String, Object> query = new LinkedHashMap<>();
        query.put("a", 1);
        query.put("b", Map.of("$gt", 5));
        IndexPlan plan = IndexPlanner.plan(query, List.of(idx));

        RangeScan rs = assertInstanceOf(RangeScan.class, plan);
        assertEquals(IndexKey.prefixHigh(idx, List.of(1, 5)), rs.from());
        assertTrue(rs.fromInclusive());
        assertEquals(IndexKey.prefixHigh(idx, List.of(1)), rs.to());
    }

    // ---------------------------------------------------------------- trailing range, descending

    @Test
    void trailingGtOnDescendingLastFieldFlipsToStructurallyBelow() {
        IndexDefinition idx = compoundIndex("a_1_b_-1", "a", 1, "b", -1);
        Map<String, Object> query = new LinkedHashMap<>();
        query.put("a", 1);
        query.put("b", Map.of("$gt", 5));
        IndexPlan plan = IndexPlanner.plan(query, List.of(idx));

        RangeScan rs = assertInstanceOf(RangeScan.class, plan);
        // domain "b > 5" on a descending field sits BELOW b==5 in comparator order.
        assertEquals(IndexKey.prefixLow(idx, List.of(1)), rs.from());
        assertTrue(rs.fromInclusive());
        assertEquals(IndexKey.of(List.of(1, 5)), rs.to());
        assertTrue(!rs.toInclusive());
    }

    @Test
    void trailingLteOnDescendingLastFieldFlipsToStructurallyAbove() {
        IndexDefinition idx = compoundIndex("a_1_b_-1", "a", 1, "b", -1);
        Map<String, Object> query = new LinkedHashMap<>();
        query.put("a", 1);
        query.put("b", Map.of("$lte", 5));
        IndexPlan plan = IndexPlanner.plan(query, List.of(idx));

        RangeScan rs = assertInstanceOf(RangeScan.class, plan);
        // domain "b <= 5" on a descending field sits AT-OR-ABOVE b==5 in comparator order.
        assertEquals(IndexKey.of(List.of(1, 5)), rs.from());
        assertTrue(rs.fromInclusive());
        assertEquals(IndexKey.prefixHigh(idx, List.of(1)), rs.to());
        assertTrue(rs.toInclusive());
    }

    // ---------------------------------------------------------------- $in

    @Test
    void inOnFullyBoundPrefixPlansInUnion() {
        IndexDefinition idx = compoundIndex("a_1_b_1", "a", 1, "b", 1);
        Map<String, Object> query = new LinkedHashMap<>();
        query.put("a", 1);
        query.put("b", Map.of("$in", List.of(10, 20, 20)));
        IndexPlan plan = IndexPlanner.plan(query, List.of(idx));

        InUnion in = assertInstanceOf(InUnion.class, plan);
        assertEquals(List.of(IndexKey.of(List.of(1, 10)), IndexKey.of(List.of(1, 20)), IndexKey.of(List.of(1, 20))),
                in.keys(), "planner does not dedup - callers executing the union must");
    }

    @Test
    void inNotCoveringFullIndexFallsBackToPartialEqualityPrefix() {
        IndexDefinition idx = compoundIndex("a_1_b_1_c_1", "a", 1, "b", 1, "c", 1);
        Map<String, Object> query = new LinkedHashMap<>();
        query.put("a", 1);
        query.put("b", Map.of("$in", List.of(10, 20)));
        IndexPlan plan = IndexPlanner.plan(query, List.of(idx));

        RangeScan rs = assertInstanceOf(RangeScan.class, plan);
        assertEquals(IndexKey.prefixLow(idx, List.of(1)), rs.from());
        assertEquals(IndexKey.prefixHigh(idx, List.of(1)), rs.to());
    }

    @Test
    void inWithEmptyListPlansInUnionWithNoKeys() {
        IndexDefinition idx = index("u_1", "u", 1);
        IndexPlan plan = IndexPlanner.plan(Map.of("u", Map.of("$in", List.of())), List.of(idx));

        InUnion in = assertInstanceOf(InUnion.class, plan);
        assertTrue(in.keys().isEmpty());
    }

    // ---------------------------------------------------------------- unsupported -> FullScan

    @Test
    void unsupportedOperatorsPlanFullScan() {
        IndexDefinition idx = index("counter_1", "counter", 1);
        assertInstanceOf(FullScan.class, IndexPlanner.plan(Map.of("counter", Map.of("$ne", 5)), List.of(idx)));
        assertInstanceOf(FullScan.class, IndexPlanner.plan(Map.of("counter", Map.of("$exists", true)), List.of(idx)));
        assertInstanceOf(FullScan.class, IndexPlanner.plan(Map.of("counter", Map.of("$regex", "^a")), List.of(idx)));
    }

    @Test
    void combinedRangeOperatorsOnOnlyFieldPlanFullScan() {
        IndexDefinition idx = index("counter_1", "counter", 1);
        Map<String, Object> query = new LinkedHashMap<>();
        Map<String, Object> range = new LinkedHashMap<>();
        range.put("$gte", 1);
        range.put("$lt", 10);
        query.put("counter", range);
        assertInstanceOf(FullScan.class, IndexPlanner.plan(query, List.of(idx)));
    }

    @Test
    void combinedRangeOperatorsFallBackToLeadingEqualityPrefix() {
        IndexDefinition idx = compoundIndex("x_1_counter_1", "x", 1, "counter", 1);
        Map<String, Object> query = new LinkedHashMap<>();
        Map<String, Object> range = new LinkedHashMap<>();
        range.put("$gte", 1);
        range.put("$lt", 10);
        query.put("x", 1);
        query.put("counter", range);
        IndexPlan plan = IndexPlanner.plan(query, List.of(idx));

        RangeScan rs = assertInstanceOf(RangeScan.class, plan);
        assertEquals(IndexKey.prefixLow(idx, List.of(1)), rs.from());
        assertEquals(IndexKey.prefixHigh(idx, List.of(1)), rs.to());
    }

    @Test
    void topLevelOrPlansFullScan() {
        IndexDefinition idx = index("a_1", "a", 1);
        Map<String, Object> query = Map.of("$or", List.of(Map.of("a", 1)));
        assertInstanceOf(FullScan.class, IndexPlanner.plan(query, List.of(idx)));
    }

    @Test
    void topLevelAndPlansFullScan() {
        IndexDefinition idx = index("a_1", "a", 1);
        Map<String, Object> query = Map.of("$and", List.of(Map.of("a", 1)));
        assertInstanceOf(FullScan.class, IndexPlanner.plan(query, List.of(idx)));
    }

    @Test
    void emptyQueryPlansFullScan() {
        IndexDefinition idx = index("a_1", "a", 1);
        assertInstanceOf(FullScan.class, IndexPlanner.plan(Map.of(), List.of(idx)));
    }

    @Test
    void noMatchingIndexPlansFullScan() {
        IndexDefinition idx = index("a_1", "a", 1);
        assertInstanceOf(FullScan.class, IndexPlanner.plan(Map.of("z", 1), List.of(idx)));
    }

    // ---------------------------------------------------------------- def selection

    @Test
    void prefersDefThatBindsMoreFields() {
        IndexDefinition narrow = index("a_1", "a", 1);
        IndexDefinition wide = compoundIndex("a_1_b_1", "a", 1, "b", 1);
        Map<String, Object> query = new LinkedHashMap<>();
        query.put("a", 1);
        query.put("b", 2);
        IndexPlan plan = IndexPlanner.plan(query, List.of(narrow, wide));

        EqualityLookup eq = assertInstanceOf(EqualityLookup.class, plan);
        assertEquals(wide, eq.def());
        assertEquals(IndexKey.of(List.of(1, 2)), eq.key());
    }
}
