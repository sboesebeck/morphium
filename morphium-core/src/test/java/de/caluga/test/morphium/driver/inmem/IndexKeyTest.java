package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.driver.inmem.IndexDefinition;
import de.caluga.morphium.driver.inmem.IndexKey;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link IndexKey} and {@link IndexDefinition} - the new index value types
 * for the InMemoryDriver index subsystem (Phase B1, pure value objects, no driver wiring yet).
 */
@Tag("inmemory")
public class IndexKeyTest {

    // ---------------------------------------------------------------- IndexDefinition.fromIndexMap

    @Test
    void fromIndexMapParsesSingleFieldAndSkipsOptions() {
        Map<String, Object> indexMap = new LinkedHashMap<>();
        indexMap.put("counter", 1);
        indexMap.put("$options", Map.of("name", "counter_1"));

        IndexDefinition def = IndexDefinition.fromIndexMap(indexMap);

        assertEquals(List.of("counter"), def.fields());
        assertEquals(1, def.direction("counter"));
    }

    @Test
    void fromIndexMapPreservesCompoundOrderAndDirections() {
        Map<String, Object> indexMap = new LinkedHashMap<>();
        indexMap.put("a", 1);
        indexMap.put("b", -1);
        indexMap.put("$options", Map.of("name", "a_1_b_-1"));

        IndexDefinition def = IndexDefinition.fromIndexMap(indexMap);

        assertEquals(List.of("a", "b"), def.fields());
        assertEquals(1, def.direction("a"));
        assertEquals(-1, def.direction("b"));
    }

    @Test
    void fromIndexMapReadsUniqueFlagFromOptions() {
        Map<String, Object> uniqueMap = new LinkedHashMap<>();
        uniqueMap.put("field", 1);
        uniqueMap.put("$options", Map.of("unique", true));
        IndexDefinition uniqueDef = IndexDefinition.fromIndexMap(uniqueMap);
        assertTrue(uniqueDef.unique());

        Map<String, Object> nonUniqueMap = new LinkedHashMap<>();
        nonUniqueMap.put("field", 1);
        nonUniqueMap.put("$options", Map.of("name", "field_1"));
        IndexDefinition nonUniqueDef = IndexDefinition.fromIndexMap(nonUniqueMap);
        assertFalse(nonUniqueDef.unique());
    }

    @Test
    void fromIndexMapReadsNameAndExpireAfterSecondsFromOptions() {
        Map<String, Object> indexMap = new LinkedHashMap<>();
        indexMap.put("createdAt", 1);
        indexMap.put("$options", Map.of("name", "ttl_idx", "expireAfterSeconds", 3600));

        IndexDefinition def = IndexDefinition.fromIndexMap(indexMap);

        assertEquals("ttl_idx", def.name());
        assertEquals(3600L, def.expireAfterSeconds());
    }

    @Test
    void fromIndexMapWithoutOptionsHasNoExpiry() {
        Map<String, Object> indexMap = new LinkedHashMap<>();
        indexMap.put("field", 1);

        IndexDefinition def = IndexDefinition.fromIndexMap(indexMap);

        assertNull(def.expireAfterSeconds());
        assertFalse(def.unique());
    }

    // ---------------------------------------------------------------- IndexKey equals/hashCode

    @Test
    void indexKeyEqualityAndHashCodeAreValueBased() {
        IndexKey k1 = IndexKey.of(List.of(1, "a"));
        IndexKey k2 = IndexKey.of(List.of(1, "a"));
        IndexKey k3 = IndexKey.of(List.of(2, "a"));

        assertEquals(k1, k2);
        assertEquals(k1.hashCode(), k2.hashCode());
        assertFalse(k1.equals(k3));
    }

    // ---------------------------------------------------------------- IndexKey.extract - MISSING semantics

    @Test
    void extractTreatsNullAndAbsentFieldAsTheSameMissingSentinel() {
        IndexDefinition def = IndexDefinition.fromIndexMap(Map.of("a", 1));

        Map<String, Object> withExplicitNull = new LinkedHashMap<>();
        withExplicitNull.put("a", null);
        Map<String, Object> withoutField = new LinkedHashMap<>();

        IndexKey explicitNullKey = IndexKey.extract(withExplicitNull, def);
        IndexKey absentKey = IndexKey.extract(withoutField, def);

        assertEquals(explicitNullKey, absentKey);
    }

    @Test
    void extractReadsArrayFieldAsWholeArrayValue() {
        IndexDefinition def = IndexDefinition.fromIndexMap(Map.of("tags", 1));
        Map<String, Object> doc = Map.of("tags", List.of(1, 2, 3));

        IndexKey key = IndexKey.extract(doc, def);

        assertEquals(IndexKey.of(List.of(List.of(1, 2, 3))), key);
    }

    @Test
    void extractWalksDottedPaths() {
        IndexDefinition def = IndexDefinition.fromIndexMap(Map.of("a.b", 1));
        Map<String, Object> doc = Map.of("a", Map.of("b", 42));

        IndexKey key = IndexKey.extract(doc, def);

        assertEquals(IndexKey.of(List.of(42)), key);
    }

    // ---------------------------------------------------------------- IndexKey.comparator

    @Test
    void comparatorOrdersIntLongDoubleNumericallyAcrossTypes() {
        Map<String, Object> indexMap = new LinkedHashMap<>();
        indexMap.put("v", 1);
        IndexDefinition def = IndexDefinition.fromIndexMap(indexMap);

        IndexKey kInt = IndexKey.of(List.of(2));
        IndexKey kLong = IndexKey.of(List.of(3L));
        IndexKey kDouble = IndexKey.of(List.of(1.5d));

        List<IndexKey> keys = new ArrayList<>(List.of(kInt, kLong, kDouble));
        Collections.sort(keys, IndexKey.comparator(def));

        assertEquals(List.of(kDouble, kInt, kLong), keys);
    }

    @Test
    void comparatorRespectsDescendingDirection() {
        Map<String, Object> indexMap = new LinkedHashMap<>();
        indexMap.put("v", -1);
        IndexDefinition def = IndexDefinition.fromIndexMap(indexMap);

        IndexKey kOne = IndexKey.of(List.of(1));
        IndexKey kTwo = IndexKey.of(List.of(2));

        List<IndexKey> keys = new ArrayList<>(List.of(kOne, kTwo));
        Collections.sort(keys, IndexKey.comparator(def));

        assertEquals(List.of(kTwo, kOne), keys);
    }

    @Test
    void comparatorSortsMissingFirstAscendingAndLastDescending() {
        Map<String, Object> ascMap = new LinkedHashMap<>();
        ascMap.put("v", 1);
        IndexDefinition ascDef = IndexDefinition.fromIndexMap(ascMap);

        IndexKey present = IndexKey.of(List.of(5));
        IndexKey missing = IndexKey.of(Collections.singletonList(IndexKey.MISSING));

        List<IndexKey> ascKeys = new ArrayList<>(List.of(present, missing));
        Collections.sort(ascKeys, IndexKey.comparator(ascDef));
        assertEquals(List.of(missing, present), ascKeys);

        Map<String, Object> descMap = new LinkedHashMap<>();
        descMap.put("v", -1);
        IndexDefinition descDef = IndexDefinition.fromIndexMap(descMap);

        List<IndexKey> descKeys = new ArrayList<>(List.of(present, missing));
        Collections.sort(descKeys, IndexKey.comparator(descDef));
        assertEquals(List.of(present, missing), descKeys);
    }

    @Test
    void comparatorIsConsistentForDottedPathExtractedValues() {
        IndexDefinition def = IndexDefinition.fromIndexMap(Map.of("a.b", 1));

        Map<String, Object> docLow = Map.of("a", Map.of("b", 1));
        Map<String, Object> docHigh = Map.of("a", Map.of("b", 2));
        Map<String, Object> docMissing = Map.of("a", Map.of());

        IndexKey keyLow = IndexKey.extract(docLow, def);
        IndexKey keyHigh = IndexKey.extract(docHigh, def);
        IndexKey keyMissing = IndexKey.extract(docMissing, def);

        List<IndexKey> keys = new ArrayList<>(List.of(keyHigh, keyLow, keyMissing));
        Collections.sort(keys, IndexKey.comparator(def));

        assertEquals(List.of(keyMissing, keyLow, keyHigh), keys);
    }

    @Test
    void comparatorOrdersMapEncodedLocalDateTimeChronologically() {
        // stored LocalDateTime values are serialised as {sec: epochSecond, n: nano}
        Map<String, Object> indexMap = new LinkedHashMap<>();
        indexMap.put("ts", 1);
        IndexDefinition def = IndexDefinition.fromIndexMap(indexMap);

        IndexKey earlier = IndexKey.of(List.of(Map.of("sec", 1000L, "n", 0)));
        IndexKey later = IndexKey.of(List.of(Map.of("sec", 2000L, "n", 500)));

        var cmp = IndexKey.comparator(def);
        assertTrue(cmp.compare(earlier, later) < 0, "earlier {sec,n} timestamp must sort first");
        assertTrue(cmp.compare(later, earlier) > 0, "later {sec,n} timestamp must sort last");

        List<IndexKey> keys = new ArrayList<>(List.of(later, earlier));
        Collections.sort(keys, cmp);
        assertEquals(List.of(earlier, later), keys);
    }

    @Test
    void comparatorOrdersMapEncodedInstantChronologically() {
        // stored Instant values are serialised as {type: "instant", seconds, nanos}
        Map<String, Object> indexMap = new LinkedHashMap<>();
        indexMap.put("ts", 1);
        IndexDefinition def = IndexDefinition.fromIndexMap(indexMap);

        IndexKey earlier = IndexKey.of(List.of(Map.of("type", "instant", "seconds", 1000L, "nanos", 0)));
        IndexKey later = IndexKey.of(List.of(Map.of("type", "instant", "seconds", 1000L, "nanos", 999)));

        var cmp = IndexKey.comparator(def);
        assertTrue(cmp.compare(earlier, later) < 0, "earlier instant must sort first");
        assertTrue(cmp.compare(later, earlier) > 0, "later instant must sort last");
        assertFalse(cmp.compare(earlier, later) == 0, "different instants must not compare equal");
    }

    @Test
    void comparatorFallbackForIncomparableTypesIsDeterministicAndAntisymmetric() {
        Map<String, Object> indexMap = new LinkedHashMap<>();
        indexMap.put("v", 1);
        IndexDefinition def = IndexDefinition.fromIndexMap(indexMap);

        IndexKey stringKey = IndexKey.of(List.of("abc"));
        IndexKey intKey = IndexKey.of(List.of(42));

        var cmp = IndexKey.comparator(def);
        int forward = cmp.compare(stringKey, intKey);
        int backward = cmp.compare(intKey, stringKey);

        assertTrue(forward != 0, "incomparable types must not compare equal");
        assertTrue(Integer.signum(forward) == -Integer.signum(backward), "fallback must be antisymmetric");
        assertEquals(forward, cmp.compare(stringKey, intKey), "fallback must be deterministic");
    }

    @Test
    void directionThrowsForUnknownField() {
        IndexDefinition def = IndexDefinition.fromIndexMap(Map.of("a", 1));
        assertThrows(IllegalArgumentException.class, () -> def.direction("b"));
    }
}
