package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * MOR-085: MongoDB sorts a missing value or null before every number, so an ascending sort starts with them and a descending sort ends with them. The InMemoryDriver put them
 * first in both directions. Runs on every driver, so the real MongoDB tells what is right.
 */
@Tag("core")
public class SortNullOrderTest extends MultiDriverTestBase {

    @Entity
    public static class Slot {
        @Id
        public String id;
        public Integer rank;
        public String label;
    }

    private static void store(Morphium m, String id, Integer rank, String label) {
        Slot s = new Slot();
        s.id = id;
        s.rank = rank;
        s.label = label;
        m.store(s);
    }

    private static List<String> ids(Morphium m, Map<String, Integer> sort) {
        return m.createQueryFor(Slot.class).sort(sort).asList().stream().map(s -> s.id).toList();
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void nullsComeFirstAscendingAndLastDescending(Morphium m) throws Exception {
        try (m) {
            store(m, "a", 2, "x");
            store(m, "b", null, "y");
            store(m, "c", 1, "z");
            store(m, "d", 3, null);

            assertEquals(List.of("b", "c", "a", "d"), ids(m, Map.of("rank", 1)));
            assertEquals(List.of("d", "a", "c", "b"), ids(m, Map.of("rank", -1)));
            assertEquals(List.of("d", "a", "b", "c"), ids(m, Map.of("label", 1)));
            assertEquals(List.of("c", "b", "a", "d"), ids(m, Map.of("label", -1)));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aSecondSortFieldBreaksTheTieOfNulls(Morphium m) throws Exception {
        try (m) {
            store(m, "a", null, "q");
            store(m, "b", null, "p");
            store(m, "c", 5, "p");

            java.util.LinkedHashMap<String, Integer> sort = new java.util.LinkedHashMap<>();
            sort.put("rank", -1);
            sort.put("label", 1);
            assertEquals(List.of("c", "b", "a"), ids(m, sort));
        }
    }
}
