package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sorting must not care which Java number type a value happens to have.
 *
 * <p>BSON has one numeric class: mongod orders 1, 1L and 1.0 by value, not by representation.
 * The in-memory sort compared them with a raw {@code Comparable.compareTo}, which throws
 * {@code ClassCastException} the moment two documents hold the same field as different types -
 * {@code Long.compareTo(Integer)} is a hard failure, not a mis-ordering.
 *
 * <p>That is easy to hit without anyone writing mixed types deliberately: a document that was
 * dumped and restored can come back with a different numeric type than one written live, so a
 * sorted query over both blows up. Seen on the ACC message bus on 2026-08-18, where it killed
 * the client connection processing the query.
 */
@Tag("inmemory")
public class MixedNumberSortTest {

    private final String db = "mixed_number_sort";

    private InMemoryDriver freshDriver() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        return drv;
    }

    private void insert(InMemoryDriver drv, String coll, Object counter) throws Exception {
        new InsertMongoCommand(drv).setDb(db).setColl(coll)
                .setDocuments(List.of(Doc.of("_id", String.valueOf(counter), "counter", counter)))
                .execute();
    }

    private List<Map<String, Object>> findSorted(InMemoryDriver drv, String coll, int direction) throws Exception {
        FindCommand cmd = new FindCommand(drv).setDb(db).setColl(coll);
        cmd.setSort(Doc.of("counter", direction));
        return cmd.execute();
    }

    @Test
    public void sortsAcrossIntegerAndLongWithoutBlowingUp() throws Exception {
        InMemoryDriver drv = freshDriver();
        String coll = "intAndLong";

        // Same field, different Java types - exactly what a restored document next to a live
        // one looks like.
        insert(drv, coll, 3);
        insert(drv, coll, 1L);
        insert(drv, coll, 2);

        List<Map<String, Object>> asc = findSorted(drv, coll, 1);

        assertEquals(3, asc.size());
        assertEquals(1L, ((Number) asc.get(0).get("counter")).longValue(), "1L must sort first, whatever its type");
        assertEquals(2L, ((Number) asc.get(1).get("counter")).longValue());
        assertEquals(3L, ((Number) asc.get(2).get("counter")).longValue());

        List<Map<String, Object>> desc = findSorted(drv, coll, -1);
        assertEquals(3L, ((Number) desc.get(0).get("counter")).longValue(), "descending must mirror ascending");
        assertEquals(1L, ((Number) desc.get(2).get("counter")).longValue());

        drv.close();
    }

    @Test
    public void sortsAcrossIntegralAndFloatingPointByValue() throws Exception {
        InMemoryDriver drv = freshDriver();
        String coll = "intAndDouble";

        insert(drv, coll, 10);
        insert(drv, coll, 2.5d);
        insert(drv, coll, 7L);

        List<Map<String, Object>> asc = findSorted(drv, coll, 1);

        assertEquals(3, asc.size());
        assertEquals(2.5d, ((Number) asc.get(0).get("counter")).doubleValue(), 0.0001, "2.5 sorts below 7");
        assertEquals(7L, ((Number) asc.get(1).get("counter")).longValue());
        assertEquals(10L, ((Number) asc.get(2).get("counter")).longValue());

        drv.close();
    }

    /**
     * The mixed integral/floating path, which is where a double conversion actually bites:
     * {@code 2^53 + 1} as a long is not representable as a double and collapses onto
     * {@code 2^53}, so comparing the two through {@code double} reports them EQUAL. That is
     * not merely mis-ordering - it breaks the comparator contract, and since this comparator
     * feeds a PriorityQueue, a false tie can drop the wrong document from a top-N result.
     */
    @Test
    public void keepsPrecisionBetweenLongAndDoubleAcrossThe53BitBoundary() throws Exception {
        InMemoryDriver drv = freshDriver();
        String coll = "longVsDouble";

        long justAbove = 9007199254740993L;       // 2^53 + 1, not representable as a double
        double atBoundary = 9007199254740992.0d;  // 2^53 - what the long above collapses to

        insert(drv, coll, atBoundary);
        insert(drv, coll, justAbove);

        List<Map<String, Object>> asc = findSorted(drv, coll, 1);

        assertEquals(2, asc.size());
        assertTrue(asc.get(0).get("counter") instanceof Double,
            "the double at 2^53 must sort BELOW the long at 2^53+1");
        assertEquals(justAbove, ((Number) asc.get(1).get("counter")).longValue());

        List<Map<String, Object>> desc = findSorted(drv, coll, -1);
        assertEquals(justAbove, ((Number) desc.get(0).get("counter")).longValue(),
            "descending must mirror it - a false tie would let the order depend on insertion");

        drv.close();
    }

    /**
     * Longs beyond 2^53 must not be ordered through {@code double} - that would silently make
     * two distinct values compare equal.
     */
    @Test
    public void keepsPrecisionForLargeLongs() throws Exception {
        InMemoryDriver drv = freshDriver();
        String coll = "largeLongs";

        long big = 9007199254740993L;   // 2^53 + 1, not representable as a double
        insert(drv, coll, big);
        insert(drv, coll, big - 1);
        insert(drv, coll, 1);

        List<Map<String, Object>> asc = findSorted(drv, coll, 1);

        assertEquals(3, asc.size());
        assertEquals(1L, ((Number) asc.get(0).get("counter")).longValue());
        assertEquals(big - 1, ((Number) asc.get(1).get("counter")).longValue(), "2^53 must not collapse onto 2^53+1");
        assertEquals(big, ((Number) asc.get(2).get("counter")).longValue());

        drv.close();
    }
}
