package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-062 / MOR-061 / MOR-060 (cherry-picked from Heiko Kopp's update-path stack): {@code Morphium.dec(Query, Map)} and
 * {@code dec(Map<Enum, Number>, Query)} handed the amounts to {@code inc} unchanged, so they added instead of subtracting;
 * {@code dec} with a {@code Number} amount negated via {@code doubleValue()} and turned an int field into a double; and the
 * Map variants of {@code inc}/{@code dec} wrote the Java field names into {@code $inc} instead of the stored names.
 */
@Tag("core")
public class DecIncMapVariantsTest extends MultiDriverTestBase {

    public enum Fields { hitCount, byteTotal }

    @Entity
    public static class Counter {
        @Id
        public MorphiumId id;
        public int hitCount;
        public long byteTotal;
    }

    private Query<Counter> fresh(Morphium m) {
        m.dropCollection(Counter.class);
        Counter c = new Counter();
        c.hitCount = 10;
        c.byteTotal = 1000;
        m.store(c);
        return m.createQueryFor(Counter.class).f("_id").eq(c.id);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void decWithAMapSubtracts(Morphium m) throws Exception {
        try (m) {
            Query<Counter> q = fresh(m);
            Map<String, Number> amounts = new LinkedHashMap<>();
            amounts.put("hitCount", 3);
            amounts.put("byteTotal", 100L);
            m.dec(q, amounts, false, false, null);
            Counter c = q.get();
            assertEquals(7, c.hitCount, "dec(Query, Map) used to add the amount");
            assertEquals(900L, c.byteTotal);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void decWithAnEnumMapSubtracts(Morphium m) throws Exception {
        try (m) {
            Query<Counter> q = fresh(m);
            Map<Enum, Number> amounts = new HashMap<>();
            amounts.put(Fields.hitCount, 4);
            amounts.put(Fields.byteTotal, 250L);
            m.dec(amounts, q, false, false, null);
            Counter c = q.get();
            assertEquals(6, c.hitCount, "dec(Map<Enum, Number>, Query) used to add the amount");
            assertEquals(750L, c.byteTotal);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void incWithAMapWritesTheStoredFieldNames(Morphium m) throws Exception {
        try (m) {
            Query<Counter> q = fresh(m);
            Map<String, Number> amounts = new LinkedHashMap<>();
            amounts.put("hitCount", 5);
            m.inc(q, amounts, false, false, null);
            assertEquals(15, q.get().hitCount, "the Java name was written into $inc, the stored field stayed as it was");
            Map<String, Object> raw = q.asMapList().get(0);
            assertFalse(raw.containsKey("hitCount"), "no field under the Java name: " + raw);
            assertTrue(raw.containsKey("hit_count"), raw.toString());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void decWithANumberKeepsTheIntegerType(Morphium m) throws Exception {
        try (m) {
            Query<Counter> q = fresh(m);
            m.dec(q, "hitCount", (Number) 3);
            assertEquals(7, q.get().hitCount);
            Object stored = q.asMapList().get(0).get("hit_count");
            assertFalse(stored instanceof Double, "dec(Number) negated via doubleValue() and turned the int field into a double: " + stored);
            Counter c = q.get();
            m.dec(c, "hitCount", (Number) 2);
            assertEquals(5, q.get().hitCount);
        }
    }
}
