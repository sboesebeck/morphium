package de.caluga.test.mongo.suite.contract;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** MOR-048: {@code Morphium.setEnum} stores each value under the name of its constant; it looked the value up in the map as if it were a key and stored null. */
@Tag("core")
public class SetEnumTest extends MultiDriverTestBase {

    public enum F { counterValue, label }

    @Entity
    @NoCache
    public static class Item {
        @Id
        public String id;
        public int counterValue;
        public String label;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void eachValueIsStoredUnderTheNameOfItsConstant(Morphium m) throws Exception {
        try (m) {
            m.dropCollection(Item.class);
            Item i = new Item();
            i.id = "a";
            i.counterValue = 1;
            i.label = "old";
            m.store(i);

            Map<Enum, Object> values = new HashMap<>();
            values.put(F.counterValue, 5);
            values.put(F.label, "new");
            m.setEnum(m.createQueryFor(Item.class).f("id").eq("a"), values, false, false);

            Map<String, Object> raw = ModifyFixturesAccess.rawDocs(m.createQueryFor(Item.class).f("id").eq("a")).get(0);
            assertEquals(5, ((Number) raw.get("counter_value")).intValue(), String.valueOf(raw));
            assertEquals("new", raw.get("label"), String.valueOf(raw));
        }
    }
}
