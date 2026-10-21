package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** MOR-084: a Short in a list, a map, a set and an array is written as a number and read back as a Short. */
@Tag("core")
public class ShortCollectionsTest extends MultiDriverTestBase {
    @Entity
    public static class Holder {
        @Id
        public String id;
        public List<Short> list;
        public Map<String, Short> map;
        public Set<Short> set;
        public short[] arr;
        public Short[] boxedArr;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void collections(Morphium m) throws Exception {
        try (m) {
            Holder h = new Holder();
            h.id = "h";
            h.list = List.of((short) 1, (short) 2);
            h.map = Map.of("a", (short) 3);
            h.set = Set.of((short) 4);
            h.arr = new short[] {5, 6};
            h.boxedArr = new Short[] {7, 8};
            m.store(h);
            Holder back = m.createQueryFor(Holder.class).f("id").eq("h").get();
            Short first = back.list.get(0);
            assertEquals((short) 1, first);
            Short viaMap = back.map.get("a");
            assertEquals((short) 3, viaMap);
            Short viaSet = back.set.iterator().next();
            assertEquals((short) 4, viaSet);
            assertEquals((short) 6, back.arr[1]);
            assertEquals((short) 8, back.boxedArr[1]);
        }
    }
}
