package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.objectmapping.ShortMapper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * MOR-084: a {@code short} or {@code Short} field is written as a number, like int and long. It was written as a Base64 Java-serialization blob
 * ({@code {_b64data: ...}}), so it could not be compared, sorted or incremented in the database.
 */
@Tag("core")
public class ShortFieldTest extends MultiDriverTestBase {

    @Entity
    public static class Bits {
        @Id
        public String id;
        public short small;
        public Short boxed;
    }

    private static Bits bits(String id, int small, Integer boxed) {
        Bits b = new Bits();
        b.id = id;
        b.small = (short) small;
        b.boxed = boxed == null ? null : boxed.shortValue();
        return b;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aShortIsStoredAsANumberAndReadBack(Morphium m) throws Exception {
        try (m) {
            Bits b = bits("b1", 5, 300);
            m.store(b);

            Map<String, Object> doc = m.getMapper().serialize(b);
            assertEquals(5, ((Number) doc.get("small")).intValue());
            assertEquals(300, ((Number) doc.get("boxed")).intValue());

            Bits back = m.createQueryFor(Bits.class).f("id").eq("b1").get();
            assertEquals((short) 5, back.small);
            assertEquals(Short.valueOf((short) 300), back.boxed);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aShortCanBeComparedAndSorted(Morphium m) throws Exception {
        try (m) {
            m.store(bits("a", 1, 10));
            m.store(bits("b", 2, 30));
            m.store(bits("c", 3, 20));

            assertEquals(2, m.createQueryFor(Bits.class).f("boxed").lt((short) 25).countAll());
            assertEquals(1, m.createQueryFor(Bits.class).f("boxed").eq((short) 30).countAll());
            assertEquals(List.of("b", "c", "a"), m.createQueryFor(Bits.class).sort(Map.of("boxed", -1, "id", 1)).asList().stream().map(x -> x.id).toList());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aShortCanBeUpdated(Morphium m) throws Exception {
        try (m) {
            m.store(bits("a", 1, 10));

            m.createQueryFor(Bits.class).f("id").eq("a").modify().set("boxed", (short) 77).inc("small", 4).updateOne();

            Bits back = m.createQueryFor(Bits.class).f("id").eq("a").get();
            assertEquals(Short.valueOf((short) 77), back.boxed);
            assertEquals((short) 5, back.small);
        }
    }

    @Test
    public void aShortThatWasWrittenAsABlobIsStillRead() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(Short.valueOf((short) 7));
        }

        ShortMapper mapper = new ShortMapper();

        assertEquals(Short.valueOf((short) 7), mapper.unmarshall(Map.of("_b64data", Base64.getEncoder().encodeToString(bytes.toByteArray()))));
        assertEquals(Short.valueOf((short) 8), mapper.unmarshall(8L));
        assertEquals(Short.valueOf((short) 9), mapper.unmarshall(9.0));
    }

    @Test
    public void aBlobThatIsNoShortIsRefused() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(new java.util.ArrayList<>(List.of("x")));
        }

        assertThrows(IllegalArgumentException.class, () -> new ShortMapper().unmarshall(Map.of("_b64data", Base64.getEncoder().encodeToString(bytes.toByteArray()))));
    }
}
