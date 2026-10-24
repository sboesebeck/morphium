package de.caluga.test.mongo.suite.contract;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.objectmapping.SingleValueMapper;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-035: a record is an embedded value (GitHub #116 for the embedded case: the entity keeps its mutable id, a record has no identity to write back), and a single-value type is a
 * scalar in the document. Both round-trip on the InMemoryDriver and on MongoDB, also when they are queried and updated.
 */
@Tag("core")
public class RecordMappingTest extends MultiDriverTestBase {

    public enum Kind { SMALL, BIG }

    @Embedded
    public record Money(String currency, long cents) {
    }

    @Embedded
    public record Line(String sku, int quantity, Kind kind, Money price, Instant added) {
    }

    /** a value object that is stored as the bare string */
    public record Email(String value) {
    }

    @Entity
    @NoCache
    public static class Order {
        @Id
        public String id;
        public Line first;
        public List<Line> lines = new ArrayList<>();
        public Map<String, Money> prices;
        public Email contact;
    }

    private static Line line(String sku, int qty) {
        return new Line(sku, qty, Kind.BIG, new Money("EUR", 1250L * qty), Instant.parse("2026-10-24T10:00:00Z"));
    }

    private void registerAndDrop(Morphium m) {
        m.getMapper().registerCustomMapperFor(Email.class, SingleValueMapper.of(Email.class));
        m.dropCollection(Order.class);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aRecordIsAnEmbeddedValueInAFieldAListAndAMap(Morphium m) throws Exception {
        try (m) {
            registerAndDrop(m);
            Order o = new Order();
            o.id = "o1";
            o.first = line("A-1", 2);
            o.lines.add(line("B-2", 1));
            o.lines.add(line("C-3", 3));
            o.prices = Map.of("net", new Money("EUR", 100), "gross", new Money("EUR", 119));
            m.store(o);

            Order read = m.createQueryFor(Order.class).f("id").eq("o1").get();

            assertEquals(line("A-1", 2), read.first);
            assertEquals(2, read.lines.size());
            assertEquals(line("C-3", 3), read.lines.get(1));
            assertEquals(new Money("EUR", 119), read.prices.get("gross"));

            Map<String, Object> raw = ModifyFixturesAccess.rawDocs(m.createQueryFor(Order.class).f("id").eq("o1")).get(0);
            Map<?, ?> first = (Map<?, ?>) raw.get("first");
            assertEquals("A-1", first.get("sku"), String.valueOf(raw));
            assertEquals(2, ((Number) first.get("quantity")).intValue(), String.valueOf(raw));
            assertEquals(1250L * 2, ((Number) ((Map<?, ?>) first.get("price")).get("cents")).longValue(), String.valueOf(raw));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aRecordCanBeSetAndQueriedLikeAnyEmbeddedValue(Morphium m) throws Exception {
        try (m) {
            registerAndDrop(m);
            Order o = new Order();
            o.id = "o2";
            m.store(o);

            m.createQueryFor(Order.class).f("id").eq("o2").set("first", line("Z-9", 4));
            m.createQueryFor(Order.class).f("id").eq("o2").push("lines", line("Y-8", 1));

            Order read = m.createQueryFor(Order.class).f("id").eq("o2").get();
            assertEquals(line("Z-9", 4), read.first);
            assertEquals(List.of(line("Y-8", 1)), read.lines);
            assertNull(read.contact);
            assertNotNull(m.createQueryFor(Order.class).f("first.sku").eq("Z-9").get());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aSingleValueTypeIsAScalarInTheDocument(Morphium m) throws Exception {
        try (m) {
            registerAndDrop(m);
            Order o = new Order();
            o.id = "o3";
            o.contact = new Email("a@example.org");
            m.store(o);

            Map<String, Object> raw = ModifyFixturesAccess.rawDocs(m.createQueryFor(Order.class).f("id").eq("o3")).get(0);
            assertEquals("a@example.org", raw.get("contact"), "a bare string, not {value: ...}: " + raw);
            assertEquals(new Email("a@example.org"), m.createQueryFor(Order.class).f("id").eq("o3").get().contact);
            assertEquals(1, m.createQueryFor(Order.class).f("contact").eq(new Email("a@example.org")).countAll(), "the query takes the value object");

            m.createQueryFor(Order.class).f("id").eq("o3").set("contact", new Email("b@example.org"));
            assertEquals("b@example.org", ModifyFixturesAccess.rawDocs(m.createQueryFor(Order.class).f("id").eq("o3")).get(0).get("contact"));
            assertTrue(m.createQueryFor(Order.class).f("id").eq("o3").get().contact.value().startsWith("b@"));
        }
    }
}
