package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.AdditionalData;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * MOR-043: {@code Query.f} resolves a dotted path to the stored field names the way the update side does ({@code AnnotationAndReflectionHelper.resolveFieldPath}), so a filter
 * and an update on the same path address the same stored field. Before, a path behind a List, a Map or an Object field was taken as written, the leading segment included,
 * and an entity with {@code @AdditionalData} took every path as written.
 */
@Tag("core")
public class QueryFieldPathTranslationTest extends MultiDriverTestBase {

    @Embedded
    public static class Line {
        public String label;
        public int amount;

        public Line() {
        }

        public Line(String label, int amount) {
            this.label = label;
            this.amount = amount;
        }
    }

    @Embedded
    public static class Box {
        public String innerValue;
    }

    @Entity
    public static class Order {
        @Id
        public String id;
        public List<Line> lineItems = new ArrayList<>();
        public Map<String, String> labelMap = new HashMap<>();
        public Box boxInfo;
        public String orderNumber;
    }

    @Entity
    public static class Open {
        @Id
        public String id;
        public String orderNumber;
        public Box boxInfo;
        @AdditionalData(readOnly = false)
        public Map<String, Object> extra = new HashMap<>();
    }

    private void fresh(Morphium m) {
        m.dropCollection(Order.class);
        Order o = new Order();
        o.id = "o1";
        o.orderNumber = "N-1";
        o.lineItems.add(new Line("first", 1));
        o.lineItems.add(new Line("second", 2));
        o.labelMap.put("color", "red");
        o.boxInfo = new Box();
        o.boxInfo.innerValue = "in";
        m.store(o);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aPathBehindAListIsTranslatedUpToTheList(Morphium m) throws Exception {
        try (m) {
            fresh(m);
            assertEquals(1, m.createQueryFor(Order.class).f("lineItems.label").eq("second").countAll(), "the list field is translated, the rest is as written");
            assertEquals(1, m.createQueryFor(Order.class).f("lineItems.amount").gt(1).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aFilterAndAnUpdateOnTheSamePathAddressTheSameField(Morphium m) throws Exception {
        try (m) {
            fresh(m);
            m.createQueryFor(Order.class).f("id").eq("o1").findOneAndUpdate(Map.of("$set", Map.of("lineItems.0.label", "changed")), false, true);
            assertEquals(1, m.createQueryFor(Order.class).f("lineItems.label").eq("changed").countAll(), "the filter finds what the update wrote");
            assertEquals("changed", m.createQueryFor(Order.class).f("id").eq("o1").get().lineItems.get(0).label);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aPathIntoAMapIsTranslatedUpToTheMap(Morphium m) throws Exception {
        try (m) {
            fresh(m);
            assertEquals(1, m.createQueryFor(Order.class).f("labelMap.color").eq("red").countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aPathThroughASingleEmbeddedFieldStillWorks(Morphium m) throws Exception {
        try (m) {
            fresh(m);
            assertEquals(1, m.createQueryFor(Order.class).f("boxInfo.innerValue").eq("in").countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anUnknownFirstSegmentIsStillRejected(Morphium m) throws Exception {
        try (m) {
            assertThrows(RuntimeException.class, () -> m.createQueryFor(Order.class).f("unknownPart.x"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anEntityWithAdditionalDataTranslatesItsKnownPathsAndTakesTheOthersAsWritten(Morphium m) throws Exception {
        try (m) {
            m.dropCollection(Open.class);
            Open o = new Open();
            o.id = "x";
            o.orderNumber = "N-7";
            o.boxInfo = new Box();
            o.boxInfo.innerValue = "in";
            o.extra.put("freeForm", "yes");
            m.store(o);
            assertEquals(1, m.createQueryFor(Open.class).f("orderNumber").eq("N-7").countAll(), "a known field is translated to the stored name");
            assertEquals(1, m.createQueryFor(Open.class).f("boxInfo.innerValue").eq("in").countAll(), "a known dotted path is translated, too");
            assertEquals(1, m.createQueryFor(Open.class).f("freeForm").eq("yes").countAll(), "a key of the additional data is taken as written");
        }
    }
}
