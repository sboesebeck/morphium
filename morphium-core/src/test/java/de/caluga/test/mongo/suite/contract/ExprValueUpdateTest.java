package de.caluga.test.mongo.suite.contract;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * MOR-038: an {@link Expr} as the value of an update operand is replaced by what it describes, and the values inside are marshalled like every other value of
 * an update document. An embedded object in an {@code Expr} reached the encoder as a Java object and failed with {@code Unhandled Data type}.
 */
@Tag("core")
public class ExprValueUpdateTest extends MultiDriverTestBase {

    @Embedded
    public static class Inner {
        public String name;
        public int count;

        public Inner() {
        }

        public Inner(String name, int count) {
            this.name = name;
            this.count = count;
        }
    }

    @Entity
    @NoCache
    public static class Holder {
        @Id
        public String id;
        public Inner inner;
        public Map<String, Object> extra;
    }

    private void seed(Morphium m) {
        m.dropCollection(Holder.class);
        Holder h = new Holder();
        h.id = "a";
        m.store(h);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anEmbeddedObjectInAnExprIsStored(Morphium m) throws Exception {
        try (m) {
            seed(m);
            Map<String, Object> set = new LinkedHashMap<>();
            set.put("inner", Expr.parse(new Inner("from expr", 3)));

            Holder r = m.createQueryFor(Holder.class).f("id").eq("a").findOneAndUpdate(Map.of("$set", set), false, true);

            assertNotNull(r.inner);
            assertEquals("from expr", r.inner.name);
            assertEquals(3, r.inner.count);
            Map<String, Object> raw = ModifyFixturesAccess.rawDocs(m.createQueryFor(Holder.class).f("id").eq("a")).get(0);
            assertEquals("from expr", ((Map<?, ?>) raw.get("inner")).get("name"), String.valueOf(raw));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anEmbeddedObjectInADocumentExprIsStored(Morphium m) throws Exception {
        try (m) {
            seed(m);
            Map<String, Expr> doc = new LinkedHashMap<>();
            doc.put("first", Expr.parse(new Inner("one", 1)));
            doc.put("list", Expr.parse(List.of(new Inner("two", 2))));
            Map<String, Object> set = new LinkedHashMap<>();
            set.put("extra", Expr.mapExpr(doc));

            m.createQueryFor(Holder.class).f("id").eq("a").findOneAndUpdate(Map.of("$set", set));

            Map<String, Object> raw = ModifyFixturesAccess.rawDocs(m.createQueryFor(Holder.class).f("id").eq("a")).get(0);
            Map<?, ?> extra = (Map<?, ?>) raw.get("extra");
            assertEquals("one", ((Map<?, ?>) extra.get("first")).get("name"), String.valueOf(raw));
            assertEquals("two", ((Map<?, ?>) ((List<?>) extra.get("list")).get(0)).get("name"), String.valueOf(raw));
        }
    }
}
