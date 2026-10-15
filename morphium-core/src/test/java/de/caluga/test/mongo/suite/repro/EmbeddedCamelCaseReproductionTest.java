package de.caluga.test.mongo.suite.repro;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reproduction of hypothesis 2 of the Morphium update-path research (docs/research/reproduction-results.md): an {@code @Embedded} class whose
 * {@code translateCamelCase} is left at its default ({@code true}) does NOT inherit
 * {@code translateCamelCase=false} from the owning {@code @Entity}; its field names are stored as
 * snake_case while the entity's own fields stay camelCase.
 * <p>
 * The mapper is independent of the driver, but the test is parameterised over the drivers anyway so the
 * raw document is read the way an application would (query + {@code asMapList()}) on a real MongoDB as well.
 * It asserts the CURRENT behaviour; if the inheritance is ever implemented the assertions must be
 * flipped to the positive expectation.
 */
@Tag("core")
public class EmbeddedCamelCaseReproductionTest extends MultiDriverTestBase {

    @Entity(translateCamelCase = false)
    public static class Owner {
        @Id
        public MorphiumId id;
        public String ownerName;
        public DefaultChild defaultChild;
        public List<DefaultChild> defaultChildren;
        public ExplicitChild explicitChild;
        public List<ExplicitChild> explicitChildren;
    }

    /** Embedded with the annotation default translateCamelCase = true. */
    @Embedded
    public static class DefaultChild {
        public String fileName;
        public String itemVersionId;
        public DefaultGrandChild innerGrandChild;
    }

    @Embedded
    public static class DefaultGrandChild {
        public String deepValue;
    }

    @Embedded(translateCamelCase = false)
    public static class ExplicitChild {
        public String fileName;
        public String itemVersionId;
    }

    /** The opposite combination: translating entity, embedded that opts out. */
    @Entity
    public static class TranslatingOwner {
        @Id
        public MorphiumId id;
        public String ownerName;
        public ExplicitChild explicitChild;
        public DefaultChild defaultChild;
    }

    private static void collectKeys(Object node, String path, Set<String> out) {
        if (node instanceof Map<?, ?> m) {
            for (var e : m.entrySet()) {
                String p = path.isEmpty() ? String.valueOf(e.getKey()) : path + "." + e.getKey();
                out.add(p);
                collectKeys(e.getValue(), p, out);
            }
        } else if (node instanceof List<?> l) {
            for (Object o : l) {
                collectKeys(o, path + "[]", out);
            }
        }
    }

    private static DefaultChild defaultChild() {
        DefaultChild c = new DefaultChild();
        c.fileName = "f.txt";
        c.itemVersionId = "v1";
        c.innerGrandChild = new DefaultGrandChild();
        c.innerGrandChild.deepValue = "d";
        return c;
    }

    private static ExplicitChild explicitChild() {
        ExplicitChild c = new ExplicitChild();
        c.fileName = "f.txt";
        c.itemVersionId = "v1";
        return c;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void embeddedDefaultUnderNonTranslatingEntity(Morphium morphium) throws Exception {
        try (morphium) {
            String driver = morphium.getDriver().getName();
            Owner o = new Owner();
            o.ownerName = "o";
            o.defaultChild = defaultChild();
            o.defaultChildren = new ArrayList<>(List.of(defaultChild()));
            o.explicitChild = explicitChild();
            o.explicitChildren = new ArrayList<>(List.of(explicitChild()));
            morphium.store(o);

            List<Map<String, Object>> docs = morphium.createQueryFor(Owner.class).asMapList();
            assertEquals(1, docs.size());
            Map<String, Object> raw = docs.get(0);
            Set<String> keys = new TreeSet<>();
            collectKeys(raw, "", keys);
            ReproSupport.report(driver, "H2 raw document", String.valueOf(raw));
            ReproSupport.report(driver, "H2 all key paths", String.valueOf(keys));

            // entity level: untouched
            assertTrue(raw.containsKey("ownerName"));
            assertTrue(raw.containsKey("defaultChild"));
            assertTrue(raw.containsKey("defaultChildren"));

            // embedded with default translateCamelCase: snake_case, even though the owner opted out
            Map<?, ?> dc = (Map<?, ?>) raw.get("defaultChild");
            assertTrue(dc.containsKey("file_name"), "default embedded: file_name expected, keys=" + dc.keySet());
            assertTrue(dc.containsKey("item_version_id"));
            assertTrue(dc.containsKey("inner_grand_child"), "nested default embedded is translated as well");
            assertTrue(((Map<?, ?>) dc.get("inner_grand_child")).containsKey("deep_value"));
            assertTrue(!dc.containsKey("fileName"));
            Map<?, ?> dcl = (Map<?, ?>) ((List<?>) raw.get("defaultChildren")).get(0);
            assertTrue(dcl.containsKey("file_name"));
            assertTrue(dcl.containsKey("item_version_id"));

            // embedded that opts out explicitly: camelCase
            Map<?, ?> ec = (Map<?, ?>) raw.get("explicitChild");
            assertTrue(ec.containsKey("fileName"), "explicit embedded keeps camelCase, keys=" + ec.keySet());
            assertTrue(ec.containsKey("itemVersionId"));
            Map<?, ?> ecl = (Map<?, ?>) ((List<?>) raw.get("explicitChildren")).get(0);
            assertTrue(ecl.containsKey("fileName"));

            // the round trip stays lossless (deserialisation uses the same per-class rule)
            Owner back = morphium.createQueryFor(Owner.class).get();
            assertEquals("f.txt", back.defaultChild.fileName);
            assertEquals("v1", back.defaultChild.itemVersionId);
            assertEquals("d", back.defaultChild.innerGrandChild.deepValue);
            assertEquals("f.txt", back.explicitChild.fileName);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void embeddedOptOutUnderTranslatingEntity(Morphium morphium) throws Exception {
        try (morphium) {
            TranslatingOwner o = new TranslatingOwner();
            o.ownerName = "o";
            o.explicitChild = explicitChild();
            o.defaultChild = defaultChild();
            morphium.store(o);
            Map<String, Object> raw = morphium.createQueryFor(TranslatingOwner.class).asMapList().get(0);
            ReproSupport.report(morphium.getDriver().getName(), "H2 opposite combination raw document", String.valueOf(raw));
            assertTrue(raw.containsKey("owner_name"), "translating entity: snake_case at top level");
            assertTrue(raw.containsKey("explicit_child"));
            assertTrue(((Map<?, ?>) raw.get("explicit_child")).containsKey("fileName"),
                       "embedded opt-out wins regardless of the owner");
            assertTrue(((Map<?, ?>) raw.get("default_child")).containsKey("file_name"));
        }
    }
}
