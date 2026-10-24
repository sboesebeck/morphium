package de.caluga.test.mongo.suite.contract;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.migration.KeyRename;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-016: documents that were stored with snake_case keys inside an embedded document (an {@code @Embedded} with the default {@code translateCamelCase=true} under an
 * entity with {@code translateCamelCase=false}) get the camelCase keys that the embedded class would write with {@code translateCamelCase=false}, in an embedded list and in a
 * single embedded document, on the existing data.
 */
@Tag("core")
public class KeyRenameTest extends MultiDriverTestBase {

    /** what the application had: the embedded translates, so its keys are snake_case */
    @Embedded
    public static class OldChild {
        public String fileName;
        public String itemVersionId;
        public Deep innerGrandChild;
    }

    @Embedded
    public static class Deep {
        public String deepValue;
    }

    @Entity(collectionName = "mor016_owner", translateCamelCase = false)
    @NoCache
    public static class OldOwner {
        @Id
        public String id;
        public String ownerName;
        public OldChild child;
        public List<OldChild> children = new ArrayList<>();
    }

    /** what the application has after the fix: the keys stay camelCase (in the application it is the same class as {@code OldChild} with another annotation) */
    @Embedded(translateCamelCase = false)
    public static class NewChild {
        public String fileName;
        public String itemVersionId;
        public NewDeep innerGrandChild;
    }

    @Embedded(translateCamelCase = false)
    public static class NewDeep {
        public String deepValue;
    }

    @Entity(collectionName = "mor016_owner", translateCamelCase = false)
    @NoCache
    public static class NewOwner {
        @Id
        public String id;
        public String ownerName;
        public NewChild child;
        // no list: the elements of a list carry the class_name of the class that wrote them, which in the application is the same class with another annotation
    }

    private static OldChild oldChild(String file, String version, String deep) {
        OldChild c = new OldChild();
        c.fileName = file;
        c.itemVersionId = version;
        c.innerGrandChild = new Deep();
        c.innerGrandChild.deepValue = deep;
        return c;
    }

    private void seed(Morphium m, int owners) {
        m.dropCollection(OldOwner.class);

        for (int i = 0; i < owners; i++) {
            OldOwner o = new OldOwner();
            o.id = "o" + i;
            o.ownerName = "owner " + i;
            o.child = oldChild("f" + i + ".txt", "v" + i, "d" + i);
            o.children.add(oldChild("a" + i, "va" + i, "da" + i));
            o.children.add(oldChild("b" + i, "vb" + i, "db" + i));
            m.store(o);
        }
    }

    private Map<String, Object> raw(Morphium m, String id) {
        return m.createQueryFor(OldOwner.class).f("id").eq(id).asMapList().get(0);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void snakeCaseKeysBecomeCamelCaseInAListAndInASingleDocument(Morphium m) throws Exception {
        try (m) {
            seed(m, 3);
            Map<String, Object> before = raw(m, "o1");
            assertTrue(((Map<?, ?>) before.get("child")).containsKey("file_name"), "the starting point has snake_case keys: " + before);

            long changed = KeyRename.in(m, OldOwner.class).at("child").snakeToCamel().apply();
            changed += KeyRename.in(m, OldOwner.class).at("children").snakeToCamel().apply();

            assertEquals(6, changed, "3 documents, 2 paths");
            Map<String, Object> after = raw(m, "o1");
            Map<?, ?> child = (Map<?, ?>) after.get("child");
            assertEquals("f1.txt", child.get("fileName"), String.valueOf(after));
            assertEquals("v1", child.get("itemVersionId"), String.valueOf(after));
            assertEquals("d1", ((Map<?, ?>) child.get("innerGrandChild")).get("deepValue"), "deeper levels, too: " + after);
            assertFalse(child.containsKey("file_name"), String.valueOf(after));
            List<?> children = (List<?>) after.get("children");
            assertEquals("va1", ((Map<?, ?>) children.get(0)).get("itemVersionId"), String.valueOf(after));
            assertEquals("db1", ((Map<?, ?>) ((Map<?, ?>) children.get(1)).get("innerGrandChild")).get("deepValue"), String.valueOf(after));
            assertEquals("owner 1", after.get("ownerName"), "keys outside the path are untouched");

            // and the application with the fixed embedded class reads what the old one wrote
            NewOwner read = m.createQueryFor(NewOwner.class).f("id").eq("o2").get();
            assertEquals("f2.txt", read.child.fileName);
            assertEquals("d2", read.child.innerGrandChild.deepValue);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void explicitRenamesChangeOnlyTheNamedKeysAndAreRepeatable(Morphium m) throws Exception {
        try (m) {
            seed(m, 2);

            long first = KeyRename.in(m, OldOwner.class).at("children").rename("file_name", "fileName").apply();
            Map<String, Object> after = raw(m, "o0");
            Map<?, ?> c0 = (Map<?, ?>) ((List<?>) after.get("children")).get(0);
            assertEquals(2, first);
            assertEquals("a0", c0.get("fileName"), String.valueOf(after));
            assertTrue(c0.containsKey("item_version_id"), "a key that was not named stays: " + after);
            assertTrue(((Map<?, ?>) after.get("child")).containsKey("file_name"), "another path stays: " + after);

            assertEquals(0, KeyRename.in(m, OldOwner.class).at("children").rename("file_name", "fileName").apply(), "a second run finds nothing to do");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aRenameThatWouldOverwriteAKeyIsRefusedAndWritesNothing(Morphium m) throws Exception {
        try (m) {
            seed(m, 1);
            m.createQueryFor(NewOwner.class).f("id").eq("o0").set("child.fileName", "already there");

            IllegalStateException e = assertThrows(IllegalStateException.class, () -> KeyRename.in(m, OldOwner.class).at("child").snakeToCamel().apply());

            assertTrue(e.getMessage().contains("file_name") && e.getMessage().contains("fileName") && e.getMessage().contains("o0"), e.getMessage());
            assertTrue(((Map<?, ?>) raw(m, "o0").get("child")).containsKey("file_name"), "nothing was written: " + raw(m, "o0"));
        }
    }
}
