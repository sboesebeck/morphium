package de.caluga.test.mongo.suite.contract;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MOR-044: a {@code @Version(fieldName = ...)} field is stored under that name by store(), and the filters and the update builder use the same name. */
@Tag("core")
public class VersionFieldNameTest extends MultiDriverTestBase {

    @Entity
    @NoCache
    public static class Versioned {
        @Id
        public String id;
        public String value;
        @Version(fieldName = "rev")
        public Long revision;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void storeFilterAndBuilderAgreeOnTheName(Morphium m) throws Exception {
        try (m) {
            m.dropCollection(Versioned.class);
            Versioned v = new Versioned();
            v.id = "a";
            v.value = "one";
            m.store(v);

            Map<String, Object> raw = ModifyFixturesAccess.rawDocs(m.createQueryFor(Versioned.class).f("id").eq("a")).get(0);
            assertTrue(raw.containsKey("rev"), "stored under the name of the annotation: " + raw);
            assertFalse(raw.containsKey("revision"), "not under the Java name: " + raw);
            assertFalse(raw.containsKey("revision".toLowerCase()) && !raw.containsKey("rev"), String.valueOf(raw));

            Versioned read = m.createQueryFor(Versioned.class).f("id").eq("a").get();
            assertNotNull(read);
            assertEquals(1L, read.revision, "read back from the stored name");

            assertEquals(1, m.createQueryFor(Versioned.class).f("revision").eq(1L).countAll(), "the filter on the Java name reaches the stored name");

            read.value = "two";
            m.store(read);
            assertEquals(2L, read.revision);

            m.createQueryFor(Versioned.class).f("id").eq("a").modify().set("value", "three").findOneAndUpdate(de.caluga.morphium.query.ReturnDocument.AFTER);
            Versioned after = m.createQueryFor(Versioned.class).f("id").eq("a").get();
            assertEquals("three", after.value);
            assertEquals(3L, after.revision, "the builder increments the same field");
            raw = ModifyFixturesAccess.rawDocs(m.createQueryFor(Versioned.class).f("id").eq("a")).get(0);
            assertFalse(raw.containsKey("revision"), "the builder did not add a second field: " + raw);
        }
    }
}
