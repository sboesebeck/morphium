package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.driver.MorphiumId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The locale of an {@link Index} annotation must end up as collation on every index the
 * annotation declares. It used to be appended as an extra entry to the positional options list:
 * without options that was a NullPointerException, with options it landed on the wrong index
 * or on none, and on field indexes the locale was ignored.
 */
@Tag("core")
public class IndexLocaleTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void localeWithoutOptions(Morphium morphium) throws Exception {
        try (morphium) {
            List<IndexDescription> idx = morphium.getIndexesFromEntity(LocaleNoOptions.class);
            assertEquals(1, idx.size());
            assertCollationDe(idx.get(0));

            morphium.dropCollection(LocaleNoOptions.class);
            morphium.ensureIndicesFor(LocaleNoOptions.class);
            assertTrue(morphium.getMissingIndicesFor(LocaleNoOptions.class).isEmpty(),
                () -> "indexes after ensureIndicesFor: " + morphium.getMissingIndicesFor(LocaleNoOptions.class));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void localeAppliesToEveryIndexWithOptions(Morphium morphium) throws Exception {
        try (morphium) {
            List<IndexDescription> idx = morphium.getIndexesFromEntity(LocaleWithOptions.class);
            assertEquals(2, idx.size());

            IndexDescription name = idx.stream().filter(i -> i.getKey().containsKey("name")).findFirst().orElseThrow();
            IndexDescription city = idx.stream().filter(i -> i.getKey().containsKey("city")).findFirst().orElseThrow();
            assertEquals(Boolean.TRUE, name.getUnique());
            assertNotEquals(Boolean.TRUE, city.getUnique());
            assertCollationDe(name);
            assertCollationDe(city);

            morphium.dropCollection(LocaleWithOptions.class);
            morphium.ensureIndicesFor(LocaleWithOptions.class);
            assertTrue(morphium.getMissingIndicesFor(LocaleWithOptions.class).isEmpty(),
                () -> "indexes after ensureIndicesFor: " + morphium.getMissingIndicesFor(LocaleWithOptions.class));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void localeOnField(Morphium morphium) throws Exception {
        try (morphium) {
            List<IndexDescription> idx = morphium.getIndexesFromEntity(LocaleOnField.class);
            assertEquals(1, idx.size());
            assertCollationDe(idx.get(0));

            morphium.dropCollection(LocaleOnField.class);
            morphium.ensureIndicesFor(LocaleOnField.class);
            assertTrue(morphium.getMissingIndicesFor(LocaleOnField.class).isEmpty(),
                () -> "indexes after ensureIndicesFor: " + morphium.getMissingIndicesFor(LocaleOnField.class));
        }
    }

    private static void assertCollationDe(IndexDescription idx) {
        Map<String, Object> collation = idx.getCollation();
        assertNotNull(collation, () -> "no collation on " + idx);
        assertEquals("de", collation.get("locale"));
        assertEquals(2, collation.get("strength"));
        assertEquals(true, collation.get("numericOrdering"));
    }

    @Entity
    @Index(value = {"name"}, locale = "de", strength = de.caluga.morphium.Collation.Strength.SECONDARY, numericOrdering = true)
    public static class LocaleNoOptions {
        @Id
        public MorphiumId id;
        public String name;
    }

    @Entity
    @Index(value = {"name", "city"}, options = {"unique:true", ""}, locale = "de",
           strength = de.caluga.morphium.Collation.Strength.SECONDARY, numericOrdering = true)
    public static class LocaleWithOptions {
        @Id
        public MorphiumId id;
        public String name;
        public String city;
    }

    @Entity
    public static class LocaleOnField {
        @Id
        public MorphiumId id;
        @Index(locale = "de", strength = de.caluga.morphium.Collation.Strength.SECONDARY, numericOrdering = true)
        public String name;
    }
}
