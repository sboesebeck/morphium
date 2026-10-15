package de.caluga.morphium;

import de.caluga.morphium.annotations.AdditionalData;
import de.caluga.morphium.annotations.Aliases;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.IgnoreNullFromDB;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.annotations.ReadOnly;
import de.caluga.morphium.annotations.Reference;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.AliasesEntity;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.ListContainer;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-041: {@link AnnotationAndReflectionHelper#resolveStoredFieldName(Class, String)} maps the Java name or the stored name of a
 * persistent field to the name {@code serialize()} stores it under, and leaves everything else unchanged. Pure unit test, no database.
 */
@Tag("core")
public class StoredFieldNameResolverTest {

    @Embedded
    public static class Child {
        public String fileName;
    }

    @Entity
    public static class Target {
        @Id
        public MorphiumId id;
    }

    /** default naming (translateCamelCase = true) */
    @Entity
    public static class Lease {
        @Id
        public String id;
        public String ownerName;
        public Date expiresAt;
        public int retryCount;
        public List<String> tagList;
        @Property(fieldName = "custom_x")
        public String renamed;
        @Aliases({"legacy_owner"})
        public String currentOwner;
        public Child childEntry;
        @Reference(fieldName = "ref_target")
        public Target target;
        @Transient
        public String ignored;
        @ReadOnly
        public String readOnlyValue;
        public static String STATIC_FIELD = "x";
        @AdditionalData
        public Map<String, Object> extra;
        @Version
        public Long entityVersion;
    }

    @Entity(translateCamelCase = false)
    public static class RawLease {
        @Id
        public MorphiumId id;
        public String ownerName;
        public String ownerNameCopy;
    }

    @Entity
    public static class Shadowed {
        @Id
        public MorphiumId id;
        public String owner_name;
        public String ownerName;
    }

    @Entity(translateCamelCase = false)
    public static class Crossed {
        @Id
        public MorphiumId id;
        @Property(fieldName = "y")
        public String x;
        @Property(fieldName = "z")
        public String y;
    }

    @Entity(translateCamelCase = false)
    public static class AliasClash {
        @Id
        public MorphiumId id;
        @Aliases({"b"})
        public String a;
        public String b;
    }

    private AnnotationAndReflectionHelper helper() {
        return new AnnotationAndReflectionHelper(true);
    }

    private String r(Class<?> c, String key) {
        return helper().resolveStoredFieldName(c, key);
    }

    @Test
    public void javaNameOfATranslatingEntityBecomesTheStoredName() {
        assertEquals("owner_name", r(Lease.class, "ownerName"));
        assertEquals("expires_at", r(Lease.class, "expiresAt"));
        assertEquals("retry_count", r(Lease.class, "retryCount"));
        assertEquals("tag_list", r(Lease.class, "tagList"));
        assertEquals("child_entry", r(Lease.class, "childEntry"));
        assertEquals("entity_version", r(Lease.class, "entityVersion"));
    }

    @Test
    public void storedNamesAreFixedPoints() {
        for (String s : List.of("owner_name", "expires_at", "retry_count", "tag_list", "custom_x", "ref_target", "_id", "child_entry")) {
            assertEquals(s, r(Lease.class, s));
        }
    }

    @Test
    public void idBecomesUnderscoreId() {
        assertEquals("_id", r(Lease.class, "id"));
        assertEquals("_id", r(RawLease.class, "id"));
    }

    @Test
    public void propertyAndReferenceNamesWorkInBothDirections() {
        assertEquals("custom_x", r(Lease.class, "renamed"));
        assertEquals("custom_x", r(Lease.class, "custom_x"));
        assertEquals("ref_target", r(Lease.class, "target"));
        assertEquals("ref_target", r(Lease.class, "ref_target"));
    }

    @Test
    public void snakeFormOfAJavaNameResolvesLikeTheFilterDoes() {
        // Query.f("owner_name") resolves through getField rule 6 for @Property fields, too
        assertEquals("custom_x", r(Lease.class, "renamed"));
        assertEquals("_id", r(RawLease.class, "id"));
    }

    @Test
    public void aliasesAreNeverResolved() {
        assertEquals("legacy_owner", r(Lease.class, "legacy_owner"));
        assertEquals("current_owner", r(Lease.class, "currentOwner"));
    }

    @Test
    public void nonTranslatingEntityKeepsJavaNames() {
        assertEquals("ownerName", r(RawLease.class, "ownerName"));
        assertEquals("owner_name", r(RawLease.class, "owner_name"), "an unknown key stays unknown, it is not rewritten to the Java name");
    }

    @Test
    public void passThroughKeys() {
        for (String k : List.of("unknown", "legacy_key", "a.b", "$", "$[x]", "$[]", "$set", "", " ", "child_entry.file_name")) {
            assertEquals(k, r(Lease.class, k));
        }
        assertEquals(null, r(Lease.class, null));
    }

    @Test
    public void transientStaticAndAdditionalDataFieldsAreNotPersistent() {
        assertEquals("ignored", r(Lease.class, "ignored"));
        assertEquals("STATIC_FIELD", r(Lease.class, "STATIC_FIELD"));
        assertEquals("extra", r(Lease.class, "extra"));
    }

    @Test
    public void readOnlyFieldsResolveBecauseAnExplicitUpdateMayWriteThem() {
        assertEquals("read_only_value", r(Lease.class, "readOnlyValue"));
    }

    @Test
    public void globalCamelCaseConversionOffUsesJavaNames() {
        AnnotationAndReflectionHelper h = new AnnotationAndReflectionHelper(false);
        assertEquals("ownerName", h.resolveStoredFieldName(Lease.class, "ownerName"));
        assertEquals("_id", h.resolveStoredFieldName(Lease.class, "id"));
    }

    @Test
    public void interfaceAndNullTypesNeverThrow() {
        assertEquals("x", helper().resolveStoredFieldName(Runnable.class, "x"));
        assertEquals("x", helper().resolveStoredFieldName(null, "x"));
        assertEquals("x", helper().resolveStoredFieldName(Map.class, "x"));
    }

    @Test
    public void duplicateJavaAndSnakeNameStoreUnderTheSameName() {
        // both fields are stored as owner_name (serialize logs "shadowed"); both keys must lead to it
        assertEquals("owner_name", r(Shadowed.class, "ownerName"));
        assertEquals("owner_name", r(Shadowed.class, "owner_name"));
    }

    @Test
    public void aMongoNameBeatsAForeignJavaName() {
        // x is stored as y, y is stored as z: the key y is the stored name of x
        assertEquals("y", r(Crossed.class, "x"));
        assertEquals("y", r(Crossed.class, "y"));
        assertEquals("z", r(Crossed.class, "z"));
    }

    @Test
    public void aliasEqualToAnotherFieldsNameFollowsSerialize() {
        assertEquals("a", r(AliasClash.class, "a"));
        assertEquals("b", r(AliasClash.class, "b"));
    }

    @Test
    public void resolutionIsIdempotent() {
        Random rnd = new Random(42);
        List<String> keys = new ArrayList<>(List.of("ownerName", "owner_name", "id", "_id", "renamed", "custom_x", "target", "ref_target", "legacy_owner",
                                                    "x", "y", "z", "a", "b", "a.b", "$", "", "ownerNameCopy", "OwnerName", "owner__name"));

        for (int i = 0; i < 200; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < 1 + rnd.nextInt(12); j++) {
                sb.append("abcXYZ_$.".charAt(rnd.nextInt(9)));
            }
            keys.add(sb.toString());
        }

        for (Class<?> c : List.of(Lease.class, RawLease.class, Shadowed.class, Crossed.class, AliasClash.class)) {
            for (String k : keys) {
                String once = r(c, k);
                assertEquals(once, r(c, once), c.getSimpleName() + " not idempotent for '" + k + "'");
            }
        }
    }

    @Test
    public void resolverAgreesWithSerialize() throws Exception {
        AnnotationAndReflectionHelper h = new AnnotationAndReflectionHelper(true);
        List<Class<?>> corpus = List.of(Lease.class, RawLease.class, Shadowed.class, Crossed.class, AliasClash.class, ComplexObject.class,
                                        AliasesEntity.class, UncachedObject.class, EmbeddedObject.class, ListContainer.class);
        de.caluga.morphium.objectmapping.MorphiumObjectMapper mapper = new ObjectMapperImpl();
        mapper.setAnnotationHelper(h);

        for (Class<?> c : corpus) {
            Object o = c.getDeclaredConstructor().newInstance();
            Map<String, Object> doc = mapper.serialize(o);
            Set<String> stored = new HashSet<>(doc.keySet());
            stored.remove("class_name");

            // every stored name is a fixed point
            for (String s : stored) {
                assertEquals(s, h.resolveStoredFieldName(c, s), c.getSimpleName() + ": stored name " + s);
            }

            // the Java name of every persistent, non-read-only field resolves to a stored name
            for (Field f : h.getAllFields(c)) {
                if (Modifier.isStatic(f.getModifiers()) || f.isAnnotationPresent(AdditionalData.class) || f.isAnnotationPresent(Transient.class)
                    || f.isAnnotationPresent(ReadOnly.class) || f.isAnnotationPresent(IgnoreNullFromDB.class) || f.getName().startsWith("$")) {
                    continue;
                }

                String resolved = h.resolveStoredFieldName(c, f.getName());
                // a null @Id is not part of the serialised document
                assertTrue(stored.contains(resolved) || resolved.equals("_id"),
                           c.getSimpleName() + ": " + f.getName() + " resolved to " + resolved + " but serialize() wrote " + stored);
            }
        }
    }

    @Test
    public void concurrentFirstUseIsConsistent() throws Exception {
        for (int round = 0; round < 20; round++) {
            AnnotationAndReflectionHelper h = new AnnotationAndReflectionHelper(true);
            int threads = 16;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<String>> res = new ArrayList<>();

            for (int i = 0; i < threads; i++) {
                res.add(pool.submit(() -> {
                    go.await();
                    return h.resolveStoredFieldName(Lease.class, "expiresAt") + "|" + h.resolveStoredFieldName(Lease.class, "id");
                }));
            }

            go.countDown();

            for (Future<String> f : res) {
                assertEquals("expires_at|_id", f.get(10, TimeUnit.SECONDS));
            }

            pool.shutdown();
        }
    }

    @Test
    public void realClassCacheOfTheDefaultConstructorIsThreadSafe() throws Exception {
        AnnotationAndReflectionHelper h = new AnnotationAndReflectionHelper(true);
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<Class<?>>> res = new ArrayList<>();
        List<Class<?>> classes = List.of(Lease.class, RawLease.class, Shadowed.class, Crossed.class, AliasClash.class, Child.class, Target.class,
                                         ComplexObject.class, UncachedObject.class, EmbeddedObject.class, ListContainer.class, AliasesEntity.class);

        for (int i = 0; i < threads * 8; i++) {
            Class<?> c = classes.get(i % classes.size());
            res.add(pool.submit(() -> {
                go.await();
                return h.getRealClass(c);
            }));
        }

        go.countDown();
        int i = 0;

        for (Future<Class<?>> f : res) {
            assertEquals(classes.get(i++ % classes.size()), f.get(10, TimeUnit.SECONDS));
        }

        pool.shutdown();
        assertNotNull(h);
    }
}
