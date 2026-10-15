package de.caluga.morphium.query;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Aliases;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.annotations.encryption.Encrypted;
import de.caluga.morphium.encryption.AESEncryptionProvider;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-041: key translation of {@link UpdateDocumentMapper} for every operator. The mapper is exercised without a database round trip.
 */
@Tag("core")
public class UpdateDocumentMapperKeyTest extends MultiDriverTestBase {

    @Embedded
    public static class Child {
        public String fileName;
    }

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
        @Encrypted(provider = AESEncryptionProvider.class, keyName = "key")
        public String secretValue;
    }

    public enum LeaseField {
        ownerName, expiresAt
    }

    private static Map<String, Object> m(Object... kv) {
        Map<String, Object> r = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            r.put((String) kv[i], kv[i + 1]);
        }
        return r;
    }

    private UpdateDocumentMapper mapper(Morphium morphium) {
        return new UpdateDocumentMapper(morphium, Lease.class, true);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void everyValueOperatorTranslatesItsKeys(Morphium morphium) throws Exception {
        try (morphium) {
            Object one = 1;

            for (String op : List.of("$set", "$setOnInsert", "$inc", "$mul", "$min", "$max", "$pop", "$currentDate", "$bit", "$pullAll", "$unset")) {
                Map<String, Object> mapped = mapper(morphium).map(m(op, m("ownerName", one, "retryCount", one, "expiresAt", one, "id", one, "renamed", one)));
                assertEquals(List.of("owner_name", "retry_count", "expires_at", "_id", "custom_x"), List.copyOf(((Map<?, ?>) mapped.get(op)).keySet()), op);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void pushAddToSetAndPullTranslateTheFieldButNotTheModifiers(Morphium morphium) throws Exception {
        try (morphium) {
            Map<String, Object> each = m("$each", List.of("a", "b"), "$position", 0, "$slice", 5, "$sort", 1);

            for (String op : List.of("$push", "$addToSet")) {
                Map<String, Object> mapped = mapper(morphium).map(m(op, m("tagList", each)));
                Map<?, ?> fields = (Map<?, ?>) mapped.get(op);
                assertEquals(List.of("tag_list"), List.copyOf(fields.keySet()));
                assertEquals(List.of("$each", "$position", "$slice", "$sort"), List.copyOf(((Map<?, ?>) fields.get("tag_list")).keySet()));
            }

            // the keys of a $pull condition are data, not field names of the entity
            Map<String, Object> pulled = mapper(morphium).map(m("$pull", m("tagList", m("fileName", "x"))));
            assertEquals(m("tag_list", m("fileName", "x")), pulled.get("$pull"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void renameTranslatesKeyAndTarget(Morphium morphium) throws Exception {
        try (morphium) {
            Map<String, Object> mapped = mapper(morphium).map(m("$rename", m("ownerName", "expiresAt", "legacy_x", "retryCount", "tagList", 5)));
            assertEquals(m("owner_name", "expires_at", "legacy_x", "retry_count", "tag_list", 5), mapped.get("$rename"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void dottedKeysGoThroughSingleEmbeddedFields(Morphium morphium) throws Exception {
        try (morphium) {
            Map<String, Object> mapped = mapper(morphium).map(m("$set", m("childEntry.fileName", "f"), "$unset", m("childEntry.fileName", ""), "$rename", m("childEntry.fileName", "childEntry.other")));
            assertEquals(m("child_entry.file_name", "f"), mapped.get("$set"));
            assertEquals(m("child_entry.file_name", ""), mapped.get("$unset"));
            assertEquals(m("child_entry.file_name", "child_entry.other"), mapped.get("$rename"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void aliasUnknownPositionalAndDollarKeysPassThrough(Morphium morphium) throws Exception {
        try (morphium) {
            Map<String, Object> in = m("$set", m("legacy_owner", 1, "unknown", 2, "tagList.$", 3, "tagList.$[x]", 4, "$weird", 5));
            Map<String, Object> mapped = mapper(morphium).map(in);
            assertEquals(m("legacy_owner", 1, "unknown", 2, "tag_list.$", 3, "tag_list.$[x]", 4, "$weird", 5), mapped.get("$set"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void enumKeysAreAccepted(Morphium morphium) throws Exception {
        try (morphium) {
            Map<Object, Object> fields = new LinkedHashMap<>();
            fields.put(LeaseField.ownerName, "x");
            fields.put(LeaseField.expiresAt, 1);
            Map<String, Object> update = new LinkedHashMap<>();
            update.put("$set", fields);
            @SuppressWarnings({"unchecked", "rawtypes"}) Map<String, Object> mapped = mapper(morphium).map(update);
            assertEquals(m("owner_name", "x", "expires_at", 1), mapped.get("$set"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void translationOffReturnsTheKeysUnchanged(Morphium morphium) throws Exception {
        try (morphium) {
            Map<String, Object> in = m("$set", m("ownerName", "x", "id", "i"), "$unset", m("expiresAt", ""), "$rename", m("tagList", "retryCount"));
            Map<String, Object> mapped = new UpdateDocumentMapper(morphium, Lease.class, false).map(in);
            assertEquals(in, mapped);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void replacementDocumentIsNotTouched(Morphium morphium) throws Exception {
        try (morphium) {
            Map<String, Object> in = m("ownerName", "x", "expiresAt", 1);
            assertEquals(in, mapper(morphium).map(in));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void keysThatCollideAfterTranslationAreMergedIfTheValuesAreEqual(Morphium morphium) throws Exception {
        try (morphium) {
            Map<String, Object> mapped = mapper(morphium).map(m("$set", m("ownerName", "x", "owner_name", "x")));
            assertEquals(m("owner_name", "x"), mapped.get("$set"));
            mapped = mapper(morphium).map(m("$set", m("ownerName", List.of("a", "b"), "owner_name", List.of("a", "b"))));
            assertEquals(m("owner_name", List.of("a", "b")), mapped.get("$set"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void keysThatCollideAfterTranslationAreRejectedIfTheValuesDiffer(Morphium morphium) throws Exception {
        try (morphium) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> mapper(morphium).map(m("$set", m("ownerName", "x", "owner_name", "y"))));
            assertTrue(e.getMessage().contains("ownerName") && e.getMessage().contains("owner_name"), e.getMessage());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void mappingIsIdempotentAndDoesNotModifyTheCallersMap(Morphium morphium) throws Exception {
        try (morphium) {
            Map<String, Object> in = m("$set", m("ownerName", "x", "childEntry.fileName", "f"), "$unset", m("expiresAt", ""), "$rename", m("tagList", "retryCount"));
            String before = in.toString();
            Map<String, Object> once = mapper(morphium).map(in);
            assertEquals(before, in.toString());
            assertEquals(once, mapper(morphium).map(once));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesInMemOnly")
    public void encryptedFieldsRejectPlaintextWritesButAllowUnsetAndRename(Morphium morphium) throws Exception {
        try (morphium) {
            for (String op : List.of("$set", "$setOnInsert", "$push", "$addToSet", "$inc")) {
                IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> mapper(morphium).map(m(op, m("secretValue", "plain"))), op);
                assertTrue(e.getMessage().contains("secretValue") && e.getMessage().contains("@Encrypted"), e.getMessage());
            }

            // independent of the translation switch and of the spelling of the key
            assertThrows(IllegalArgumentException.class, () -> new UpdateDocumentMapper(morphium, Lease.class, false).map(m("$set", m("secretValue", "plain"))));
            assertThrows(IllegalArgumentException.class, () -> mapper(morphium).map(m("$set", m("secret_value", "plain"))));

            assertFalse(mapper(morphium).map(m("$unset", m("secretValue", ""))).isEmpty());
            assertFalse(mapper(morphium).map(m("$rename", m("secretValue", "ownerName"))).isEmpty());
        }
    }
}
