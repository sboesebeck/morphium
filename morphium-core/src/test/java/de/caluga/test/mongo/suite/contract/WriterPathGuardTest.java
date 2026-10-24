package de.caluga.test.mongo.suite.contract;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Reference;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.annotations.encryption.Encrypted;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.encryption.AESEncryptionProvider;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-049: the writer path ({@code Query.set}, {@code push}, {@code pushAll}) refuses what {@code findOneAndUpdate} and the update builder refuse: an entity for a
 * {@code @Reference} field would be embedded as a full document, and a plaintext value for an {@code @Encrypted} field would bypass the encryption and could not be read
 * back. Nothing is written.
 */
@Tag("core")
public class WriterPathGuardTest extends MultiDriverTestBase {

    @Entity
    @NoCache
    public static class Target {
        @Id
        public MorphiumId id;
        public String label;
    }

    @Entity
    @NoCache
    public static class Holder {
        @Id
        public String id;
        @Reference
        public Target target;
        @Reference
        public List<Target> targets;
        @Encrypted(provider = AESEncryptionProvider.class, keyName = "key")
        public String secret;
        public String plain;
    }

    private Target seed(Morphium m) {
        m.dropCollection(Holder.class);
        m.dropCollection(Target.class);
        Target t = new Target();
        t.label = "t";
        m.store(t);
        Holder h = new Holder();
        h.id = "h";
        m.store(h);
        return t;
    }

    private void assertRejected(Morphium m, String what, Runnable write) throws Exception {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, write::run, what);
        assertTrue(e.getMessage().contains(what), e.getMessage());
        Map<String, Object> raw = ModifyFixturesAccess.rawDocs(m.createQueryFor(Holder.class).f("id").eq("h")).get(0);
        assertNull(raw.get("target"), "nothing may have been written: " + raw);
        assertNull(raw.get("targets"), "nothing may have been written: " + raw);
        assertNull(raw.get("secret"), "nothing may have been written: " + raw);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void setOfAnEntityIntoAReferenceFieldIsRejected(Morphium m) throws Exception {
        try (m) {
            Target t = seed(m);
            assertRejected(m, "@Reference", () -> m.createQueryFor(Holder.class).f("id").eq("h").set("target", t));
            assertRejected(m, "@Reference", () -> m.createQueryFor(Holder.class).f("id").eq("h").set(Map.of("target", t)));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void pushOfAnEntityIntoAReferenceListIsRejected(Morphium m) throws Exception {
        try (m) {
            Target t = seed(m);
            assertRejected(m, "@Reference", () -> m.createQueryFor(Holder.class).f("id").eq("h").push("targets", t));
            assertRejected(m, "@Reference", () -> m.createQueryFor(Holder.class).f("id").eq("h").pushAll("targets", List.of(t)));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aPlaintextValueForAnEncryptedFieldIsRejected(Morphium m) throws Exception {
        try (m) {
            seed(m);
            assertRejected(m, "@Encrypted", () -> m.createQueryFor(Holder.class).f("id").eq("h").set("secret", "plain text"));
            assertRejected(m, "@Encrypted", () -> m.createQueryFor(Holder.class).f("id").eq("h").set(Map.of("secret", "plain text")));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void otherFieldsAndTheIdOfAReferencedEntityAreWritten(Morphium m) throws Exception {
        try (m) {
            Target t = seed(m);
            m.createQueryFor(Holder.class).f("id").eq("h").set("plain", "ok");
            m.createQueryFor(Holder.class).f("id").eq("h").set("target", t.id);
            Map<String, Object> raw = ModifyFixturesAccess.rawDocs(m.createQueryFor(Holder.class).f("id").eq("h")).get(0);
            assertEquals("ok", raw.get("plain"));
            assertEquals(t.id.toString(), String.valueOf(raw.get("target") instanceof Map ? ((Map<?, ?>) raw.get("target")).get("refid") : raw.get("target")), String.valueOf(raw));
        }
    }
}
