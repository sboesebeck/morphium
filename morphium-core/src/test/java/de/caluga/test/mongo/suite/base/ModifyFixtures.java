package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Aliases;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.annotations.Reference;
import de.caluga.morphium.annotations.Version;
import de.caluga.morphium.annotations.encryption.Encrypted;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.encryption.AESEncryptionProvider;
import de.caluga.morphium.query.Query;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Entities and helpers shared by the tests of the update builder ({@code Query.modify()}, MOR-006). */
final class ModifyFixtures {
    private ModifyFixtures() {
    }

    public enum Mode {
        FAST, SLOW
    }

    public enum LeaseFields {
        ownerName, expiresAt
    }

    @Embedded
    public static class Child {
        public String fileName;

        public Child() {
        }

        public Child(String fileName) {
            this.fileName = fileName;
        }
    }

    /** default naming, versioned: the shape of the lease entity of the application */
    @Entity
    public static class Lease {
        @Id
        public String id;
        public String ownerName;
        public Instant expiresAt;
        @Version
        public Long entityVersion;
        public int retryCount;
        public Mode mode;
        public List<Mode> modes;
        public Child child;
        public List<Child> children;
        public Set<Child> childSet;
        public Map<String, Child> childMap;
        @Property(fieldName = "custom_x")
        public String renamed;
        @Aliases({"legacy_owner"})
        public String currentOwner;
        public Object anything;
    }

    @Entity
    public static class Counter {
        @Id
        public String id;
        public long hits;
        public double score;
    }

    /** no @Version */
    @Entity
    public static class Plain {
        @Id
        public String id;
        public String key;
        public String payload;
        public int count;
        public List<String> tags;
        public Instant at;
    }

    @Entity(translateCamelCase = false)
    public static class RawLease {
        @Id
        public String id;
        public String ownerName;
        public Child defaultChild;
    }

    @Entity
    public static class Secret {
        @Id
        public String id;
        @Encrypted(provider = AESEncryptionProvider.class, keyName = "key")
        public String secretValue;
    }

    @Entity
    public static class Target {
        @Id
        public MorphiumId id;
        public String label;
    }

    @Entity
    public static class Holder {
        @Id
        public String id;
        @Reference
        public Target target;
    }

    /** reads the raw documents of the query without the projection to known fields, so that stray fields show up */
    static List<Map<String, Object>> rawDocs(Query<?> q) throws Exception {
        FindCommand cmd = q.getFindCmd();

        try {
            cmd.setProjection(new Doc());
            return cmd.execute();
        } finally {
            cmd.releaseConnection();
        }
    }

    static Map<String, Object> raw(Morphium m, Class<?> type, String id) throws Exception {
        List<Map<String, Object>> docs = rawDocs(m.createQueryFor(type).f("_id").eq(id));
        assertEquals(1, docs.size(), String.valueOf(docs));
        return docs.get(0);
    }
}
