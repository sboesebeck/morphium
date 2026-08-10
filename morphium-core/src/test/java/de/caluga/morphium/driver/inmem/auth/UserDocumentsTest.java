package de.caluga.morphium.driver.inmem.auth;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * UserDocuments must produce admin.system.users documents in exactly the shape
 * real mongod writes, so tooling that inspects system.users cannot tell the
 * difference between PoppyDB and a real server.
 */
@Tag("core")
public class UserDocumentsTest {

    @SuppressWarnings("unchecked")
    private Map<String, Object> credentialsOf(Map<String, Object> userDoc) {
        return (Map<String, Object>) userDoc.get("credentials");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> credentialFor(Map<String, Object> userDoc, String mechanism) {
        return (Map<String, Object>) credentialsOf(userDoc).get(mechanism);
    }

    @Test
    public void buildUserDocumentHasMongodShape() {
        List<Object> roles = List.of(Map.of("role", "readWrite", "db", "testdb"));
        Map<String, Object> doc = UserDocuments.buildUserDocument("admin", "stephan", "secret", roles, null);

        assertThat(doc.get("_id")).isEqualTo("admin.stephan");
        assertThat(doc.get("user")).isEqualTo("stephan");
        assertThat(doc.get("db")).isEqualTo("admin");
        assertThat(doc.get("roles")).isSameAs(roles);

        Map<String, Object> creds = credentialsOf(doc);
        assertThat(creds).containsOnlyKeys("SCRAM-SHA-1", "SCRAM-SHA-256");

        for (String mech : creds.keySet()) {
            Map<String, Object> c = credentialFor(doc, mech);
            assertThat(c).containsKeys("iterationCount", "salt", "storedKey", "serverKey");
        }
    }

    @Test
    public void saltsDifferBetweenMechanisms() {
        Map<String, Object> doc = UserDocuments.buildUserDocument("admin", "u", "pw", List.of(), null);
        String saltSha1 = (String) credentialFor(doc, "SCRAM-SHA-1").get("salt");
        String saltSha256 = (String) credentialFor(doc, "SCRAM-SHA-256").get("salt");
        assertThat(saltSha1).isNotEqualTo(saltSha256);
    }

    @Test
    public void saltsAreRandomAcrossCalls() {
        Map<String, Object> doc1 = UserDocuments.buildUserDocument("admin", "u", "pw", List.of(), null);
        Map<String, Object> doc2 = UserDocuments.buildUserDocument("admin", "u", "pw", List.of(), null);
        assertThat(credentialFor(doc1, "SCRAM-SHA-256").get("salt"))
        .isNotEqualTo(credentialFor(doc2, "SCRAM-SHA-256").get("salt"));
        assertThat(credentialFor(doc1, "SCRAM-SHA-1").get("salt"))
        .isNotEqualTo(credentialFor(doc2, "SCRAM-SHA-1").get("salt"));
    }

    @Test
    public void mongodDefaultIterationCounts() {
        Map<String, Object> doc = UserDocuments.buildUserDocument("admin", "u", "pw", List.of(), null);
        assertThat(credentialFor(doc, "SCRAM-SHA-1").get("iterationCount")).isEqualTo(10000);
        assertThat(credentialFor(doc, "SCRAM-SHA-256").get("iterationCount")).isEqualTo(15000);
    }

    @Test
    public void mechanismsParameterFiltersCredentials() {
        Map<String, Object> doc = UserDocuments.buildUserDocument("admin", "u", "pw", List.of(),
                List.of("SCRAM-SHA-256"));
        assertThat(credentialsOf(doc)).containsOnlyKeys("SCRAM-SHA-256");
    }

    @Test
    public void emptyMechanismsListMeansBoth() {
        Map<String, Object> doc = UserDocuments.buildUserDocument("admin", "u", "pw", List.of(), List.of());
        assertThat(credentialsOf(doc)).containsOnlyKeys("SCRAM-SHA-1", "SCRAM-SHA-256");
    }

    @Test
    public void extractCredentialsRoundTrips() {
        Map<String, Object> doc = UserDocuments.buildUserDocument("admin", "stephan", "secret", List.of(), null);

        ScramCredentials extracted = UserDocuments.extractCredentials(doc, "SCRAM-SHA-256");
        assertThat(extracted).isNotNull();

        // Deriving again with the same salt/iterations must yield identical keys -
        // otherwise the stored document could never authenticate a real client.
        ScramCredentials fresh = ScramCredentials.derive(ScramCredentials.Mechanism.SCRAM_SHA_256,
                "stephan", "secret", extracted.getSalt(), extracted.getIterationCount());
        assertThat(extracted.getStoredKey()).isEqualTo(fresh.getStoredKey());
        assertThat(extracted.getServerKey()).isEqualTo(fresh.getServerKey());
    }

    @Test
    public void extractCredentialsReturnsNullForMissingMechanism() {
        Map<String, Object> doc = UserDocuments.buildUserDocument("admin", "u", "pw", List.of(),
                List.of("SCRAM-SHA-256"));
        assertThat(UserDocuments.extractCredentials(doc, "SCRAM-SHA-1")).isNull();
    }

    @Test
    public void userIdIsDbDotUser() {
        assertThat(UserDocuments.userId("admin", "stephan")).isEqualTo("admin.stephan");
        assertThat(UserDocuments.userId("mydb", "app.user")).isEqualTo("mydb.app.user");
    }

    @Test
    public void validateCreateUserAcceptsValidCommand() {
        Map<String, Object> cmd = Map.of(
                "createUser", "stephan",
                "pwd", "secret",
                "roles", List.of("root"),
                "$db", "admin");
        assertThat(UserDocuments.validateCreateUser(cmd)).isNull();
    }

    @Test
    public void validateCreateUserAcceptsEmptyRolesAndValidMechanisms() {
        Map<String, Object> cmd = Map.of(
                "createUser", "stephan",
                "pwd", "secret",
                "roles", List.of(),
                "mechanisms", List.of("SCRAM-SHA-1", "SCRAM-SHA-256"),
                "$db", "admin");
        assertThat(UserDocuments.validateCreateUser(cmd)).isNull();
    }

    @Test
    public void validateCreateUserRejectsMissingPwd() {
        Map<String, Object> cmd = Map.of("createUser", "stephan", "roles", List.of(), "$db", "admin");
        assertThat(UserDocuments.validateCreateUser(cmd)).contains("pwd");
    }

    @Test
    public void validateCreateUserRejectsBlankPwd() {
        Map<String, Object> cmd = Map.of("createUser", "stephan", "pwd", "  ", "roles", List.of(), "$db", "admin");
        assertThat(UserDocuments.validateCreateUser(cmd)).contains("pwd");
    }

    @Test
    public void validateCreateUserRejectsMissingRoles() {
        Map<String, Object> cmd = Map.of("createUser", "stephan", "pwd", "secret", "$db", "admin");
        assertThat(UserDocuments.validateCreateUser(cmd)).contains("roles");
    }

    @Test
    public void validateCreateUserRejectsUnknownMechanism() {
        Map<String, Object> cmd = Map.of(
                "createUser", "stephan",
                "pwd", "secret",
                "roles", List.of(),
                "mechanisms", List.of("PLAIN"),
                "$db", "admin");
        assertThat(UserDocuments.validateCreateUser(cmd)).contains("PLAIN");
    }

    @Test
    public void validateCreateUserRejectsBlankUser() {
        Map<String, Object> cmd = Map.of("createUser", "  ", "pwd", "secret", "roles", List.of(), "$db", "admin");
        assertThat(UserDocuments.validateCreateUser(cmd)).isNotNull();
    }

    @Test
    public void storedCredentialValuesAreBsonCompatibleTypes() {
        // The wire layer serializes these maps as-is; mongod stores base64 STRINGS
        // for salt/storedKey/serverKey and an int32 iterationCount. byte[] here
        // would surface as BinData and break drivers/tools reading system.users.
        Map<String, Object> doc = UserDocuments.buildUserDocument("admin", "u", "pw", List.of(), null);

        for (String mech : List.of("SCRAM-SHA-1", "SCRAM-SHA-256")) {
            Map<String, Object> c = credentialFor(doc, mech);
            assertThat(c.get("iterationCount")).isInstanceOf(Integer.class);
            assertThat(c.get("salt")).isInstanceOf(String.class);
            assertThat(c.get("storedKey")).isInstanceOf(String.class);
            assertThat(c.get("serverKey")).isInstanceOf(String.class);
            // and they must actually be valid base64
            assertThat(Base64.getDecoder().decode((String) c.get("salt"))).hasSize(16);
            Base64.getDecoder().decode((String) c.get("storedKey"));
            Base64.getDecoder().decode((String) c.get("serverKey"));
        }
    }
}
