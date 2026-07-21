package de.caluga.morphium.driver.inmem.auth;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds and validates user documents in the exact shape mongod keeps in
 * admin.system.users, so tooling inspecting that collection behaves the same
 * against the in-memory server as against a real one.
 */
public final class UserDocuments {

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int SALT_LENGTH = 16;
    // mongod defaults (scramIterationCount / scramSHA256IterationCount)
    private static final int SHA1_ITERATIONS = 10000;
    private static final int SHA256_ITERATIONS = 15000;

    private UserDocuments() {
    }

    /**
     * Builds a full admin.system.users document the way mongod's createUser
     * does. Each mechanism gets its own independently generated salt - sharing
     * one would tie the two credential sets together cryptographically.
     *
     * @param mechanisms mechanism names to create credentials for; null or
     *                   empty means both SCRAM mechanisms
     */
    public static Map<String, Object> buildUserDocument(String db, String user, String rawPassword,
            List<Object> roles, List<String> mechanisms) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("_id", userId(db, user));
        doc.put("user", user);
        doc.put("db", db);

        Map<String, Object> credentials = new LinkedHashMap<>();
        for (ScramCredentials.Mechanism m : resolveMechanisms(mechanisms)) {
            byte[] salt = new byte[SALT_LENGTH];
            RANDOM.nextBytes(salt);
            int iterations = m == ScramCredentials.Mechanism.SCRAM_SHA_1 ? SHA1_ITERATIONS : SHA256_ITERATIONS;
            credentials.put(m.mechanismName(),
                    ScramCredentials.derive(m, user, rawPassword, salt, iterations).toStoredDocument());
        }
        doc.put("credentials", credentials);
        doc.put("roles", roles != null ? roles : new ArrayList<>());
        return doc;
    }

    /**
     * Extracts the ScramCredentials for the given mechanism from a stored user
     * document, or null if the user has no credentials for that mechanism.
     */
    @SuppressWarnings("unchecked")
    public static ScramCredentials extractCredentials(Map<String, Object> userDoc, String mechanismName) {
        if (userDoc == null || !(userDoc.get("credentials") instanceof Map)) {
            return null;
        }
        Map<String, Object> credentials = (Map<String, Object>) userDoc.get("credentials");
        Object stored = credentials.get(mechanismName);
        if (!(stored instanceof Map)) {
            return null;
        }
        ScramCredentials.Mechanism mechanism = mechanismByName(mechanismName);
        if (mechanism == null) {
            return null;
        }
        return ScramCredentials.fromStoredDocument(mechanism, (Map<String, Object>) stored);
    }

    /** The _id under which mongod stores a user: "&lt;db&gt;.&lt;user&gt;". */
    public static String userId(String db, String user) {
        return db + "." + user;
    }

    /**
     * Validates a createUser command document. Returns an error message, or
     * null if the command is valid. Mirrors mongod: roles is mandatory (an
     * empty list is fine), pwd must be a non-blank string, and mechanisms may
     * only name supported SCRAM mechanisms.
     */
    public static String validateCreateUser(Map<String, Object> cmd) {
        Object name = cmd.get("createUser");
        if (!(name instanceof String) || ((String) name).isBlank()) {
            return "createUser must be a non-empty string";
        }
        Object pwd = cmd.get("pwd");
        if (!(pwd instanceof String) || ((String) pwd).isBlank()) {
            return "pwd must be a non-empty string";
        }
        if (!cmd.containsKey("roles")) {
            return "\"createUser\" command requires a \"roles\" array";
        }
        if (!(cmd.get("roles") instanceof List)) {
            return "roles must be an array";
        }
        Object mechanisms = cmd.get("mechanisms");
        if (mechanisms != null) {
            if (!(mechanisms instanceof List)) {
                return "mechanisms must be an array";
            }
            List<?> list = (List<?>) mechanisms;
            if (list.isEmpty()) {
                return "mechanisms field must not be empty";
            }
            for (Object m : list) {
                if (!(m instanceof String) || mechanismByName((String) m) == null) {
                    return "Unknown auth mechanism '" + m + "'";
                }
            }
        }
        return null;
    }

    /**
     * Resolves by comparing mechanismName() instead of relying on
     * Mechanism.fromName(), whose unknown-name behavior (null vs throw) is not
     * part of the agreed API contract.
     */
    private static ScramCredentials.Mechanism mechanismByName(String name) {
        for (ScramCredentials.Mechanism m : ScramCredentials.Mechanism.values()) {
            if (m.mechanismName().equals(name)) {
                return m;
            }
        }
        return null;
    }

    private static List<ScramCredentials.Mechanism> resolveMechanisms(List<String> mechanisms) {
        if (mechanisms == null || mechanisms.isEmpty()) {
            return List.of(ScramCredentials.Mechanism.values());
        }
        List<ScramCredentials.Mechanism> result = new ArrayList<>();
        for (String name : mechanisms) {
            ScramCredentials.Mechanism m = mechanismByName(name);
            if (m == null) {
                throw new IllegalArgumentException("Unknown auth mechanism '" + name + "'");
            }
            result.add(m);
        }
        return result;
    }
}
