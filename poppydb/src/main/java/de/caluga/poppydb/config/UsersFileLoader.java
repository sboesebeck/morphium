package de.caluga.poppydb.config;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reads, parses and validates a PoppyDB {@code users-file} (see {@code docs/poppydb.md} /
 * {@code docs/superpowers/specs/2026-08-04-poppydb-users-file-design.md}) into a
 * {@link UsersFileSpec}. Used both by PoppyDB startup (fatal on any problem) and by
 * {@code ConfigInspector.deepCheck} ({@code --check-config}, which reports problems without
 * starting).
 * <p>
 * Accepted top-level JSON shapes:
 * <ul>
 *   <li>a bare array of user entries - unversioned, {@code version} comes back {@code null}</li>
 *   <li>an object with exactly the fields {@code version} (positive integer) and {@code users}
 *       (array) - any other top-level field is an error, and so is omitting either of these two
 *       once the object form is chosen</li>
 * </ul>
 * Per entry: {@code user} and {@code pwd} are required non-empty strings, {@code db} defaults to
 * {@code "admin"}, {@code roles} and {@code mechanisms} are optional and stored/validated only
 * shallowly (roles are mongod-shaped but not enforced anywhere in PoppyDB, matching the rest of
 * the codebase). Any unknown entry field is an error naming both the field and the zero-based
 * entry index.
 * <p>
 * {@code ~} in the given path is expanded via {@link ConfigLoader#expandHome(String)} at
 * file-open time - the raw {@code users-file} config/CLI value is stored verbatim (like the
 * other {@code PATH}-typed keys) and only expanded here, matching how the {@code *-file} secret
 * keys resolve their path.
 * <p>
 * Every fatal problem (missing/unreadable file, unsafe permissions, malformed JSON, any
 * validation failure) is reported as a {@link ConfigException} - this class never calls
 * {@code System.exit(...)} and never logs or includes a {@code pwd} value anywhere, including
 * in exception messages. Non-fatal findings (currently: group/other-readable permissions) are
 * collected into {@link UsersFileSpec#warnings()} instead of thrown, so callers can decide how
 * to surface them (a warning log line at startup, a warnings list in {@code --check-config}
 * output).
 */
public final class UsersFileLoader {
    private static final Logger log = LoggerFactory.getLogger(UsersFileLoader.class);

    private static final Set<String> TOP_LEVEL_KEYS = Set.of("version", "users");
    private static final Set<String> ENTRY_KEYS = Set.of("user", "db", "pwd", "roles", "mechanisms");
    private static final String DEFAULT_DB = "admin";

    private UsersFileLoader() {
    }

    /**
     * Loads and validates the users file at {@code rawPath} ({@code ~} expanded here).
     *
     * @throws ConfigException on any fatal problem (see class javadoc)
     */
    public static UsersFileSpec load(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            throw new ConfigException("Users file path must not be null/blank");
        }
        Path path = Paths.get(ConfigLoader.expandHome(rawPath));

        if (!Files.exists(path)) {
            throw new ConfigException("Users file not found: " + path);
        }
        if (!Files.isRegularFile(path)) {
            throw new ConfigException("Users file is not a regular file: " + path);
        }
        if (!Files.isReadable(path)) {
            throw new ConfigException("Users file is not readable: " + path);
        }

        List<String> warnings = new ArrayList<>();
        checkFilePermissions(path, warnings);

        String content;
        try {
            content = Files.readString(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new ConfigException("Failed to read users file " + path + ": " + e.getMessage(), e);
        }

        Object parsed;
        try {
            parsed = new JSONParser().parse(content);
        } catch (ParseException e) {
            // Never interpolate e/e.toString()/e.getMessage() and never attach e as cause: for
            // UNEXPECTED_TOKEN, json-simple's ParseException.toString() (and thus any
            // stack-trace/"Caused by" logging of this exception) renders the offending token's
            // VALUE verbatim, which can be a raw secret scalar trailing a malformed users file.
            // Only safe, structured fields (position/error type) go into the message.
            throw new ConfigException(String.format(
                    "Failed to parse users file %s as JSON: malformed JSON at position %d (error type %d)",
                    path, e.getPosition(), e.getErrorType()));
        }

        return parseTopLevel(path, parsed, warnings);
    }

    // ---- top-level shape -----------------------------------------------------------------------

    private static UsersFileSpec parseTopLevel(Path path, Object parsed, List<String> warnings) {
        if (parsed instanceof List<?> bareArray) {
            return new UsersFileSpec(null, parseUsers(path, bareArray), warnings);
        }
        if (parsed instanceof Map<?, ?> obj) {
            for (Object keyObj : obj.keySet()) {
                String key = String.valueOf(keyObj);
                if (!TOP_LEVEL_KEYS.contains(key)) {
                    throw new ConfigException(String.format(
                            "Users file %s: unknown top-level field '%s' - only 'version' and 'users' are "
                            + "allowed (use a bare JSON array instead for an unversioned file)", path, key));
                }
            }
            if (!obj.containsKey("version")) {
                throw new ConfigException(String.format(
                        "Users file %s: object form requires a 'version' field - use a bare JSON array "
                        + "instead for an unversioned file", path));
            }
            if (!obj.containsKey("users")) {
                throw new ConfigException(String.format(
                        "Users file %s: missing required top-level field 'users'", path));
            }

            long version = parseVersion(path, obj.get("version"));

            Object usersRaw = obj.get("users");
            if (!(usersRaw instanceof List<?> list)) {
                throw new ConfigException(String.format(
                        "Users file %s: 'users' must be a JSON array, got: %s", path, describe(usersRaw)));
            }
            return new UsersFileSpec(version, parseUsers(path, list), warnings);
        }
        throw new ConfigException(String.format(
                "Users file %s: top-level JSON value must be either an array of users or an object with "
                + "'version' and 'users' fields, got: %s", path, describe(parsed)));
    }

    private static long parseVersion(Path path, Object raw) {
        if (!(raw instanceof Long v)) {
            throw new ConfigException(String.format(
                    "Users file %s: 'version' must be a positive integer, got: %s", path, describe(raw)));
        }
        if (v <= 0) {
            throw new ConfigException(String.format(
                    "Users file %s: 'version' must be a positive integer, got: %d", path, v));
        }
        return v;
    }

    // ---- entries ---------------------------------------------------------------------------------

    private static List<UserSpec> parseUsers(Path path, List<?> rawList) {
        List<UserSpec> result = new ArrayList<>();
        for (int i = 0; i < rawList.size(); i++) {
            result.add(parseEntry(path, i, rawList.get(i)));
        }
        checkNoDuplicateUsers(path, result);
        return result;
    }

    /**
     * Rejects two entries naming the same (user, db) pair - mongod identifies a user by that
     * pair, so both would createUser the same principal. Without this check the file loaded
     * silently (last entry wins in {@link de.caluga.poppydb.PoppyDB#applyBootstrapUser}, since
     * later entries createUser/updateUser over the earlier one's result) - a copy-paste error in
     * the file would drop a user's intended password/roles with no diagnostic at all.
     */
    private static void checkNoDuplicateUsers(Path path, List<UserSpec> users) {
        Set<String> seen = new HashSet<>();
        for (UserSpec user : users) {
            String key = user.user() + "@" + user.db();
            if (!seen.add(key)) {
                throw new ConfigException(String.format(
                        "Users file %s: duplicate entry for user '%s' on db '%s' - each (user, db) pair "
                        + "must appear at most once", path, user.user(), user.db()));
            }
        }
    }

    private static UserSpec parseEntry(Path path, int index, Object raw) {
        if (!(raw instanceof Map<?, ?> entry)) {
            throw new ConfigException(String.format(
                    "Users file %s: entry #%d must be a JSON object, got: %s", path, index, describe(raw)));
        }
        for (Object keyObj : entry.keySet()) {
            String key = String.valueOf(keyObj);
            if (!ENTRY_KEYS.contains(key)) {
                throw new ConfigException(String.format(
                        "Users file %s: entry #%d has unknown field '%s'", path, index, key));
            }
        }

        String user = requireNonEmptyString(path, index, entry, "user");
        String pwd = requireNonEmptyString(path, index, entry, "pwd");
        String db = optionalNonEmptyString(path, index, entry, "db", DEFAULT_DB);
        List<Object> roles = optionalList(path, index, entry, "roles");
        List<String> mechanisms = optionalStringList(path, index, entry, "mechanisms");

        return new UserSpec(user, db, pwd, roles, mechanisms);
    }

    private static String requireNonEmptyString(Path path, int index, Map<?, ?> entry, String field) {
        if (!entry.containsKey(field)) {
            throw new ConfigException(String.format(
                    "Users file %s: entry #%d is missing required field '%s'", path, index, field));
        }
        Object raw = entry.get(field);
        if (!(raw instanceof String s)) {
            throw new ConfigException(String.format(
                    "Users file %s: entry #%d field '%s' must be a non-empty string", path, index, field));
        }
        if (s.isBlank()) {
            throw new ConfigException(String.format(
                    "Users file %s: entry #%d field '%s' must not be empty", path, index, field));
        }
        return s;
    }

    private static String optionalNonEmptyString(Path path, int index, Map<?, ?> entry, String field,
            String defaultValue) {
        if (!entry.containsKey(field)) {
            return defaultValue;
        }
        Object raw = entry.get(field);
        if (!(raw instanceof String s) || s.isBlank()) {
            throw new ConfigException(String.format(
                    "Users file %s: entry #%d field '%s' must be a non-empty string", path, index, field));
        }
        return s;
    }

    private static List<Object> optionalList(Path path, int index, Map<?, ?> entry, String field) {
        if (!entry.containsKey(field)) {
            return List.of();
        }
        Object raw = entry.get(field);
        if (!(raw instanceof List<?> list)) {
            throw new ConfigException(String.format(
                    "Users file %s: entry #%d field '%s' must be a JSON array", path, index, field));
        }
        return List.copyOf(list);
    }

    private static List<String> optionalStringList(Path path, int index, Map<?, ?> entry, String field) {
        if (!entry.containsKey(field)) {
            return List.of();
        }
        Object raw = entry.get(field);
        if (!(raw instanceof List<?> list)) {
            throw new ConfigException(String.format(
                    "Users file %s: entry #%d field '%s' must be a JSON array of strings", path, index, field));
        }
        List<String> result = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            Object item = list.get(i);
            if (!(item instanceof String s)) {
                throw new ConfigException(String.format(
                        "Users file %s: entry #%d field '%s' item #%d must be a string",
                        path, index, field, i));
            }
            result.add(s);
        }
        return List.copyOf(result);
    }

    private static String describe(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String) {
            return "string";
        }
        if (value instanceof Boolean) {
            return "boolean (" + value + ")";
        }
        if (value instanceof Long || value instanceof Double) {
            return "number (" + value + ")";
        }
        if (value instanceof List) {
            return "array";
        }
        if (value instanceof Map) {
            return "object";
        }
        return value.getClass().getSimpleName();
    }

    // ---- permission checks (mirrors ConfigLoader.checkFilePermissions's style) ------------------

    private static void checkFilePermissions(Path file, List<String> warnings) {
        FileSystem fs = file.getFileSystem();
        if (!fs.supportedFileAttributeViews().contains("posix")) {
            log.debug("Skipping permission check for users file {} - no POSIX file attribute view available",
                    file);
            return;
        }
        try {
            Set<PosixFilePermission> perms = Files.getPosixFilePermissions(file);
            boolean groupOrOtherWrite = perms.contains(PosixFilePermission.GROUP_WRITE)
                    || perms.contains(PosixFilePermission.OTHERS_WRITE);
            boolean groupOrOtherRead = perms.contains(PosixFilePermission.GROUP_READ)
                    || perms.contains(PosixFilePermission.OTHERS_READ);

            if (groupOrOtherWrite) {
                throw new ConfigException(String.format(
                        "Users file %s contains secrets and is writable by group/others (%s) - refusing to "
                        + "start, chmod 600 required", file, PosixFilePermissions.toString(perms)));
            }
            if (groupOrOtherRead) {
                String msg = String.format(
                        "Users file %s contains secrets and is readable by group/others (%s) - recommended: "
                        + "chmod 600", file, PosixFilePermissions.toString(perms));
                log.warn(msg);
                warnings.add(msg);
            }
        } catch (IOException e) {
            log.debug("Could not check permissions of users file {}: {}", file, e.getMessage());
        }
    }
}
