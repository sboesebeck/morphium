package de.caluga.poppydb.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Covers {@link UsersFileLoader} in isolation: both accepted top-level shapes, per-entry
 * validation (required/optional/unknown fields), version-wrapper validation, JSON parse-error
 * wrapping with position info, {@code ~}-expansion at load time and POSIX permission checks
 * (mirroring {@link ConfigLoader}'s secret-file behavior: group/other-writable is fatal,
 * group/other-readable is a collected warning). No test here ever asserts a raw {@code pwd}
 * value appears in an exception message - that must never happen.
 */
class UsersFileLoaderTest {

    private static final String SECRET_PWD = "sup3rSecretPwd!!";

    private static boolean posixSupported() {
        return FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
    }

    private static Path writeUsersFile(Path dir, String name, String content) throws IOException {
        Path file = dir.resolve(name);
        Files.writeString(file, content, StandardCharsets.UTF_8);
        return file;
    }

    // ---- top-level shapes ---------------------------------------------------------------------

    @Test
    void bareArrayShapeParsesWithNullVersion(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                [ { "user": "app", "pwd": "%s" } ]
                """.formatted(SECRET_PWD));

        UsersFileSpec spec = UsersFileLoader.load(file.toString());

        assertThat(spec.version()).isNull();
        assertThat(spec.users()).hasSize(1);
        assertThat(spec.users().get(0).user()).isEqualTo("app");
        assertThat(spec.users().get(0).pwd()).isEqualTo(SECRET_PWD);
    }

    @Test
    void versionedWrapperShapeParsesVersionAndUsers(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                { "version": 3, "users": [ { "user": "app", "pwd": "%s" } ] }
                """.formatted(SECRET_PWD));

        UsersFileSpec spec = UsersFileLoader.load(file.toString());

        assertThat(spec.version()).isEqualTo(3L);
        assertThat(spec.users()).hasSize(1);
    }

    @Test
    void objectFormWithoutVersionFieldThrows(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                { "users": [ { "user": "app", "pwd": "%s" } ] }
                """.formatted(SECRET_PWD));

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("version");
    }

    @Test
    void objectFormWithoutUsersFieldThrows(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                { "version": 1 }
                """);

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("users");
    }

    @Test
    void unknownTopLevelFieldThrows(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                { "version": 1, "users": [], "extra": true }
                """);

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("extra");
    }

    @Test
    void topLevelScalarThrows(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", "\"just a string\"");

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class);
    }

    @Test
    void usersFieldNotAnArrayThrows(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                { "version": 1, "users": "nope" }
                """);

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("users");
    }

    // ---- version validation --------------------------------------------------------------------

    @Test
    void zeroVersionThrows(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                { "version": 0, "users": [] }
                """);

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("version");
    }

    @Test
    void negativeVersionThrows(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                { "version": -1, "users": [] }
                """);

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("version");
    }

    @Test
    void nonIntegerVersionThrows(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                { "version": 1.5, "users": [] }
                """);

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("version");
    }

    @Test
    void stringVersionThrows(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                { "version": "3", "users": [] }
                """);

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("version");
    }

    @Test
    void emptyUsersArrayIsValid(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                { "version": 1, "users": [] }
                """);

        UsersFileSpec spec = UsersFileLoader.load(file.toString());

        assertThat(spec.version()).isEqualTo(1L);
        assertThat(spec.users()).isEmpty();
    }

    // ---- entry validation -----------------------------------------------------------------------

    @Test
    void defaultsDbToAdminWhenMissing(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                [ { "user": "monitor", "pwd": "%s" } ]
                """.formatted(SECRET_PWD));

        UsersFileSpec spec = UsersFileLoader.load(file.toString());

        assertThat(spec.users().get(0).db()).isEqualTo("admin");
    }

    @Test
    void explicitDbIsHonored(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                [ { "user": "app", "db": "mydb", "pwd": "%s" } ]
                """.formatted(SECRET_PWD));

        UsersFileSpec spec = UsersFileLoader.load(file.toString());

        assertThat(spec.users().get(0).db()).isEqualTo("mydb");
    }

    @Test
    void rolesAndMechanismsAreCarriedThroughOpaquely(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                [ { "user": "app", "db": "mydb", "pwd": "%s",
                    "roles": [ { "role": "readWrite", "db": "mydb" } ],
                    "mechanisms": [ "SCRAM-SHA-256" ] } ]
                """.formatted(SECRET_PWD));

        UsersFileSpec spec = UsersFileLoader.load(file.toString());

        UserSpec u = spec.users().get(0);
        assertThat(u.roles()).hasSize(1);
        assertThat(u.mechanisms()).containsExactly("SCRAM-SHA-256");
    }

    @Test
    void missingUserFieldThrowsWithEntryIndexAndFieldName(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                [ { "pwd": "%s" } ]
                """.formatted(SECRET_PWD));

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("#0")
                .hasMessageContaining("user");
    }

    @Test
    void missingPwdFieldThrowsWithEntryIndexAndFieldNameAndDoesNotLeakPwd(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                [ { "user": "app" }, { "user": "monitor", "pwd": "%s" } ]
                """.formatted(SECRET_PWD));

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("#0")
                .hasMessageContaining("pwd")
                .hasMessageNotContaining(SECRET_PWD);
    }

    @Test
    void emptyStringUserThrows(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                [ { "user": "", "pwd": "%s" } ]
                """.formatted(SECRET_PWD));

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("user");
    }

    @Test
    void emptyStringPwdThrowsAndDoesNotLeakIt(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                [ { "user": "app", "pwd": "" } ]
                """);

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("pwd");
    }

    @Test
    void userFieldWrongTypeThrows(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                [ { "user": 42, "pwd": "%s" } ]
                """.formatted(SECRET_PWD));

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("user");
    }

    @Test
    void pwdFieldWrongTypeThrowsAndDoesNotLeakValue(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                [ { "user": "app", "pwd": 12345 } ]
                """);

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("pwd")
                .hasMessageNotContaining("12345");
    }

    @Test
    void dbWrongTypeThrows(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                [ { "user": "app", "pwd": "%s", "db": 1 } ]
                """.formatted(SECRET_PWD));

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("db");
    }

    @Test
    void rolesWrongTypeThrows(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                [ { "user": "app", "pwd": "%s", "roles": "readWrite" } ]
                """.formatted(SECRET_PWD));

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("roles");
    }

    @Test
    void mechanismsWrongTypeThrows(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                [ { "user": "app", "pwd": "%s", "mechanisms": "SCRAM-SHA-256" } ]
                """.formatted(SECRET_PWD));

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("mechanisms");
    }

    @Test
    void mechanismsItemWrongTypeThrows(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                [ { "user": "app", "pwd": "%s", "mechanisms": [ 1 ] } ]
                """.formatted(SECRET_PWD));

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("mechanisms");
    }

    @Test
    void unknownFieldInEntryThrowsNamingFieldAndIndex(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                [ { "user": "app", "pwd": "%s" }, { "user": "b", "pwd": "%s", "bogus": 1 } ]
                """.formatted(SECRET_PWD, SECRET_PWD));

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("#1")
                .hasMessageContaining("bogus")
                .hasMessageNotContaining(SECRET_PWD);
    }

    @Test
    void entryNotAnObjectThrows(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", """
                [ "not-an-object" ]
                """);

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("#0");
    }

    // ---- file existence / readability ------------------------------------------------------------

    @Test
    void missingFileThrows(@TempDir Path dir) {
        Path missing = dir.resolve("nope.json");

        assertThatThrownBy(() -> UsersFileLoader.load(missing.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(missing.toString());
    }

    @Test
    void directoryInsteadOfFileThrows(@TempDir Path dir) throws IOException {
        Path sub = dir.resolve("adir");
        Files.createDirectory(sub);

        assertThatThrownBy(() -> UsersFileLoader.load(sub.toString()))
                .isInstanceOf(ConfigException.class);
    }

    // ---- malformed JSON / position info ------------------------------------------------------------

    @Test
    void malformedJsonWrapsParseExceptionWithPosition(@TempDir Path dir) throws IOException {
        Path file = writeUsersFile(dir, "users.json", "{ this is not json ]");

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("position");
    }

    // ---- ~-expansion (component 1 amendment: THIS loader's responsibility) ------------------------

    @Test
    void tildeIsExpandedAtLoadTimeUsingConfigLoaderExpandHome() {
        // The path does not need to exist for this test - we only assert the loader expands it
        // BEFORE looking at the filesystem, by checking the resulting "not found" error names the
        // fully expanded path (never the literal "~/...").
        String rawPath = "~/definitely-does-not-exist-poppydb-users-file-test.json";
        String expected = ConfigLoader.expandHome(rawPath);

        assertThatThrownBy(() -> UsersFileLoader.load(rawPath))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(expected)
                .hasMessageNotContaining("~/definitely-does-not-exist");
    }

    // ---- permission checks (POSIX only) --------------------------------------------------------

    @Test
    void groupWritableUsersFileThrows(@TempDir Path dir) throws IOException {
        assumeTrue(posixSupported(), "POSIX permissions not supported on this platform");
        Path file = writeUsersFile(dir, "users.json", """
                [ { "user": "app", "pwd": "%s" } ]
                """.formatted(SECRET_PWD));
        Files.setPosixFilePermissions(file, Set.of(
                PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE));

        assertThatThrownBy(() -> UsersFileLoader.load(file.toString()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("writable");
    }

    @Test
    void groupReadableUsersFileWarnsButDoesNotThrow(@TempDir Path dir) throws IOException {
        assumeTrue(posixSupported(), "POSIX permissions not supported on this platform");
        Path file = writeUsersFile(dir, "users.json", """
                [ { "user": "app", "pwd": "%s" } ]
                """.formatted(SECRET_PWD));
        Files.setPosixFilePermissions(file, Set.of(
                PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.GROUP_READ));

        UsersFileSpec spec = UsersFileLoader.load(file.toString());

        assertThat(spec.warnings()).isNotEmpty();
        assertThat(spec.warnings().get(0)).contains("readable");
    }

    @Test
    void ownerOnlyPermissionsProduceNoWarnings(@TempDir Path dir) throws IOException {
        assumeTrue(posixSupported(), "POSIX permissions not supported on this platform");
        Path file = writeUsersFile(dir, "users.json", """
                [ { "user": "app", "pwd": "%s" } ]
                """.formatted(SECRET_PWD));
        Files.setPosixFilePermissions(file, Set.of(
                PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));

        UsersFileSpec spec = UsersFileLoader.load(file.toString());

        assertThat(spec.warnings()).isEmpty();
    }

    // ---- secrecy: UserSpec never renders pwd via toString --------------------------------------

    @Test
    void userSpecToStringDoesNotLeakPwd() {
        UserSpec spec = new UserSpec("app", "admin", SECRET_PWD, List.of(), List.of());

        assertThat(spec.toString()).doesNotContain(SECRET_PWD);
    }
}
