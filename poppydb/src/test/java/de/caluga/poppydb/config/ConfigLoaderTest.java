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
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Covers {@link ConfigLoader} in isolation - discovery ordering with injected search paths
 * (never touching the real {@code /etc} or {@code ~}), key normalization/validation, the
 * {@code *-file} secret indirection, permission checks and error/warning behavior. No test
 * here relies on {@code System.exit} - {@link ConfigLoader} throws {@link ConfigException}
 * for every fatal condition instead.
 */
class ConfigLoaderTest {

    private static boolean posixSupported() {
        return FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
    }

    private static Path writeConfig(Path dir, String name, String content) throws IOException {
        Path file = dir.resolve(name);
        Files.writeString(file, content, StandardCharsets.UTF_8);
        return file;
    }

    // ---- discovery -----------------------------------------------------------------------

    @Test
    void discoveryPicksFirstExistingCandidateInOrder(@TempDir Path dir) throws IOException {
        Path second = writeConfig(dir, "second.conf", "port=27020\n");
        Path third = writeConfig(dir, "third.conf", "port=27030\n");
        Path missingFirst = dir.resolve("first.conf"); // does not exist

        ConfigLoader loader = new ConfigLoader(List.of(missingFirst, second, third));
        Path found = loader.discover(null, false);

        assertThat(found).isEqualTo(second);
    }

    @Test
    void discoveryReturnsNullWhenNoCandidateExistsAndNoErrorIsThrown(@TempDir Path dir) {
        ConfigLoader loader = new ConfigLoader(List.of(dir.resolve("nope1"), dir.resolve("nope2")));

        Path found = loader.discover(null, false);

        assertThat(found).isNull();
    }

    @Test
    void noConfigSkipsDefaultSearchPaths(@TempDir Path dir) throws IOException {
        Path candidate = writeConfig(dir, "found.conf", "port=27020\n");
        ConfigLoader loader = new ConfigLoader(List.of(candidate));

        Path found = loader.discover(null, true);

        assertThat(found).isNull();
    }

    @Test
    void explicitCfgWinsEvenWithNoConfig(@TempDir Path dir) throws IOException {
        Path explicit = writeConfig(dir, "explicit.conf", "port=27099\n");
        ConfigLoader loader = new ConfigLoader(List.of());

        Path found = loader.discover(explicit, true);

        assertThat(found).isEqualTo(explicit);
    }

    @Test
    void envVarWinsOverDefaultSearchPaths(@TempDir Path dir) throws IOException {
        Path envCfg = writeConfig(dir, "env.conf", "port=27098\n");
        Path defaultCandidate = writeConfig(dir, "default.conf", "port=1\n");
        ConfigLoader loader = new ConfigLoader(List.of(defaultCandidate), envCfg.toString());

        Path found = loader.discover(null, false);

        assertThat(found).isEqualTo(envCfg);
    }

    @Test
    void explicitCfgPointingToMissingFileThrows(@TempDir Path dir) {
        Path missing = dir.resolve("does-not-exist.conf");
        ConfigLoader loader = new ConfigLoader(List.of());

        assertThatThrownBy(() -> loader.discover(missing, false))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("not found");
    }

    @Test
    void explicitCfgPointingToDirectoryWithoutConfigFileThrows(@TempDir Path dir) throws IOException {
        Path emptyDir = Files.createDirectory(dir.resolve("emptydir"));
        ConfigLoader loader = new ConfigLoader(List.of());

        assertThatThrownBy(() -> loader.discover(emptyDir, false))
                .isInstanceOf(ConfigException.class);
    }

    @Test
    void directoryCandidateWithConfigFileInsideIsUsed(@TempDir Path dir) throws IOException {
        Path subdir = Files.createDirectory(dir.resolve("poppydb"));
        Path configInside = writeConfig(subdir, "config", "port=27077\n");
        ConfigLoader loader = new ConfigLoader(List.of());

        Path found = loader.discover(subdir, false);

        assertThat(found).isEqualTo(configInside);
    }

    // ---- loading & key normalization ------------------------------------------------------

    @Test
    void loadsSimpleKeyValuePairs(@TempDir Path dir) throws IOException {
        Path cfg = writeConfig(dir, "config", "port=27017\nbind=0.0.0.0\n");
        Properties props = new ConfigLoader().load(cfg);

        assertThat(props.getProperty("port")).isEqualTo("27017");
        assertThat(props.getProperty("bind")).isEqualTo("0.0.0.0");
    }

    @Test
    void keyNormalizationTreatsDashUnderscoreDotAndCaseAsEquivalent(@TempDir Path dir) throws IOException {
        Path cfg = writeConfig(dir, "config", "MAX_BSON_SIZE=1234\n");
        Properties props = new ConfigLoader().load(cfg);

        assertThat(props.getProperty("max-bson-size")).isEqualTo("1234");
    }

    @Test
    void poppydbPrefixIsStripped(@TempDir Path dir) throws IOException {
        Path cfg = writeConfig(dir, "config", "poppydb.port=27018\n");
        Properties props = new ConfigLoader().load(cfg);

        assertThat(props.getProperty("port")).isEqualTo("27018");
    }

    @Test
    void usersFileKeyIsAcceptedAsPathType(@TempDir Path dir) throws IOException {
        Path cfg = writeConfig(dir, "config", "users-file=/etc/poppydb/users.json\n");
        Properties props = new ConfigLoader().load(cfg);

        assertThat(props.getProperty("users-file")).isEqualTo("/etc/poppydb/users.json");
    }

    @Test
    void usersFileKeyTranslatesToCliFlag(@TempDir Path dir) throws IOException {
        Path cfg = writeConfig(dir, "config", "users-file=/etc/poppydb/users.json\n");
        ConfigLoader loader = new ConfigLoader();
        Properties props = loader.load(cfg);

        List<String> tokens = loader.toArgs(props);

        assertThat(tokens).containsSequence("--users-file", "/etc/poppydb/users.json");
    }

    @Test
    void prefixedAndUnprefixedVariantOfSameKeyCollide(@TempDir Path dir) throws IOException {
        Path cfg = writeConfig(dir, "config", "poppydb.port=1\nport=2\n");
        ConfigLoader loader = new ConfigLoader();

        assertThatThrownBy(() -> loader.load(cfg))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("port");
    }

    @Test
    void differentSpellingsOfSameNormalizedKeyCollide(@TempDir Path dir) throws IOException {
        Path cfg = writeConfig(dir, "config", "max-bson-size=1000\nmaxBsonSize=2000\n");
        ConfigLoader loader = new ConfigLoader();

        assertThatThrownBy(() -> loader.load(cfg))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("max-bson-size");
    }

    @Test
    void exactDuplicateKeyIsOnlyAWarningNotAnError(@TempDir Path dir) throws IOException {
        Path cfg = writeConfig(dir, "config", "port=1\nport=2\n");
        Properties props = new ConfigLoader().load(cfg);

        // last occurrence wins, like java.util.Properties itself - no exception expected
        assertThat(props.getProperty("port")).isEqualTo("2");
    }

    @Test
    void unknownKeyThrows(@TempDir Path dir) throws IOException {
        Path cfg = writeConfig(dir, "config", "totally-bogus-key=1\n");
        ConfigLoader loader = new ConfigLoader();

        assertThatThrownBy(() -> loader.load(cfg))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown configuration key");
    }

    @Test
    void unknownKeySuggestsClosestMatch(@TempDir Path dir) throws IOException {
        Path cfg = writeConfig(dir, "config", "bnid=0.0.0.0\n");
        ConfigLoader loader = new ConfigLoader();

        assertThatThrownBy(() -> loader.load(cfg))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("did you mean 'bind'");
    }

    @Test
    void reservedKeysCannotBeSetInsideConfigFile(@TempDir Path dir) throws IOException {
        Path cfg = writeConfig(dir, "config", "no-config=true\n");
        ConfigLoader loader = new ConfigLoader();

        assertThatThrownBy(() -> loader.load(cfg))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("cannot be set inside a configuration file");
    }

    @Test
    void unparsableIntValueThrows(@TempDir Path dir) throws IOException {
        Path cfg = writeConfig(dir, "config", "port=abc\n");
        ConfigLoader loader = new ConfigLoader();

        assertThatThrownBy(() -> loader.load(cfg))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("port")
                .hasMessageContaining("abc");
    }

    @Test
    void booleanParsingAcceptsYesNoOnOff(@TempDir Path dir) throws IOException {
        Path cfg = writeConfig(dir, "config", "ssl=yes\nauth=off\n");
        Properties props = new ConfigLoader().load(cfg);

        assertThat(props.getProperty("ssl")).isEqualTo("true");
        assertThat(props.getProperty("auth")).isEqualTo("false");
    }

    @Test
    void booleanParsingRejectsGarbageInsteadOfSilentlyFalse(@TempDir Path dir) throws IOException {
        Path cfg = writeConfig(dir, "config", "ssl=maybe\n");
        ConfigLoader loader = new ConfigLoader();

        assertThatThrownBy(() -> loader.load(cfg))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("ssl");
    }

    @Test
    void emptyConfigFileLoadsToEmptyProperties(@TempDir Path dir) throws IOException {
        Path cfg = writeConfig(dir, "config", "");
        Properties props = new ConfigLoader().load(cfg);

        assertThat(props.stringPropertyNames()).isEmpty();
    }

    // ---- *-file secret indirection ---------------------------------------------------------

    @Test
    void fileIndirectionResolvesContentAndStripsTrailingNewline(@TempDir Path dir) throws IOException {
        Path secretFile = writeConfig(dir, "root.pw", "s3cr3t\n");
        Properties props = new Properties();
        props.setProperty("root-password-file", secretFile.toString());

        Properties resolved = new ConfigLoader().resolveFileRefs(props);

        assertThat(resolved.getProperty("root-password")).isEqualTo("s3cr3t");
        assertThat(resolved.getProperty("root-password-file")).isNull();
    }

    @Test
    void fileIndirectionStripsOnlyOneTrailingLineEnding(@TempDir Path dir) throws IOException {
        Path secretFile = writeConfig(dir, "root.pw", "s3cr3t\n\n");
        Properties props = new Properties();
        props.setProperty("root-password-file", secretFile.toString());

        Properties resolved = new ConfigLoader().resolveFileRefs(props);

        assertThat(resolved.getProperty("root-password")).isEqualTo("s3cr3t\n");
    }

    @Test
    void settingBothDirectValueAndFileIndirectionThrows(@TempDir Path dir) throws IOException {
        Path secretFile = writeConfig(dir, "root.pw", "s3cr3t\n");
        Properties props = new Properties();
        props.setProperty("root-password", "inline");
        props.setProperty("root-password-file", secretFile.toString());
        ConfigLoader loader = new ConfigLoader();

        assertThatThrownBy(() -> loader.resolveFileRefs(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("root-password");
    }

    @Test
    void missingSecretFileThrows(@TempDir Path dir) {
        Properties props = new Properties();
        props.setProperty("ssl-keystore-password-file", dir.resolve("nope").toString());
        ConfigLoader loader = new ConfigLoader();

        assertThatThrownBy(() -> loader.resolveFileRefs(props))
                .isInstanceOf(ConfigException.class);
    }

    // ---- permission checks (POSIX only) -----------------------------------------------------

    @Test
    void groupReadableSecretConfigFileWarnsButDoesNotThrow(@TempDir Path dir) throws IOException {
        assumeTrue(posixSupported(), "POSIX permissions not supported on this platform");
        Path cfg = writeConfig(dir, "config", "root-password=secret\n");
        Files.setPosixFilePermissions(cfg, Set.of(
                PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.GROUP_READ));
        Properties props = new Properties();
        props.setProperty("root-password", "secret");
        ConfigLoader loader = new ConfigLoader();

        loader.checkPermissions(cfg, props); // must not throw
    }

    @Test
    void groupWritableSecretConfigFileThrows(@TempDir Path dir) throws IOException {
        assumeTrue(posixSupported(), "POSIX permissions not supported on this platform");
        Path cfg = writeConfig(dir, "config", "root-password=secret\n");
        Files.setPosixFilePermissions(cfg, Set.of(
                PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE));
        Properties props = new Properties();
        props.setProperty("root-password", "secret");
        ConfigLoader loader = new ConfigLoader();

        assertThatThrownBy(() -> loader.checkPermissions(cfg, props))
                .isInstanceOf(ConfigException.class);
    }

    @Test
    void nonSecretConfigFileIsNeverPermissionChecked(@TempDir Path dir) throws IOException {
        assumeTrue(posixSupported(), "POSIX permissions not supported on this platform");
        Path cfg = writeConfig(dir, "config", "port=27017\n");
        Files.setPosixFilePermissions(cfg, Set.of(
                PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.GROUP_WRITE, PosixFilePermission.OTHERS_WRITE));
        Properties props = new Properties();
        props.setProperty("port", "27017");
        ConfigLoader loader = new ConfigLoader();

        loader.checkPermissions(cfg, props); // no secret keys present -> must not throw
    }

    // ---- toArgs / precedence -----------------------------------------------------------------

    @Test
    void toArgsTranslatesScalarAndBooleanKeys(@TempDir Path dir) throws IOException {
        Path cfg = writeConfig(dir, "config", "port=27020\nbind=0.0.0.0\nssl=true\nauth=false\n");
        ConfigLoader loader = new ConfigLoader();
        Properties props = loader.load(cfg);

        List<String> tokens = loader.toArgs(props);

        assertThat(tokens).contains("--port", "27020", "--bind", "0.0.0.0", "--ssl");
        assertThat(tokens).doesNotContain("--auth");
    }

    @Test
    void configValueIsUsedWhenNoCliOverrideIsGiven() throws Exception {
        List<String> configTokens = List.of("--port", "27020");
        String[] effectiveArgs = configTokens.toArray(new String[0]); // no real CLI args appended

        assertThat(parsePort(effectiveArgs, 17017)).isEqualTo(27020);
    }

    @Test
    void cliArgumentOverridesConfigFileValue() throws Exception {
        // Config tokens come first, real CLI args after - mirrors PoppyDBCLI.main's
        // effectiveArgs construction, so "last assignment wins" gives the CLI arg precedence.
        List<String> configTokens = List.of("--port", "27020");
        String[] realCliArgs = {"--port", "27099"};
        String[] effectiveArgs = concat(configTokens, realCliArgs);

        assertThat(parsePort(effectiveArgs, 17017)).isEqualTo(27099);
    }

    @Test
    void noSslCliFlagOverridesConfigSslTrue() {
        List<String> configTokens = List.of("--ssl");
        String[] realCliArgs = {"--no-ssl"};
        String[] effectiveArgs = concat(configTokens, realCliArgs);

        boolean sslEnabled = false;
        for (int i = 0; i < effectiveArgs.length; i++) {
            if (effectiveArgs[i].equals("--ssl")) {
                sslEnabled = true;
            } else if (effectiveArgs[i].equals("--no-ssl")) {
                sslEnabled = false;
            }
        }

        assertThat(sslEnabled).isFalse();
    }

    /** Minimal re-implementation of PoppyDBCLI's --port parsing to exercise precedence directly. */
    private static int parsePort(String[] effectiveArgs, int defaultPort) {
        int port = defaultPort;
        for (int i = 0; i < effectiveArgs.length; i++) {
            if (effectiveArgs[i].equals("--port") || effectiveArgs[i].equals("-p")) {
                port = Integer.parseInt(effectiveArgs[i + 1]);
                i++;
            }
        }
        return port;
    }

    private static String[] concat(List<String> configTokens, String[] realCliArgs) {
        String[] result = new String[configTokens.size() + realCliArgs.length];
        for (int i = 0; i < configTokens.size(); i++) {
            result[i] = configTokens.get(i);
        }
        System.arraycopy(realCliArgs, 0, result, configTokens.size(), realCliArgs.length);
        return result;
    }
}
