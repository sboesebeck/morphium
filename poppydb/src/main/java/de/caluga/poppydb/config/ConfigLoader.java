package de.caluga.poppydb.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Loads PoppyDB's optional {@code java.util.Properties}-format configuration file and turns it
 * into a list of CLI tokens that {@link de.caluga.poppydb.PoppyDBCLI} feeds into its existing
 * argument parser <em>ahead of</em> the real command line arguments - so the parser's own
 * "last assignment wins" behaviour transparently gives CLI arguments precedence over the
 * config file, and the config file precedence over built-in defaults.
 * <p>
 * Search order (first match wins, no merging across files) - see {@code docs/poppydb.md}:
 * <ol>
 *   <li>explicit {@code --cfg}/{@code -f} path</li>
 *   <li>{@code $POPPYDB_CONF}</li>
 *   <li>{@code ${XDG_CONFIG_HOME:-~/.config}/poppydb/config}</li>
 *   <li>{@code ${XDG_CONFIG_HOME:-~/.config}/poppydb.conf}</li>
 *   <li>{@code /etc/poppydb/config}</li>
 *   <li>{@code /etc/poppydb.conf}</li>
 * </ol>
 * This class never calls {@code System.exit(...)} - every fatal condition is reported via
 * {@link ConfigException} so callers (and tests) can handle it explicitly.
 */
public class ConfigLoader {
    private static final Logger log = LoggerFactory.getLogger(ConfigLoader.class);

    public static final String POPPYDB_CONF_ENV_VAR = "POPPYDB_CONF";
    private static final String PREFIX = "poppydb.";

    private enum Type { INT, LONG, BOOL, LOG_LEVEL, COMPRESSOR, STRING, PATH, SECRET }

    /** Canonical (kebab-case) key -> type. Also the whitelist for unknown-key detection. */
    private static final LinkedHashMap<String, Type> KNOWN_KEYS = new LinkedHashMap<>();
    /** normalized(alias or canonical) -> canonical key. */
    private static final Map<String, String> ALIASES = new LinkedHashMap<>();
    /** Keys that name a config-file setting but map to a CLI-only flag, not a value flag. */
    private static final Map<String, String> BOOLEAN_FLAGS = new LinkedHashMap<>();
    /** canonical key -> long CLI flag, for straightforward value flags. */
    private static final Map<String, String> FLAG_FOR_KEY = new LinkedHashMap<>();
    /** normalized reserved words that must never appear as a config-file key. */
    private static final Set<String> RESERVED_KEYS = Set.of("cfg", "f", "noconfig", "help", "h");
    /** secret-value keys whose *content* must never be logged verbatim. */
    private static final Set<String> SECRET_KEYS = Set.of("root-password", "ssl-keystore-password");

    private static void define(String canonical, Type type, String flag, String... aliases) {
        KNOWN_KEYS.put(canonical, type);
        ALIASES.put(normalize(canonical), canonical);
        for (String alias : aliases) {
            ALIASES.put(normalize(alias), canonical);
        }
        if (flag != null) {
            FLAG_FOR_KEY.put(canonical, flag);
        }
    }

    static {
        define("port", Type.INT, "--port");
        define("bind", Type.STRING, "--bind", "host");
        define("log-level", Type.LOG_LEVEL, "--log-level");
        define("memory-warn", Type.INT, "--memory-warn");
        define("memory-reject", Type.INT, "--memory-reject");
        define("max-bson-size", Type.INT, "--max-bson-size");
        define("compressor", Type.COMPRESSOR, "--compressor");
        define("rs-name", Type.STRING, "--rs-name");
        define("rs-seed", Type.STRING, "--rs-seed");
        define("rs-priorities", Type.STRING, "--rs-priorities");
        define("ssl", Type.BOOL, null, "tls");
        define("ssl-keystore", Type.PATH, "--sslKeystore", "tls-keystore");
        define("ssl-keystore-password", Type.SECRET, "--sslKeystorePassword", "tls-keystore-password");
        define("ssl-keystore-password-file", Type.PATH, null, "tls-keystore-password-file");
        define("auth", Type.BOOL, null);
        define("root-user", Type.STRING, "--rootUser");
        define("root-password", Type.SECRET, "--rootPassword");
        define("root-password-file", Type.PATH, null);
        define("users-file", Type.PATH, "--users-file");
        define("dump-dir", Type.PATH, "--dump-dir");
        define("dump-interval", Type.LONG, "--dump-interval");
        define("max-connections", Type.INT, "--max-connections");
        define("socket-timeout", Type.INT, "--socket-timeout");

        BOOLEAN_FLAGS.put("ssl", "--ssl");
        BOOLEAN_FLAGS.put("auth", "--auth");
    }

    /** file-ref key -> the direct value key it resolves into. */
    private static final Map<String, String> FILE_REF_PAIRS = Map.of(
        "root-password-file", "root-password",
        "ssl-keystore-password-file", "ssl-keystore-password"
    );

    private final List<Path> searchPathCandidates;
    private final String poppydbConfEnvValue;

    public ConfigLoader() {
        this(defaultSearchPaths(), System.getenv(POPPYDB_CONF_ENV_VAR));
    }

    /** Test-friendly constructor: inject the search path candidates, skip $POPPYDB_CONF. */
    public ConfigLoader(List<Path> searchPathCandidates) {
        this(searchPathCandidates, null);
    }

    /** Test-friendly constructor: inject both the search path candidates and the env var value. */
    public ConfigLoader(List<Path> searchPathCandidates, String poppydbConfEnvValue) {
        this.searchPathCandidates = List.copyOf(searchPathCandidates);
        this.poppydbConfEnvValue = poppydbConfEnvValue;
    }

    static List<Path> defaultSearchPaths() {
        String xdg = System.getenv("XDG_CONFIG_HOME");
        Path configHome = (xdg != null && !xdg.isBlank())
                ? Paths.get(expandHome(xdg))
                : Paths.get(System.getProperty("user.home"), ".config");
        return List.of(
            configHome.resolve("poppydb").resolve("config"),
            configHome.resolve("poppydb.conf"),
            Paths.get("/etc/poppydb/config"),
            Paths.get("/etc/poppydb.conf")
        );
    }

    /** Expands a leading {@code ~} using {@code user.home}, like a shell would. */
    public static String expandHome(String raw) {
        if (raw == null) {
            return null;
        }
        if (raw.equals("~")) {
            return System.getProperty("user.home");
        }
        if (raw.startsWith("~/")) {
            return System.getProperty("user.home") + raw.substring(1);
        }
        return raw;
    }

    // ---- discovery -------------------------------------------------------------------------

    /**
     * Finds the single configuration file to use, or {@code null} if none applies. Never merges
     * multiple files - first match wins.
     *
     * @param explicitCfg    path given via {@code --cfg}/{@code -f}, or {@code null}
     * @param skipDiscovery  {@code true} if {@code --no-config} was given (skips steps 2-6 below,
     *                       but an explicit {@code --cfg}/{@code -f} still wins if also given)
     */
    public Path discover(Path explicitCfg, boolean skipDiscovery) {
        if (explicitCfg != null) {
            return requireValidCandidate(explicitCfg, "--cfg/-f");
        }
        if (poppydbConfEnvValue != null && !poppydbConfEnvValue.isBlank()) {
            return requireValidCandidate(Paths.get(expandHome(poppydbConfEnvValue)), "$POPPYDB_CONF");
        }
        if (skipDiscovery) {
            log.debug("Configuration file discovery skipped (--no-config)");
            return null;
        }
        for (int i = 0; i < searchPathCandidates.size(); i++) {
            Path resolved = resolveDirectoryCandidate(searchPathCandidates.get(i));
            if (Files.isRegularFile(resolved)) {
                warnAboutLowerPriorityCandidates(i);
                return resolved;
            }
        }
        log.debug("No PoppyDB configuration file found at any default search path");
        return null;
    }

    private void warnAboutLowerPriorityCandidates(int selectedIndex) {
        List<String> extras = new ArrayList<>();
        for (int j = selectedIndex + 1; j < searchPathCandidates.size(); j++) {
            Path other = resolveDirectoryCandidate(searchPathCandidates.get(j));
            if (Files.isRegularFile(other)) {
                extras.add(searchPathCandidates.get(j).toString());
            }
        }
        if (!extras.isEmpty()) {
            log.warn("Ignoring lower-priority config candidate(s) {} (using {})",
                    extras, searchPathCandidates.get(selectedIndex));
        }
    }

    /** A candidate pointing at a directory means "look for a file named 'config' inside it". */
    private static Path resolveDirectoryCandidate(Path p) {
        return Files.isDirectory(p) ? p.resolve("config") : p;
    }

    private static Path requireValidCandidate(Path p, String source) {
        Path resolved = resolveDirectoryCandidate(p);
        if (!Files.exists(resolved)) {
            throw new ConfigException(String.format(
                    "Configuration file specified via %s not found: %s", source, resolved));
        }
        if (!Files.isRegularFile(resolved)) {
            throw new ConfigException(String.format(
                    "Configuration path specified via %s is not a regular file and contains no usable "
                    + "'config' file: %s", source, resolved));
        }
        if (!Files.isReadable(resolved)) {
            throw new ConfigException(String.format(
                    "Configuration file specified via %s is not readable: %s", source, resolved));
        }
        return resolved;
    }

    // ---- loading & validation ----------------------------------------------------------------

    /**
     * Loads a config file: UTF-8, strips an optional {@code poppydb.} prefix, normalizes keys,
     * validates them against the known-key whitelist and per-key types, and returns a
     * {@link Properties} keyed by canonical (kebab-case) key name.
     */
    public Properties load(Path path) {
        Properties raw = new Properties();
        try (Reader r = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            raw.load(r);
        } catch (IOException e) {
            throw new ConfigException("Failed to read configuration file " + path + ": " + e.getMessage(), e);
        }

        detectDuplicatesAndCollisions(path, scanRawKeys(path));

        Properties canonical = new Properties();
        for (String rawKey : raw.stringPropertyNames()) {
            String stripped = stripPrefix(rawKey);
            String normalized = normalize(stripped);

            if (RESERVED_KEYS.contains(normalized)) {
                throw new ConfigException(String.format(
                        "'%s' cannot be set inside a configuration file (%s)", stripped, path));
            }

            String canonicalKey = ALIASES.get(normalized);
            if (canonicalKey == null) {
                String suggestion = suggest(normalized);
                throw new ConfigException(String.format(
                        "Unknown configuration key '%s' in %s%s", rawKey, path,
                        suggestion != null ? " - did you mean '" + suggestion + "'?" : ""));
            }

            String value = raw.getProperty(rawKey);
            canonical.setProperty(canonicalKey, validateAndNormalize(path, canonicalKey, value));
        }

        logLoadedKeys(path, canonical);
        return canonical;
    }

    private void logLoadedKeys(Path path, Properties canonical) {
        if (!log.isDebugEnabled()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (String key : canonical.stringPropertyNames()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(key).append('=').append(isSecret(key) ? "***" : canonical.getProperty(key));
        }
        log.debug("Loaded configuration from {}: {}", path, sb);
    }

    private static boolean isSecret(String canonicalKey) {
        return SECRET_KEYS.contains(canonicalKey);
    }

    private String validateAndNormalize(Path path, String canonicalKey, String value) {
        Type type = KNOWN_KEYS.get(canonicalKey);
        switch (type) {
            case INT:
                try {
                    Integer.parseInt(value.trim());
                } catch (NumberFormatException e) {
                    throw new ConfigException(String.format(
                            "Invalid value for '%s' in %s: '%s' is not a valid integer", canonicalKey, path, value));
                }
                return value.trim();
            case LONG:
                try {
                    Long.parseLong(value.trim());
                } catch (NumberFormatException e) {
                    throw new ConfigException(String.format(
                            "Invalid value for '%s' in %s: '%s' is not a valid number", canonicalKey, path, value));
                }
                return value.trim();
            case BOOL:
                return normalizeBool(path, canonicalKey, value);
            case LOG_LEVEL: {
                String v = value.trim().toUpperCase(Locale.ROOT);
                if (!Set.of("ERROR", "WARN", "INFO", "DEBUG", "TRACE").contains(v)) {
                    throw new ConfigException(String.format(
                            "Invalid value for '%s' in %s: '%s' - use ERROR, WARN, INFO, DEBUG or TRACE",
                            canonicalKey, path, value));
                }
                return v;
            }
            case COMPRESSOR: {
                String v = value.trim().toLowerCase(Locale.ROOT);
                if (!Set.of("none", "snappy", "zstd", "zlib").contains(v)) {
                    throw new ConfigException(String.format(
                            "Invalid value for '%s' in %s: '%s' - use none, snappy, zstd or zlib",
                            canonicalKey, path, value));
                }
                return v;
            }
            case STRING:
            case PATH:
            case SECRET:
            default:
                return value;
        }
    }

    private static String normalizeBool(Path path, String canonicalKey, String value) {
        String v = value.trim().toLowerCase(Locale.ROOT);
        switch (v) {
            case "true": case "yes": case "on": case "1":
                return "true";
            case "false": case "no": case "off": case "0":
                return "false";
            default:
                throw new ConfigException(String.format(
                        "Invalid value for '%s' in %s: '%s' - use true/false/yes/no/on/off/1/0",
                        canonicalKey, path, value));
        }
    }

    // ---- duplicate / collision / unknown-key diagnostics -------------------------------------

    private static List<String> scanRawKeys(Path path) {
        List<String> keys = new ArrayList<>();
        try {
            for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                String t = line.strip();
                if (t.isEmpty() || t.startsWith("#") || t.startsWith("!")) {
                    continue;
                }
                int sep = findSeparator(t);
                if (sep < 0) {
                    continue;
                }
                keys.add(t.substring(0, sep).strip());
            }
        } catch (IOException e) {
            throw new ConfigException("Failed to read configuration file " + path + ": " + e.getMessage(), e);
        }
        return keys;
    }

    private static int findSeparator(String line) {
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '\\') {
                i++; // skip the escaped character
                continue;
            }
            if (c == '=' || c == ':' || Character.isWhitespace(c)) {
                return i;
            }
        }
        return -1;
    }

    private void detectDuplicatesAndCollisions(Path path, List<String> rawKeys) {
        Map<String, Integer> exactCounts = new LinkedHashMap<>();
        Map<String, Set<String>> byNormalized = new LinkedHashMap<>();

        for (String rawKey : rawKeys) {
            exactCounts.merge(rawKey, 1, Integer::sum);
            String normalized = normalize(stripPrefix(rawKey));
            byNormalized.computeIfAbsent(normalized, k -> new LinkedHashSet<>()).add(rawKey);
        }

        for (Map.Entry<String, Integer> e : exactCounts.entrySet()) {
            if (e.getValue() > 1) {
                log.warn("Duplicate key '{}' in configuration file {} - last occurrence wins", e.getKey(), path);
            }
        }

        for (Map.Entry<String, Set<String>> e : byNormalized.entrySet()) {
            if (e.getValue().size() > 1) {
                throw new ConfigException(String.format(
                        "Conflicting spellings of the same setting in %s: %s all normalize to '%s' - keep only one",
                        path, e.getValue(), e.getKey()));
            }
        }
    }

    // ---- key normalization --------------------------------------------------------------------

    static String stripPrefix(String rawKey) {
        String trimmed = rawKey.trim();
        if (trimmed.toLowerCase(Locale.ROOT).startsWith(PREFIX)) {
            return trimmed.substring(PREFIX.length());
        }
        return trimmed;
    }

    static String normalize(String key) {
        return key.trim().toLowerCase(Locale.ROOT).replaceAll("[-_.]", "");
    }

    private static String suggest(String normalizedUnknownKey) {
        String best = null;
        int bestDistance = Integer.MAX_VALUE;
        for (String canonical : KNOWN_KEYS.keySet()) {
            int d = levenshtein(normalizedUnknownKey, normalize(canonical));
            if (d < bestDistance) {
                bestDistance = d;
                best = canonical;
            }
        }
        if (best != null && bestDistance <= Math.max(2, best.length() / 2)) {
            return best;
        }
        return null;
    }

    static int levenshtein(String a, String b) {
        int[][] dp = new int[a.length() + 1][b.length() + 1];
        for (int i = 0; i <= a.length(); i++) {
            dp[i][0] = i;
        }
        for (int j = 0; j <= b.length(); j++) {
            dp[0][j] = j;
        }
        for (int i = 1; i <= a.length(); i++) {
            for (int j = 1; j <= b.length(); j++) {
                int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
                dp[i][j] = Math.min(Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1), dp[i - 1][j - 1] + cost);
            }
        }
        return dp[a.length()][b.length()];
    }

    // ---- *-file secret indirection ------------------------------------------------------------

    /**
     * Resolves {@code root-password-file}/{@code ssl-keystore-password-file} into their direct
     * counterparts (UTF-8, one trailing {@code \r}/{@code \n} sequence stripped). Setting both the
     * direct value and the {@code *-file} indirection for the same secret is a hard error.
     */
    public Properties resolveFileRefs(Properties props) {
        Properties result = new Properties();
        result.putAll(props);

        for (Map.Entry<String, String> pair : FILE_REF_PAIRS.entrySet()) {
            String fileKey = pair.getKey();
            String valueKey = pair.getValue();
            String filePath = result.getProperty(fileKey);
            if (filePath == null) {
                continue;
            }
            if (result.getProperty(valueKey) != null) {
                throw new ConfigException(String.format(
                        "Both '%s' and '%s' are set - remove one, they conflict", valueKey, fileKey));
            }

            Path path = Paths.get(expandHome(filePath));
            String content;
            try {
                content = Files.readString(path, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new ConfigException(String.format(
                        "Failed to read secret file '%s' referenced by '%s': %s", path, fileKey, e.getMessage()), e);
            }
            result.setProperty(valueKey, stripTrailingLineEnding(content));
            result.remove(fileKey);
        }
        return result;
    }

    private static String stripTrailingLineEnding(String s) {
        if (s.endsWith("\r\n")) {
            return s.substring(0, s.length() - 2);
        }
        if (s.endsWith("\n") || s.endsWith("\r")) {
            return s.substring(0, s.length() - 1);
        }
        return s;
    }

    // ---- permission checks ---------------------------------------------------------------------

    /**
     * Checks POSIX permissions of any file that carries a secret: the configuration file itself
     * if it embeds {@code root-password}/{@code ssl-keystore-password} directly, and any file
     * referenced via {@code *-file}. Group/other-readable is a warning, group/other-writable is
     * fatal (a world-writable config carrying secrets is a privilege escalation).
     */
    public void checkPermissions(Path configFile, Properties props) {
        boolean inlineSecret = props.getProperty("root-password") != null
                || props.getProperty("ssl-keystore-password") != null;
        if (inlineSecret) {
            checkFilePermissions(configFile, "Configuration file");
        }
        checkReferencedSecretFile(props, "root-password-file");
        checkReferencedSecretFile(props, "ssl-keystore-password-file");
    }

    private void checkReferencedSecretFile(Properties props, String fileKey) {
        String filePath = props.getProperty(fileKey);
        if (filePath == null || filePath.isBlank()) {
            return;
        }
        checkFilePermissions(Paths.get(expandHome(filePath)), "Secret file (" + fileKey + ")");
    }

    private void checkFilePermissions(Path file, String label) {
        FileSystem fs = file.getFileSystem();
        if (!fs.supportedFileAttributeViews().contains("posix")) {
            log.debug("Skipping permission check for {} {} - no POSIX file attribute view available", label, file);
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
                        "%s %s contains secrets and is writable by group/others (%s) - refusing to start, "
                        + "chmod 600 required", label, file, PosixFilePermissions.toString(perms)));
            }
            if (groupOrOtherRead) {
                log.warn("{} {} contains secrets and is readable by group/others ({}) - recommended: chmod 600",
                        label, file, PosixFilePermissions.toString(perms));
            }
        } catch (IOException e) {
            log.debug("Could not check permissions of {} {}: {}", label, file, e.getMessage());
        }
    }

    // ---- config -> CLI token translation --------------------------------------------------------

    /**
     * Translates already-validated, canonical-keyed properties into the CLI token list that
     * {@link de.caluga.poppydb.PoppyDBCLI} prepends to the real command line arguments.
     */
    public List<String> toArgs(Properties props) {
        List<String> tokens = new ArrayList<>();
        for (String key : props.stringPropertyNames()) {
            String value = props.getProperty(key);

            String booleanFlag = BOOLEAN_FLAGS.get(key);
            if (booleanFlag != null) {
                if ("true".equals(value)) {
                    tokens.add(booleanFlag);
                }
                continue;
            }

            String flag = FLAG_FOR_KEY.get(key);
            if (flag == null) {
                // *-file keys are resolved (and removed) by resolveFileRefs before this runs;
                // ignore anything unmapped defensively rather than emitting a bogus CLI token.
                continue;
            }
            tokens.add(flag);
            tokens.add(value);
        }
        return tokens;
    }
}
