package de.caluga.poppydb;

import de.caluga.morphium.driver.wire.SslHelper;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Read-only inspection of a parsed {@link ServerOptions}: semantic validation (--check-config and
 * every startup), deep filesystem checks (--check-config only) and rendering of the effective
 * configuration (--print-config). Static methods, no state, never calls System.exit.
 */
class ConfigInspector {

    record Result(List<String> errors, List<String> warnings) {}

    // OFF/ALL are accepted for backward compatibility with the pre-refactor CLI, which passed
    // --log-level straight to logback's Level.toLevel and therefore silently accepted them too -
    // they stay undocumented (help text and the error message above only advertise the five
    // "real" levels).
    private static final Set<String> LOG_LEVELS = Set.of("ERROR", "WARN", "INFO", "DEBUG", "TRACE", "OFF", "ALL");
    private static final Set<String> COMPRESSORS = Set.of("none", "snappy", "zstd", "zlib");

    /** Collects ALL semantic problems instead of failing at the first one. Never throws. */
    static Result validate(ServerOptions opts) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        if (opts.port < 1 || opts.port > 65535) {
            errors.add("port must be between 1 and 65535, got: " + opts.port);
        }
        boolean warnOk = opts.memoryWarnPct >= 1 && opts.memoryWarnPct <= 100;
        boolean rejectOk = opts.memoryRejectPct >= 1 && opts.memoryRejectPct <= 100;
        if (!warnOk) {
            errors.add("memory-warn must be between 1 and 100, got: " + opts.memoryWarnPct);
        }
        if (!rejectOk) {
            errors.add("memory-reject must be between 1 and 100, got: " + opts.memoryRejectPct);
        }
        if (warnOk && rejectOk && opts.memoryWarnPct > opts.memoryRejectPct) {
            errors.add("memory-warn (" + opts.memoryWarnPct + ") must not exceed memory-reject ("
                    + opts.memoryRejectPct + ")");
        }
        if (opts.maxBsonSizeBytes < 0) {
            errors.add("max-bson-size must be >= 0 (0 = off), got: " + opts.maxBsonSizeBytes);
        }
        if (opts.maxConnections < 1) {
            errors.add("max-connections must be >= 1, got: " + opts.maxConnections);
        }
        if (opts.socketTimeoutSec < 0) {
            errors.add("socket-timeout must be >= 0, got: " + opts.socketTimeoutSec);
        }
        if (opts.dumpIntervalSec < 0) {
            errors.add("dump-interval must be >= 0, got: " + opts.dumpIntervalSec);
        }
        if (!LOG_LEVELS.contains(opts.logLevel.toUpperCase(Locale.ROOT))) {
            errors.add("Unknown log level '" + opts.logLevel + "' - use ERROR, WARN, INFO, DEBUG or TRACE");
        }
        if (!COMPRESSORS.contains(opts.compressor.toLowerCase(Locale.ROOT))) {
            errors.add("Unknown compressor '" + opts.compressor + "' - use none, snappy, zstd or zlib");
        }
        if ((opts.rootUser == null) != (opts.rootPassword == null)) {
            errors.add("root-user and root-password must be given together");
        }
        if (!opts.rsPriorities.isBlank() && opts.rsSeed.isBlank()) {
            errors.add("rs-priorities requires rs-seed");
        } else {
            try {
                opts.seedHosts();
                opts.seedPriorities();
            } catch (IllegalArgumentException e) {
                errors.add(e.getMessage());
            }
        }

        if (opts.ssl && opts.sslKeystore == null) {
            warnings.add("ssl is enabled but no ssl-keystore is configured - TLS clients will not be able to connect");
        }
        if (opts.sslKeystore == null && opts.sslKeystorePassword != null) {
            warnings.add("ssl-keystore-password is set but ssl-keystore is not - it has no effect");
        }
        if (opts.auth && opts.rootUser == null) {
            warnings.add("auth is enabled but no root-user is configured - only users from a restored dump can authenticate");
        }
        if (opts.dumpIntervalSec > 0 && opts.dumpDir == null) {
            warnings.add("dump-interval is set but dump-dir is not - periodic dumps are disabled");
        }
        return new Result(errors, warnings);
    }

    /**
     * Deep checks that touch the filesystem/crypto - only run by --check-config, never at normal
     * startup (startup keeps its existing fail-on-use behavior). Finds the problems that
     * otherwise only appear once the server starts: unloadable keystore (wrong password),
     * missing keystore file, unusable dump directory.
     */
    static Result deepCheck(ServerOptions opts) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        if (opts.sslKeystore != null) {
            try {
                Path ks = Paths.get(opts.sslKeystore);
                if (!Files.isRegularFile(ks)) {
                    errors.add("SSL keystore not found: " + ks);
                } else {
                    try {
                        SslHelper.createServerSslContext(opts.sslKeystore, opts.sslKeystorePassword);
                    } catch (Exception e) {
                        errors.add("Cannot load SSL keystore " + ks + ": " + e.getMessage());
                    }
                }
            } catch (InvalidPathException e) {
                errors.add("Invalid ssl-keystore path '" + opts.sslKeystore + "': " + e.getMessage());
            }
        }

        if (opts.dumpDir != null) {
            try {
                Path dir = Paths.get(opts.dumpDir);
                if (Files.exists(dir) && !Files.isDirectory(dir)) {
                    errors.add("dump-dir " + dir + " exists but is not a directory");
                } else if (Files.isDirectory(dir) && !Files.isWritable(dir)) {
                    errors.add("dump-dir " + dir + " is not writable");
                } else if (!Files.exists(dir)) {
                    warnings.add("dump-dir " + dir + " does not exist yet");
                }
            } catch (InvalidPathException e) {
                errors.add("Invalid dump-dir path '" + opts.dumpDir + "': " + e.getMessage());
            }
        }
        return new Result(errors, warnings);
    }

    /** Renders the effective configuration as a reloadable properties file with source comments. */
    static String render(ServerOptions opts, Path configFile) {
        StringBuilder sb = new StringBuilder();
        sb.append("# PoppyDB effective configuration (--print-config)\n");
        sb.append("# Config file: ").append(configFile != null ? configFile : "none").append('\n');
        sb.append("# Precedence: command line > config file > built-in defaults\n");
        sb.append("# This output is itself a loadable PoppyDB configuration file.\n\n");

        appendKey(sb, opts, "port", String.valueOf(opts.port));
        appendKey(sb, opts, "bind", opts.bind);
        appendKey(sb, opts, "log-level", opts.logLevel.toUpperCase(Locale.ROOT));
        appendKey(sb, opts, "memory-warn", String.valueOf(opts.memoryWarnPct));
        appendKey(sb, opts, "memory-reject", String.valueOf(opts.memoryRejectPct));
        appendKey(sb, opts, "max-bson-size", String.valueOf(opts.maxBsonSizeBytes));
        appendKey(sb, opts, "compressor", opts.compressor.toLowerCase(Locale.ROOT));
        appendKey(sb, opts, "rs-name", opts.rsName);
        appendKey(sb, opts, "rs-seed", opts.rsSeed);
        appendKey(sb, opts, "rs-priorities", opts.rsPriorities);
        appendKey(sb, opts, "ssl", String.valueOf(opts.ssl));
        appendKey(sb, opts, "ssl-keystore", opts.sslKeystore);
        appendSecret(sb, opts, "ssl-keystore-password", opts.sslKeystorePassword);
        appendKey(sb, opts, "auth", String.valueOf(opts.auth));
        appendKey(sb, opts, "root-user", opts.rootUser);
        appendSecret(sb, opts, "root-password", opts.rootPassword);
        appendKey(sb, opts, "users-file", opts.usersFile);
        appendKey(sb, opts, "dump-dir", opts.dumpDir);
        appendKey(sb, opts, "dump-interval", String.valueOf(opts.dumpIntervalSec));
        appendKey(sb, opts, "max-connections", String.valueOf(opts.maxConnections));
        appendKey(sb, opts, "socket-timeout", String.valueOf(opts.socketTimeoutSec));
        return sb.toString();
    }

    private static void appendKey(StringBuilder sb, ServerOptions opts, String key, String value) {
        if (value == null || value.isEmpty()) {
            sb.append("# ").append(key).append("  (unset)\n");
            sb.append("# ").append(key).append("=\n\n");
            return;
        }
        sb.append("# ").append(key).append("  (").append(sourceText(opts.sourceOf(key))).append(")\n");
        sb.append(key).append('=').append(escapeValue(value)).append("\n\n");
    }

    /** Escapes a value per java.util.Properties conventions so the rendered line always
     *  reloads to the identical string: backslash, CR/LF/TAB/FF, and a leading space. */
    private static String escapeValue(String v) {
        StringBuilder sb = new StringBuilder(v.length() + 8);
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            switch (c) {
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                case '\f': sb.append("\\f"); break;
                default: sb.append(c);
            }
        }
        if (sb.length() > 0 && sb.charAt(0) == ' ') {
            sb.insert(0, '\\');
        }
        return sb.toString();
    }

    private static void appendSecret(StringBuilder sb, ServerOptions opts, String key, String value) {
        if (value == null || value.isEmpty()) {
            sb.append("# ").append(key).append("  (unset)\n");
            sb.append("# ").append(key).append("=\n\n");
            return;
        }
        sb.append("# ").append(key).append("  (set, ").append(sourceText(opts.sourceOf(key)))
          .append(") - value not shown\n");
        sb.append("# ").append(key).append("=***\n\n");
    }

    private static String sourceText(ServerOptions.Source s) {
        switch (s) {
            case CONFIG_FILE:
                return "from config file";
            case CLI:
                return "from command line";
            default:
                return "default";
        }
    }
}
