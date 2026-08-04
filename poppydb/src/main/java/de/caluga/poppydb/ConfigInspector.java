package de.caluga.poppydb;

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

    private static final Set<String> LOG_LEVELS = Set.of("ERROR", "WARN", "INFO", "DEBUG", "TRACE");
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
}
