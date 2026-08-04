package de.caluga.poppydb;

import de.caluga.morphium.driver.wire.SslHelper;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import de.caluga.poppydb.config.ConfigException;
import de.caluga.poppydb.config.ConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.lang.Thread.sleep;

public class PoppyDBCLI {
    private static final Logger log = LoggerFactory.getLogger(PoppyDBCLI.class);

    public static void main(String[] args) throws Exception {
        // Pre-scan: --help/-h anywhere, --cfg/-f <path> and --no-config are resolved before the
        // real argument parser runs, since the config file (if any) contributes its own tokens
        // to the front of the effective argument list (see below).
        Path explicitCfg = null;
        boolean skipConfigDiscovery = false;
        boolean printConfig = false;
        boolean checkConfig = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--help":
                case "-h":
                    printHelp();
                    return;
                case "--cfg":
                case "-f":
                    if (i + 1 >= args.length) {
                        log.error("Option {} requires a value", args[i]);
                        System.exit(1);
                        return;
                    }
                    explicitCfg = Paths.get(ConfigLoader.expandHome(args[i + 1]));
                    i++;
                    break;
                case "--no-config":
                    skipConfigDiscovery = true;
                    break;
                case "--print-config":
                    printConfig = true;
                    break;
                case "--check-config":
                    checkConfig = true;
                    break;
                default:
                    break;
            }
        }

        if (printConfig || checkConfig) {
            redirectConsoleLoggingToStderr();
        }

        ConfigLoader configLoader = new ConfigLoader();
        Path cfgFile;
        try {
            cfgFile = configLoader.discover(explicitCfg, skipConfigDiscovery);
        } catch (ConfigException e) {
            if (checkConfig) {
                System.err.println("Configuration check FAILED:");
                System.err.println("  - " + e.getMessage());
            } else {
                log.error(e.getMessage());
            }
            System.exit(1);
            return;
        }

        // No config file at the default search paths and nothing on the command line either:
        // keep the historic behavior of printing help instead of starting with all defaults.
        if (cfgFile == null && args.length == 0) {
            printHelp();
            return;
        }

        List<String> configTokens = new ArrayList<>();
        if (cfgFile != null) {
            try {
                Properties cfgProps = configLoader.load(cfgFile);
                configLoader.checkPermissions(cfgFile, cfgProps);
                cfgProps = configLoader.resolveFileRefs(cfgProps);
                configTokens = configLoader.toArgs(cfgProps);
            } catch (ConfigException e) {
                if (checkConfig) {
                    System.err.println("Configuration check FAILED:");
                    System.err.println("  - " + e.getMessage());
                } else {
                    log.error(e.getMessage());
                }
                System.exit(1);
                return;
            }
            if (!printConfig && !checkConfig) {
                log.info("Using configuration file {}", cfgFile);
            }
        }

        // Config-file tokens first, real CLI args after: the existing "last assignment wins"
        // parser below then automatically gives CLI args precedence over the config file, and
        // the config file precedence over the built-in defaults - for every single setting.
        String[] effectiveArgs = new String[configTokens.size() + args.length];
        for (int i = 0; i < configTokens.size(); i++) {
            effectiveArgs[i] = configTokens.get(i);
        }
        System.arraycopy(args, 0, effectiveArgs, configTokens.size(), args.length);

        if (printConfig || checkConfig) {
            System.exit(runInspection(effectiveArgs, configTokens.size(), cfgFile,
                    printConfig, checkConfig, System.out, System.err));
            return;
        }

        PoppyDB srv;
        try {
            srv = configureServer(effectiveArgs);
        } catch (ConfigException e) {
            log.error(e.getMessage());
            System.exit(1);
            return;
        }

        try {
            srv.start();
        } catch (Exception e) {
            log.error("Failed to start PoppyDB: {}", e.getMessage());
            System.exit(1);
        }

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered");
            srv.shutdown();
        }));

        while (srv.isRunning()) {
            log.info("PoppyDB alive - connections: {}", srv.getConnectionCount());
            sleep(10000);
        }
    }

    /** In inspection mode stdout belongs exclusively to the printed config / OK line -
     *  all logging (e.g. ConfigLoader warnings) moves to stderr. */
    private static void redirectConsoleLoggingToStderr() {
        ch.qos.logback.classic.Logger root =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        var it = root.iteratorForAppenders();
        while (it.hasNext()) {
            var appender = it.next();
            if (appender instanceof ch.qos.logback.core.ConsoleAppender<?> console) {
                console.stop();
                console.setTarget("System.err");
                console.start();
            }
        }
    }

    /**
     * Parses the effective argument list (config-file tokens followed by the real CLI args, see
     * {@link #main(String[])}) and builds a fully configured but not yet started {@link PoppyDB}.
     * Package-private so tests can exercise the real parsing/wiring logic end-to-end without
     * going through {@code main}'s blocking "keep alive" loop.
     */
    static PoppyDB configureServer(String[] effectiveArgs) throws Exception {
        ServerOptions opts = parse(effectiveArgs, 0);
        ConfigInspector.Result r = ConfigInspector.validate(opts);
        r.warnings().forEach(log::warn);
        if (!r.errors().isEmpty()) {
            throw new ConfigException("Invalid configuration: " + String.join("; ", r.errors()));
        }
        return buildServer(opts);
    }

    /**
     * Backs --print-config/--check-config: parses, optionally prints the effective config,
     * optionally validates it (semantic + deep checks), and returns the process exit code.
     * Pure function of its inputs - System.exit stays in main() so tests can call this.
     */
    static int runInspection(String[] effectiveArgs, int configTokenCount, Path cfgFile,
                             boolean print, boolean check, PrintStream out, PrintStream err) {
        ServerOptions opts;
        try {
            opts = parse(effectiveArgs, configTokenCount);
        } catch (ConfigException e) {
            if (check) {
                err.println("Configuration check FAILED:");
                err.println("  - " + e.getMessage());
            } else {
                err.println(e.getMessage());
            }
            return 1;
        }

        if (print) {
            out.print(ConfigInspector.render(opts, cfgFile));
        }

        if (check) {
            ConfigInspector.Result semantic = ConfigInspector.validate(opts);
            ConfigInspector.Result deep = ConfigInspector.deepCheck(opts);
            List<String> errors = new ArrayList<>(semantic.errors());
            errors.addAll(deep.errors());
            List<String> warnings = new ArrayList<>(semantic.warnings());
            warnings.addAll(deep.warnings());

            for (String w : warnings) {
                err.println("WARNING: " + w);
            }
            if (!errors.isEmpty()) {
                err.println("Configuration check FAILED:");
                for (String e : errors) {
                    err.println("  - " + e);
                }
                return 1;
            }
            out.println("Configuration OK (" + (cfgFile != null ? cfgFile : "no config file") + ")");
        }
        return 0;
    }

    /**
     * Pure argument parsing into a {@link ServerOptions}. Tokens with index < configTokenCount
     * originated from the config file; everything after came from the real command line - that
     * boundary drives the per-key {@link ServerOptions.Source} tracking used by --print-config.
     * Throws {@link ConfigException} instead of calling System.exit so --check-config (and tests)
     * can report errors without killing the JVM.
     */
    static ServerOptions parse(String[] effectiveArgs, int configTokenCount) {
        ServerOptions opts = new ServerOptions();
        int idx = 0;

        while (idx < effectiveArgs.length) {
            ServerOptions.Source src = idx < configTokenCount
                    ? ServerOptions.Source.CONFIG_FILE
                    : ServerOptions.Source.CLI;

            switch (effectiveArgs[idx]) {
                case "--help":
                case "-h":
                    printHelp();
                    System.exit(0);
                    break;

                // Already handled by the pre-scan in main(); tolerate them here too in case they
                // show up in the real (post-config-token) part of effectiveArgs.
                case "--cfg":
                case "-f":
                    idx += 2;
                    break;

                case "--no-config":
                case "--print-config":
                case "--check-config":
                    idx += 1;
                    break;

                case "-p":
                case "--port":
                    opts.port = intValue(effectiveArgs, idx);
                    opts.sources.put("port", src);
                    idx += 2;
                    break;

                case "-b":
                case "--bind":
                    opts.bind = value(effectiveArgs, idx);
                    opts.sources.put("bind", src);
                    idx += 2;
                    break;

                case "--memory-warn":
                    opts.memoryWarnPct = intValue(effectiveArgs, idx);
                    opts.sources.put("memory-warn", src);
                    idx += 2;
                    break;

                case "--memory-reject":
                    opts.memoryRejectPct = intValue(effectiveArgs, idx);
                    opts.sources.put("memory-reject", src);
                    idx += 2;
                    break;

                case "--max-bson-size":
                    opts.maxBsonSizeBytes = intValue(effectiveArgs, idx);
                    opts.sources.put("max-bson-size", src);
                    idx += 2;
                    break;

                case "--log-level":
                    opts.logLevel = value(effectiveArgs, idx);
                    opts.sources.put("log-level", src);
                    idx += 2;
                    break;

                case "--rs-name":
                    opts.rsName = value(effectiveArgs, idx);
                    opts.sources.put("rs-name", src);
                    idx += 2;
                    break;

                case "--rs-seed":
                    opts.rsSeed = value(effectiveArgs, idx);
                    opts.sources.put("rs-seed", src);
                    idx += 2;
                    break;

                case "--rs-priorities":
                    opts.rsPriorities = value(effectiveArgs, idx);
                    opts.sources.put("rs-priorities", src);
                    idx += 2;
                    break;

                case "-c":
                case "--compressor":
                    opts.compressor = value(effectiveArgs, idx);
                    opts.sources.put("compressor", src);
                    idx += 2;
                    break;

                case "--ssl":
                case "--tls":
                    opts.ssl = true;
                    opts.sources.put("ssl", src);
                    idx += 1;
                    break;

                case "--no-ssl":
                    opts.ssl = false;
                    opts.sources.put("ssl", src);
                    idx += 1;
                    break;

                case "--auth":
                    opts.auth = true;
                    opts.sources.put("auth", src);
                    idx += 1;
                    break;

                case "--no-auth":
                    opts.auth = false;
                    opts.sources.put("auth", src);
                    idx += 1;
                    break;

                case "--rootUser":
                    opts.rootUser = value(effectiveArgs, idx);
                    opts.sources.put("root-user", src);
                    idx += 2;
                    break;

                case "--rootPassword":
                    opts.rootPassword = value(effectiveArgs, idx);
                    opts.sources.put("root-password", src);
                    idx += 2;
                    break;

                case "--sslKeystore":
                case "--tlsKeystore":
                    opts.sslKeystore = value(effectiveArgs, idx);
                    opts.sources.put("ssl-keystore", src);
                    idx += 2;
                    break;

                case "--sslKeystorePassword":
                case "--tlsKeystorePassword":
                    opts.sslKeystorePassword = value(effectiveArgs, idx);
                    opts.sources.put("ssl-keystore-password", src);
                    idx += 2;
                    break;

                case "--dump-dir":
                case "-d":
                    opts.dumpDir = value(effectiveArgs, idx);
                    opts.sources.put("dump-dir", src);
                    idx += 2;
                    break;

                case "--dump-interval":
                    opts.dumpIntervalSec = longValue(effectiveArgs, idx);
                    opts.sources.put("dump-interval", src);
                    idx += 2;
                    break;

                case "--max-connections":
                    opts.maxConnections = intValue(effectiveArgs, idx);
                    opts.sources.put("max-connections", src);
                    idx += 2;
                    break;

                case "--socket-timeout":
                    opts.socketTimeoutSec = intValue(effectiveArgs, idx);
                    opts.sources.put("socket-timeout", src);
                    idx += 2;
                    break;

                default:
                    throw new ConfigException("unknown parameter " + effectiveArgs[idx]);
            }
        }
        return opts;
    }

    /** Builds the configured-but-unstarted server from validated options - the old wiring code. */
    static PoppyDB buildServer(ServerOptions opts) throws Exception {
        // Apply the log level first so all subsequent startup logging honors it.
        ch.qos.logback.classic.Level level = ch.qos.logback.classic.Level.toLevel(opts.logLevel, null);
        if (level == null) {
            throw new ConfigException("Unknown log level " + opts.logLevel
                    + " - use ERROR, WARN, INFO, DEBUG or TRACE");
        }
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME))
                .setLevel(level);

        int compressorId;
        switch (opts.compressor.toLowerCase(java.util.Locale.ROOT)) {
            case "snappy":
                compressorId = OpCompressed.COMPRESSOR_SNAPPY;
                break;
            case "zstd":
                compressorId = OpCompressed.COMPRESSOR_ZSTD;
                break;
            case "zlib":
                compressorId = OpCompressed.COMPRESSOR_ZLIB;
                break;
            case "none":
                compressorId = OpCompressed.COMPRESSOR_NOOP;
                break;
            default:
                throw new ConfigException("Unknown parameter for compressor " + opts.compressor);
        }

        log.info("Starting server...");

        List<String> hosts;
        Map<String, Integer> hostPriorities;
        try {
            hosts = opts.seedHosts();
            hostPriorities = opts.seedPriorities();
        } catch (IllegalArgumentException e) {
            throw new ConfigException(e.getMessage(), e);
        }
        if (!hosts.isEmpty()) {
            if (opts.rsPriorities.isBlank()) {
                log.info("All nodes have equal election priority (50)");
            } else {
                log.info("Election priorities: {}", hostPriorities);
            }
        }

        var srv = new PoppyDB(opts.port, opts.bind, opts.maxConnections, opts.socketTimeoutSec, compressorId);
        srv.setMemoryWatermarks(opts.memoryWarnPct, opts.memoryRejectPct);
        srv.setMaxBsonObjectSize(opts.maxBsonSizeBytes);

        // Configure replica set - election is always enabled for multi-node replica sets
        boolean enableElection = !opts.rsName.isEmpty() && hosts.size() > 1;
        if (enableElection) {
            log.info("Replica set configured with {} members, election enabled", hosts.size());
        }
        srv.configureReplicaSet(opts.rsName, hosts, hostPriorities, enableElection, null);

        if (opts.ssl) {
            log.info("SSL/TLS enabled");
            if (opts.sslKeystore != null) {
                log.info("Loading keystore from: {}", opts.sslKeystore);
                try {
                    SSLContext sslContext = SslHelper.createServerSslContext(opts.sslKeystore, opts.sslKeystorePassword);
                    srv.setSslContext(sslContext);
                } catch (Exception e) {
                    throw new ConfigException("Failed to load SSL keystore: " + e.getMessage(), e);
                }
            }
            srv.setSslEnabled(true);
        }

        if (opts.auth) {
            log.info("Auth enforcement enabled (--auth): clients must authenticate via SCRAM");
            srv.setAuthRequired(true);
        }

        if (opts.rootUser != null || opts.rootPassword != null) {
            if (opts.rootUser == null || opts.rootPassword == null) {
                throw new ConfigException("--rootUser and --rootPassword must be given together");
            }
            srv.setRootUser(opts.rootUser, opts.rootPassword);
        }

        if (opts.dumpDir != null) {
            java.io.File dir = new java.io.File(opts.dumpDir);
            srv.setDumpDirectory(dir);
            log.info("Persistence enabled: dump directory = {}", dir.getAbsolutePath());

            if (opts.dumpIntervalSec > 0) {
                srv.setDumpIntervalMs(opts.dumpIntervalSec * 1000);
                log.info("Periodic dumps every {} seconds", opts.dumpIntervalSec);
            }

            try {
                int restored = srv.restoreFromDump();
                if (restored > 0) {
                    log.info("Restored {} databases from previous dump", restored);
                }
            } catch (Exception e) {
                log.warn("Failed to restore from dump (starting fresh): {}", e.getMessage());
            }
        }

        return srv;
    }

    private static String value(String[] arr, int idx) {
        if (idx + 1 >= arr.length) {
            throw new ConfigException("Option " + arr[idx] + " requires a value");
        }
        return arr[idx + 1];
    }

    private static int intValue(String[] arr, int idx) {
        String v = value(arr, idx);
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            throw new ConfigException("Invalid value for " + arr[idx] + ": '" + v + "' is not a valid integer");
        }
    }

    private static long longValue(String[] arr, int idx) {
        String v = value(arr, idx);
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            throw new ConfigException("Invalid value for " + arr[idx] + ": '" + v + "' is not a valid number");
        }
    }

    private static void printHelp() {
        System.out.println("Usage: java -jar poppydb.jar [options]");
        System.out.println();
        System.out.println("PoppyDB is a MongoDB-compatible in-memory server using async I/O (Netty).");
        System.out.println("It can handle thousands of concurrent connections efficiently.");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -p, --port <port>          : Port to listen on (default: 17017)");
        System.out.println("  -b, --bind <host>          : Host to bind to (default: localhost)");
        System.out.println("  --log-level <level>        : Log verbosity: ERROR, WARN, INFO, DEBUG, TRACE (default: INFO)");
        System.out.println("  --memory-warn <percent>    : Log a warning when heap occupancy crosses this percentage (default: 75, 100 = off)");
        System.out.println("  --memory-reject <percent>  : Reject document-creating writes (code 146 ExceededMemoryLimit) above this");
        System.out.println("                               heap percentage; updates/deletes/TTL keep working (default: 90, 100 = off)");
        System.out.println("  --max-bson-size <bytes>    : BSON document size limit, enforced like mongod (code 10334 BSONObjectTooLarge,");
        System.out.println("                               update results get mongod's 16KB margin; default: 16777216 = 16MB, 0 = off)");
        System.out.println("  -c, --compressor <type>    : Compressor to use (none, snappy, zstd, zlib; default: none)");
        System.out.println("  --rs-name <name>           : Name of the replica set");
        System.out.println("  --rs-seed <hosts>          : Comma-separated list of hosts in the replica set");
        System.out.println("                               Example: localhost:27017,localhost:27018,localhost:27019");
        System.out.println("  --rs-priorities <list>     : Comma-separated list of election priorities (0-100) matching seed order");
        System.out.println("                               Default: all nodes have equal priority (50)");
        System.out.println("                               Higher priority = more likely to become primary");
        System.out.println("                               Priority 0 = node can never become primary (like MongoDB arbiter)");
        System.out.println("                               Example: --rs-priorities 100,50,50");
        System.out.println("                               IMPORTANT: All nodes must use the same --rs-seed and --rs-priorities!");
        System.out.println();
        System.out.println("SSL/TLS Options:");
        System.out.println("  --ssl, --tls               : Enable SSL/TLS encrypted connections");
        System.out.println("  --no-ssl                   : Force SSL/TLS off, overriding a config file's ssl=true");
        System.out.println("  --sslKeystore <path>       : Path to JKS or PKCS12 keystore file");
        System.out.println("  --sslKeystorePassword <pw> : Password for the keystore");
        System.out.println("  --auth                     : Require SCRAM authentication (SCRAM-SHA-1/-256).");
        System.out.println("                               Unauthenticated connections may only run the");
        System.out.println("                               handshake/SASL/ping commands.");
        System.out.println("  --no-auth                  : Force auth off, overriding a config file's auth=true");
        System.out.println("  --rootUser <name>          : Initial admin user, created at startup if absent");
        System.out.println("  --rootPassword <pw>        : Password for the initial admin user");
        System.out.println();
        System.out.println("Persistence Options:");
        System.out.println("  -d, --dump-dir <path>      : Directory for periodic database dumps");
        System.out.println("                               Enables persistence: restores on startup, dumps on shutdown");
        System.out.println("  --dump-interval <seconds>  : Interval between periodic dumps (default: 0 = only on shutdown)");
        System.out.println();
        System.out.println("Connection Management Options:");
        System.out.println("  --max-connections <num>    : Maximum concurrent connections (default: 500)");
        System.out.println("  --socket-timeout <seconds> : Idle connection timeout in seconds (default: 300)");
        System.out.println();
        System.out.println("Configuration File Options:");
        System.out.println("  --cfg, -f <path>           : Load settings from this configuration file (java.util.Properties");
        System.out.println("                               format, key=value). Command line arguments always win over");
        System.out.println("                               values from the file, which in turn win over the defaults above.");
        System.out.println("  --no-config                : Do not search the default configuration file locations below");
        System.out.println("                               (an explicit --cfg/-f still applies if also given)");
        System.out.println("                               Without --cfg/-f/--no-config, the first existing file among:");
        System.out.println("                                 $POPPYDB_CONF");
        System.out.println("                                 ${XDG_CONFIG_HOME:-~/.config}/poppydb/config");
        System.out.println("                                 ${XDG_CONFIG_HOME:-~/.config}/poppydb.conf");
        System.out.println("                                 /etc/poppydb/config");
        System.out.println("                                 /etc/poppydb.conf");
        System.out.println("                               is used (no merging - first match wins). See docs/poppydb.md.");
        System.out.println("  --print-config             : Print the effective configuration (defaults + config file +");
        System.out.println("                               command line merged, secrets redacted) as a reusable config");
        System.out.println("                               file with per-key source annotations, then exit");
        System.out.println("  --check-config             : Validate the effective configuration without starting the");
        System.out.println("                               server: syntax, semantic cross-checks and deep checks");
        System.out.println("                               (keystore loadable, dump-dir usable). Exit code 0 = OK, 1 = errors");
        System.out.println();
        System.out.println("  -h, --help                 : Print this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar poppydb.jar -p 27017");
        System.out.println("  java -jar poppydb.jar -p 27018 --ssl --sslKeystore server.jks --sslKeystorePassword changeit");
        System.out.println("  java -jar poppydb.jar -p 27017 --dump-dir /var/poppydb/data --dump-interval 300");
        System.out.println("  java -jar poppydb.jar --rs-name myrs --rs-seed localhost:27017,localhost:27018,localhost:27019");
        System.out.println("  java -jar poppydb.jar --cfg /etc/poppydb/config");
        System.out.println("  java -jar poppydb.jar --cfg /etc/poppydb/config --check-config");
        System.out.println("  java -jar poppydb.jar --no-config --print-config > poppydb.conf.template");
    }
}
