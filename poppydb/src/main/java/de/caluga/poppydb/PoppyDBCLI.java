package de.caluga.poppydb;

import de.caluga.morphium.driver.wire.SslHelper;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import de.caluga.poppydb.config.ConfigException;
import de.caluga.poppydb.config.ConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
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
                default:
                    break;
            }
        }

        ConfigLoader configLoader = new ConfigLoader();
        Path cfgFile;
        try {
            cfgFile = configLoader.discover(explicitCfg, skipConfigDiscovery);
        } catch (ConfigException e) {
            log.error(e.getMessage());
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
                log.error(e.getMessage());
                System.exit(1);
                return;
            }
            log.info("Using configuration file {}", cfgFile);
        }

        // Config-file tokens first, real CLI args after: the existing "last assignment wins"
        // parser below then automatically gives CLI args precedence over the config file, and
        // the config file precedence over the built-in defaults - for every single setting.
        String[] effectiveArgs = new String[configTokens.size() + args.length];
        for (int i = 0; i < configTokens.size(); i++) {
            effectiveArgs[i] = configTokens.get(i);
        }
        System.arraycopy(args, 0, effectiveArgs, configTokens.size(), args.length);

        PoppyDB srv = configureServer(effectiveArgs);

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

    /**
     * Parses the effective argument list (config-file tokens followed by the real CLI args, see
     * {@link #main(String[])}) and builds a fully configured but not yet started {@link PoppyDB}.
     * Package-private so tests can exercise the real parsing/wiring logic end-to-end without
     * going through {@code main}'s blocking "keep alive" loop.
     */
    static PoppyDB configureServer(String[] effectiveArgs) throws Exception {
        int idx = 0;
        log.info("Starting up server... parsing commandline params");
        String host = "localhost";
        int port = 17017;
        int memoryWarnPct = 75;
        int memoryRejectPct = 90;
        int maxBsonSizeBytes = 16 * 1024 * 1024;
        String rsNameArg = "";
        String hostSeedArg = "";
        List<String> hostsArg = new ArrayList<>();
        Map<String, Integer> hostPrioritiesArg = new java.util.concurrent.ConcurrentHashMap<>();
        String prioritiesArg = "";  // Optional explicit priorities
        int compressorId = OpCompressed.COMPRESSOR_NOOP;

        // SSL configuration
        boolean sslEnabled = false;
        boolean authRequired = false;
        String rootUser = null;
        String rootPassword = null;
        String keystorePath = null;
        String keystorePassword = null;

        // Persistence configuration
        String dumpDir = null;
        long dumpIntervalSec = 0;

        // Connection management configuration
        int maxConnections = 500;
        int socketTimeoutSec = 300;

        while (idx < effectiveArgs.length) {
            switch (effectiveArgs[idx]) {
                case "--help":
                case "-h":
                    printHelp();
                    System.exit(0);
                    break;

                // Already handled by the pre-scan above; tolerate them here too in case they
                // show up in the real (post-config-token) part of effectiveArgs.
                case "--cfg":
                case "-f":
                    idx += 2;
                    break;

                case "--no-config":
                    idx += 1;
                    break;

                case "-p":
                case "--port":
                    requireValue(effectiveArgs, idx);
                    port = Integer.parseInt(effectiveArgs[idx + 1]);
                    idx += 2;
                    break;

                case "-b":
                case "--bind":
                    requireValue(effectiveArgs, idx);
                    host = effectiveArgs[idx + 1];
                    idx += 2;
                    break;

                case "--memory-warn":
                    requireValue(effectiveArgs, idx);
                    memoryWarnPct = Integer.parseInt(effectiveArgs[idx + 1]);
                    idx += 2;
                    break;

                case "--memory-reject":
                    requireValue(effectiveArgs, idx);
                    memoryRejectPct = Integer.parseInt(effectiveArgs[idx + 1]);
                    idx += 2;
                    break;

                case "--max-bson-size":
                    requireValue(effectiveArgs, idx);
                    maxBsonSizeBytes = Integer.parseInt(effectiveArgs[idx + 1]);
                    idx += 2;
                    break;

                case "--log-level": {
                    requireValue(effectiveArgs, idx);
                    ch.qos.logback.classic.Level level = ch.qos.logback.classic.Level.toLevel(effectiveArgs[idx + 1], null);

                    if (level == null) {
                        log.error("Unknown log level {} - use ERROR, WARN, INFO, DEBUG or TRACE", effectiveArgs[idx + 1]);
                        System.exit(1);
                    }

                    ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME))
                    .setLevel(level);
                    idx += 2;
                    break;
                }
                case "--rs-name":
                    requireValue(effectiveArgs, idx);
                    rsNameArg = effectiveArgs[idx + 1];
                    idx += 2;
                    break;
                case "--rs-seed":
                    requireValue(effectiveArgs, idx);
                    hostSeedArg = effectiveArgs[idx + 1];
                    idx += 2;
                    hostsArg = new ArrayList<>();
                    // Parse hosts - priorities will be assigned after all args are parsed
                    for (String s : hostSeedArg.split(",")) {
                        s = s.trim();
                        int rsport = 27017;
                        String hst = s;

                        if (hst.contains(":")) {
                            rsport = Integer.parseInt(hst.split(":")[1]);
                            hst = hst.split(":")[0];
                        }

                        String entry = hst + ":" + rsport;
                        hostsArg.add(entry);
                    }
                    break;

                case "--rs-priorities":
                    requireValue(effectiveArgs, idx);
                    prioritiesArg = effectiveArgs[idx + 1];
                    idx += 2;
                    break;

                case "-c":
                case "--compressor":
                    requireValue(effectiveArgs, idx);
                    if (effectiveArgs[idx + 1].equals("snappy")) {
                        compressorId = OpCompressed.COMPRESSOR_SNAPPY;
                    } else if (effectiveArgs[idx + 1].equals("zstd")) {
                        compressorId = OpCompressed.COMPRESSOR_ZSTD;
                    } else if (effectiveArgs[idx + 1].equals("none")) {
                        compressorId = OpCompressed.COMPRESSOR_NOOP;
                    } else if (effectiveArgs[idx + 1].equals("zlib")) {
                        compressorId = OpCompressed.COMPRESSOR_ZLIB;
                    } else {
                        log.error("Unknown parameter for compressor {}", effectiveArgs[idx + 1]);
                        System.exit(1);
                    }

                    idx += 2;
                    break;

                case "--ssl":
                case "--tls":
                    sslEnabled = true;
                    idx += 1;
                    break;

                case "--no-ssl":
                    sslEnabled = false;
                    idx += 1;
                    break;

                case "--auth":
                    authRequired = true;
                    idx += 1;
                    break;

                case "--no-auth":
                    authRequired = false;
                    idx += 1;
                    break;

                case "--rootUser":
                    requireValue(effectiveArgs, idx);
                    rootUser = effectiveArgs[idx + 1];
                    idx += 2;
                    break;

                case "--rootPassword":
                    requireValue(effectiveArgs, idx);
                    rootPassword = effectiveArgs[idx + 1];
                    idx += 2;
                    break;

                case "--sslKeystore":
                case "--tlsKeystore":
                    requireValue(effectiveArgs, idx);
                    keystorePath = effectiveArgs[idx + 1];
                    idx += 2;
                    break;

                case "--sslKeystorePassword":
                case "--tlsKeystorePassword":
                    requireValue(effectiveArgs, idx);
                    keystorePassword = effectiveArgs[idx + 1];
                    idx += 2;
                    break;

                case "--dump-dir":
                case "-d":
                    requireValue(effectiveArgs, idx);
                    dumpDir = effectiveArgs[idx + 1];
                    idx += 2;
                    break;

                case "--dump-interval":
                    requireValue(effectiveArgs, idx);
                    dumpIntervalSec = Long.parseLong(effectiveArgs[idx + 1]);
                    idx += 2;
                    break;

                case "--max-connections":
                    requireValue(effectiveArgs, idx);
                    maxConnections = Integer.parseInt(effectiveArgs[idx + 1]);
                    idx += 2;
                    break;

                case "--socket-timeout":
                    requireValue(effectiveArgs, idx);
                    socketTimeoutSec = Integer.parseInt(effectiveArgs[idx + 1]);
                    idx += 2;
                    break;

                default:
                    log.error("unknown parameter " + effectiveArgs[idx]);
                    System.exit(1);
            }
        }

        log.info("Starting server...");

        // Assign priorities to hosts
        // Default: all nodes have equal priority (50)
        // If --rs-priorities is specified, use those values
        hostPrioritiesArg = new java.util.concurrent.ConcurrentHashMap<>();
        if (!hostsArg.isEmpty()) {
            if (prioritiesArg.isEmpty()) {
                // Default: all equal priority
                for (String h : hostsArg) {
                    hostPrioritiesArg.put(h, 50);
                }
                log.info("All nodes have equal election priority (50)");
            } else {
                // Parse explicit priorities
                String[] prioValues = prioritiesArg.split(",");
                if (prioValues.length != hostsArg.size()) {
                    log.error("Number of priorities ({}) must match number of hosts ({})",
                            prioValues.length, hostsArg.size());
                    System.exit(1);
                }
                for (int i = 0; i < hostsArg.size(); i++) {
                    int prio = Integer.parseInt(prioValues[i].trim());
                    if (prio < 0 || prio > 100) {
                        log.error("Priority must be between 0 and 100, got: {}", prio);
                        System.exit(1);
                    }
                    hostPrioritiesArg.put(hostsArg.get(i), prio);
                }
                log.info("Election priorities: {}", hostPrioritiesArg);
            }
        }

        var srv = new PoppyDB(port, host, maxConnections, socketTimeoutSec, compressorId);
        srv.setMemoryWatermarks(memoryWarnPct, memoryRejectPct);
        srv.setMaxBsonObjectSize(maxBsonSizeBytes);

        // Configure replica set - election is always enabled for multi-node replica sets
        boolean enableElection = !rsNameArg.isEmpty() && hostsArg.size() > 1;
        if (enableElection) {
            log.info("Replica set configured with {} members, election enabled", hostsArg.size());
        }
        srv.configureReplicaSet(rsNameArg, hostsArg, hostPrioritiesArg, enableElection, null);

        // Configure SSL if enabled
        if (sslEnabled) {
            log.info("SSL/TLS enabled");
            if (keystorePath != null) {
                log.info("Loading keystore from: {}", keystorePath);
                try {
                    SSLContext sslContext = SslHelper.createServerSslContext(keystorePath, keystorePassword);
                    srv.setSslContext(sslContext);
                } catch (Exception e) {
                    log.error("Failed to load SSL keystore: {}", e.getMessage());
                    System.exit(1);
                }
            }
            srv.setSslEnabled(true);
        }

        // Configure auth enforcement if enabled
        if (authRequired) {
            log.info("Auth enforcement enabled (--auth): clients must authenticate via SCRAM");
            srv.setAuthRequired(true);
        }

        if (rootUser != null || rootPassword != null) {
            if (rootUser == null || rootPassword == null) {
                log.error("--rootUser and --rootPassword must be given together");
                System.exit(1);
            }
            srv.setRootUser(rootUser, rootPassword);
        }

        // Configure persistence if enabled
        if (dumpDir != null) {
            java.io.File dir = new java.io.File(dumpDir);
            srv.setDumpDirectory(dir);
            log.info("Persistence enabled: dump directory = {}", dir.getAbsolutePath());

            if (dumpIntervalSec > 0) {
                srv.setDumpIntervalMs(dumpIntervalSec * 1000);
                log.info("Periodic dumps every {} seconds", dumpIntervalSec);
            }

            // Restore previous state if dump files exist
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

    /** Bounds-check helper for value-reading cases in the argument loop above. */
    private static void requireValue(String[] arr, int idx) {
        if (idx + 1 >= arr.length) {
            log.error("Option {} requires a value", arr[idx]);
            System.exit(1);
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
        System.out.println();
        System.out.println("  -h, --help                 : Print this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar poppydb.jar -p 27017");
        System.out.println("  java -jar poppydb.jar -p 27018 --ssl --sslKeystore server.jks --sslKeystorePassword changeit");
        System.out.println("  java -jar poppydb.jar -p 27017 --dump-dir /var/poppydb/data --dump-interval 300");
        System.out.println("  java -jar poppydb.jar --rs-name myrs --rs-seed localhost:27017,localhost:27018,localhost:27019");
        System.out.println("  java -jar poppydb.jar --cfg /etc/poppydb/config");
    }
}
