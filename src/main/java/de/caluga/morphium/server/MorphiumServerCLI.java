package de.caluga.morphium.server;

import de.caluga.morphium.driver.wire.SslHelper;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import de.caluga.morphium.server.netty.NettyMorphiumServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.Thread.sleep;

public class MorphiumServerCLI {
    private static final Logger log = LoggerFactory.getLogger(MorphiumServerCLI.class);

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || (args.length == 1 && (args[0].equals("--help") || args[0].equals("-h")))) {
            printHelp();
            return;
        }
        int idx = 0;
        log.info("Starting up server... parsing commandline params");
        String host = "localhost";
        int port = 17017;
        int maxThreads = 1000;
        int minThreads = 10;
        String rsNameArg = "";
        String hostSeedArg = "";
        List<String> hostsArg = new ArrayList<>();
        Map<String, Integer> hostPrioritiesArg = new java.util.concurrent.ConcurrentHashMap<>();
        int compressorId = OpCompressed.COMPRESSOR_NOOP;

        // SSL configuration
        boolean sslEnabled = false;
        String keystorePath = null;
        String keystorePassword = null;

        // Persistence configuration
        String dumpDir = null;
        long dumpIntervalSec = 0;

        // Connection management configuration
        int maxConnections = 500;
        int socketTimeoutSec = 60;

        // Async I/O mode (Netty)
        boolean useNetty = false;

        while (idx < args.length) {
            switch (args[idx]) {
                case "--help":
                case "-h":
                    printHelp();
                    System.exit(0);
                    break;
                case "-p":
                case "--port":
                    port = Integer.parseInt(args[idx + 1]);
                    idx += 2;
                    break;

                case "-mt":
                case "--maxThreads":
                    maxThreads = Integer.parseInt(args[idx + 1]);
                    idx += 2;
                    break;

                case "-mint":
                case "--minThreads":
                    minThreads = Integer.parseInt(args[idx + 1]);
                    idx += 2;
                    break;

                case "-b":
                case "--bind":
                    host = args[idx + 1];
                    idx += 2;
                    break;
                case "--rs-name":
                    rsNameArg = args[idx + 1];
                    idx += 2;
                    break;
                case "--rs-seed":
                    hostSeedArg = args[idx + 1];
                    idx += 2;
                    hostsArg = new ArrayList<>();
                    hostPrioritiesArg = new java.util.concurrent.ConcurrentHashMap<>();
                    int basePrio = 1000;
                    for (int i = 0; i < hostSeedArg.split(",").length; i++) {
                        var s = hostSeedArg.split(",")[i].trim();
                        int rsport = 27017;
                        String hst = s;
                        int prio = basePrio - i;

                        if (hst.contains(":")) {
                            rsport = Integer.parseInt(hst.split(":")[1]);
                            hst = hst.split(":")[0];
                        }

                        String entry = hst + ":" + rsport;
                        hostsArg.add(entry);
                        hostPrioritiesArg.put(entry, prio);
                    }
                    break;

                case "-c":
                case "--compressor":
                    if (args[idx + 1].equals("snappy")) {
                        compressorId = OpCompressed.COMPRESSOR_SNAPPY;
                    } else if (args[idx + 1].equals("zstd")) {
                        compressorId = OpCompressed.COMPRESSOR_ZSTD;
                    } else if (args[idx + 1].equals("none")) {
                        compressorId = OpCompressed.COMPRESSOR_NOOP;
                    } else if (args[idx + 1].equals("zlib")) {
                        compressorId = OpCompressed.COMPRESSOR_ZLIB;
                    } else {
                        log.error("Unknown parameter for compressor {}", args[idx + 1]);
                        System.exit(1);
                    }

                    idx += 2;
                    break;

                case "--ssl":
                case "--tls":
                    sslEnabled = true;
                    idx += 1;
                    break;

                case "--sslKeystore":
                case "--tlsKeystore":
                    keystorePath = args[idx + 1];
                    idx += 2;
                    break;

                case "--sslKeystorePassword":
                case "--tlsKeystorePassword":
                    keystorePassword = args[idx + 1];
                    idx += 2;
                    break;

                case "--dump-dir":
                case "-d":
                    dumpDir = args[idx + 1];
                    idx += 2;
                    break;

                case "--dump-interval":
                    dumpIntervalSec = Long.parseLong(args[idx + 1]);
                    idx += 2;
                    break;

                case "--max-connections":
                    maxConnections = Integer.parseInt(args[idx + 1]);
                    idx += 2;
                    break;

                case "--socket-timeout":
                    socketTimeoutSec = Integer.parseInt(args[idx + 1]);
                    idx += 2;
                    break;

                case "--use-netty":
                case "--async":
                    useNetty = true;
                    idx += 1;
                    break;

                default:
                    log.error("unknown parameter " + args[idx]);
                    System.exit(1);
            }
        }

        log.info("Starting server...");

        if (useNetty) {
            // Use Netty async I/O server (recommended for high concurrency)
            log.info("Using Netty async I/O server");
            var srv = new NettyMorphiumServer(port, host, maxConnections, socketTimeoutSec, compressorId);
            srv.configureReplicaSet(rsNameArg, hostsArg, hostPrioritiesArg);

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

            srv.start();

            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown hook triggered");
                srv.shutdown();
            }));

            while (srv.isRunning()) {
                log.info("NettyMorphiumServer alive - connections: {}", srv.getConnectionCount());
                sleep(10000);
            }
        } else {
            // Use blocking I/O server (legacy)
            log.info("Using blocking I/O server (use --use-netty for async I/O)");
            var srv = new MorphiumServer(port, host, maxThreads, minThreads, compressorId);
            srv.configureReplicaSet(rsNameArg, hostsArg, hostPrioritiesArg);

            // Configure connection management
            srv.setMaxConnections(maxConnections);
            srv.setSocketReadTimeoutMs(socketTimeoutSec * 1000);
            log.info("Connection limits: max={}, socketTimeout={}s", maxConnections, socketTimeoutSec);

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

            srv.start();

            if (!rsNameArg.isEmpty()) {
                log.info("Building replicaset with seed {}", hostSeedArg);
                srv.startReplicaReplication();
            }

            while (srv.isRunning()) {
                log.info("Alive and kickin'");
                sleep(10000);
            }
        }
    }


    private static void printHelp() {
        System.out.println("Usage: java -jar morphium-server.jar [options]");
        System.out.println("Options:");
        System.out.println("  -p, --port <port>          : Port to listen on (default: 17017)");
        System.out.println("  -b, --bind <host>          : Host to bind to (default: localhost)");
        System.out.println("  -mt, --maxThreads <threads>: Maximum number of threads (default: 1000)");
        System.out.println("  -mint, --minThreads <threads>: Minimum number of threads (default: 10)");
        System.out.println("  -c, --compressor <type>    : Compressor to use (none, snappy, zstd, zlib; default: none)");
        System.out.println("  --rs-name <name>           : Name of the replica set");
        System.out.println("  --rs-seed <hosts>          : Comma-separated list of hosts to seed the replica set.");
        System.out.println("                               The first host in the list will have the highest priority.");
        System.out.println();
        System.out.println("SSL/TLS Options:");
        System.out.println("  --ssl, --tls               : Enable SSL/TLS encrypted connections");
        System.out.println("  --sslKeystore <path>       : Path to JKS or PKCS12 keystore file");
        System.out.println("  --sslKeystorePassword <pw> : Password for the keystore");
        System.out.println();
        System.out.println("Persistence Options:");
        System.out.println("  -d, --dump-dir <path>      : Directory for periodic database dumps");
        System.out.println("                               Enables persistence: restores on startup, dumps on shutdown");
        System.out.println("  --dump-interval <seconds>  : Interval between periodic dumps (default: 0 = only on shutdown)");
        System.out.println();
        System.out.println("Connection Management Options (for production reliability):");
        System.out.println("  --max-connections <num>    : Maximum concurrent connections (default: 500)");
        System.out.println("  --socket-timeout <seconds> : Idle connection timeout in seconds (default: 60)");
        System.out.println("                               Connections idle for 3x this time are closed automatically");
        System.out.println();
        System.out.println("Async I/O Options:");
        System.out.println("  --use-netty, --async       : Use Netty async I/O server (recommended for high concurrency)");
        System.out.println("                               Handles 10,000+ concurrent connections with few threads");
        System.out.println("                               Better for parallel test execution and messaging");
        System.out.println();
        System.out.println("  -h, --help                 : Print this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar morphium-server.jar -p 27018 --ssl --sslKeystore server.jks --sslKeystorePassword changeit");
        System.out.println("  java -jar morphium-server.jar -p 27017 --dump-dir /var/morphium/data --dump-interval 300");
    }
}
