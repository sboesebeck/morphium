package de.caluga.morphium.server;

import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

                default:
                    log.error("unknown parameter " + args[idx]);
                    System.exit(1);
            }
        }

        log.info("Starting server...");
        var srv = new MorphiumServer(port, host, maxThreads, minThreads, compressorId);
        srv.configureReplicaSet(rsNameArg, hostsArg, hostPrioritiesArg);
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


    private static void printHelp() {
        System.out.println("Usage: java -jar morphium-server.jar [options]");
        System.out.println("Options:");
        System.out.println("  -p, --port <port>          : Port to listen on (default: 17017)");
        System.out.println("  -b, --bind <host>          : Host to bind to (default: localhost)");
        System.out.println("  -mt, --maxThreads <threads>: Maximum number of threads (default: 1000)");
        System.out.println("  -mint, --minThreads <threads>: Minimum number of threads (default: 10)");
        System.out.println("  -c, --compressor <type>    : Compressor to use (none, snappy, zstd, zlib; default: none)");
        System.out.println("  --rs-name <name>           : Name of the replica set");
        System.out.println("  --rs-seed <hosts>          : Comma-separated list of hosts to seed the replica set. The first host in the list will have the highest priority.");
        System.out.println("  -h, --help                 : Print this help message");
    }
}
