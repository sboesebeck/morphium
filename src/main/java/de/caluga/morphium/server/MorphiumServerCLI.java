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

                case "-h":
                case "--host":
                    host = args[idx + 1];
                    idx += 2;
                    break;

                case "-rs":
                case "--replicaset":
                    rsNameArg = args[idx + 1];
                    hostSeedArg = args[idx + 2];
                    idx += 3;
                    hostsArg = new ArrayList<>();
                    hostPrioritiesArg = new java.util.concurrent.ConcurrentHashMap<>();
                    int basePrio = 1000;

                    for (int i = 0; i < hostSeedArg.split(",").length; i++) {
                        var s = hostSeedArg.split(",")[i].trim();
                        int rsport = 27017;
                        String hst = s;
                        int prio = basePrio - i;

                        if (s.contains("|")) {
                            var parts = s.split("\\|");
                            hst = parts[0];

                            try {
                                prio = Integer.parseInt(parts[1]);
                            } catch (NumberFormatException e) {
                                log.warn("Invalid priority {}, using default {}", parts[1], prio);
                            }
                        }

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
}
