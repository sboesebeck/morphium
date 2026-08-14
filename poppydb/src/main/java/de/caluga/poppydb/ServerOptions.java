package de.caluga.poppydb;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Plain data carrier for the effective PoppyDB server configuration - the former local variables
 * of {@code PoppyDBCLI.configureServer()}, promoted to fields so the parse / validate / build /
 * print steps can share one object. Deliberately NOT an API class: package-private, public
 * fields, no getters/setters, no logic beyond the two rs-seed helpers that parser, validator and
 * server wiring all need.
 */
class ServerOptions {

    /** Where an effective value came from - used by --print-config's per-key annotations. */
    enum Source { DEFAULT, CONFIG_FILE, CLI }

    String bind = "localhost";
    int port = 17017;
    String logLevel = "INFO";
    int memoryWarnPct = 75;
    int memoryRejectPct = 90;
    int maxBsonSizeBytes = 16 * 1024 * 1024;
    String compressor = "none";
    String rsName = "";
    String rsSeed = "";
    String rsPriorities = "";
    boolean ssl = false;
    String sslKeystore = null;
    String sslKeystorePassword = null;
    boolean auth = false;
    String rootUser = null;
    String rootPassword = null;
    String usersFile = null;
    String dumpDir = null;
    long dumpIntervalSec = 0;
    int maxConnections = 500;
    int socketTimeoutSec = 300;
    // Replay-buffer byte budget, raw input form (spec: 2026-08-14-replay-buffer-byte-budget.md).
    // Suffix k/m/g = fixed bytes, suffix % = percent of max heap (resolved once at startup),
    // plain number = bytes, 0 = byte cap off. Kept as the raw string so --print-config can show
    // both the input form and the resolved value.
    String replayBuffer = "256m";

    /** canonical config key (see ConfigLoader) -> origin of the effective value. */
    final Map<String, Source> sources = new LinkedHashMap<>();

    Source sourceOf(String key) {
        return sources.getOrDefault(key, Source.DEFAULT);
    }

    /** rs-seed split into host:port entries, default port 27017 - same rules as the old parser. */
    List<String> seedHosts() {
        List<String> hosts = new ArrayList<>();
        if (rsSeed.isBlank()) {
            return hosts;
        }
        for (String s : rsSeed.split(",")) {
            s = s.trim();
            String hst = s;
            int rsport = 27017;
            if (hst.contains(":")) {
                try {
                    rsport = Integer.parseInt(hst.split(":")[1]);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid port in rs-seed entry '" + s + "'");
                }
                hst = hst.split(":")[0];
            }
            hosts.add(hst + ":" + rsport);
        }
        return hosts;
    }

    /**
     * Election priorities per seed host: all 50 unless rs-priorities is given, then one value per
     * host in seed order, each 0-100. Throws IllegalArgumentException with a user-readable message
     * on any mismatch - surfaced by ConfigInspector.validate() and by buildServer().
     */
    Map<String, Integer> seedPriorities() {
        List<String> hosts = seedHosts();
        Map<String, Integer> prios = new java.util.concurrent.ConcurrentHashMap<>();
        if (hosts.isEmpty()) {
            return prios;
        }
        if (rsPriorities.isBlank()) {
            for (String h : hosts) {
                prios.put(h, 50);
            }
            return prios;
        }
        String[] vals = rsPriorities.split(",");
        if (vals.length != hosts.size()) {
            throw new IllegalArgumentException("Number of priorities (" + vals.length
                    + ") must match number of hosts (" + hosts.size() + ")");
        }
        for (int i = 0; i < hosts.size(); i++) {
            int prio;
            try {
                prio = Integer.parseInt(vals[i].trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Priority '" + vals[i].trim() + "' is not a number");
            }
            if (prio < 0 || prio > 100) {
                throw new IllegalArgumentException("Priority must be between 0 and 100, got: " + prio);
            }
            prios.put(hosts.get(i), prio);
        }
        return prios;
    }

    /**
     * replay-buffer resolved to bytes against the current JVM's max heap. Throws
     * IllegalArgumentException with a user-readable message on invalid input - surfaced by
     * ConfigInspector.validate() and by buildServer(), same contract as {@link #seedPriorities()}.
     */
    long replayBufferBytes() {
        return parseReplayBufferBytes(replayBuffer, Runtime.getRuntime().maxMemory());
    }

    /**
     * Parses a replay-buffer value: {@code 512m}/{@code 1g}/{@code 64k} = fixed bytes, {@code 5%}
     * = percent of {@code maxHeap} (resolved here, the max heap is fixed for the JVM's lifetime),
     * a plain number = bytes, {@code 0} = byte cap off. {@code maxHeap} is a parameter so tests
     * can resolve percentages deterministically.
     */
    static long parseReplayBufferBytes(String input, long maxHeap) {
        String v = input == null ? "" : input.trim().toLowerCase(java.util.Locale.ROOT);

        if (v.isEmpty()) {
            throw new IllegalArgumentException("replay-buffer must not be empty - use e.g. 256m, 5% or 0 (off)");
        }

        try {
            if (v.endsWith("%")) {
                double pct = Double.parseDouble(v.substring(0, v.length() - 1).trim());

                if (pct < 0 || pct > 100) {
                    throw new IllegalArgumentException("replay-buffer percentage must be between 0 and 100, got: " + input);
                }

                return (long) (maxHeap * pct / 100.0);
            }

            long factor = 1;
            String num = v;

            if (v.endsWith("k")) {
                factor = 1024;
                num = v.substring(0, v.length() - 1);
            } else if (v.endsWith("m")) {
                factor = 1024 * 1024;
                num = v.substring(0, v.length() - 1);
            } else if (v.endsWith("g")) {
                factor = 1024L * 1024 * 1024;
                num = v.substring(0, v.length() - 1);
            }

            long bytes = Long.parseLong(num.trim()) * factor;

            if (bytes < 0) {
                throw new IllegalArgumentException("replay-buffer must be >= 0 (0 = off), got: " + input);
            }

            return bytes;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("replay-buffer '" + input
                + "' is not a valid size - use a byte count with optional k/m/g suffix (e.g. 256m) or a percentage of the max heap (e.g. 5%)");
        }
    }
}
