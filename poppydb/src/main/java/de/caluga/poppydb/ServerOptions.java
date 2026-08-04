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
    String dumpDir = null;
    long dumpIntervalSec = 0;
    int maxConnections = 500;
    int socketTimeoutSec = 300;

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
}
