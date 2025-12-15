package de.caluga.test.support;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.config.CollectionCheckSettings.CappedCheck;
import de.caluga.morphium.config.CollectionCheckSettings.IndexCheck;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;

import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Centralized test configuration loader to remove dependency on local files.
 *
 * Precedence: System properties -> Env vars -> test resources -> defaults.
 */
public final class TestConfig {
    private TestConfig() {}

    public static MorphiumConfig load() {
        Properties props = loadTestProperties();
        MorphiumConfig cfg = new MorphiumConfig();

        // Defaults suitable for tests
        cfg.connectionSettings()
           .setDatabase(valueOr(props, "morphium.database", "morphium_test"))
           .setMaxWaitTime(intProp(props, "morphium.maxWaitTime", 10000))
           .setMaxConnections(intProp(props, "morphium.maxConnections", 100))
           .setConnectionTimeout(intProp(props, "morphium.connectionTimeout", 2000))
           .setMinConnections(intProp(props, "morphium.minConnections", 1));

        cfg.cacheSettings()
           .setWriteCacheTimeout(intProp(props, "morphium.writeCacheTimeout", 1000))
           .setGlobalCacheValidTime(intProp(props, "morphium.globalCacheValidTime", 1000))
           .setHousekeepingTimeout(intProp(props, "morphium.housekeepingTimeout", 500));

        cfg.driverSettings()
           .setRetryReads(booleanProp(props, "morphium.retryReads", false))
           .setRetryWrites(booleanProp(props, "morphium.retryWrites", false))
           // change streams on external clusters need more than the previous 1s default
           .setReadTimeout(intProp(props, "morphium.readTimeout", 10000))
           // Avoid multi-minute "hangs" when no primary is available, but still allow enough time for
           // primary discovery on slower/remote clusters.
           .setServerSelectionTimeout(intProp(props, "morphium.serverSelectionTimeout", 15000))
           .setDriverName(mapDriverName(valueOr(props, "morphium.driver", "pooled")))
           .setMaxConnectionLifeTime(intProp(props, "morphium.maxConnectionLifeTime", 60000))
           .setMaxConnectionIdleTime(intProp(props, "morphium.maxConnectionIdleTime", 30000))
           .setHeartbeatFrequency(intProp(props, "morphium.heartbeatFrequency", 500))
           // Tests should be deterministic; reading from secondaries can cause flakiness due to replication lag.
           .setDefaultReadPreference(ReadPreference.primary());

        // In-memory driver: default to shared databases in tests so multiple Morphium instances
        // behave like multiple clients connecting to the same server.
        if (InMemoryDriver.driverName.equals(cfg.driverSettings().getDriverName())) {
            cfg.driverSettings().setInMemorySharedDatabases(
                booleanProp(props, "morphium.inMemorySharedDatabases", true)
            );
        } else {
            cfg.driverSettings().setInMemorySharedDatabases(
                booleanProp(props, "morphium.inMemorySharedDatabases", false)
            );
        }

        cfg.collectionCheckSettings()
           .setIndexCheck(IndexCheck.CREATE_ON_WRITE_NEW_COL)
           .setCappedCheck(CappedCheck.CREATE_ON_WRITE_NEW_COL);

        cfg.objectMappingSettings().setCheckForNew(true);

        cfg.messagingSettings()
           .setThreadPoolMessagingCoreSize(intProp(props, "morphium.messaging.core", 50))
           .setThreadPoolMessagingMaxSize(intProp(props, "morphium.messaging.max", 1500))
           .setThreadPoolMessagingKeepAliveTime(intProp(props, "morphium.messaging.keepAlive", 10000));

        cfg.writerSettings()
           .setMaximumRetriesBufferedWriter(intProp(props, "morphium.writer.maxRetriesBuffered", 1000))
           .setMaximumRetriesWriter(intProp(props, "morphium.writer.maxRetries", 1000))
           .setMaximumRetriesAsyncWriter(intProp(props, "morphium.writer.maxRetriesAsync", 1000))
           .setRetryWaitTimeAsyncWriter(intProp(props, "morphium.writer.retryWaitAsync", 1000))
           .setRetryWaitTimeWriter(intProp(props, "morphium.writer.retryWait", 1000))
           .setRetryWaitTimeBufferedWriter(intProp(props, "morphium.writer.retryWaitBuffered", 1000))
           .setThreadConnectionMultiplier(intProp(props, "morphium.writer.threadConnMultiplier", 2));

        // Hosts/URI/auth
        applyUriOrSeedsAndAuth(cfg, props);

        return cfg;
    }

    public static MorphiumConfig forDriver(String driverName) {
        MorphiumConfig cfg = load();
        cfg.driverSettings().setDriverName(mapDriverName(driverName));
        return cfg;
    }

    private static void applyUriOrSeedsAndAuth(MorphiumConfig cfg, Properties props) {
        String uri = firstNonEmpty(
                                     sysProp("morphium.uri"),
                                     env("MONGODB_URI"),
                                     env("MORPHIUM_URI"),
                                     props.getProperty("morphium.uri")
                     );

        if (uri != null && !uri.isBlank()) {
            parseMongoUriIntoConfig(cfg, uri.trim());
            applyExplicitAuth(cfg, props, false);
            return;
        }

        // host seed via properties/env
        String hostSeed = firstNonEmpty(
                                          sysProp("morphium.hostSeed"),
                                          env("MORPHIUM_HOSTSEED"),
                                          props.getProperty("morphium.hostSeed")
                          );

        if (hostSeed != null && !hostSeed.isBlank()) {
            for (String s : hostSeed.split(",")) {
                cfg.clusterSettings().addHostToSeed(s.trim());
            }
        } else {
            // default replicaset-ish local setup
            cfg.clusterSettings()
               .addHostToSeed("localhost", 27017)
               .addHostToSeed("localhost", 27018)
               .addHostToSeed("localhost", 27019);
        }

        applyExplicitAuth(cfg, props, true);
    }

    private static void parseMongoUriIntoConfig(MorphiumConfig cfg, String uri) {
        // Minimal parser for mongodb URI supporting multiple hosts and basic options
        // Format: mongodb://[user:pass@]host1:port1,host2:port2[/db][?authSource=...&replicaSet=...]
        String raw = uri;
        if (raw.startsWith("mongodb://")) raw = raw.substring("mongodb://".length());

        String credentials = null;
        String hostsAndDb;
        int atIdx = raw.indexOf('@');
        if (atIdx >= 0) {
            credentials = raw.substring(0, atIdx);
            hostsAndDb = raw.substring(atIdx + 1);
        } else {
            hostsAndDb = raw;
        }

        String pathAndQuery;
        int slashIdx = hostsAndDb.indexOf('/');
        String hostsPart = slashIdx >= 0 ? hostsAndDb.substring(0, slashIdx) : hostsAndDb;
        pathAndQuery = slashIdx >= 0 ? hostsAndDb.substring(slashIdx + 1) : "";

        String db = null;
        String query = null;
        int qIdx = pathAndQuery.indexOf('?');
        if (qIdx >= 0) {
            db = pathAndQuery.substring(0, qIdx);
            query = pathAndQuery.substring(qIdx + 1);
        } else if (!pathAndQuery.isEmpty()) {
            db = pathAndQuery;
        }

        if (credentials != null) {
            String[] parts = credentials.split(":", 2);
            if (parts.length == 2) {
                String user = urlDecode(parts[0]);
                String pass = urlDecode(parts[1]);
                cfg.authSettings().setMongoLogin(user).setMongoPassword(pass);
            }
        }

        for (String h : hostsPart.split(",")) {
            h = h.trim();
            if (!h.isEmpty()) {
                cfg.clusterSettings().addHostToSeed(h);
            }
        }

        if (db != null && !db.isBlank()) {
            cfg.connectionSettings().setDatabase(db);
        }

        if (query != null) {
            Map<String, String> q = parseQuery(query);
            String authSrc = q.get("authSource");
            if (authSrc != null) cfg.authSettings().setMongoAuthDb(authSrc);
            // optional: required replica set name can be set if needed
            String rs = q.get("replicaSet");
            if (rs != null && !rs.isBlank()) cfg.clusterSettings().setRequiredReplicaSetName(rs);
        }
    }

    private static void applyExplicitAuth(MorphiumConfig cfg, Properties props, boolean logIfMissing) {
        String user = firstNonEmpty(sysProp("morphium.user"), env("MORPHIUM_USER"), props.getProperty("morphium.user"));
        String pass = firstNonEmpty(sysProp("morphium.pass"), env("MORPHIUM_PASS"), props.getProperty("morphium.pass"));
        String authDb = firstNonEmpty(sysProp("morphium.authDb"), env("MORPHIUM_AUTHDB"), props.getProperty("morphium.authDb"));

        boolean hasExisting = cfg.authSettings().getMongoLogin() != null && cfg.authSettings().getMongoPassword() != null;

        if (user != null && pass != null) {
            org.slf4j.LoggerFactory.getLogger(TestConfig.class).info("Using authentication credentials for user {}", user);
            cfg.authSettings().setMongoLogin(user).setMongoPassword(pass);
            cfg.authSettings().setMongoAuthDb(authDb != null && !authDb.isBlank() ? authDb : defaultAuthDb());
        } else if (!hasExisting && logIfMissing) {
            org.slf4j.LoggerFactory.getLogger(TestConfig.class).warn("no authentication to mongo defined");
        } else if (hasExisting && authDb != null && !authDb.isBlank()) {
            cfg.authSettings().setMongoAuthDb(authDb);
        }
    }

    private static String defaultAuthDb() {
        return "admin";
    }

    private static String mapDriverName(String name) {
        if (name == null) return PooledDriver.driverName;
        String n = name.trim().toLowerCase(Locale.ROOT);
        switch (n) {
            case "pooled":
            case "pooleddriver":
                return PooledDriver.driverName;
            case "single":
            case "singleconnect":
            case "singlemongoconnectdriver":
                return SingleMongoConnectDriver.driverName;
            case "inmem":
            case "inmemory":
            case "inmemorydriver":
                return InMemoryDriver.driverName;
            default:
                return name; // assume caller passed a valid driver name constant
        }
    }

    private static Properties loadTestProperties() {
        Properties p = new Properties();
        try (InputStream in = Thread.currentThread().getContextClassLoader()
                                  .getResourceAsStream("morphium-test.properties")) {
            if (in != null) p.load(in);
        } catch (Exception ignore) {}
        return p;
    }

    private static String valueOr(Properties p, String key, String def) {
        String v = firstNonEmpty(sysProp(key), env(key.toUpperCase(Locale.ROOT).replace('.', '_')), p.getProperty(key));
        return v != null ? v : def;
    }

    private static int intProp(Properties p, String key, int def) {
        String v = valueOr(p, key, null);
        if (v == null) return def;
        try { return Integer.parseInt(v.trim()); } catch (Exception e) { return def; }
    }

    private static boolean booleanProp(Properties p, String key, boolean def) {
        String v = valueOr(p, key, null);
        if (v == null) return def;
        return v.equalsIgnoreCase("true") || v.equalsIgnoreCase("yes") || v.equals("1");
    }

    private static String firstNonEmpty(String... vals) {
        for (String v : vals) {
            if (v != null && !v.isBlank()) return v;
        }
        return null;
    }

    private static String sysProp(String k) { return System.getProperty(k); }
    private static String env(String k) { return System.getenv(k); }

    private static Map<String, String> parseQuery(String q) {
        Map<String, String> m = new HashMap<>();
        for (String p : q.split("&")) {
            if (p.isBlank()) continue;
            String[] kv = p.split("=", 2);
            String k = urlDecode(kv[0]);
            String v = kv.length > 1 ? urlDecode(kv[1]) : "";
            m.put(k, v);
        }
        return m;
    }

    private static String urlDecode(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }
}
